//! Router placement, HTTP lifetimes and transport stay at the API boundary.
mod dispatcher;
pub(crate) mod websocket;

use axum::{
    Router,
    body::Body,
    http::{HeaderName, Request, StatusCode},
    response::{IntoResponse, Response},
};
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    engine::middleware::{MiddlewareError, http as core},
    lifecycle::{CancellationToken, ConnectionLifecycle, Deadline},
    middleware::compose,
    routing::RequestSettings,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::BodyExt;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use tokio::time::Instant;
use tower::ServiceExt;

tokio::task_local! {static CURRENT: Origin;}

#[derive(Clone)]
pub(crate) struct Origin {
    pub(crate) dispatcher: Arc<dyn core::Dispatcher>,
    pub(crate) context: core::Context,
    pub(crate) registry: PluginMiddlewareRegistry,
    pub(crate) settings: Option<RequestSettings>,
    _lifetime: Arc<Lifetime>,
}
pub(crate) fn current() -> Option<Origin> {
    CURRENT.try_with(Clone::clone).ok()
}
pub(crate) fn service_origin(
    services: &gateway_admin::AdminServices,
    actor: &gateway_admin::model::MutationContext,
) -> Option<gateway_admin::plugins::service::Origin> {
    current().map(|origin| service_origin_from(origin, services, actor))
}
fn service_origin_from(
    origin: Origin,
    services: &gateway_admin::AdminServices,
    actor: &gateway_admin::model::MutationContext,
) -> gateway_admin::plugins::service::Origin {
    let mut origin = gateway_admin::plugins::service::Origin {
        // Admin's audit boundary issues its own request ID after HTTP placement.
        registry: origin.registry,
        request_id: actor.request_id.clone(),
        call_id: origin.context.call_id,
        cancellation: origin.context.cancellation,
        deadline_at: origin.context.deadline_at,
        call_scope: origin.context.call_scope,
        dispatcher: None,
    };
    origin.dispatcher = Some(
        services
            .service_settings()
            .dispatcher(origin.clone(), actor.clone()),
    );
    origin
}
pub(crate) fn plugin_invocation(
    services: &gateway_admin::AdminServices,
    actor: &gateway_admin::model::MutationContext,
) -> Option<gateway_admin::model::plugins::PluginInvocationContext> {
    plugin_invocation_from(current(), services, actor)
}
pub(crate) fn plugin_invocation_from(
    origin: Option<Origin>,
    services: &gateway_admin::AdminServices,
    actor: &gateway_admin::model::MutationContext,
) -> Option<gateway_admin::model::plugins::PluginInvocationContext> {
    origin.map(
        |origin| gateway_admin::model::plugins::PluginInvocationContext {
            services: service_origin_from(origin.clone(), services, actor).dispatcher,
            context: origin.context,
            http: Some(origin.dispatcher),
        },
    )
}
pub(crate) async fn scope<T>(
    origin: Option<Origin>,
    future: impl std::future::Future<Output = T>,
) -> T {
    match origin {
        Some(origin) => CURRENT.scope(origin, future).await,
        None => future.await,
    }
}
type SettingsSource = Arc<dyn Fn() -> Option<RequestSettings> + Send + Sync>;
type RegistrySource = Arc<dyn Fn() -> Option<PluginMiddlewareRegistry> + Send + Sync>;

pub(crate) fn wrap(
    router: Router,
    plugins: RegistrySource,
    timeout: Option<Duration>,
    request_id_header: HeaderName,
    settings: SettingsSource,
    lifecycle: Arc<dyn ConnectionLifecycle>,
) -> (Router, Arc<dyn core::Dispatcher>) {
    let dispatcher = Arc::new(dispatcher::RouterDispatcher {
        registry: None,
        router: router.clone(),
        plugins: plugins.clone(),
        timeout,
        header: request_id_header.clone(),
        settings: settings.clone(),
        lifecycle: lifecycle.clone(),
        origin: None,
    });
    // A route layer runs after matching; a fallback service permits URI rematching.
    let router =
        Router::new().fallback_service(tower::service_fn(move |request: Request<Body>| {
            let router = router.clone();
            let plugins = plugins.clone();
            let settings = settings.clone();
            let lifecycle = lifecycle.clone();
            let header = request_id_header.clone();
            async move {
                Ok::<_, Infallible>(
                    handle(
                        router, request, plugins, timeout, header, settings, lifecycle,
                    )
                    .await,
                )
            }
        }));
    (router, dispatcher)
}
async fn handle(
    router: Router,
    mut request: Request<Body>,
    plugins: RegistrySource,
    timeout: Option<Duration>,
    header: HeaderName,
    settings: SettingsSource,
    lifecycle: Arc<dyn ConnectionLifecycle>,
) -> Response {
    let started = Instant::now();
    let inherited = request.extensions_mut().remove::<Origin>();
    if is_recovery_request(&request) {
        let response = dispatch(router, request, started, timeout).await;
        // An internal request still owns its original graph permit until its body
        // ends. Skipping plugin placement must not free that permit at headers.
        return match inherited {
            None => response,
            Some(origin) => response.map(|body| {
                Body::new(ResponseBody {
                    body: Some(core::guard_body(
                        body.map_err(|error| Box::new(error) as _).boxed_unsync(),
                        origin.context.cancellation.clone(),
                        origin.context.deadline_at,
                    )),
                    lifetime: Some(Arc::new(Lifetime {
                        cancellation: origin.context.cancellation,
                        _parent: Some(origin._lifetime),
                        _call_scope: origin.context.call_scope,
                    })),
                })
            }),
        };
    }
    let is_inherited = inherited.is_some();
    let (registry, context, parent, baseline) = if let Some(origin) = inherited {
        (
            origin.registry,
            origin.context,
            Some(origin._lifetime),
            origin.settings,
        )
    } else {
        let Some(registry) = plugins() else {
            return dispatch(router, request, started, timeout).await;
        };
        let context = core::Context {
            request_id: request
                .headers()
                .get(&header)
                .and_then(|value| value.to_str().ok())
                .map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned),
            call_id: uuid::Uuid::now_v7().to_string(),
            parent_call_id: None,
            call_scope: Default::default(),
            model_parent: None,
            cancellation: CancellationToken::linked([lifecycle.cancellation()]),
            deadline_at: Deadline::from_timeout(SystemTime::now(), None)
                .expect("unbounded deadline"),
        };
        (registry, context, None, None)
    };
    let plan = match registry.freeze_http(context.clone()) {
        Ok(plan) => plan,
        Err(_) => return error(&MiddlewareError::Fault),
    };
    let context = core::Context {
        cancellation: plan.cancellation(),
        ..context
    };
    let lifetime = Arc::new(Lifetime {
        cancellation: context.cancellation.clone(),
        _parent: parent,
        _call_scope: context.call_scope.clone(),
    });
    let configuration = request
        .extensions()
        .get::<core::Settings>()
        .cloned()
        .unwrap_or_else(|| core::Settings {
            timeout,
            runtime: if is_inherited { baseline } else { settings() },
        });
    request.extensions_mut().insert(configuration.clone());
    request
        .extensions_mut()
        .insert(Arc::new(websocket::Upgrade {
            registry: registry.clone(),
            context: context.clone(),
            lifecycle: lifecycle.clone(),
        }) as Arc<dyn core::upgrade::WebSocketUpgrade>);
    let origin = Origin {
        dispatcher: Arc::new(dispatcher::RouterDispatcher {
            registry: Some(registry.clone()),
            router: router.clone(),
            plugins: plugins.clone(),
            timeout,
            header: header.clone(),
            settings: settings.clone(),
            lifecycle: lifecycle.clone(),
            origin: None,
        }),
        context: context.clone(),
        registry,
        settings: configuration.runtime,
        _lifetime: lifetime.clone(),
    };
    request
        .extensions_mut()
        .insert(Arc::new(dispatcher::RouterDispatcher {
            registry: None,
            router: router.clone(),
            plugins,
            timeout,
            header,
            settings,
            lifecycle,
            origin: Some(origin.clone()),
        }) as Arc<dyn core::Dispatcher>);
    let request = request.map(|body| body.map_err(|error| Box::new(error) as _).boxed_unsync());
    let next = compose(vec![], move |request: core::Request| {
        Box::pin(async move {
            let configuration = request
                .extensions()
                .get::<core::Settings>()
                .cloned()
                .ok_or(MiddlewareError::InvalidState)?;
            let origin = Origin {
                settings: configuration.runtime,
                ..origin
            };
            let response = CURRENT
                .scope(
                    origin,
                    dispatch(
                        router,
                        request.map(Body::new),
                        started,
                        configuration.timeout,
                    ),
                )
                .await;
            Ok(response.map(|body| body.map_err(|error| Box::new(error) as _).boxed_unsync()))
        })
    });
    match plan.run(request, next).await {
        Ok(mut response) => {
            if let Some(upgraded) = response
                .extensions_mut()
                .remove::<core::upgrade::Upgraded>()
            {
                let Some(task) = upgraded.take_task(response.status()) else {
                    return error(&MiddlewareError::InvalidState);
                };
                let lifetime = lifetime.clone();
                tokio::spawn(async move {
                    let cancellation = lifetime.cancellation.clone();
                    let _lifetime = lifetime;
                    tokio::select! {biased;()=cancellation.cancelled()=>{},_=task=>{}}
                });
            }
            response.map(|body| {
                Body::new(ResponseBody {
                    body: Some(body),
                    lifetime: Some(lifetime),
                })
            })
        }
        Err(failure) => error(&failure),
    }
}
fn is_recovery_request(request: &Request<Body>) -> bool {
    let path = request.uri().path();
    crate::admin::is_recovery_route(path)
        || (matches!(*request.method(), http::Method::GET | http::Method::HEAD)
            && (matches!(path, "/" | "/login" | "/plugins" | "/favicon.svg")
                || path.starts_with("/assets/")))
}
async fn dispatch(
    router: Router,
    request: Request<Body>,
    started: Instant,
    timeout: Option<Duration>,
) -> Response {
    let call = router.oneshot(request);
    let result = match timeout {
        Some(timeout) => {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return StatusCode::REQUEST_TIMEOUT.into_response();
            };
            match tokio::time::timeout(remaining, call).await {
                Ok(result) => result,
                Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
            }
        }
        None => call.await,
    };
    match result {
        Ok(response) => response,
        Err(never) => match never {},
    }
}
fn error(failure: &MiddlewareError) -> Response {
    let status = if failure.is_rejected() {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::BAD_GATEWAY
    };
    (status,axum::Json(serde_json::json!({"error":{"message":"HTTP middleware failed","type":"middleware_error"}}))).into_response()
}
struct Lifetime {
    cancellation: CancellationToken,
    _parent: Option<Arc<Lifetime>>,
    _call_scope: gateway_core::engine::nested::ExtensionCallScope,
}
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
struct ResponseBody {
    body: Option<core::Body>,
    lifetime: Option<Arc<Lifetime>>,
}
impl HttpBody for ResponseBody {
    type Data = bytes::Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let Some(body) = self.body.as_mut() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(body).poll_frame(cx);
        if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.body.take();
            self.lifetime.take();
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(HttpBody::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(SizeHint::default, HttpBody::size_hint)
    }
}
