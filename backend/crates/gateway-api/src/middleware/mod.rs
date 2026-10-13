//! Router placement, HTTP lifetimes and transport stay at the API boundary.
mod websocket;

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
    pub(crate) context: core::Context,
    pub(crate) registry: PluginMiddlewareRegistry,
    pub(crate) settings: Option<RequestSettings>,
    _lifetime: Arc<Lifetime>,
}
pub(crate) fn current() -> Option<Origin> {
    CURRENT.try_with(Clone::clone).ok()
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
) -> Router {
    // A route layer runs after matching; a fallback service permits URI rematching.
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
    }))
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
        excluded_plugin_instances: Default::default(),
        cancellation: CancellationToken::linked([lifecycle.cancellation()]),
        deadline_at: Deadline::from_timeout(SystemTime::now(), None).expect("unbounded deadline"),
    };
    let plan = match registry.freeze_http(context.clone()) {
        Ok(plan) => plan,
        Err(_) => return error(&MiddlewareError::Fault),
    };
    if plan.is_empty() {
        return dispatch(router, request, started, timeout).await;
    }
    let context = core::Context {
        cancellation: plan.cancellation(),
        ..context
    };
    let lifetime = Arc::new(Lifetime(context.cancellation.clone()));
    request.extensions_mut().insert(core::Settings {
        timeout,
        runtime: settings(),
    });
    request
        .extensions_mut()
        .insert(Arc::new(websocket::Upgrade {
            context: context.clone(),
            lifecycle,
        }) as Arc<dyn core::upgrade::WebSocketUpgrade>);
    let origin = Origin {
        context: context.clone(),
        registry,
        settings: None,
        _lifetime: lifetime.clone(),
    };
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
                    let cancellation = lifetime.0.clone();
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
struct Lifetime(CancellationToken);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.cancel();
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
