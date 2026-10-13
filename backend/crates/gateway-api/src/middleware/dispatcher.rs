//! Internal HTTP uses the original Router and publication; normal handlers authenticate it.
use super::*;
use futures::future::BoxFuture;

#[derive(Clone)]
pub(super) struct RouterDispatcher {
    pub(super) registry: Option<PluginMiddlewareRegistry>,
    pub(super) router: Router,
    pub(super) plugins: RegistrySource,
    pub(super) timeout: Option<Duration>,
    pub(super) header: HeaderName,
    pub(super) settings: SettingsSource,
    pub(super) lifecycle: Arc<dyn ConnectionLifecycle>,
    pub(super) origin: Option<Origin>,
}

impl core::Dispatcher for RouterDispatcher {
    fn service_factory(
        &self,
    ) -> Option<Arc<dyn gateway_core::engine::middleware::service::Factory>> {
        if let Some(parent) = self
            .origin
            .as_ref()
            .and_then(|origin| origin.context.model_parent.as_ref())
        {
            return parent.service_factory();
        }
        self.origin
            .as_ref()
            .map(|origin| &origin.registry)
            .or(self.registry.as_ref())
            .and_then(|registry| registry.service_factory())
    }
    fn request_settings(&self) -> Option<RequestSettings> {
        self.origin
            .as_ref()
            .and_then(|origin| origin.settings.clone())
            .or_else(|| (self.settings)())
    }
    fn dispatch(
        &self,
        context: core::Context,
        mut request: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        if context.validate().is_err()
            || !context.call_scope.is_http_child()
            || context.parent_call_id.is_none()
            || self.origin.as_ref().is_some_and(|origin| {
                context.parent_call_id.as_deref() != Some(origin.context.call_id.as_str())
                    || context.request_id != origin.context.request_id
                    || !context
                        .call_scope
                        .is_direct_child_of(&origin.context.call_scope)
            })
            || request.uri().scheme().is_some()
            || request.uri().authority().is_some()
            || !request.uri().path().starts_with('/')
        {
            return Box::pin(async { Err(MiddlewareError::InvalidState) });
        }
        let origin = match &self.origin {
            Some(origin) => origin.clone(),
            None => {
                let Some(registry) = self.registry.clone().or_else(|| (self.plugins)()) else {
                    return Box::pin(async { Err(MiddlewareError::Fault) });
                };
                Origin {
                    dispatcher: Arc::new(Self {
                        registry: Some(registry.clone()),
                        origin: None,
                        ..self.clone()
                    }),
                    context: context.clone(),
                    registry,
                    settings: context
                        .model_parent
                        .as_ref()
                        .map(|parent| parent.settings().clone())
                        .or_else(|| self.request_settings()),
                    _lifetime: Arc::new(Lifetime {
                        cancellation: context.cancellation.clone(),
                        _parent: None,
                        _call_scope: context.call_scope.clone(),
                    }),
                }
            }
        };
        let router = self.router.clone();
        let plugins = self.plugins.clone();
        let settings = self.settings.clone();
        let lifecycle = self.lifecycle.clone();
        let timeout = self.timeout;
        let header = self.header.clone();
        let cancellation = context.cancellation.clone();
        let deadline = context.deadline_at;
        if let Ok(value) = context.request_id.parse() {
            request.headers_mut().insert(header.clone(), value);
        }
        request.extensions_mut().insert(Origin {
            context: context.clone(),
            ..origin
        });
        Box::pin(async move {
            context.call_scope.observe_upstream_effect();
            let response = tokio::select! {biased;
                ()=cancellation.cancelled()=>return Err(MiddlewareError::Fault),
                ()=deadline.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
                response=handle(router, request.map(Body::new), plugins, timeout, header, settings, lifecycle)=>response,
            };
            Ok(response.map(|body| body.map_err(|error| Box::new(error) as _).boxed_unsync()))
        })
    }
}
