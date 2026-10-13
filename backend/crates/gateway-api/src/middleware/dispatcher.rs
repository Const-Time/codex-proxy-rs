//! Internal HTTP uses the original Router and publication; normal handlers authenticate it.
use super::*;
use futures::future::BoxFuture;

pub(super) struct RouterDispatcher {
    pub(super) router: Router,
    pub(super) plugins: RegistrySource,
    pub(super) timeout: Option<Duration>,
    pub(super) header: HeaderName,
    pub(super) settings: SettingsSource,
    pub(super) lifecycle: Arc<dyn ConnectionLifecycle>,
    pub(super) origin: Origin,
}

impl core::Dispatcher for RouterDispatcher {
    fn request_settings(&self) -> Option<RequestSettings> {
        self.origin.settings.clone()
    }
    fn dispatch(
        &self,
        context: core::Context,
        mut request: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        if context.parent_call_id.as_deref() != Some(self.origin.context.call_id.as_str())
            || context.request_id != self.origin.context.request_id
            || !context
                .call_scope
                .is_direct_child_of(&self.origin.context.call_scope)
            || request.uri().scheme().is_some()
            || request.uri().authority().is_some()
            || !request.uri().path().starts_with('/')
        {
            return Box::pin(async { Err(MiddlewareError::InvalidState) });
        }
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
            context,
            ..self.origin.clone()
        });
        Box::pin(async move {
            let response = tokio::select! {biased;
                ()=cancellation.cancelled()=>return Err(MiddlewareError::Fault),
                ()=deadline.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
                response=handle(router, request.map(Body::new), plugins, timeout, header, settings, lifecycle)=>response,
            };
            Ok(response.map(|body| body.map_err(|error| Box::new(error) as _).boxed_unsync()))
        })
    }
}
