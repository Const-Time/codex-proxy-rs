//! Child HTTP calls use host-issued ancestry and the existing lazy resource bank.
use super::*;

impl Invocation {
    pub(super) async fn dispatch(
        &self,
        request: wire::Request,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        let method = request.method.parse().map_err(|_| invalid())?;
        let uri: http::Uri = request.uri.parse().map_err(|_| invalid())?;
        if uri.scheme().is_some() || uri.authority().is_some() || !uri.path().starts_with('/') {
            return Err(invalid());
        }
        let headers = headers(request.headers)?;
        let dispatcher = self.dispatcher.as_ref().ok_or_else(|| {
            PluginFault::new(
                ErrorCode::Unsupported,
                "host HTTP service is not configured",
            )
        })?;
        let settings = resolve_settings(
            self.request_settings()
                .or_else(|| dispatcher.request_settings()),
            request.settings,
            request.timeout_ms,
            &self.instance_id,
        )?;
        let context = self.context.child(self.instance_id.clone()).map_err(|_| {
            PluginFault::new(ErrorCode::Capacity, "host HTTP call graph limit reached")
        })?;
        let http_version = version(request.version);
        let body = self.resources.body(request.body, payload).await?;
        let mut request = core::Request::new(body);
        *request.method_mut() = method;
        *request.uri_mut() = uri;
        *request.version_mut() = http_version;
        *request.headers_mut() = headers;
        request.extensions_mut().insert(settings);
        self.context.call_scope.observe_http_dispatch();
        let response = dispatcher
            .dispatch(context, request)
            .await
            .map_err(|error| fault(&error))?;
        self.resources.dispatched_response(response)
    }
}
