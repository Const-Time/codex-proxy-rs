//! OpenAI protocol bridges for Core-owned, per-attempt middleware.
mod body;
pub(super) mod output;
mod raw;
pub(super) mod request;

use crate::openai::responses::request::forwarded_request_headers;
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    engine::{AttemptContext, attempt_middleware::*, middleware::*, provider::ProviderRequest},
    operation::Operation,
};
use std::sync::Arc;

pub(super) struct ResponsesAttempt {
    pub registry: Arc<PluginMiddlewareRegistry>,
    pub context: MiddlewareContext,
    pub format: super::WireFormat,
}
impl AttemptMiddleware for ResponsesAttempt {
    fn invoke(
        &self,
        original: ProviderRequest,
        attempt: AttemptContext,
        next: AttemptNext,
    ) -> BoxFuture<'static, Result<AttemptOutput, MiddlewareError>> {
        let registry = self.registry.clone();
        let mut context = self.context.clone();
        let format = self.format;
        Box::pin(async move {
            context.attempt_index = Some(attempt.attempt_index().get());
            context.provider = Some(original.candidate().provider().clone());
            context.model = original
                .candidate()
                .upstream_model()
                .map(|model| model.as_str().to_owned());
            context.operation = original.operation().kind();
            context.cancellation = attempt.cancellation().clone();
            context.deadline_at = attempt.deadline();
            context.excluded_plugin_instances = attempt
                .nested_context()
                .map_or_else(Default::default, |context| {
                    context.scope().excluded_instances().clone()
                });
            let plan = registry
                .freeze(context)
                .map_err(|_| MiddlewareError::Fault)?;
            let Operation::Generate(generate) = original.operation() else {
                return Err(MiddlewareError::InvalidState);
            };
            let payload = generate.protocol_payload();
            if payload.protocol() != "openai" {
                return Err(MiddlewareError::InvalidState);
            }
            let body = Bytes::from(
                serde_json::to_vec(payload.body()).map_err(|_| MiddlewareError::InvalidState)?,
            );
            let headers = super::headers::from_context(payload.context())?;
            let expected = request::decode(original.operation(), &body, &headers, format)
                .map_err(|_| MiddlewareError::InvalidState)?;
            let request = MiddlewareRequest::new(
                "openai",
                forwarded_request_headers(&headers)
                    .map(|(name, value)| {
                        MiddlewareHeader::new(
                            name.as_str(),
                            Bytes::copy_from_slice(value.as_bytes()),
                        )
                    })
                    .collect(),
                body,
            );
            let request = match attempt.settings() {
                Some(settings) => request.with_settings(settings.clone()),
                None => request,
            };
            let request = match attempt.nested_context() {
                Some(context) => request.with_execution_context(context.clone()),
                None => request,
            };
            let terminal =
                gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
                    Box::pin(async move {
                        if request.protocol() != "openai" || request.has_capability_declaration() {
                            return Err(MiddlewareError::InvalidState);
                        }
                        let headers = super::headers::filtered_headers(request.headers())?;
                        let decoded =
                            request::decode(original.operation(), request.body(), &headers, format)
                                .map_err(|_| MiddlewareError::InvalidState)?;
                        // Routing and continuation have already been resolved by Core.
                        if decoded.metadata().requested_model()
                            != expected.metadata().requested_model()
                            || decoded.metadata().continuation()
                                != expected.metadata().continuation()
                            || decoded.metadata().store() != expected.metadata().store()
                            || decoded.metadata().stream() != expected.metadata().stream()
                        {
                            return Err(MiddlewareError::InvalidState);
                        }
                        let (mut operation, _) = decoded.into_parts();
                        let Operation::Generate(rewritten) = &mut operation else {
                            return Err(MiddlewareError::InvalidState);
                        };
                        let Operation::Generate(original_generate) = original.operation() else {
                            return Err(MiddlewareError::InvalidState);
                        };
                        rewritten.inherit_capability_requirements(original_generate);
                        if rewritten.protocol_payload().context().get("use_websocket")
                            != original_generate
                                .protocol_payload()
                                .context()
                                .get("use_websocket")
                        {
                            return Err(MiddlewareError::InvalidState);
                        }
                        if let Some(state) = original
                            .operation()
                            .provider_session_state(original.candidate().provider().as_str())
                        {
                            operation.set_provider_session_state(state.clone());
                        }
                        body::SourceBody::response(next.run(operation).await?, format).await
                    })
                });
            let response = plan.run(request, terminal).await?;
            output::decode(response, format)
        })
    }
}

/// Bind the raw Images/Search wire adapter to one root publication snapshot.
pub fn raw_json_attempt_middleware(
    registry: Arc<PluginMiddlewareRegistry>,
    mut context: MiddlewareContext,
) -> Arc<dyn AttemptMiddleware> {
    context.mount = MiddlewareMount::Attempt;
    Arc::new(raw::RawAttempt { registry, context })
}
