//! Responses protocol bridge for Core-owned, per-attempt middleware.
mod body;
mod output;

use super::super::{decode_request_with_headers, request::forwarded_request_headers};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::STANDARD};
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
    pub streaming: bool,
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
        let streaming = self.streaming;
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
            let mut headers = HeaderMap::new();
            if let Some(values) = payload
                .context()
                .get("opaque_request_headers")
                .and_then(|v| v.as_array())
            {
                for value in values {
                    let fields = value
                        .as_array()
                        .filter(|a| a.len() == 2)
                        .ok_or(MiddlewareError::InvalidState)?;
                    let name = HeaderName::from_bytes(
                        fields[0]
                            .as_str()
                            .ok_or(MiddlewareError::InvalidState)?
                            .as_bytes(),
                    )
                    .map_err(|_| MiddlewareError::InvalidState)?;
                    let value = STANDARD
                        .decode(fields[1].as_str().ok_or(MiddlewareError::InvalidState)?)
                        .map_err(|_| MiddlewareError::InvalidState)?;
                    headers.append(
                        name,
                        HeaderValue::from_bytes(&value)
                            .map_err(|_| MiddlewareError::InvalidState)?,
                    );
                }
            }
            let expected = decode_request_with_headers(&body, &headers)
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
            let terminal =
                gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
                    Box::pin(async move {
                        if request.protocol() != "openai" {
                            return Err(MiddlewareError::InvalidState);
                        }
                        let headers = super::request::filtered_headers(request.headers())?;
                        let decoded = decode_request_with_headers(request.body(), &headers)
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
                        let Operation::Generate(rewritten) = &operation else {
                            return Err(MiddlewareError::InvalidState);
                        };
                        let Operation::Generate(original_generate) = original.operation() else {
                            return Err(MiddlewareError::InvalidState);
                        };
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
                        body::SourceBody::response(next.run(operation).await?, streaming).await
                    })
                });
            let response = plan.run(request, terminal).await?;
            output::decode(response, streaming)
        })
    }
}
