use super::sent;
use futures::Stream;
use gateway_core::{
    error::{ProviderError, ProviderErrorKind as Kind},
    event::{
        FinishReason, GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseObservation,
        ProviderResponseTimings, ResponseMeta, UpstreamHttpVersion,
    },
    lifecycle::{CancellationToken, Deadline},
    routing::UpstreamModelId,
    upstream::UpstreamTransport,
};
use gateway_protocol::{genai::GENAI_PROTOCOL, openai::sse::MAX_SSE_EVENT_BUFFER_BYTES};
use serde_json::Value;
use std::time::Instant;

/// Decode a Cloud Code count response into one native JSON event without generation metering.
pub fn decode_count_response(
    mut response: reqwest::Response,
    model: UpstreamModelId,
    deadline: Deadline,
    cancel: CancellationToken,
    started_at: Instant,
) -> impl Stream<Item = Result<ProviderEvent, ProviderError>> + Send + 'static {
    let headers_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    async_stream::try_stream! {
        let mut body = Vec::new();
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(sent(Kind::Cancelled)),
                _ = deadline.wait() => Err(sent(Kind::Timeout)),
                result = response.chunk() => result.map_err(|_| sent(Kind::Transport)),
            }?;
            let Some(chunk) = chunk else { break; };
            if body.len().saturating_add(chunk.len()) > MAX_SSE_EVENT_BUFFER_BYTES {
                Err(sent(Kind::MessageTooBig))?;
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body).map_err(|_| sent(Kind::Protocol))?;
        // The upstream protobuf JSON omits the default zero count.
        if value.as_object().is_some_and(serde_json::Map::is_empty) {
            body = br#"{"totalTokens":0}"#.to_vec();
        } else if value.get("totalTokens").and_then(Value::as_u64).is_none() {
            Err(sent(Kind::Protocol))?;
        }
        let transport = UpstreamTransport::new("http")
            .map_err(|_| sent(Kind::ProviderInfrastructureUnavailable))?;
        let timings = ProviderResponseTimings {
            headers_ms: Some(headers_ms),
            ..Default::default()
        };
        let mut observation = ProviderResponseObservation::new(transport)
            .with_status_code(response.status().as_u16())
            .with_timings(timings);
        if let Some(version) = UpstreamHttpVersion::parse(&format!("{:?}", response.version())) {
            observation = observation.with_http_version(version);
        }
        let meta = ResponseMeta::new(uuid::Uuid::now_v7().to_string(), model.as_str());
        let wire = ProtocolWireEvent::raw_json(GENAI_PROTOCOL, body.into())
            .map_err(|_| sent(Kind::Protocol))?;
        let mut event = ProviderEvent::wire(wire)
            .with_fact(GatewayEvent::Started(meta.clone()))
            .with_fact(GatewayEvent::Completed(meta.with_finish_reason(FinishReason::Stop)));
        event.attach_observation(observation);
        yield event;
    }
}
