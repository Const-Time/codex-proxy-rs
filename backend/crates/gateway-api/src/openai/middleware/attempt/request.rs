//! Re-decode plugin bytes without accepting host connection or transport identity.
use crate::openai::responses::{
    DecodedResponsesRequest, OpenAiRequestHeaders,
    request::{RequestDecodeSource, decode_request_object},
};
use axum::http::HeaderMap;
use bytes::Bytes;
use gateway_core::{engine::middleware::MiddlewareError, operation::Operation};

pub(super) fn decode(
    original: &Operation,
    body: &Bytes,
    headers: &HeaderMap,
    format: super::super::WireFormat,
) -> Result<DecodedResponsesRequest, MiddlewareError> {
    let Operation::Generate(generate) = original else {
        return Err(MiddlewareError::InvalidState);
    };
    let context = generate.protocol_payload().context();
    let mut object = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(body)
        .map_err(|_| MiddlewareError::InvalidState)?;
    let original_transport = context.get("use_websocket").and_then(|v| v.as_bool());
    if let Some(proposed) = object.get("use_websocket").and_then(|v| v.as_bool())
        && Some(proposed) != original_transport
    {
        return Err(MiddlewareError::InvalidState);
    }
    if let Some(value) = original_transport {
        object.insert("use_websocket".into(), value.into());
    }
    if format.websocket()
        && object
            .get("stream")
            .is_some_and(|v| v.as_bool() != Some(true))
    {
        return Err(MiddlewareError::InvalidState);
    }
    let mut headers = OpenAiRequestHeaders::from_headers(headers);
    if format.websocket()
        && let Some(id) = context
            .get("downstream_websocket_connection_id")
            .and_then(|v| v.as_str())
    {
        headers = headers.with_downstream_websocket_connection_id(id);
    }
    decode_request_object(
        object,
        &headers,
        if format.websocket() {
            RequestDecodeSource::WebSocketFrame
        } else {
            RequestDecodeSource::Http
        },
    )
    .map_err(|_| MiddlewareError::InvalidState)
}
