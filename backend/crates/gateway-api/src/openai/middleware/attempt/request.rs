//! Re-decode plugin bytes without accepting host connection or transport identity.
use crate::openai::responses::{
    DecodedResponsesRequest, OpenAiRequestHeaders,
    request::{RequestDecodeSource, decode_request_object},
};
use axum::http::HeaderMap;
use bytes::Bytes;
use gateway_core::{engine::middleware::MiddlewareError, operation::Operation};

pub(in crate::openai::middleware) fn decode(
    original: &Operation,
    body: &Bytes,
    headers: &HeaderMap,
    format: super::super::WireFormat,
) -> Result<DecodedResponsesRequest, MiddlewareError> {
    let Operation::Generate(generate) = original else {
        return Err(MiddlewareError::InvalidState);
    };
    let context = generate.protocol_payload().context();
    let native = match format.protocol() {
        "openai" => None,
        gateway_protocol::chat_completions::CHAT_PROTOCOL => {
            Some(crate::openai::responses::http::GenerationProtocol::Chat)
        }
        gateway_protocol::anthropic::ANTHROPIC_PROTOCOL => {
            Some(crate::openai::responses::http::GenerationProtocol::Messages { count: false })
        }
        gateway_protocol::genai::GENAI_PROTOCOL => {
            let model = context
                .get("requested_model")
                .and_then(serde_json::Value::as_str)
                .ok_or(MiddlewareError::InvalidState)?;
            Some(crate::openai::responses::http::GenerationProtocol::GenAi {
                model: model.to_owned(),
                streaming: format.streaming(),
            })
        }
        _ => return Err(MiddlewareError::InvalidState),
    };
    if let Some(native) = native {
        let decoded = native
            .decode(body, headers, |_| Ok(()))
            .map_err(|_| MiddlewareError::InvalidState)?;
        let Operation::Generate(rewritten) = decoded.operation() else {
            return Err(MiddlewareError::InvalidState);
        };
        let original = generate.protocol_payload();
        let replacement = rewritten.protocol_payload();
        match format.protocol() {
            gateway_protocol::genai::GENAI_PROTOCOL => {
                let level = |payload: &gateway_core::operation::ProtocolPayload| {
                    gateway_protocol::request::request_observation(
                        payload.protocol(),
                        payload.body(),
                        payload.context(),
                        None,
                    )
                    .reasoning_effort
                };
                if level(original) != level(replacement) {
                    return Err(MiddlewareError::InvalidState);
                }
            }
            gateway_protocol::chat_completions::CHAT_PROTOCOL => {
                let search = |payload: &gateway_core::operation::ProtocolPayload| {
                    payload
                        .body()
                        .get("web_search_options")
                        .is_some_and(serde_json::Value::is_object)
                };
                if search(original) != search(replacement) {
                    return Err(MiddlewareError::InvalidState);
                }
            }
            _ => {}
        }
        return Ok(decoded);
    }
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
