use super::super::responses::{DecodedResponsesRequest, RequestDecodeError};
use axum::http::HeaderMap;
use gateway_protocol::chat_completions::{CHAT_PROTOCOL, ChatError, parse_request};

pub(in crate::openai) fn decode(
    body: &[u8],
    headers: &HeaderMap,
) -> Result<DecodedResponsesRequest, RequestDecodeError> {
    let chat = parse_request(body).map_err(decode_error)?;
    let model = chat.body["model"].as_str().unwrap_or_default().to_owned();
    let stream = chat
        .body
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    DecodedResponsesRequest::for_protocol(CHAT_PROTOCOL, chat.body, headers, model, stream)
}

fn decode_error(error: ChatError) -> RequestDecodeError {
    match error {
        ChatError::MalformedJson => RequestDecodeError::MalformedJson,
        ChatError::ExpectedObject => RequestDecodeError::ExpectedObject,
        ChatError::MissingField { field } => RequestDecodeError::MissingField { field },
        ChatError::InvalidType { field, expected } => {
            RequestDecodeError::InvalidType { field, expected }
        }
        ChatError::EmptyField { field } => RequestDecodeError::EmptyField { field },
        ChatError::InvalidValue { field } => RequestDecodeError::InvalidValue { field },
        ChatError::UnknownField { field } => RequestDecodeError::UnknownField { field },
        ChatError::UnsupportedField { field } => RequestDecodeError::UnsupportedField { field },
        ChatError::UnsupportedCombination { field, .. } => RequestDecodeError::UnsupportedField {
            field: field.into(),
        },
        ChatError::InvalidResponse | ChatError::IncompleteStream => {
            RequestDecodeError::CanonicalContract {
                field: "chat request".into(),
            }
        }
    }
}
