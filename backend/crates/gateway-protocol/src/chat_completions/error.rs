//! Chat 解析与直接转换的安全错误事实

use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::openai::events;

/// 协议层错误不持有请求内容或上游凭据
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChatError {
    #[error("Request body must be valid JSON.")]
    MalformedJson,
    #[error("Request body must be a JSON object.")]
    ExpectedObject,
    #[error("Required parameter `{field}` is missing.")]
    MissingField { field: String },
    #[error("Parameter `{field}` must be {expected}.")]
    InvalidType {
        field: String,
        expected: &'static str,
    },
    #[error("Parameter `{field}` must not be empty.")]
    EmptyField { field: String },
    #[error("Parameter `{field}` has an invalid value.")]
    InvalidValue { field: String },
    #[error("Unknown parameter `{field}`.")]
    UnknownField { field: String },
    #[error("Parameter `{field}` is not supported by this upstream.")]
    UnsupportedField { field: String },
    #[error("The requested `{field}` is not supported with `{other}`.")]
    UnsupportedCombination {
        field: &'static str,
        other: &'static str,
    },
    #[error("The upstream response cannot be represented as a Chat completion.")]
    InvalidResponse,
    #[error("The upstream stream ended before its terminal event.")]
    IncompleteStream,
}

impl ChatError {
    /// A valid request option or combination that the selected wire protocol cannot represent.
    #[must_use]
    pub const fn is_target_unsupported(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedField { .. } | Self::UnsupportedCombination { .. }
        )
    }
    /// 供 API 出口构造稳定的机器码
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MalformedJson => "invalid_json",
            Self::ExpectedObject => "invalid_request_body",
            Self::MissingField { .. } => "missing_required_parameter",
            Self::InvalidType { .. } => "invalid_type",
            Self::EmptyField { .. } | Self::InvalidValue { .. } => "invalid_value",
            Self::UnknownField { .. } => "unknown_parameter",
            Self::UnsupportedField { .. } | Self::UnsupportedCombination { .. } => {
                "unsupported_parameter"
            }
            Self::InvalidResponse => "invalid_upstream_response",
            Self::IncompleteStream => "incomplete_upstream_response",
        }
    }

    /// 字段路径仅包含协议结构，不包含用户值
    pub fn parameter(&self) -> Option<&str> {
        match self {
            Self::MissingField { field }
            | Self::InvalidType { field, .. }
            | Self::EmptyField { field }
            | Self::InvalidValue { field }
            | Self::UnknownField { field }
            | Self::UnsupportedField { field } => Some(field),
            Self::UnsupportedCombination { field, .. } => Some(field),
            _ => None,
        }
    }
}

pub(super) fn invalid(field: &str) -> ChatError {
    ChatError::InvalidValue {
        field: field.to_owned(),
    }
}

pub(super) fn unsupported(field: &str) -> ChatError {
    ChatError::UnsupportedField {
        field: field.to_owned(),
    }
}

fn error_object(document: &Value) -> &Value {
    [
        document.pointer("/response/error"),
        document.get("error"),
        document.get("detail"),
    ]
    .into_iter()
    .flatten()
    .find(|value| value.is_object())
    .unwrap_or(document)
}

/// Public error fields and typed recovery information for Chat.
pub fn public_error_fields(document: &Value) -> Map<String, Value> {
    let source = error_object(document);
    let mut error = ["message", "type", "code", "param"]
        .into_iter()
        .filter_map(|field| {
            let value = [source, document]
                .into_iter()
                .filter_map(|value| value.get(field))
                .find(|value| match value {
                    Value::String(text) => field != "type" || !text.starts_with("response."),
                    Value::Null => field == "param",
                    _ => false,
                })?;
            Some((field.into(), value.clone()))
        })
        .collect::<Map<_, _>>();
    if events::upstream_no_more_retry(document) || events::upstream_no_more_retry(source) {
        error.insert(
            "headers".into(),
            json!({"x-retry-metadata": "NO_MORE_RETRY"}),
        );
    }
    if let Some(action) = [source, document]
        .into_iter()
        .find_map(|value| value.pointer("/recovery/action").and_then(Value::as_str))
    {
        error.insert("recovery".into(), json!({"action": action}));
    }
    error
}

/// Read retry timing from the selected error shape using Responses semantics.
pub fn error_retry_after_seconds(document: &Value) -> Option<u64> {
    events::retry_after_seconds_from_value(document)
        .or_else(|| events::retry_after_seconds_from_value(error_object(document)))
}

/// Build a Chat SSE error from public fields, never copying upstream extension objects.
pub fn stream_error(document: &Value) -> Value {
    let mut error = public_error_fields(document);
    if error.get("message").and_then(Value::as_str).is_none() {
        error.insert("message".into(), "Response stream failed".into());
    }
    if error
        .get("type")
        .and_then(Value::as_str)
        .is_none_or(|kind| kind == "error")
    {
        error.insert("type".into(), "server_error".into());
    }
    if let Some(seconds) = error_retry_after_seconds(document) {
        error.entry("headers").or_insert_with(|| json!({}))["retry-after"] =
            seconds.to_string().into();
    }
    json!({"error": error})
}
