//! Chat Completions 与 Responses 之间的 API 协议适配

mod delivery;
mod http;
pub(in crate::openai) mod request;

pub(super) use http::chat_completions;

pub(in crate::openai) use delivery::ChatProjection;

pub(in crate::openai) fn error_sse(value: &serde_json::Value) -> bytes::Bytes {
    let error = gateway_protocol::chat_completions::stream_error(value);
    gateway_protocol::openai::sse::encode_sse_event("", &error.to_string()).into()
}
