//! Chat Completions 与 Responses 之间的 API 协议适配

mod delivery;
mod http;
mod request;
mod response;

pub(super) use http::chat_completions;

pub(in crate::openai) use delivery::ChatProjection;

pub(in crate::openai) fn error_sse(value: &serde_json::Value) -> bytes::Bytes {
    let error = value
        .pointer("/response/error")
        .or_else(|| value.get("error"))
        .unwrap_or(value);
    gateway_protocol::openai::sse::encode_sse_event(
        "",
        &serde_json::json!({"error":error}).to_string(),
    )
    .into()
}
