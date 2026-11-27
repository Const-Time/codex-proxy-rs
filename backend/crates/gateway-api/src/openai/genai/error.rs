//! Google RPC JSON 错误体和 SSE 失败帧，保留上游的原始错误详情

use axum::{
    body::{Body, Bytes, to_bytes},
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use serde_json::{Value, json};

pub(super) fn payload(status: StatusCode, message: &str) -> Value {
    let kind = match status.as_u16() {
        400 | 405 | 413 | 415 | 422 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        404 => "NOT_FOUND",
        408 | 504 => "DEADLINE_EXCEEDED",
        409 => "ABORTED",
        429 => "RESOURCE_EXHAUSTED",
        499 => "CANCELLED",
        501 => "UNIMPLEMENTED",
        502 | 503 => "UNAVAILABLE",
        _ => "INTERNAL",
    };
    json!({"error": {"code": status.as_u16(), "status": kind, "message": message}})
}

pub(super) async fn project_response(mut response: Response) -> Response {
    // GenAI accepts a custom API key header, which shared caches do not treat as Authorization.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    if response.status().is_success() {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = to_bytes(body, 1024 * 1024).await.unwrap_or_default();
    let value = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    if value.pointer("/error/code").is_some_and(Value::is_number) {
        return Response::from_parts(parts, Body::from(bytes));
    }
    let message = value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            parts
                .status
                .canonical_reason()
                .unwrap_or("The gateway could not complete the request.")
        });
    let data = payload(parts.status, message);
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, Body::from(data.to_string()))
}

pub(in crate::openai) fn error_frame(status: StatusCode, message: &str) -> Bytes {
    Bytes::from(format!("data: {}\n\n", payload(status, message)))
}
