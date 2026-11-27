//! Native Anthropic Messages ingress.

use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Extension, MatchedPath, State, connect_info::ConnectInfo},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::post,
};
use gateway_core::engine::middleware::MiddlewareError;
use serde_json::{Value, json};

use super::{
    ingress::AuthenticatedBody,
    responses::http::{GenerationProtocol, respond_authenticated},
};
use crate::ApiState;

const MESSAGES: &str = "/v1/messages";
const COUNT: &str = "/v1/messages/count_tokens";

pub(super) fn router() -> Router<ApiState> {
    Router::new()
        .route(MESSAGES, post(handle))
        .route(COUNT, post(handle))
        .layer(axum::middleware::map_response(project_response))
}

async fn handle(
    state: State<ApiState>,
    path: MatchedPath,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    headers: HeaderMap,
    input: AuthenticatedBody,
) -> Response {
    let count = path.as_str() == COUNT;
    respond_authenticated(
        state,
        connect_info,
        ingress_id,
        headers,
        input.body,
        if count { COUNT } else { MESSAGES },
        (input.client, Some(input.memory)),
        GenerationProtocol::Messages { count },
    )
    .await
}

async fn project_response(mut response: Response) -> Response {
    if response.status().is_success() {
        if response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_none_or(|value| value != "text/event-stream")
        {
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("private, no-store"),
            );
        }
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let payload = if parts.status.is_redirection() {
        parts.status = StatusCode::BAD_GATEWAY;
        parts.headers.remove(header::LOCATION);
        error_payload("api_error", "The Messages upstream returned a redirect.")
    } else {
        let bytes = to_bytes(body, 1024 * 1024).await.unwrap_or_default();
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let message = value
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_else(|| parts.status.canonical_reason().unwrap_or("Request failed"));
        let kind = value
            .pointer("/error/type")
            .and_then(Value::as_str)
            .filter(|kind| is_messages_error_type(kind))
            .unwrap_or_else(|| error_type_for_status(parts.status));
        error_payload(kind, message)
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    parts.headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    Response::from_parts(parts, Body::from(payload.to_string()))
}

fn error_type_for_status(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 | 405 | 415 | 422 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ => "api_error",
    }
}

fn is_messages_error_type(kind: &str) -> bool {
    matches!(
        kind,
        "invalid_request_error"
            | "authentication_error"
            | "billing_error"
            | "permission_error"
            | "not_found_error"
            | "request_too_large"
            | "rate_limit_error"
            | "api_error"
            | "overloaded_error"
    )
}

fn error_payload(kind: &str, message: &str) -> Value {
    json!({"type":"error", "error":{"type": kind, "message": message}})
}

pub(super) fn error_frame(error: &MiddlewareError) -> Bytes {
    use super::error::gateway_error_from_engine;
    let error = match error {
        MiddlewareError::Engine(error) => Some(gateway_error_from_engine(error)),
        MiddlewareError::Provider(error) => {
            Some(gateway_core::error::GatewayError::from_provider(error))
        }
        MiddlewareError::Gateway(error) => Some(error.clone()),
        _ => None,
    };
    let payload = match error.as_ref().and_then(|error| {
        error
            .client_error_type()
            .filter(|kind| !kind.trim().is_empty())
            .map(|kind| (kind, error.client_message()))
    }) {
        Some((kind, message)) => error_payload(kind, message),
        None => error_payload("api_error", "Upstream stream ended before completion"),
    };
    Bytes::from(format!("event: error\ndata: {payload}\n\n"))
}
