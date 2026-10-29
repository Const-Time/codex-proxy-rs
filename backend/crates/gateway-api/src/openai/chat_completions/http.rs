//! Bound uploads before converting Chat to the shared execution path.
use super::{ChatProjection, request::convert_request};
use crate::{
    ApiState,
    openai::{
        error::protocol_error_response,
        ingress::AuthenticatedBody,
        responses::{http::respond_authenticated, request::decompress_request_body_accounted},
    },
};
use axum::{
    body::Bytes,
    extract::{Extension, State, connect_info::ConnectInfo},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_ENCODING, CONTENT_LENGTH},
    },
    response::Response,
};
use std::net::SocketAddr;
pub(in crate::openai) async fn chat_completions(
    state: State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    mut headers: HeaderMap,
    mut input: AuthenticatedBody,
) -> Response {
    let mut exhausted = false;
    let decoded = decompress_request_body_accounted(&input.body, &headers, &mut |bytes| {
        if input.memory.reserve(bytes) {
            Ok(())
        } else {
            exhausted = true;
            Err(crate::openai::responses::RequestDecodeError::DecompressedBodyTooLarge)
        }
    });
    let converted = match decoded.and_then(|bytes| convert_request(&bytes)) {
        Ok(value) => value,
        Err(_) if exhausted => return crate::openai::ingress::exhausted(),
        Err(error) => {
            return protocol_error_response(StatusCode::BAD_REQUEST, error.protocol_body());
        }
    };
    let model = converted
        .body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let bytes = match serde_json::to_vec(&converted.body) {
        Ok(bytes) => bytes,
        Err(_) => {
            return protocol_error_response(
                StatusCode::BAD_REQUEST,
                crate::openai::responses::RequestDecodeError::MalformedJson.protocol_body(),
            );
        }
    };
    if !input.memory.reserve(bytes.len()) {
        return crate::openai::ingress::exhausted();
    }
    headers.remove(CONTENT_ENCODING);
    headers.remove(CONTENT_LENGTH);
    respond_authenticated(
        state,
        connect_info,
        ingress_id,
        headers,
        Bytes::from(bytes),
        "/v1/chat/completions",
        (input.client, Some(input.memory)),
        Some(ChatProjection::new(model, converted.include_usage)),
    )
    .await
}
