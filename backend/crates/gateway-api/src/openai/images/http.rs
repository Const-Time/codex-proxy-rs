//! Codex Images 非流式 HTTP adapter。

use std::net::SocketAddr;

use axum::{
    body::Bytes,
    extract::{Extension, State, connect_info::ConnectInfo},
    http::HeaderMap,
    response::Response,
};
use gateway_core::error::{GatewayError, GatewayErrorKind};
use gateway_core::operation::{ImageRequest, ImageRequestKind, Operation, RawJsonPayload};
use serde_json::Value;

use crate::ApiState;
use crate::openai::{
    auth::{authenticate_client, client_access_error_response},
    error::gateway_error_response,
    provider_endpoint::collect_raw_json_response,
    responses::{OpenAiRequestHeaders, request_client_context},
};

const OPENAI_PROTOCOL: &str = "openai";
const IMAGE_TURN_ID_CONTEXT_KEY: &str = "image_turn_id";

/// `POST /v1/images/generations`。
pub(crate) async fn image_generations(
    State(state): State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_image_request(
        state,
        connect_info,
        headers,
        body,
        ImageRequestKind::Generation,
        "/v1/images/generations",
    )
    .await
}

/// `POST /v1/images/edits`。
pub(crate) async fn image_edits(
    State(state): State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_image_request(
        state,
        connect_info,
        headers,
        body,
        ImageRequestKind::Edit,
        "/v1/images/edits",
    )
    .await
}

async fn handle_image_request(
    state: ApiState,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    body: Bytes,
    kind: ImageRequestKind,
    endpoint: &'static str,
) -> Response {
    let service = state.openai();
    let client = match authenticate_client(service, &headers) {
        Ok(client) => client,
        Err(error) => return client_access_error_response(error),
    };
    let (client_ip, user_agent) = request_client_context(
        &headers,
        connect_info.map(|Extension(ConnectInfo(address))| address),
    );
    let operation = match image_operation(body, &headers, kind) {
        Ok(operation) => operation,
        Err(error) => return gateway_error_response(&error),
    };
    let started = match service
        .start_provider_endpoint(client, operation, client_ip, user_agent, endpoint)
        .await
    {
        Ok(started) => started,
        Err(error) => return gateway_error_response(&error),
    };
    collect_raw_json_response(started).await
}

fn image_operation(
    body: Bytes,
    headers: &HeaderMap,
    kind: ImageRequestKind,
) -> Result<Operation, GatewayError> {
    let mut context = OpenAiRequestHeaders::from_headers(headers).session_context();
    if let Some(turn_id) = headers
        .get("x-codex-image-turn-id")
        .and_then(|value| value.to_str().ok())
    {
        context.insert(
            IMAGE_TURN_ID_CONTEXT_KEY.to_owned(),
            Value::String(turn_id.to_owned()),
        );
    }
    let payload = RawJsonPayload::new(OPENAI_PROTOCOL, body)
        .map_err(|_| {
            GatewayError::new(
                GatewayErrorKind::Internal,
                "OpenAI protocol identifier is invalid",
            )
        })?
        .with_context(context);
    Ok(Operation::GenerateImage(ImageRequest::from_raw_json(
        kind, payload,
    )))
}
