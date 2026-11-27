use crate::{
    ApiState,
    openai::{
        ingress::AuthenticatedBody,
        responses::http::{GenerationProtocol, respond_authenticated},
    },
};
use axum::{
    extract::{Extension, State, connect_info::ConnectInfo},
    http::HeaderMap,
    response::Response,
};
use std::net::SocketAddr;

pub(in crate::openai) async fn chat_completions(
    state: State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    headers: HeaderMap,
    input: AuthenticatedBody,
) -> Response {
    respond_authenticated(
        state,
        connect_info,
        ingress_id,
        headers,
        input.body,
        "/v1/chat/completions",
        (input.client, Some(input.memory)),
        GenerationProtocol::Chat,
    )
    .await
}
