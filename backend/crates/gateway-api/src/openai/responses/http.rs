//! OpenAI Responses HTTP 与 SSE adapter。

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    extract::{Extension, State, connect_info::ConnectInfo},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, USER_AGENT},
    },
    response::{IntoResponse, Response},
};
use futures::{StreamExt, stream};
use gateway_core::diagnostics::TraceContext;
use gateway_core::engine::execution::{ClientTransport, ExecutionSession, StartedExecution};
use gateway_core::engine::{CommitRequirement, EngineError};
use gateway_core::error::{GatewayError, GatewayErrorKind};
use gateway_core::event::{ProviderEvent, ProviderResponseHeader};
use gateway_core::lifecycle::ConnectionGuard;
use gateway_protocol::openai::sse::{DONE_SSE_FRAME, response_failed_sse_event_with_id};

use crate::ApiState;
use crate::openai::{
    auth::{authenticate_client, client_access_error_response},
    error::{
        engine_error_response, gateway_error_contract, gateway_error_from_engine,
        gateway_error_response, protocol_error_response, runtime_unavailable_response,
    },
};

use super::{
    DecodedResponsesRequest, OpenAiResponsesEncoder, ProtocolErrorBody, RequestDecodeError,
    ResponseEncodeError, request::decompress_request_body_accounted,
};

const SSE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub(in crate::openai) enum GenerationProtocol {
    Responses,
    Chat,
    GenAi { model: String, streaming: bool },
    GenAiCount { model: String },
    Messages { count: bool },
}

impl GenerationProtocol {
    pub(in crate::openai) fn name(&self) -> &'static str {
        match self {
            Self::Responses => "openai",
            Self::Chat => gateway_protocol::chat_completions::CHAT_PROTOCOL,
            Self::GenAi { .. } | Self::GenAiCount { .. } => gateway_protocol::genai::GENAI_PROTOCOL,
            Self::Messages { .. } => gateway_protocol::anthropic::ANTHROPIC_PROTOCOL,
        }
    }

    pub(in crate::openai) fn decode(
        &self,
        body: &[u8],
        headers: &HeaderMap,
        mut reserve: impl FnMut(usize) -> Result<(), RequestDecodeError>,
    ) -> Result<DecodedResponsesRequest, RequestDecodeError> {
        match self {
            Self::Responses => {
                super::request::decode_request_with_headers_accounted(body, headers, reserve)
            }
            Self::Chat => {
                let body = decompress_request_body_accounted(body, headers, &mut reserve)?;
                crate::openai::chat_completions::request::decode(&body, headers)
            }
            Self::GenAi { model, streaming } => {
                let body = decompress_request_body_accounted(body, headers, &mut reserve)?;
                let body = decode_object(&body)?;
                if !body
                    .get("contents")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|contents| !contents.is_empty())
                {
                    return Err(RequestDecodeError::InvalidType {
                        field: "contents".into(),
                        expected: "a nonempty array",
                    });
                }
                DecodedResponsesRequest::for_protocol(
                    self.name(),
                    body,
                    headers,
                    model.clone(),
                    *streaming,
                )
            }
            Self::GenAiCount { model } => {
                let body = decompress_request_body_accounted(body, headers, &mut reserve)?;
                let request = decode_object(&body)?;
                gateway_protocol::genai::count_tokens_request(&request).map_err(|_| {
                    RequestDecodeError::InvalidType {
                        field: "generateContentRequest".into(),
                        expected: "a JSON object",
                    }
                })?;
                DecodedResponsesRequest::for_count(self.name(), &body, headers, model.clone())
            }
            Self::Messages { count } => {
                let body = decompress_request_body_accounted(body, headers, &mut reserve)?;
                let request = decode_object(&body)?;
                let model = request
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                    .filter(|model| !model.trim().is_empty())
                    .ok_or_else(|| RequestDecodeError::InvalidValue {
                        field: "model".into(),
                    })?
                    .trim()
                    .to_owned();
                let validation = if *count {
                    gateway_protocol::anthropic::validate_count_request(&request)
                } else {
                    gateway_protocol::anthropic::validate_request(&request)
                };
                validation.map_err(|error| RequestDecodeError::NativeContract {
                    message: error.to_string(),
                })?;
                if *count {
                    DecodedResponsesRequest::for_count(self.name(), &body, headers, model)
                } else {
                    let streaming = request
                        .get("stream")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    DecodedResponsesRequest::for_protocol(
                        self.name(),
                        request,
                        headers,
                        model,
                        streaming,
                    )
                }
            }
        }
    }
}

fn decode_object(
    body: &[u8],
) -> Result<serde_json::Map<String, serde_json::Value>, RequestDecodeError> {
    let body: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| RequestDecodeError::MalformedJson)?;
    match body {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(RequestDecodeError::ExpectedObject),
    }
}

/// `POST /v1/responses`。
pub(crate) async fn responses(
    State(state): State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    headers: HeaderMap,
    body: crate::openai::ingress::AuthenticatedBody,
) -> Response {
    respond_authenticated(
        State(state),
        connect_info,
        ingress_id,
        headers,
        body.body,
        "/v1/responses",
        (body.client, Some(body.memory)),
        GenerationProtocol::Responses,
    )
    .await
}
pub(crate) async fn responses_at_endpoint(
    State(state): State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    headers: HeaderMap,
    body: Bytes,
    endpoint: &'static str,
) -> Response {
    let service = state.openai();
    let authentication = if endpoint == "/api/admin/plugins/models/responses" {
        crate::openai::auth::authenticate_builtin_client(service, &headers)
    } else {
        authenticate_client(service, &headers).await
    };
    let client = match authentication {
        Ok(client) => client,
        Err(error) => return client_access_error_response(error),
    };
    respond_authenticated(
        State(state),
        connect_info,
        ingress_id,
        headers,
        body,
        endpoint,
        (client, None),
        GenerationProtocol::Responses,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "shared authenticated HTTP ingress with an optional wire projection"
)]
pub(in crate::openai) async fn respond_authenticated(
    State(state): State<ApiState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    mut headers: HeaderMap,
    body: Bytes,
    endpoint: &'static str,
    authenticated: (
        gateway_core::engine::execution::AuthenticatedClient,
        Option<crate::openai::ingress::IngressMemory>,
    ),
    generation: GenerationProtocol,
) -> Response {
    headers.remove("x-goog-api-key");
    headers.remove("x-api-key");
    let (client, mut memory) = authenticated;
    let service = state.openai();
    let access = client.bound_access();
    let mut capacity_exhausted = false;
    let decoded = match generation.decode(&body, &headers, |bytes| {
        if memory.as_mut().is_none_or(|memory| memory.reserve(bytes)) {
            Ok(())
        } else {
            capacity_exhausted = true;
            Err(super::error::RequestDecodeError::DecompressedBodyTooLarge)
        }
    }) {
        Ok(decoded) => decoded,
        Err(_) if capacity_exhausted => return crate::openai::ingress::exhausted(),
        Err(error) => {
            return protocol_error_response(StatusCode::BAD_REQUEST, error.protocol_body());
        }
    };
    let (client_ip, user_agent) = request_client_context(
        &headers,
        connect_info.map(|Extension(ConnectInfo(address))| address),
    );
    let decoded = decoded.with_client_context(client_ip, user_agent);
    let streaming = decoded.metadata().stream();
    let connection_guard = if streaming {
        match service.try_register_connection() {
            Ok(guard) => Some(guard),
            Err(_) => return runtime_unavailable_response().into_response(),
        }
    } else {
        None
    };
    if let Some(registry) = service.middleware_registry() {
        return crate::openai::middleware::request::respond(
            service.clone(),
            registry,
            crate::openai::middleware::request::Ingress {
                client,
                generation,
                decoded,
                headers,
                body,
                endpoint,
                connection: connection_guard,
                ingress_id: ingress_id
                    .and_then(|Extension(id)| id.header_value().to_str().ok().map(str::to_owned)),
            },
        )
        .await;
    }
    let chat = crate::openai::chat_completions::ChatProjection::for_request(&decoded);
    let started = match service
        .start_response(
            client,
            decoded,
            if streaming {
                ClientTransport::HttpSse
            } else {
                ClientTransport::HttpJson
            },
            endpoint,
        )
        .await
    {
        Ok(started) => started,
        Err(error) => return gateway_error_response(&error),
    };
    let trace = started.session.trace();
    trace.headers("client.request", serde_json::json!({
        "ingressRequestId": ingress_id.as_ref().and_then(|Extension(id)| id.header_value().to_str().ok()),
    }), headers.iter().map(|(name, value)| (name.as_str(), value.as_bytes())));
    trace.capture("client.request.body", &body);
    let request_id_header = HeaderValue::from_str(started.request_id.as_str()).ok();
    let StartedExecution {
        stream, session, ..
    } = started;
    let protocol = generation.name();
    let mut response = if protocol != "openai" {
        let (admitted, keepalive) = tokio::sync::oneshot::channel();
        let _ = admitted.send(session.response_commit());
        let response = async move {
            match crate::openai::middleware::generation_response(
                session,
                if stream {
                    ClientTransport::HttpSse
                } else {
                    ClientTransport::HttpJson
                },
                protocol,
                chat,
            )
            .await
            {
                Ok(response) => {
                    crate::openai::middleware::delivery::deliver_generation(
                        response,
                        stream,
                        connection_guard,
                        protocol,
                    )
                    .await
                }
                Err(error) => {
                    crate::openai::middleware::delivery::generation_error_response(&error, protocol)
                }
            }
        };
        if stream && protocol == gateway_protocol::chat_completions::CHAT_PROTOCOL {
            super::keepalive::response_with_encoder(
                response,
                keepalive,
                Some(crate::openai::chat_completions::error_sse),
            )
            .await
        } else {
            response.await
        }
    } else if stream {
        stream_execution_response(session, connection_guard).await
    } else {
        drop(connection_guard);
        collect_execution_response(session).await
    };
    if let Some(id) = request_id_header {
        response.headers_mut().insert("x-gateway-request-id", id);
    }
    match access {
        Some(access) => crate::openai::revocation::protect(response, access),
        None => response,
    }
}

/// 从 socket 与标准转发头提取旧 Usage 页面使用的诊断事实。
pub(in crate::openai) fn request_client_context(
    headers: &HeaderMap,
    peer_address: Option<SocketAddr>,
) -> (Option<IpAddr>, Option<String>) {
    let client_ip = ["cf-connecting-ip", "x-real-ip"]
        .into_iter()
        .find_map(|name| header_ip(headers, name))
        .or_else(|| forwarded_client_ip(headers))
        .or_else(|| peer_address.map(|address| address.ip()));
    let user_agent = headers
        .get(USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    (client_ip, user_agent)
}

fn header_ip(headers: &HeaderMap, name: &str) -> Option<IpAddr> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| value.parse().ok())
}

fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    let addresses = headers
        .get("x-forwarded-for")?
        .to_str()
        .ok()?
        .split(',')
        .filter_map(|value| value.trim().parse::<IpAddr>().ok())
        .collect::<Vec<_>>();
    addresses
        .iter()
        .copied()
        .find(|address| !is_private_or_loopback(*address))
        .or_else(|| addresses.first().copied())
}

const fn is_private_or_loopback(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => address.is_private() || address.is_loopback(),
        IpAddr::V6(address) => address.is_unique_local() || address.is_loopback(),
    }
}

/// 编码完整 canonical event 集合，并在完整 JSON 成功后提交下游。
pub async fn collect_execution_response(session: Box<dyn ExecutionSession>) -> Response {
    let mut execution = PendingExecution::new(session);
    let Some(session) = execution.session_mut() else {
        return internal_gateway_response("gateway response session is unavailable");
    };
    let events = match session.collect_uncommitted().await {
        Ok(events) => events,
        Err(error) => {
            let response_headers = session.response_headers().to_vec();
            let response = engine_error_response_with_headers(&error, &response_headers);
            return execution.record_response_status(response).await;
        }
    };

    let encoded = match encode_collected_events(&events) {
        Ok(encoded) => encoded,
        Err(error) => {
            let response =
                protocol_error_response(StatusCode::INTERNAL_SERVER_ERROR, error.protocol_body());
            let response = execution.record_response_status(response).await;
            execution.cancel_and_finalize().await;
            return response;
        }
    };
    let response_headers = session.response_headers().to_vec();
    session.trace().record(
        "downstream.encoded",
        serde_json::json!({"transport": "http_json", "bytes": encoded.len()}),
    );
    session.trace().dump("downstream.body", &encoded);
    let response = json_body_response(encoded, &response_headers);

    let Some(session) = execution.session_mut() else {
        return internal_gateway_response("gateway response session is unavailable");
    };
    if let Err(error) = session
        .commit_downstream(Some(StatusCode::OK.as_u16()))
        .await
    {
        execution.cancel_and_finalize().await;
        let response = engine_error_response_with_headers(&error, &response_headers);
        return execution.record_response_status(response).await;
    }
    if !session.is_finalized() {
        execution.cancel_and_finalize().await;
        return internal_gateway_response("gateway response was not finalized after commit");
    }
    execution.disarm();
    response
}

#[derive(Debug)]
enum BufferedResponseEncodeError {
    Canonical(ResponseEncodeError),
    Json,
}

impl BufferedResponseEncodeError {
    fn protocol_body(&self) -> ProtocolErrorBody {
        match self {
            Self::Canonical(error) => error.protocol_body(),
            Self::Json => ResponseEncodeError::Serialization.protocol_body(),
        }
    }
}

fn encode_collected_events(
    events: &[ProviderEvent],
) -> Result<Vec<u8>, BufferedResponseEncodeError> {
    if let Some(body) = crate::openai::endpoint::raw_json_body(events)
        .map_err(BufferedResponseEncodeError::Canonical)?
    {
        return Ok(body.to_vec());
    }
    let mut encoder = OpenAiResponsesEncoder::new();
    for event in events {
        encoder.push_sse(event);
    }
    let response = encoder
        .finish()
        .map_err(BufferedResponseEncodeError::Canonical)?;
    serde_json::to_vec(&response).map_err(|_| BufferedResponseEncodeError::Json)
}

fn json_body_response(encoded: Vec<u8>, response_headers: &[ProviderResponseHeader]) -> Response {
    let mut response = Response::new(Body::from(encoded));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    apply_response_headers(response, response_headers)
}

fn apply_response_headers(
    mut response: Response,
    response_headers: &[ProviderResponseHeader],
) -> Response {
    let connection_options = super::response_connection_options(response_headers);
    for header in response_headers {
        if !super::response_header_is_forwardable(header.name(), &connection_options) {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(header.name().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(header.value()) else {
            continue;
        };
        response.headers_mut().append(name, value);
    }
    response
}

fn engine_error_response_with_headers(
    error: &EngineError,
    response_headers: &[ProviderResponseHeader],
) -> Response {
    apply_response_headers(engine_error_response(error), response_headers)
}

/// 编码首个 SSE frame 后提交下游，再持续驱动同一执行会话。
pub async fn stream_execution_response(
    session: Box<dyn ExecutionSession>,
    connection_guard: Option<Box<dyn ConnectionGuard>>,
) -> Response {
    let (admitted, keepalive) = tokio::sync::oneshot::channel();
    let commit = session.response_commit();
    commit.prefer_early_http_error();
    let _ = admitted.send(commit);
    super::keepalive::response(
        stream_execution_response_inner(session, connection_guard),
        keepalive,
    )
    .await
}

async fn stream_execution_response_inner(
    session: Box<dyn ExecutionSession>,
    connection_guard: Option<Box<dyn ConnectionGuard>>,
) -> Response {
    let mut execution = PendingExecution::new(session);
    let Some(session) = execution.session_mut() else {
        return internal_gateway_response("gateway response session is unavailable");
    };
    let first = match session.next_event().await {
        Ok(Some(event)) => event,
        Ok(None) => {
            let response = gateway_error_response(&GatewayError::new(
                GatewayErrorKind::Internal,
                "gateway response ended before its first event",
            ));
            let response = execution.record_response_status(response).await;
            execution.cancel_and_finalize().await;
            return response;
        }
        Err(error) => {
            let response_headers = session.response_headers().to_vec();
            let response = engine_error_response_with_headers(&error, &response_headers);
            return execution.record_response_status(response).await;
        }
    };
    let first_requirement = first.commit_requirement();
    let first_events = first.into_provider_events();
    if first_requirement != CommitRequirement::CommitBeforeDelivery {
        let response = internal_gateway_response("gateway first event did not require commit");
        let response = execution.record_response_status(response).await;
        execution.cancel_and_finalize().await;
        return response;
    }
    let mut encoder = OpenAiResponsesEncoder::new();
    let mut frames = Vec::new();
    for event in &first_events {
        frames.extend(encoder.push_sse(event));
    }
    if frames.is_empty() {
        let response = internal_gateway_response("gateway commit batch encoded no output");
        let response = execution.record_response_status(response).await;
        execution.cancel_and_finalize().await;
        return response;
    }
    let Some(session) = execution.session_mut() else {
        return internal_gateway_response("gateway response session is unavailable");
    };
    let response_headers = session.response_headers().to_vec();
    if let Err(error) = session
        .commit_downstream(Some(StatusCode::OK.as_u16()))
        .await
    {
        execution.cancel_and_finalize().await;
        let response = engine_error_response_with_headers(&error, &response_headers);
        return execution.record_response_status(response).await;
    }
    let Some(session) = execution.into_session() else {
        return internal_gateway_response("gateway response session is unavailable");
    };
    let mut state = ResponsesStreamState::new(session, encoder, frames, connection_guard);
    if state.encoder.is_completed() {
        state.finish_completed(Vec::new()).await;
    }
    let output = Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            if let Some(chunk) = state.pending.pop_front() {
                state.trace.dump("downstream.chunk", &chunk);
                state.handed_off_bytes += chunk.len() as u64;
                return Some((Ok::<Bytes, Infallible>(chunk), state));
            }
            if state.output_finished {
                return None;
            }
            state.advance().await;
        }
    }));
    // 保活只轮询已固定的输出 stream；不能取消并重建 advance/next_event future，
    // 否则一次心跳就可能丢失正在等待的上游事件或执行终态清理。
    let body = Body::from_stream(stream::unfold(output, |mut output| async move {
        let chunk = tokio::select! {
            biased;
            chunk = output.next() => chunk?,
            () = tokio::time::sleep(SSE_KEEPALIVE_INTERVAL) => {
                Ok(Bytes::from_static(b": keep-alive\n\n"))
            }
        };
        Some((chunk, output))
    }));
    event_stream_response(body, &response_headers)
}

fn event_stream_response(body: Body, response_headers: &[ProviderResponseHeader]) -> Response {
    let mut response = apply_response_headers(Response::new(body), response_headers);
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

fn internal_gateway_response(message: &'static str) -> Response {
    gateway_error_response(&GatewayError::new(GatewayErrorKind::Internal, message))
}

pub(in crate::openai) struct PendingExecution {
    session: Option<Box<dyn ExecutionSession>>,
}

impl PendingExecution {
    pub(in crate::openai) fn is_finalized(&self) -> bool {
        self.session
            .as_ref()
            .is_none_or(|session| session.is_finalized())
    }
    pub(in crate::openai) fn new(session: Box<dyn ExecutionSession>) -> Self {
        Self {
            session: Some(session),
        }
    }

    pub(in crate::openai) fn session_mut(
        &mut self,
    ) -> Option<&mut (dyn ExecutionSession + 'static)> {
        self.session.as_deref_mut()
    }

    pub(in crate::openai) async fn cancel_and_finalize(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.cancel();
            if !session.is_finalized() {
                let _ = session.next_event().await;
            }
        }
    }

    pub(in crate::openai) async fn record_response_status(
        &mut self,
        response: Response,
    ) -> Response {
        if let Some(session) = self.session.as_mut() {
            let _ = session
                .record_client_status(response.status().as_u16())
                .await;
        }
        response
    }

    pub(in crate::openai) fn disarm(&mut self) {
        self.session = None;
    }

    fn into_session(mut self) -> Option<Box<dyn ExecutionSession>> {
        self.session.take()
    }
}

impl Drop for PendingExecution {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        if session.is_finalized() {
            return;
        }
        session.trace().record(
            "downstream.cancelled",
            serde_json::json!({"reason": "response_guard_dropped"}),
        );
        session.cancel();
        detach_finalize(session);
    }
}

struct ResponsesStreamState {
    trace: TraceContext,
    handed_off_bytes: u64,
    session: Option<Box<dyn ExecutionSession>>,
    encoder: OpenAiResponsesEncoder,
    pending: VecDeque<Bytes>,
    output_finished: bool,
    execution_terminal: bool,
    _connection_guard: Option<Box<dyn ConnectionGuard>>,
}

impl ResponsesStreamState {
    fn new(
        session: Box<dyn ExecutionSession>,
        encoder: OpenAiResponsesEncoder,
        initial_frames: Vec<Bytes>,
        connection_guard: Option<Box<dyn ConnectionGuard>>,
    ) -> Self {
        Self {
            trace: session.trace(),
            handed_off_bytes: 0,
            session: Some(session),
            encoder,
            pending: initial_frames.into_iter().collect(),
            output_finished: false,
            execution_terminal: false,
            _connection_guard: connection_guard,
        }
    }

    async fn advance(&mut self) {
        let next = match self.session.as_mut() {
            Some(session) => session.next_event().await,
            None => {
                self.finish_with_gateway_error(GatewayError::new(
                    GatewayErrorKind::Internal,
                    "gateway response session is unavailable",
                ));
                return;
            }
        };
        match next {
            Ok(Some(event)) => {
                let requirement = event.commit_requirement();
                let events = event.into_provider_events();
                if requirement != CommitRequirement::AlreadyCommitted {
                    self.cancel_execution().await;
                    self.finish_with_gateway_error(GatewayError::new(
                        GatewayErrorKind::Internal,
                        "gateway requested another downstream commit",
                    ));
                } else {
                    for event in events {
                        self.push_event(event).await;
                        if self.output_finished {
                            break;
                        }
                    }
                }
            }
            Ok(None) => {
                self.execution_terminal = self
                    .session
                    .as_ref()
                    .is_some_and(|session| session.is_finalized());
                if self.execution_terminal {
                    self.pending
                        .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
                    self.output_finished = true;
                } else {
                    self.finish_with_gateway_error(GatewayError::new(
                        GatewayErrorKind::Internal,
                        "gateway response ended without finalizing the execution",
                    ));
                }
            }
            Err(error) => {
                self.execution_terminal = self
                    .session
                    .as_ref()
                    .is_some_and(|session| session.is_finalized());
                if self.encoder.has_wire_failure() {
                    self.pending
                        .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
                    self.output_finished = true;
                } else {
                    self.finish_with_gateway_error(gateway_error_from_engine(&error));
                }
            }
        }
    }

    async fn push_event(&mut self, event: ProviderEvent) {
        let frames = self.encoder.push_sse(&event);
        if self.encoder.is_completed() {
            self.finish_completed(frames).await;
        } else {
            self.pending.extend(frames);
        }
    }

    async fn finish_completed(&mut self, frames: Vec<Bytes>) {
        let next = match self.session.as_mut() {
            Some(session) => session.next_event().await,
            None => {
                self.finish_with_gateway_error(GatewayError::new(
                    GatewayErrorKind::Internal,
                    "gateway response session is unavailable",
                ));
                return;
            }
        };
        match next {
            Ok(None)
                if self
                    .session
                    .as_ref()
                    .is_some_and(|session| session.is_finalized()) =>
            {
                self.pending.extend(frames);
                self.execution_terminal = true;
                self.pending
                    .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
                self.output_finished = true;
            }
            Ok(None) => {
                self.cancel_execution().await;
                self.finish_with_gateway_error(GatewayError::new(
                    GatewayErrorKind::Internal,
                    "gateway response was not finalized after its terminal event",
                ));
            }
            Ok(Some(_)) => {
                self.cancel_execution().await;
                self.finish_with_gateway_error(GatewayError::new(
                    GatewayErrorKind::Internal,
                    "gateway response continued after its terminal event",
                ));
            }
            Err(error) => {
                self.execution_terminal = self
                    .session
                    .as_ref()
                    .is_some_and(|session| session.is_finalized());
                self.finish_with_gateway_error(gateway_error_from_engine(&error));
            }
        }
    }

    async fn cancel_execution(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.cancel();
            if !session.is_finalized() {
                let _ = session.next_event().await;
            }
            self.execution_terminal = session.is_finalized();
        }
    }

    fn finish_with_gateway_error(&mut self, error: GatewayError) {
        let (_, default_type, default_code) = gateway_error_contract(error.kind());
        self.pending
            .push_back(Bytes::from(response_failed_sse_event_with_id(
                self.encoder.response_id(),
                error.client_error_type().unwrap_or(default_type),
                super::super::error::client_error_code(
                    error.client_error_code().unwrap_or(default_code),
                ),
                error.client_message(),
            )));
        self.pending
            .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
        self.output_finished = true;
    }
}

impl Drop for ResponsesStreamState {
    fn drop(&mut self) {
        self.trace.record("downstream.body.closed", serde_json::json!({
            "handedOffBytes": self.handed_off_bytes,
            "pendingChunks": self.pending.len(),
            "outputFinished": self.output_finished, "executionTerminal": self.execution_terminal,
        }));
        if self.execution_terminal {
            return;
        }
        let Some(session) = self.session.take() else {
            return;
        };
        session.cancel();
        // Body drop 后由 Core 继续状态机清理，HTTP body 生命周期不等待它。
        detach_finalize(session);
    }
}

fn detach_finalize(session: Box<dyn ExecutionSession>) {
    let finalize = session.detach_finalize();
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        drop(runtime.spawn(finalize));
    }
}
