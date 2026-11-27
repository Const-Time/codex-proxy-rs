//! Chat HTTP 夹具和共享执行、鉴权、插件及终结边界

use std::{
    collections::VecDeque,
    io::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    engine::{
        CommitRequirement, CoordinatedEvent, EngineError, ModelRequestId,
        execution::{
            AuthenticatedClient, ClientAuthenticationError, ExecutionService, ExecutionSession,
            ResponseCommit, StartExecution, StartProviderExecution, StartedExecution,
        },
    },
    error::{
        ClientVisibleUpstreamError, ClientVisibleUpstreamResponse, GatewayError, ProviderError,
        ProviderErrorKind,
    },
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseHeader, ResponseMeta},
    operation::Operation,
    routing::PublicModelId,
    upstream::UpstreamSendState,
};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::{chat_request, completed, created, message};
use crate::openai::{api_router, authenticated_client};

pub(super) const PATH: &str = "/v1/chat/completions";
const KEY: &str = "sk_chat_fixture";

#[derive(Default)]
pub(super) struct Trace {
    pub commits: AtomicUsize,
    pub cancelled: AtomicBool,
    pub finalized: AtomicBool,
    pub detached: AtomicUsize,
    response_commit: ResponseCommit,
}

#[derive(Default)]
pub(super) enum Finish {
    #[default]
    Success,
    Error,
    Pending,
}

pub(super) struct Session {
    events: VecDeque<Vec<ProviderEvent>>,
    pub trace: Arc<Trace>,
    pub finish: Finish,
    pub delay: Option<Duration>,
    pub fail_commit: bool,
    pub response_headers: Vec<ProviderResponseHeader>,
    provider_error: Option<ProviderError>,
    committed: bool,
    buffered: bool,
}

impl Session {
    pub fn new(events: Vec<ProviderEvent>) -> Self {
        Self {
            events: events.into_iter().map(|event| vec![event]).collect(),
            trace: Arc::default(),
            finish: Finish::Success,
            delay: None,
            fail_commit: false,
            response_headers: Vec::new(),
            provider_error: None,
            committed: false,
            buffered: false,
        }
    }

    pub fn with_batch(mut self, events: Vec<ProviderEvent>) -> Self {
        self.events.push_back(events);
        self
    }

    fn take_provider_error(&mut self) -> Option<EngineError> {
        let error = self.provider_error.take()?;
        self.trace.finalized.store(true, Ordering::SeqCst);
        Some(EngineError::Provider(error))
    }
}

impl ExecutionSession for Session {
    fn response_commit(&self) -> ResponseCommit {
        self.trace.response_commit.clone()
    }

    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            if let Some(delay) = self.delay.take() {
                tokio::time::sleep(delay).await;
            }
            if let Some(event) = self.events.pop_front() {
                let requirement = if self.committed {
                    CommitRequirement::AlreadyCommitted
                } else {
                    CommitRequirement::CommitBeforeDelivery
                };
                return CoordinatedEvent::try_batch(event, requirement).map(Some);
            }
            if let Some(error) = self.take_provider_error() {
                return Err(error);
            }
            match self.finish {
                Finish::Success => {
                    self.trace.finalized.store(true, Ordering::SeqCst);
                    Ok(None)
                }
                Finish::Error => {
                    self.trace.finalized.store(true, Ordering::SeqCst);
                    Err(EngineError::ProviderMetadataMismatch)
                }
                Finish::Pending => futures::future::pending().await,
            }
        })
    }

    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        self.buffered = true;
        Box::pin(async move {
            if let Some(error) = self.take_provider_error() {
                return Err(error);
            }
            Ok(self.events.drain(..).flatten().collect())
        })
    }

    fn response_headers(&self) -> &[ProviderResponseHeader] {
        &self.response_headers
    }

    fn commit_downstream(&mut self, status: Option<u16>) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            self.trace.commits.fetch_add(1, Ordering::SeqCst);
            if self.fail_commit {
                return Err(EngineError::ProviderMetadataMismatch);
            }
            self.trace.response_commit.record(status);
            self.committed = true;
            if self.buffered {
                self.trace.finalized.store(true, Ordering::SeqCst);
            }
            Ok(())
        })
    }

    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            self.trace.response_commit.record(Some(status));
            Ok(())
        })
    }

    fn is_finalized(&self) -> bool {
        self.trace.finalized.load(Ordering::SeqCst)
    }
    fn cancel(&self) {
        self.trace.cancelled.store(true, Ordering::SeqCst);
    }
    fn detach_finalize(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.trace.detached.fetch_add(1, Ordering::SeqCst);
            self.trace.finalized.store(true, Ordering::SeqCst);
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct Captured {
    pub body: Value,
    pub context: Value,
    pub endpoint: String,
    pub protocol: String,
}

pub(super) struct Execution {
    pub captured: Mutex<Option<Captured>>,
    pub client: AuthenticatedClient,
    session: Mutex<Option<Session>>,
}

impl Execution {
    pub fn new(session: Session) -> Self {
        Self {
            captured: Mutex::default(),
            client: authenticated_client(KEY),
            session: Mutex::new(Some(session)),
        }
    }
}

impl ExecutionService for Execution {
    fn authenticate(&self, key: &str) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        if key == KEY {
            Ok(self.client.clone())
        } else {
            Err(ClientAuthenticationError::InvalidKey)
        }
    }
    fn public_models(&self, _: &AuthenticatedClient) -> Vec<PublicModelId> {
        Vec::new()
    }
    fn contains_public_model(&self, _: &AuthenticatedClient, _: &PublicModelId) -> bool {
        true
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            let (protocol, body, context) = match &request.operation {
                Operation::Generate(generation) => {
                    let payload = generation.protocol_payload();
                    (
                        payload.protocol(),
                        Value::Object(payload.body().clone()),
                        payload.context(),
                    )
                }
                Operation::CountTokens(count) => {
                    let payload = count.payload();
                    (
                        payload.protocol(),
                        serde_json::from_slice(payload.body()).unwrap(),
                        payload.context(),
                    )
                }
                _ => panic!("unexpected operation"),
            };
            *self.captured.lock().unwrap() = Some(Captured {
                body,
                context: Value::Object(context.clone()),
                endpoint: request.metadata.endpoint,
                protocol: protocol.to_owned(),
            });
            Ok(StartedExecution {
                request_id: ModelRequestId::new("req_chat_fixture").unwrap(),
                created_at: std::time::SystemTime::now(),
                stream: request.metadata.stream,
                session: Box::new(self.session.lock().unwrap().take().expect("one request")),
            })
        })
    }
    fn start_provider_endpoint(
        &self,
        _: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async { panic!("Chat must use model execution") })
    }
}

pub(super) struct Fixture {
    pub router: axum::Router,
    pub execution: Arc<Execution>,
    pub trace: Arc<Trace>,
}

impl Fixture {
    pub async fn new(session: Session) -> Self {
        Self::with_execution(Execution::new(session)).await
    }
    pub async fn with_execution(execution: Execution) -> Self {
        let trace = execution
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .trace
            .clone();
        let execution = Arc::new(execution);
        Self {
            router: api_router(execution.clone()).await,
            execution,
            trace,
        }
    }
    pub async fn post(&self, body: Value) -> Response {
        self.router.clone().oneshot(request(body)).await.unwrap()
    }
    pub fn captured(&self) -> Captured {
        self.execution
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("started request")
    }
}

pub(super) fn request(body: Value) -> Request<Body> {
    Request::post(PATH)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

pub(super) async fn json_body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn rejects_missing_authentication_before_decoding_or_execution() {
    let fixture = Fixture::new(Session::new(Vec::new())).await;
    for authorization in [None, Some("Bearer sk_invalid")] {
        let mut request = Request::post(PATH);
        if let Some(authorization) = authorization {
            request = request.header("authorization", authorization);
        }
        let response = fixture
            .router
            .clone()
            .oneshot(request.body(Body::from("broken json")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    assert!(fixture.execution.captured.lock().unwrap().is_none());
}

#[tokio::test]
async fn dropping_chat_body_cancels_and_finalizes_the_original_execution() {
    let mut session = Session::new(vec![created()]);
    session.finish = Finish::Pending;
    let fixture = Fixture::new(session).await;
    let response = fixture.post(chat_request(true)).await;
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(fixture.trace.cancelled.load(Ordering::SeqCst));
    assert_eq!(fixture.trace.detached.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gzip_and_headers_reuse_the_responses_ingress_contract() {
    let mut session = Session::new(vec![completed(json!([message("ok")]))]);
    session.response_headers = vec![
        ProviderResponseHeader::new("x-future-multi", Bytes::from_static(b"first")),
        ProviderResponseHeader::new("x-future-multi", Bytes::from_static(b"second")),
        ProviderResponseHeader::new("connection", Bytes::from_static(b"x-hop-only")),
        ProviderResponseHeader::new("x-hop-only", Bytes::from_static(b"removed")),
    ];
    let fixture = Fixture::new(session).await;
    let body = chat_request(false).to_string();
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(body.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    let request = Request::post(PATH)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header("content-encoding", "gzip")
        .header("content-length", compressed.len())
        .header("session_id", "chat-session")
        .header("x-goog-api-key", "fixture-google-key")
        .header("x-future-input", "first")
        .header("x-future-input", "second")
        .body(Body::from(compressed))
        .unwrap();
    let response = fixture.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get_all("x-future-multi")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(!response.headers().contains_key("x-hop-only"));
    assert!(!response.headers().contains_key("content-encoding"));
    assert_eq!(
        json_body(response).await["choices"][0]["message"]["content"],
        "ok"
    );
    let captured = fixture.captured();
    assert_eq!(captured.context["session_id"], "chat-session");
    let headers = captured.context["opaque_request_headers"]
        .as_array()
        .unwrap();
    assert_eq!(
        headers
            .iter()
            .filter(|header| header[0] == "x-future-input")
            .count(),
        2
    );
    assert!(!headers.iter().any(|header| matches!(
        header[0].as_str(),
        Some("content-encoding" | "content-length" | "authorization" | "x-goog-api-key")
    )));
}

#[tokio::test]
async fn commit_failure_does_not_deliver_buffered_or_first_terminal_chat_success() {
    for streaming in [false, true] {
        let mut session = Session::new(vec![completed(json!([message("not delivered")]))]);
        session.fail_commit = true;
        let fixture = Fixture::new(session).await;
        let response = fixture.post(chat_request(streaming)).await;
        assert!(response.status().is_server_error());
        let body = json_body(response).await;
        assert!(body.get("error").is_some());
        assert!(body.get("choices").is_none());
        assert!(fixture.trace.cancelled.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn the_same_execution_fixture_finishes_native_responses_with_one_commit() {
    for already_started in [false, true] {
        let mut events = Vec::new();
        if already_started {
            events.push(created());
        }
        events.push(completed(json!([message("native baseline")])));
        let fixture = Fixture::new(Session::new(events)).await;
        let mut request = request(json!({"model":"model-a","input":"hello","stream":true}));
        *request.uri_mut() = "/v1/responses".parse().unwrap();
        let response = fixture.router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("event: response.completed"), "{text}");
        assert!(text.contains("native baseline"), "{text}");
        assert!(!text.contains("response.failed"), "{text}");
        assert_eq!(text.matches("data: [DONE]").count(), 1, "{text}");
        assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
        assert!(fixture.trace.finalized.load(Ordering::SeqCst));
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.trace.response_commit.status_code(), Some(200));
    }
}

#[tokio::test]
async fn finalization_failure_never_leaks_success_finish_or_usage() {
    for already_started in [false, true] {
        let mut events = Vec::new();
        if already_started {
            events.push(created());
        }
        events.push(completed(json!([])));
        let mut session = Session::new(events);
        session.finish = Finish::Error;
        let fixture = Fixture::new(session).await;
        let mut body = chat_request(true);
        body["stream_options"] = json!({"include_usage":true});
        let response = fixture.post(body).await;
        // Before any HTTP output the fork preserves an ordinary error status.
        if !already_started {
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert!(json_body(response).await.get("error").is_some());
            assert!(fixture.trace.finalized.load(Ordering::SeqCst));
            continue;
        }
        // 已输出首帧后终结错误只能作为流内错误交付
        assert_eq!(response.status(), StatusCode::OK);
        let (_, chunks) = super::response::sse_body(response).await;
        assert!(chunks.iter().any(|chunk| chunk.get("error").is_some()));
        assert!(
            !chunks
                .iter()
                .any(|chunk| chunk["choices"][0]["finish_reason"].is_string())
        );
        assert!(!chunks.iter().any(|chunk| chunk["choices"] == json!([])));
        assert!(fixture.trace.finalized.load(Ordering::SeqCst));
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.trace.response_commit.status_code(), Some(200));
    }
}

#[tokio::test(start_paused = true)]
async fn error_after_admission_keepalive_uses_chat_error_encoding() {
    let mut session = Session::new(Vec::new());
    session.delay = Some(Duration::from_secs(31));
    session.finish = Finish::Error;
    let fixture = Fixture::new(session).await;
    let response = fixture.post(chat_request(true)).await;
    let (body, chunks) = super::response::sse_body(response).await;
    assert!(body.starts_with(": keep-alive\n\n"));
    assert!(chunks.iter().any(|chunk| chunk.get("error").is_some()));
    assert!(!body.contains("response.failed"));
    assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.trace.response_commit.status_code(), Some(200));
}

#[tokio::test]
async fn direct_provider_http_error_has_only_public_chat_fields() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};

    for stream in [false, true] {
        let mut session = Session::new(Vec::new());
        session.provider_error = Some(provider_http_error());
        let fixture = Fixture::new(session).await;
        assert_projected_provider_http_error(fixture.post(chat_request(stream)).await).await;
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);

        let fixture = PluginFixture::new(Mode::Rewrite, true).await;
        *fixture.effects.upstream_http_error.lock().unwrap() = Some(provider_http_error());
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::post(PATH)
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .body(Body::from(chat_request(stream).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_projected_provider_http_error(response).await;
    }
}

async fn assert_projected_provider_http_error(response: Response) {
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::RETRY_AFTER], "17");
    assert!(!response.headers().contains_key("x-goog-api-key"));
    assert_eq!(
        json_body(response).await,
        json!({"error":{"message":"fixture upstream error","code":"bad_input","param":null}}),
    );
}

fn provider_http_error() -> ProviderError {
    let body = json!({
        "error": {
            "message": "fixture upstream error",
            "code": "bad_input",
            "param": null,
            "resets_in_seconds": 49,
            "private_details": {"reasoning": "private-reasoning"}
        },
        "output": [{"type":"reasoning","encrypted_content":"private-reasoning"}]
    });
    upstream_http_error(
        ProviderErrorKind::InvalidRequest,
        400,
        body,
        vec![
            ProviderResponseHeader::new("retry-after", Bytes::from_static(b"17")),
            ProviderResponseHeader::new("x-retry-metadata", Bytes::from_static(b"NO_MORE_RETRY")),
            ProviderResponseHeader::new("x-goog-api-key", Bytes::from_static(b"fixture-key")),
        ],
    )
}

fn upstream_http_error(
    kind: ProviderErrorKind,
    status: u16,
    body: Value,
    headers: Vec<ProviderResponseHeader>,
) -> ProviderError {
    ProviderError::new(kind, UpstreamSendState::Sent)
        .with_status(status)
        .with_client_visible_upstream_response(
            ClientVisibleUpstreamResponse::new(
                status,
                Some(b"application/problem+json".to_vec()),
                Bytes::from(body.to_string()),
            )
            .with_headers(headers),
        )
}

#[tokio::test(start_paused = true)]
async fn provider_http_error_after_keepalive_hides_responses_reasoning() {
    let mut session = Session::new(Vec::new());
    session.delay = Some(Duration::from_secs(31));
    session.provider_error = Some(provider_http_error());
    let fixture = Fixture::new(session).await;
    let response = fixture.post(chat_request(true)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let (body, chunks) = super::response::sse_body(response).await;
    assert!(body.starts_with(": keep-alive\n\n"));
    assert!(!body.contains("private-reasoning"));
    assert_eq!(chunks.last().unwrap()["error"]["code"], "bad_input");
    assert_eq!(
        chunks.last().unwrap()["error"]["headers"]["retry-after"],
        "17"
    );
    assert_eq!(
        chunks.last().unwrap()["error"]["headers"]["x-retry-metadata"],
        "NO_MORE_RETRY"
    );
}

#[tokio::test(start_paused = true)]
async fn body_only_rate_limit_hint_reaches_chat_clients() {
    for (stream, after_keepalive) in [(false, false), (true, false), (true, true)] {
        let mut session = Session::new(Vec::new());
        session.delay = after_keepalive.then_some(Duration::from_secs(31));
        session.provider_error = Some(upstream_http_error(
            ProviderErrorKind::RateLimited,
            429,
            json!({"resets_in_seconds":23,"error":{
                "message":"slow down","code":"rate_limit_exceeded","resets_at":1,
                "recovery":{"action":"retry","secret":"private-fixture"},
                "headers":{"x-retry-metadata":"NO_MORE_RETRY","x-goog-api-key":"private-fixture"}
            }}),
            Vec::new(),
        ));
        let fixture = Fixture::new(session).await;
        let response = fixture.post(chat_request(stream)).await;
        if after_keepalive {
            assert_eq!(response.status(), StatusCode::OK);
            let (body, chunks) = super::response::sse_body(response).await;
            assert!(body.starts_with(": keep-alive\n\n"));
            assert!(body.ends_with("data: [DONE]\n\n"));
            assert!(!body.contains("private-fixture"));
            let error = &chunks.last().unwrap()["error"];
            assert_eq!(error["type"], "server_error");
            assert_eq!(error["headers"]["retry-after"], "23");
            assert_eq!(error["headers"]["x-retry-metadata"], "NO_MORE_RETRY");
            assert_eq!(error["recovery"], json!({"action":"retry"}));
        } else {
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(response.headers()[header::RETRY_AFTER], "23");
            assert_eq!(
                json_body(response).await["error"]["code"],
                "rate_limit_exceeded"
            );
        }
    }
}

#[tokio::test]
async fn detail_and_top_level_errors_preserve_public_identity() {
    let upstream = json!({
        "message":"workspace disabled",
        "code":"deactivated_workspace",
        "private_details":{"nested":{"encrypted_content":"fixture-private"}},
    });
    for document in [
        upstream.clone(),
        json!({"detail":upstream}),
        json!({"code":"deactivated_workspace","detail":{"message":"workspace disabled"}}),
    ] {
        let mut session = Session::new(Vec::new());
        session.provider_error = Some(upstream_http_error(
            ProviderErrorKind::PermissionDenied,
            402,
            document,
            Vec::new(),
        ));
        let fixture = Fixture::new(session).await;
        let response = fixture.post(chat_request(false)).await;
        assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
        assert_eq!(
            json_body(response).await,
            json!({"error":{"message":"workspace disabled","code":"deactivated_workspace"}}),
        );
    }
}

#[tokio::test]
async fn execution_keeps_chat_endpoint_and_response_payload() {
    let fixture = Fixture::new(Session::new(vec![completed(json!([message("ok")]))])).await;
    assert_eq!(
        fixture.post(chat_request(false)).await.status(),
        StatusCode::OK
    );
    let captured = fixture.execution.captured.lock().unwrap();
    assert_eq!(captured.as_ref().unwrap().endpoint, PATH);
    assert_eq!(captured.as_ref().unwrap().protocol, "chat_completions");
    assert_eq!(captured.as_ref().unwrap().body, chat_request(false));
}

#[tokio::test]
async fn native_genai_response_stays_native_and_commits_once() {
    let response =
        json!({"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]});
    for (action, streaming) in [
        ("generateContent", false),
        ("streamGenerateContent?alt=sse", true),
    ] {
        let wire = if streaming {
            ProtocolWireEvent::json("genai", None, response.clone()).unwrap()
        } else {
            ProtocolWireEvent::raw_json("genai", Bytes::from(response.to_string())).unwrap()
        };
        let terminal = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Completed(ResponseMeta::new(
                "resp_genai",
                "gemini",
            ))],
            wire,
        );
        let fixture = Fixture::new(Session::new(vec![terminal])).await;
        let request = json!({"contents":[{"role":"user","parts":[{"text":"hello"}]}]});
        let reply = fixture
            .router
            .clone()
            .oneshot(
                Request::post(format!("/v1beta/models/gemini@001:{action}"))
                    .header("x-goog-api-key", KEY)
                    .header("content-type", "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        assert_eq!(reply.headers()[header::CACHE_CONTROL], "private, no-store");
        if streaming {
            let body = to_bytes(reply.into_body(), usize::MAX).await.unwrap();
            let body = std::str::from_utf8(&body).unwrap();
            assert_eq!(body, format!("data: {response}\n\n"));
        } else {
            assert_eq!(json_body(reply).await, response);
        }
        let captured = fixture.captured();
        assert_eq!(captured.protocol, "genai");
        assert_eq!(captured.body, request);
        assert_eq!(captured.context["stream"], streaming);
        assert_eq!(captured.context["requested_model"], "gemini@001");
        assert!(
            !captured.context["opaque_request_headers"]
                .to_string()
                .contains("x-goog-api-key")
        );
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_count_endpoints_preserve_request_and_response_documents() {
    for (protocol, endpoint, key_header, input, output) in [
        (
            "genai",
            "/v1beta/models/gemini:countTokens",
            "x-goog-api-key",
            json!({"generateContentRequest":{"contents":[{"parts":[{"text":"count this"}]}]}}),
            json!({"totalTokens":3}),
        ),
        (
            "anthropic",
            "/v1/messages/count_tokens",
            "x-api-key",
            json!({"model":"claude","messages":[{"role":"user","content":"count this"}]}),
            json!({"input_tokens":3}),
        ),
    ] {
        let terminal = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Completed(ResponseMeta::new(
                "resp_count",
                "fixture",
            ))],
            ProtocolWireEvent::raw_json(protocol, Bytes::from(output.to_string())).unwrap(),
        );
        let fixture = Fixture::new(Session::new(vec![terminal])).await;
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post(endpoint)
                    .header(key_header, KEY)
                    .header("content-type", "application/json")
                    .body(Body::from(input.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await, output);
        let captured = fixture.captured();
        assert_eq!(captured.protocol, protocol);
        assert_eq!(captured.body, input);
        assert!(!captured.context.to_string().contains(KEY));
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_messages_formats_have_no_openai_wrapper_or_done_frame() {
    for streaming in [false, true] {
        let message = json!({"type":"message","id":"msg_fixture","content":[],"model":"claude"});
        let start = json!({"type":"message_start","message":message});
        let stop = json!({"type":"message_stop"});
        let wire = if streaming {
            ProtocolWireEvent::json("anthropic", Some("message_stop".into()), stop.clone()).unwrap()
        } else {
            ProtocolWireEvent::raw_json("anthropic", Bytes::from(message.to_string())).unwrap()
        };
        let terminal = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Completed(ResponseMeta::new(
                "msg_fixture",
                "claude",
            ))],
            wire,
        );
        let prelude = ": keep-alive\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n";
        let session = if streaming {
            Session::new(Vec::new())
                .with_batch(vec![
                    ProviderEvent::wire(
                        ProtocolWireEvent::raw_sse(
                            "anthropic",
                            Bytes::from_static(prelude.as_bytes()),
                        )
                        .unwrap(),
                    ),
                    ProviderEvent::canonical_with_wire(
                        vec![GatewayEvent::Started(ResponseMeta::new(
                            "msg_fixture",
                            "claude",
                        ))],
                        ProtocolWireEvent::json(
                            "anthropic",
                            Some("message_start".into()),
                            start.clone(),
                        )
                        .unwrap(),
                    ),
                ])
                .with_batch(vec![terminal])
        } else {
            Session::new(vec![terminal])
        };
        let fixture = Fixture::new(session).await;
        let request = json!({"model":"claude","max_tokens":100,"stream":streaming,
            "messages":[{"role":"user","content":"hello"}]});
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .header("x-api-key", KEY)
                    .header("anthropic-version", "2023-06-01")
                    .header("content-type", "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        if streaming {
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                body,
                format!(
                    "{prelude}event: message_start\ndata: {start}\n\nevent: message_stop\ndata: {stop}\n\n"
                )
            );
        } else {
            assert_eq!(json_body(response).await, message);
        }
        let captured = fixture.captured();
        assert_eq!(captured.protocol, "anthropic");
        assert_eq!(captured.body, request);
        assert!(captured.context.to_string().contains("anthropic-version"));
        assert!(!captured.context.to_string().contains(KEY));
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_messages_does_not_commit_when_the_start_boundary_contains_only_a_prelude() {
    for frame in [
        "data: [DONE]\n\n",
        ": keep-alive\n\n",
        "event: ping\ndata: {\"type\":\"ping\"}\n\n",
    ] {
        let wire =
            ProtocolWireEvent::raw_sse("anthropic", Bytes::from_static(frame.as_bytes())).unwrap();
        let fixture = Fixture::new(Session::new(vec![ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Started(ResponseMeta::new(
                "msg_fixture",
                "claude",
            ))],
            wire,
        )]))
        .await;
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .header("x-api-key", KEY)
                    .body(Body::from(
                        json!({"model":"claude","stream":true,"max_tokens":100,
                    "messages":[{"role":"user","content":"hello"}]})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{frame:?}");
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);
        assert_eq!(json_body(response).await["error"]["type"], "api_error");
    }
}

#[tokio::test]
async fn native_messages_rejects_a_plugin_replacing_the_message_start_with_a_prelude() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};
    use gateway_admin::model::plugins::PluginMiddlewareMount;

    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        for replacement in [
            b": keep-alive\n\n".as_slice(),
            b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
            b"data: [DONE]\n\n",
        ] {
            let fixture =
                PluginFixture::with_mounts(Mode::MessagesPrelude(replacement), vec![mount]).await;
            let response = fixture
                .app
                .clone()
                .oneshot(
                    Request::post("/v1/messages")
                        .header("x-api-key", "sk_middleware")
                        .header("anthropic-version", "2023-06-01")
                        .body(Body::from(
                            json!({"model":"model-a","stream":true,"max_tokens":100,
                        "messages":[{"role":"user","content":"hello"}]})
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response.status().is_server_error(),
                "status={} replacement={replacement:?}",
                response.status()
            );
            assert_eq!(fixture.effects.calls.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.effects.commits.load(Ordering::SeqCst), 0);
            assert_eq!(json_body(response).await["error"]["type"], "api_error");
            assert!(
                fixture
                    .plugin
                    .seen
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|context| context.endpoint == "/v1/messages")
            );
            fixture.manager.shutdown().await;
        }
    }
}

fn messages_stream_request(key: &str, model: &str) -> Request<Body> {
    Request::post("/v1/messages")
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":model,"stream":true,"max_tokens":100,
                "messages":[{"role":"user","content":"hello"}]})
            .to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn native_messages_attempt_preserves_preludes_added_to_the_first_observation() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};
    use futures::StreamExt;
    use gateway_admin::model::plugins::PluginMiddlewareMount;

    let fixture = PluginFixture::with_mounts(
        Mode::MessagesExpandPrelude(&[
            b"\xef\xbb\xbf: keep-alive\r\n\r\n",
            b"event: ping\r\ndata: {\"type\":\"ping\"}\r\n\r\n",
        ]),
        vec![PluginMiddlewareMount::Attempt],
    )
    .await;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .app
            .clone()
            .oneshot(messages_stream_request("sk_middleware", "model-a")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut output = response.into_body().into_data_stream();
    let start = json!({"type":"message_start","message":{"id":"msg_fixture","model":"model-a"}});
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        format!(
            "\u{feff}: keep-alive\r\n\r\nevent: ping\r\ndata: {{\"type\":\"ping\"}}\r\n\r\nevent: message_start\ndata: {start}\n\n"
        )
    );
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    );
    assert!(output.next().await.is_none());
    fixture.settled().await;
    assert_eq!(fixture.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.effects.charges.lock().unwrap()[0]
            .amount_usd
            .scaled(),
        25
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_messages_plugin_json_uses_only_sse_data_fields() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};
    use gateway_admin::model::plugins::PluginMiddlewareMount;

    let valid = b"event: message_start\rdata: {\"type\":\"message_start\",\rx-plugin: ignored\rdata: \"message\":{\"id\":\"msg_fixture\",\"model\":\"model-a\"}}\r\r";
    let bare_lf = b"event: message_start\ndata: {\"type\":\"message_start\",\n\"message\":{\"id\":\"msg_fixture\",\"model\":\"model-a\"}}\n\n";
    let bare_cr = b"data: {\"type\":\"message_start\",\r\"message\":{\"id\":\"msg_fixture\",\"model\":\"model-a\"}}\n\n";
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        for (replacement, is_valid) in [
            (valid.as_slice(), true),
            (bare_lf.as_slice(), false),
            (bare_cr.as_slice(), false),
        ] {
            let fixture =
                PluginFixture::with_mounts(Mode::MessagesPrelude(replacement), vec![mount]).await;
            let response = fixture
                .app
                .clone()
                .oneshot(messages_stream_request("sk_middleware", "model-a"))
                .await
                .unwrap();
            if is_valid {
                assert_eq!(response.status(), StatusCode::OK);
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                assert!(body.starts_with(replacement));
                assert!(
                    body.ends_with(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
                );
            } else {
                assert!(response.status().is_server_error(), "{}", response.status());
                assert_eq!(json_body(response).await["error"]["type"], "api_error");
            }
            assert_eq!(
                fixture.effects.commits.load(Ordering::SeqCst),
                usize::from(is_valid)
            );
            fixture.manager.shutdown().await;
        }
    }
}

#[tokio::test]
async fn native_messages_does_not_recognize_a_midstream_bom_as_sse_data() {
    let frame = |data| ProviderEvent::wire(ProtocolWireEvent::raw_sse("anthropic", data).unwrap());
    let prelude = Bytes::from_static(b": keep-alive\n\n");
    let opening = Bytes::from_static(b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fixture\",\"model\":\"claude\"}}\n\n");
    let masked_opening = Bytes::from([b"\xef\xbb\xbf".as_slice(), &opening].concat());
    let fixture = Fixture::new(Session::new(vec![frame(prelude), frame(masked_opening)])).await;
    let response = fixture
        .router
        .clone()
        .oneshot(messages_stream_request(KEY, "claude"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);

    let prefixed_opening = Bytes::from(
        [
            b": upstream keep-alive\n\n\xef\xbb\xbfevent: upstream_extension\n".as_slice(),
            opening.as_ref(),
        ]
        .concat(),
    );
    let stop = Bytes::from_static(b"data: {\"type\":\"message_stop\"}\r\r");
    let fixture = Fixture::new(
        Session::new(vec![frame(prefixed_opening.clone())]).with_batch(vec![frame(stop.clone())]),
    )
    .await;
    let response = fixture
        .router
        .clone()
        .oneshot(messages_stream_request(KEY, "claude"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, [prefixed_opening.as_ref(), stop.as_ref()].concat());
    assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);

    let masked_stop = Bytes::from_static(b"\xef\xbb\xbfdata: {\"type\":\"message_stop\"}\n\n");
    let mut session =
        Session::new(vec![frame(opening.clone())]).with_batch(vec![frame(masked_stop.clone())]);
    session.provider_error = Some(ProviderError::new(
        ProviderErrorKind::Timeout,
        UpstreamSendState::Sent,
    ));
    let fixture = Fixture::new(session).await;
    let response = fixture
        .router
        .clone()
        .oneshot(messages_stream_request(KEY, "claude"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let prefix = [opening.as_ref(), masked_stop.as_ref()].concat();
    let error = body.strip_prefix(prefix.as_slice()).unwrap();
    let events = gateway_protocol::openai::sse::parse_sse_events_strict(
        std::str::from_utf8(error).unwrap(),
        false,
    )
    .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.as_deref(), Some("error"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&events[0].data).unwrap(),
        json!({"type":"error","error":{"type":"api_error","message":"Upstream stream ended before completion"}})
    );
    assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_messages_redirection_is_a_gateway_error_without_location() {
    for (path, status) in [("/v1/messages", 307), ("/v1/messages/count_tokens", 308)] {
        let mut session = Session::new(Vec::new());
        session.provider_error = Some(upstream_http_error(
            ProviderErrorKind::Unavailable,
            status,
            json!({"error":{"message":"upstream redirect detail"}}),
            vec![ProviderResponseHeader::new(
                "location",
                Bytes::from_static(b"https://redirect.example.invalid/messages"),
            )],
        ));
        let fixture = Fixture::new(session).await;
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post(path)
                    .header("x-api-key", KEY)
                    .body(Body::from(
                        json!({"model":"claude","max_tokens":100,
                    "messages":[{"role":"user","content":"hello"}]})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(!response.headers().contains_key(header::LOCATION));
        let body = json_body(response).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(
            body["error"]["message"],
            "The Messages upstream returned a redirect."
        );
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn native_messages_stream_rejects_malformed_first_data_and_preserves_later_errors() {
    for valid_first_frame in [false, true] {
        let wire = if valid_first_frame {
            ProtocolWireEvent::json(
                "anthropic",
                Some("message_start".into()),
                json!({"type":"message_start","message":{"id":"msg_fixture","model":"claude"}}),
            )
            .unwrap()
        } else {
            ProtocolWireEvent::raw_sse("anthropic", Bytes::from_static(b"data: invalid JSON\n\n"))
                .unwrap()
        };
        let mut session = Session::new(vec![ProviderEvent::wire(wire)]);
        session.provider_error = Some(
            ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::Sent)
                .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                    "The compatible upstream timed out.",
                    None,
                    Some("timeout_error".into()),
                )),
        );
        let fixture = Fixture::new(session).await;
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .header("x-api-key", KEY)
                    .body(Body::from(
                        json!({"model":"claude","stream":true,"max_tokens":100,
                    "messages":[{"role":"user","content":"hello"}]})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        if valid_first_frame {
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body = std::str::from_utf8(&bytes).unwrap();
            let terminal = json!({"type":"error","error":
                {"type":"timeout_error","message":"The compatible upstream timed out."}});
            assert!(body.starts_with("event: message_start\n"));
            assert!(body.ends_with(&format!("event: error\ndata: {terminal}\n\n")));
            assert_eq!(body.matches("event: error\n").count(), 1);
            assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert_eq!(json_body(response).await["error"]["type"], "api_error");
            assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn native_messages_emits_one_error_even_if_the_source_fails_after_a_wire_error() {
    let start = json!({"type":"message_start","message":{"id":"msg_fixture","model":"claude"}});
    let wire_error =
        json!({"type":"error","error":{"type":"overloaded_error","message":"wire failure"}});
    for malformed_tail in [false, true] {
        let opening = ProviderEvent::wire(
            ProtocolWireEvent::json("anthropic", Some("message_start".into()), start.clone())
                .unwrap(),
        );
        let mut batch = vec![ProviderEvent::wire(
            ProtocolWireEvent::json("anthropic", Some("error".into()), wire_error.clone()).unwrap(),
        )];
        if malformed_tail {
            batch.push(ProviderEvent::wire(
                ProtocolWireEvent::raw_sse(
                    "anthropic",
                    Bytes::from_static(b"data: invalid JSON\n\n"),
                )
                .unwrap(),
            ));
        }
        let mut session = Session::new(vec![opening]).with_batch(batch);
        session.provider_error = Some(ProviderError::new(
            ProviderErrorKind::Timeout,
            UpstreamSendState::Sent,
        ));
        let fixture = Fixture::new(session).await;
        let response = fixture
            .router
            .clone()
            .oneshot(messages_stream_request(KEY, "claude"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let expected = if malformed_tail {
            json!({"type":"error","error":{"type":"api_error","message":"Upstream stream ended before completion"}})
        } else {
            wire_error.clone()
        };
        assert_eq!(
            body,
            format!("event: message_start\ndata: {start}\n\nevent: error\ndata: {expected}\n\n")
        );
        assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
        assert!(fixture.trace.finalized.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn native_genai_catalog_paginates_visible_models_and_checks_page_tokens() {
    use crate::openai::{
        IgnoredClientApiKeyUsage, execution_for_snapshot, snapshot_with_capabilities,
    };
    use gateway_core::{
        operation::OperationKind, routing::ModelCapabilities, runtime::RuntimeSnapshotHandle,
    };
    let snapshot = snapshot_with_capabilities(
        KEY,
        "antigravity",
        ModelCapabilities::new(
            [OperationKind::Generate, OperationKind::CountTokens].into(),
            None,
        )
        .with_generation_protocol(gateway_protocol::genai::GENAI_PROTOCOL),
    );
    let service = execution_for_snapshot(
        RuntimeSnapshotHandle::new(snapshot),
        Arc::new(IgnoredClientApiKeyUsage),
    );
    let mut execution = Execution::new(Session::new(Vec::new()));
    execution.client = service.authenticate(KEY).unwrap();
    let fixture = Fixture::with_execution(execution).await;
    let get = |path: String| {
        fixture.router.clone().oneshot(
            Request::get(path)
                .header("x-goog-api-key", KEY)
                .body(Body::empty())
                .unwrap(),
        )
    };

    let response = get("/v1beta/models?pageSize=1".into()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = json_body(response).await;
    assert_eq!(page["models"][0]["name"], "models/model-a");
    assert_eq!(
        page["models"][0]["supportedGenerationMethods"],
        json!(["generateContent", "streamGenerateContent", "countTokens"])
    );
    assert_eq!(page["models"].as_array().unwrap().len(), 1);
    let token = page["nextPageToken"].as_str().unwrap();
    let response = get(format!("/v1beta/models?pageSize=1&pageToken={token}"))
        .await
        .unwrap();
    let page = json_body(response).await;
    assert_eq!(page["models"][0]["name"], "models/model-b");
    assert!(page.get("nextPageToken").is_none());

    let response = get(format!("/v1beta/models?pageSize=2&pageToken={token}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn direct_chat_stream_receives_one_done_without_responses_conversion() {
    let chunk = json!({"id":"chatcmpl-native","object":"chat.completion.chunk","created":1,
        "model":"gemini","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"stop"}]});
    let event = ProviderEvent::canonical_with_wire(
        vec![GatewayEvent::Completed(ResponseMeta::new(
            "resp_chat_native",
            "gemini",
        ))],
        ProtocolWireEvent::json("chat_completions", None, chunk.clone()).unwrap(),
    );
    let fixture = Fixture::new(Session::new(vec![event])).await;
    let reply = fixture.post(chat_request(true)).await;
    assert_eq!(reply.status(), StatusCode::OK);
    let body = to_bytes(reply.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, format!("data: {chunk}\n\ndata: [DONE]\n\n"));
    assert_eq!(fixture.trace.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn attempt_error_hides_responses_reasoning_and_preserves_retry_timing() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};
    use gateway_admin::model::plugins::PluginMiddlewareMount;

    for (streaming, after_keepalive) in [(false, false), (true, false), (true, true)] {
        let fixture = PluginFixture::with_mounts(
            Mode::InspectChatError,
            vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        )
        .await;
        fixture
            .effects
            .private_reasoning
            .store(true, Ordering::SeqCst);
        fixture
            .effects
            .chat_attempt_error
            .store(true, Ordering::SeqCst);
        let release = Arc::new(tokio::sync::Notify::new());
        if after_keepalive {
            *fixture.effects.completion.lock().unwrap() = Some(release.clone());
        }
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::post(PATH)
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .body(Body::from(chat_request(streaming).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        if after_keepalive {
            assert_eq!(response.status(), StatusCode::OK);
            release.notify_one();
            let (body, chunks) = super::response::sse_body(response).await;
            assert!(body.starts_with(": keep-alive\n\n"));
            assert!(!body.contains("opaque-reasoning"));
            assert_eq!(
                chunks.last().unwrap()["error"]["headers"]["retry-after"],
                "27"
            );
            continue;
        }
        let status = response.status();
        assert!(!response.headers().contains_key("x-goog-api-key"));
        assert_eq!(response.headers()["x-fixture-response"], "preserved");
        assert_eq!(response.headers()[header::RETRY_AFTER], "27");
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "streaming={streaming}: {}",
            String::from_utf8_lossy(&bytes),
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            json!({"error":{"message":"fixture attempt error"}}),
        );
    }
}

#[tokio::test]
async fn request_plugin_rate_limit_promotes_body_retry_timing() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};

    for streaming in [false, true] {
        let fixture = PluginFixture::new(Mode::ShortCircuitChatRateLimit, true).await;
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::post(PATH)
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .body(Body::from(chat_request(streaming).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "29");
        assert_eq!(
            json_body(response).await,
            json!({"error":{"message":"plugin rate limit"}})
        );
    }
}

#[tokio::test]
async fn request_plugins_keep_authority_endpoint_and_one_charge_for_chat() {
    use crate::openai::middleware::fixture::{Fixture as PluginFixture, Mode};
    for streaming in [false, true] {
        let f = PluginFixture::new(Mode::InspectChat, true).await;
        f.effects.private_reasoning.store(true, Ordering::SeqCst);
        let response = f
            .app
            .clone()
            .oneshot(
                Request::post(PATH)
                    .header("authorization", "Bearer sk_middleware")
                    .header("x-goog-api-key", "fixture-google-key")
                    .header("content-type", "application/json")
                    .body(Body::from(chat_request(streaming).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("private-chat-reasoning"), "{text}");
        assert!(!text.contains("encrypted_content"), "{text}");
        if streaming {
            assert_eq!(text.matches("data: [DONE]").count(), 1, "{text}");
        }
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("chat.completion")
        );
        f.settled().await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        assert!(
            f.plugin
                .seen
                .lock()
                .unwrap()
                .iter()
                .all(|context| context.endpoint == PATH)
        );
        assert!(
            matches!(&f.effects.operations.lock().unwrap()[0],Operation::Generate(request) if request.protocol_payload().body().get("messages")==Some(&json!([{"role":"user","content":"changed input"}])))
        );
    }
}
