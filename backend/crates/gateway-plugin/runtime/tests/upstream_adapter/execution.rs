use super::*;
use futures::{StreamExt, future::BoxFuture};
use gateway_admin::model::plugins::{
    PluginAccess, PluginExecutionScope, PluginUpstreamAdapterBinding,
};
use gateway_core::{
    account::{
        AccountSelectionPolicy, CredentialRevision, OutboundProxy, ProviderAccountId,
        RotationStrategy,
    },
    engine::{
        AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
        middleware::MiddlewareHeader,
        provider::ProviderCallMetadata,
        upstream_adapter::{
            UpstreamAccountConnection, UpstreamAdapter, UpstreamAdapterInvocation,
            UpstreamAdapterPlan,
        },
    },
    error::{
        ContinuationFailure, ContinuationRecoveryDisposition, ProviderError, ProviderErrorKind,
    },
    event::GatewayEvent,
    identity::ProviderKind,
    lifecycle::CancellationToken,
    metering::{CalculatedCost, Usage},
    operation::{GenerateRequest, Operation, ProtocolPayload, ProviderSessionState},
    policy::ClientApiKeyId,
    routing::{PublicModelId, UpstreamModelId},
    upstream::{UpstreamSendState, UpstreamTransport},
};
use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) struct Account {
    id: ProviderAccountId,
    revision: u64,
    failures: AtomicUsize,
    proxy: Option<OutboundProxy>,
}
impl Account {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            id: ProviderAccountId::new("acct_fixture").unwrap(),
            revision: 3,
            failures: AtomicUsize::new(0),
            proxy: None,
        })
    }
}
impl UpstreamAccountConnection for Account {
    fn account_id(&self) -> &ProviderAccountId {
        &self.id
    }
    fn credential_revision(&self) -> CredentialRevision {
        CredentialRevision::new(self.revision).unwrap()
    }
    fn authentication_kind(&self) -> &str {
        "oauth"
    }
    fn outbound_proxy(&self) -> Option<&OutboundProxy> {
        self.proxy.as_ref()
    }
    fn authorization(&self) -> Result<Vec<MiddlewareHeader>, ProviderError> {
        Ok(vec![MiddlewareHeader::new(
            "authorization",
            bytes::Bytes::from_static(b"Bearer fixture-provider-secret"),
        )])
    }
    fn credential_updates(
        &self,
    ) -> Option<&dyn gateway_core::engine::upstream_adapter::UpstreamCredentialUpdate> {
        None
    }
    fn calculate_cost(&self, tier: Option<&str>, usage: &Usage) -> Option<CalculatedCost> {
        assert_eq!(tier, Some("priority"));
        assert_eq!(usage.input_tokens, Some(10));
        Some(CalculatedCost::from_usd_ticks(500).unwrap())
    }
    fn record_failure(&self, error: ProviderError) -> BoxFuture<'_, ProviderError> {
        self.failures.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { error })
    }
}
struct Fixture {
    _cache: tempfile::TempDir,
    manager: PluginManager,
    adapter: Arc<dyn UpstreamAdapter>,
    access: PluginAccess,
}
impl Fixture {
    async fn new(url: &str, transport: &str) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = PluginManager::new(store, runtime);
        let artifact = manager
            .upload(upstream_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut input = update(&artifact.sha256, 0, true);
        input.configuration = json!({"adapter_url":url,"adapter_transport":transport});
        input.upstream_adapter = Some(PluginUpstreamAdapterBinding {
            scope: PluginExecutionScope::All {},
        });
        let installed = manager.update(None, input, &actor()).await.unwrap();
        let access = manager.access(&installed.id, installed.revision).unwrap();
        let selection = invocation("synthetic", Account::new(), None, CancellationToken::new());
        let adapter = manager
            .middleware_registry()
            .unwrap()
            .select(
                &selection.context,
                selection.metadata.provider(),
                selection.metadata.upstream_model().unwrap(),
            )
            .unwrap()
            .unwrap();
        Self {
            _cache: cache,
            manager,
            adapter,
            access,
        }
    }
    async fn stop(self) {
        self.manager.shutdown().await;
    }
    fn stream(
        &self,
        behavior: &str,
        account: Arc<Account>,
        state: Option<ProviderSessionState>,
        cancellation: CancellationToken,
    ) -> gateway_core::engine::provider::EventStream {
        invocation(behavior, account, state, cancellation).into_stream(self.adapter.clone())
    }
}
pub(super) fn invocation(
    behavior: &str,
    account: Arc<Account>,
    state: Option<ProviderSessionState>,
    cancellation: CancellationToken,
) -> UpstreamAdapterInvocation {
    let mut operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({"behavior":behavior,"input":"private fixture prompt"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap(),
    ));
    if let Some(state) = state {
        operation.set_provider_session_state(state);
    }
    let context = AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_fixture").unwrap(),
            ClientApiKeyId::new("key_fixture").unwrap(),
        )
        .with_request_metadata(
            Some(PublicModelId::new("public-model").unwrap()),
            "http_json",
        ),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(20),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(2).unwrap(),
            Duration::ZERO,
        ),
        AccountAttemptContext::new(BTreeSet::new(), None, None),
        None,
        cancellation,
    );
    let metadata = ProviderCallMetadata::new(
        ProviderKind::new("openai").unwrap(),
        UpstreamModelId::new("native-model").unwrap(),
        account.id.clone(),
        UpstreamTransport::new("http_sse").unwrap(),
    );
    UpstreamAdapterInvocation {
        operation,
        context,
        metadata,
        account,
    }
}
pub(crate) struct Server {
    pub(super) url: String,
    pub(super) requests: Arc<Mutex<Vec<String>>>,
    pub(super) entered: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(super) async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let saved = requests.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let resume = release.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .map(|n| n.parse::<usize>().unwrap())
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let credential_barrier = bytes.ends_with(b"credential-barrier");
                saved
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(bytes).unwrap());
                signal.notify_one();
                if credential_barrier {
                    resume.notified().await;
                }
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\nfixture-response").await.unwrap();
            }
        });
        Self {
            url,
            requests,
            entered,
            release,
            task,
        }
    }
}
#[tokio::test]
async fn sdk_adapter_executes_cold_http_and_streaming_reads_with_host_credentials_and_cost() {
    for mode in ["http", "http-stream"] {
        let server = Server::new().await;
        let f = Fixture::new(&server.url, "http_sse").await;
        let account = Account::new();
        let mut stream = f.stream(mode, account.clone(), None, CancellationToken::new());
        assert!(server.requests.lock().unwrap().is_empty());
        let mut events = vec![];
        while let Some(event) = stream.next().await {
            events.push(event.unwrap());
        }
        assert_eq!(events.len(), 2);
        assert!(
            events[1]
                .canonical_facts()
                .iter()
                .any(|e| matches!(e, GatewayEvent::CalculatedCost(_)))
        );
        assert_eq!(
            events[1].response_observation().unwrap().service_tier(),
            Some("priority")
        );
        assert!(matches!(
            events[1].canonical_facts().last(),
            Some(GatewayEvent::Completed(_))
        ));
        let requests = server.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("authorization: Bearer fixture-provider-secret"));
        assert!(requests[0].contains("x-fixture: adapter"));
        assert_eq!(account.failures.load(Ordering::SeqCst), 0);
        f.stop().await;
    }
}
#[tokio::test]
async fn sdk_adapter_tail_error_keeps_metering_but_never_delivers_completed_wire() {
    for mode in ["tail", "tail-fault"] {
        let server = Server::new().await;
        let f = Fixture::new(&server.url, "http_sse").await;
        let account = Account::new();
        let mut stream = f.stream(mode, account.clone(), None, CancellationToken::new());
        assert!(stream.next().await.unwrap().is_ok());
        let mut error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.kind(),
            if mode == "tail" {
                ProviderErrorKind::Protocol
            } else {
                ProviderErrorKind::Unavailable
            }
        );
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        let retained = error.take_atomic_client_events();
        assert_eq!(retained.len(), 2);
        assert!(retained.iter().all(|e| e.wire_event().is_none()));
        assert!(
            retained
                .iter()
                .flat_map(|e| e.canonical_facts())
                .all(|e| matches!(e, GatewayEvent::Usage(_) | GatewayEvent::CalculatedCost(_)))
        );
        assert_eq!(account.failures.load(Ordering::SeqCst), 1);
        if mode == "tail" {
            assert!(f.access.is_revoked());
        }
        assert!(!format!("{error:?}").contains("private fixture tail fault"));
        f.stop().await;
    }
}
#[tokio::test]
async fn sdk_adapter_validates_sequence_and_preserves_host_send_state_on_failure() {
    for (mode, kind, sent) in [
        (
            "invalid",
            ProviderErrorKind::Protocol,
            UpstreamSendState::NotSent,
        ),
        (
            "failure",
            ProviderErrorKind::RateLimited,
            UpstreamSendState::Sent,
        ),
    ] {
        let server = Server::new().await;
        let f = Fixture::new(&server.url, "http_sse").await;
        let account = Account::new();
        let mut stream = f.stream(mode, account.clone(), None, CancellationToken::new());
        let error = loop {
            match stream.next().await.unwrap() {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert_eq!(error.kind(), kind);
        assert_eq!(error.send_state(), sent);
        assert_eq!(account.failures.load(Ordering::SeqCst), 1);
        if mode == "failure" {
            assert_eq!(error.retry_after(), Some(Duration::from_millis(1200)));
            assert_eq!(f.manager.list().await.unwrap()[0].status, "ready");
        }
        f.stop().await;
    }
}
#[tokio::test]
async fn sdk_adapter_continuation_is_owned_by_instance_key_account_and_credential_revision() {
    let server = Server::new().await;
    let f = Fixture::new(&server.url, "http_sse").await;
    let account = Account::new();
    let mut stream = f.stream(
        "continuation",
        account.clone(),
        None,
        CancellationToken::new(),
    );
    let mut state = None;
    while let Some(event) = stream.next().await {
        state = event.unwrap().take_session_update().or(state);
    }
    let state = state.unwrap();
    assert!(!state.extension_owner().unwrap().connection_local);
    let mut stream = f.stream(
        "continuation",
        account.clone(),
        Some(state.clone()),
        CancellationToken::new(),
    );
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    for mismatch in [
        "key",
        "account",
        "credential",
        "generation",
        "incarnation",
        "adapter",
    ] {
        let mut modified = state.clone();
        let mut call = invocation(
            "continuation",
            account.clone(),
            None,
            CancellationToken::new(),
        );
        if ["generation", "incarnation", "adapter"].contains(&mismatch) {
            let mut owner = state.extension_owner().unwrap().clone();
            match mismatch {
                "generation" => owner.generation += 1,
                "incarnation" => owner.incarnation = "other".into(),
                _ => owner.adapter_id = "other".into(),
            };
            modified = modified.with_extension_owner(owner);
        } else {
            let mut payload = state.payload().clone();
            let field = match mismatch {
                "key" => "client_key_id",
                "account" => "account_id",
                _ => "credential_revision",
            };
            payload[field] = if mismatch == "credential" {
                json!(4)
            } else {
                json!("other")
            };
            modified = ProviderSessionState::new("openai", payload)
                .unwrap()
                .with_extension_owner(state.extension_owner().unwrap().clone());
        }
        call.operation.set_provider_session_state(modified);
        let mut stream = call.into_stream(f.adapter.clone());
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.kind(),
            ProviderErrorKind::ContinuationRecoveryRequired
        );
        assert_eq!(
            error.continuation_failure(),
            Some(ContinuationFailure::HistoryUnavailable)
        );
        assert_eq!(
            error.continuation_recovery_disposition(),
            Some(ContinuationRecoveryDisposition::ClientReplayRequired)
        );
        assert_eq!(error.send_state(), UpstreamSendState::NotSent);
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
    f.stop().await;
}
#[tokio::test]
async fn sdk_adapter_revocation_cancels_a_pending_call_and_rejects_retained_adapter_handles() {
    let server = Server::new().await;
    let f = Fixture::new(&server.url, "http_sse").await;
    let account = Account::new();
    let adapter = f.adapter.clone();
    let mut stream = f.stream("wait", account.clone(), None, CancellationToken::new());
    let waiting = tokio::spawn(async move { stream.next().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), server.entered.notified())
        .await
        .unwrap();
    f.access.revoke();
    let error = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert_ne!(error.send_state(), UpstreamSendState::NotSent);
    let mut stream =
        invocation("http", account, None, CancellationToken::new()).into_stream(adapter);
    assert!(stream.next().await.unwrap().is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    f.stop().await;
}

#[tokio::test]
async fn sdk_adapter_websocket_continuation_reuses_one_connection_and_revocation_closes_idle_socket()
 {
    use futures::SinkExt;
    for mode in ["websocket", "websocket-read-cancel"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (finished, closed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket =
                tokio_tungstenite::accept_hdr_async(socket, check_websocket_authorization)
                    .await
                    .unwrap();
            for _ in 0..2 {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().into_text().unwrap(),
                    "fixture-ping"
                );
                if mode == "websocket-read-cancel" {
                    assert_eq!(
                        socket.next().await.unwrap().unwrap().into_text().unwrap(),
                        "fixture-resume"
                    );
                }
                socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        "fixture-pong".into(),
                    ))
                    .await
                    .unwrap();
            }
            assert!(matches!(
                socket.next().await,
                None | Some(Err(_)) | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))
            ));
            let _ = finished.send(());
        });
        let f = Fixture::new(&url, "websocket").await;
        let account = Account::new();
        let mut state = None;
        for _ in 0..2 {
            let mut stream = f.stream(
                mode,
                account.clone(),
                state.take(),
                CancellationToken::new(),
            );
            while let Some(event) = stream.next().await {
                state = event.unwrap().take_session_update().or(state);
            }
            assert!(
                state
                    .as_ref()
                    .unwrap()
                    .extension_owner()
                    .unwrap()
                    .connection_local
            );
            assert_eq!(
                Arc::strong_count(&account),
                1,
                "idle connections must not hold the prior account lease"
            );
        }
        f.access.revoke();
        tokio::time::timeout(Duration::from_secs(3), closed)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        f.stop().await;
    }
}

// The upstream handshake API fixes this error type; this fixture only accepts valid headers.
#[expect(
    clippy::result_large_err,
    reason = "tungstenite handshake callback signature"
)]
fn check_websocket_authorization(
    request: &tokio_tungstenite::tungstenite::handshake::server::Request,
    response: tokio_tungstenite::tungstenite::handshake::server::Response,
) -> Result<
    tokio_tungstenite::tungstenite::handshake::server::Response,
    tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
> {
    assert_eq!(
        request.headers()["authorization"],
        "Bearer fixture-provider-secret"
    );
    Ok(response)
}

#[tokio::test]
async fn upstream_generic_http_inherits_selected_proxy_without_account_authentication() {
    for (mode, omit_auth) in [
        ("callback-external-unary", true),
        ("callback-external-stream", false),
    ] {
        let proxy = Server::new().await;
        let f = Fixture::new("http://fixture.invalid/", "http_sse").await;
        let account = Arc::new(Account {
            id: ProviderAccountId::new("acct_fixture").unwrap(),
            revision: 3,
            failures: AtomicUsize::new(0),
            proxy: Some(OutboundProxy::parse(&proxy.url).unwrap()),
        });
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/external", target.local_addr().unwrap());
        let mut input = invocation(mode, account.clone(), None, CancellationToken::new());
        input.operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                "openai",
                json!({"behavior":mode,"input":"fixture","external_url":url,"omit_auth":omit_auth})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap(),
        ));
        let mut stream = input.into_stream(f.adapter.clone());
        while let Some(event) = stream.next().await {
            event.unwrap();
        }
        {
            let requests = proxy.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with(&format!("POST {url} HTTP/1.1")));
            if omit_auth {
                assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
            } else {
                assert!(requests[0].contains("authorization: Bearer plugin-explicit"));
            }
            assert!(!requests[0].contains("fixture-provider-secret"));
        }
        assert_eq!(account.failures.load(Ordering::SeqCst), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), target.accept())
                .await
                .is_err(),
            "must not connect directly"
        );
        f.stop().await;
    }
}
