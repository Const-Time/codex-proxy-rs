mod lifecycle;
mod maintenance;
mod regressions;
mod websocket;
use crate::{
    admin::{
        TestOAuthPending, initialized_account_scope, initialized_provider_request,
        provider_ports_with, valid_config,
    },
    support::{MemoryAccountStore, account_policy, profile, secret},
};
use async_trait::async_trait;
use base64::Engine as _;
use chrono::Utc;
use futures::SinkExt as _;
use futures::StreamExt as _;
use gateway_admin::model::{MutationActor, MutationContext};
use gateway_admin::{
    model::turn_state::*,
    ports::{
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
        turn_state::TurnStateStore,
    },
};
use gateway_core::engine::ProviderAccountStateOwner;
use gateway_core::{
    account::ProviderAccountId,
    engine::{AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext},
    lifecycle::CancellationToken,
    operation::{GenerateRequest, Operation, ProtocolPayload, ProviderSessionState},
    policy::ClientApiKeyId,
    routing::ProviderKind,
};
use provider_openai::{config::OpenAiConfig, credential::ImportCodexOAuthCredential};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const COMPLETED_SESSION_SSE: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_initialized_session\",\"model\":\"gpt-5.4\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_initialized_session\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
);

#[derive(Default)]
struct StateStore {
    state: Mutex<(u64, Option<Vec<u8>>)>,
    fail: std::sync::atomic::AtomicBool,
    probes: Mutex<Vec<Value>>,
    block_next_load: std::sync::atomic::AtomicBool,
    load_started: tokio::sync::Notify,
    release_load: tokio::sync::Notify,
}

#[async_trait]
impl TurnStateStore for StateStore {
    async fn begin_probe(&self, probe: &TurnStateProbeStart) -> AdminStoreResult<()> {
        self.probes.lock().unwrap().push(json!({"id":probe.id,"phase":probe.phase,"model":probe.model,"account":probe.account_id}));
        Ok(())
    }
    async fn finish_probe(&self, id: &str, result: &TurnStateProbeResult) -> AdminStoreResult<()> {
        self.probes.lock().unwrap().push(json!({"id":id,"succeeded":result.succeeded,"input":result.input_tokens,"sent":result.sent_state,"returned":result.returned_state,"message":result.message}));
        Ok(())
    }
    async fn load(&self) -> AdminStoreResult<(u64, Option<Vec<u8>>)> {
        if self
            .block_next_load
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.load_started.notify_one();
            self.release_load.notified().await;
        }
        let state = self.state.lock().unwrap();
        Ok((state.0.max(1), state.1.clone()))
    }
    async fn save(
        &self,
        revision: u64,
        bytes: Vec<u8>,
        _: Option<&MutationContext>,
    ) -> AdminStoreResult<u64> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Unavailable,
                "test",
                "offline",
            ));
        }
        let mut state = self.state.lock().unwrap();
        if state.0.max(1) != revision {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::StaleRevision,
                "test",
                "CAS",
            ));
        }
        *state = (revision + 1, Some(bytes));
        Ok(revision + 1)
    }
}

fn mutation() -> MutationContext {
    MutationContext {
        actor: MutationActor::AdminApiKey,
        request_id: "state-policy-test".to_owned(),
    }
}

fn token(blocks: usize, salt: u8) -> String {
    use base64::engine::general_purpose::URL_SAFE;
    let mut bytes = vec![salt; 57 + 16 * blocks];
    bytes[0] = 0x80;
    bytes[1..9].copy_from_slice(&(Utc::now().timestamp() as u64).to_be_bytes());
    URL_SAFE.encode(bytes)
}

async fn accounts() -> Arc<MemoryAccountStore> {
    let accounts = Arc::new(MemoryAccountStore::default());
    for id in ["acct_state", "acct_other"] {
        accounts
            .seed_oauth_credential(ImportCodexOAuthCredential {
                account_id: id.to_owned(),
                name: id.to_owned(),
                secret: secret(id),
                verified_account: profile(id),
                next_refresh_at: Some(Utc::now() + chrono::Duration::minutes(30)),
                enabled: true,
            })
            .await;
    }
    accounts
}

async fn bundle(
    config: &OpenAiConfig,
    accounts: &Arc<MemoryAccountStore>,
    state: &Arc<StateStore>,
) -> provider_openai::ProviderBundle {
    provider_openai::initialize_with_turn_state(
        config.clone(),
        provider_ports_with(Arc::clone(accounts), Arc::new(TestOAuthPending::default())),
        state.clone(),
    )
    .await
    .unwrap()
}

async fn setup(service: &dyn gateway_admin::ports::turn_state::TurnStateService) -> TurnStateView {
    let view = service.view().await.unwrap();
    service
        .configure(
            TurnStateSettingsInput {
                revision: view.revision,
                policy: TurnStatePolicy {
                    enabled: true,
                    ..Default::default()
                },
                pools: vec![TurnStatePoolInput {
                    id: "test-pool".to_owned(),
                    name: "test".to_owned(),
                    enabled: true,
                    mode: "gateway".to_owned(),
                    endpoint: Some("http://127.0.0.1:9".to_owned()),
                    bearer: None,
                    json_pointer: String::new(),
                }],
                targets: vec![TurnStateTargetInput {
                    account_id: "acct_state".to_owned(),
                    model: "gpt-5.4".to_owned(),
                    pool_id: "test-pool".to_owned(),
                    enabled: true,
                }],
            },
            &mutation(),
        )
        .await
        .unwrap()
}

async fn set_account(
    service: &dyn gateway_admin::ports::turn_state::TurnStateService,
    fingerprint_convergence: bool,
    takeover: bool,
) -> TurnStateView {
    service
        .configure_account(
            "acct_state",
            TurnStateAccountUpdate {
                maintenance: None,
                revision: service.view().await.unwrap().revision,
                fingerprint_convergence,
                takeover,
            },
            &mutation(),
        )
        .await
        .unwrap()
}

fn operation(state: Option<&str>, websocket: bool) -> Operation {
    let mut context = Map::from_iter([
        ("use_websocket".to_owned(), json!(websocket)),
        (
            "session_id".to_owned(),
            json!("0191ff0c-0022-7000-8000-112233445566"),
        ),
        (
            "thread_id".to_owned(),
            json!("0191ff0c-0022-7000-8000-112233445566"),
        ),
    ]);
    if let Some(state) = state {
        context.insert("turn_state".to_owned(), json!(state));
    }
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({"model":"gpt-5.4","input":"unchanged user input"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap()
        .with_context(context),
    ))
}

async fn execute(
    bundle: &provider_openai::ProviderBundle,
    operation: Operation,
    account: &str,
) -> Value {
    execute_with_session(bundle, operation, account).await.0
}

async fn execute_with_session(
    bundle: &provider_openai::ProviderBundle,
    operation: Operation,
    account: &str,
) -> (Value, Option<ProviderSessionState>) {
    let owner = ProviderAccountStateOwner::new(
        ProviderKind::new("openai").unwrap(),
        ProviderAccountId::new(account).unwrap(),
    );
    let context = AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new(format!("req_{}", Uuid::new_v4())).unwrap(),
            ClientApiKeyId::new("key_openai_initialized").unwrap(),
        ),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(30),
        account_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, Some(owner))
            .with_account_scope(initialized_account_scope(account)),
        None,
        CancellationToken::new(),
    );
    let mut stream = bundle
        .core_provider()
        .execute(initialized_provider_request(operation, account), context)
        .await
        .unwrap();
    let mut metadata = Value::Null;
    let mut session = None;
    while let Some(event) = stream.next().await {
        let event = event.expect("business request");
        if let Some(update) = event.session_update() {
            session = Some(update.clone());
        }
        if let Some(value) = event
            .response_observation()
            .and_then(|o| o.provider_metadata())
        {
            metadata = serde_json::from_str(value.as_json()).unwrap();
        }
    }
    (metadata, session)
}

#[tokio::test]
async fn account_switch_is_default_off_persisted_cas_and_isolated() {
    let config = valid_config();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let first = bundle(&config.config, &accounts, &store).await;
    let service = first.turn_state_service().unwrap();
    assert!(service.view().await.unwrap().accounts.is_empty());
    assert!(
        service
            .configure_account(
                "missing",
                TurnStateAccountUpdate {
                    maintenance: None,
                    revision: 1,
                    fingerprint_convergence: false,
                    takeover: true,
                },
                &mutation()
            )
            .await
            .is_err()
    );
    let view = set_account(service.as_ref(), true, false).await;
    assert_eq!(view.accounts.len(), 1);
    assert!(view.accounts[0].fingerprint_convergence);
    assert!(!view.accounts[0].takeover);
    let second = bundle(&config.config, &accounts, &store).await;
    assert_eq!(
        second
            .turn_state_service()
            .unwrap()
            .view()
            .await
            .unwrap()
            .accounts,
        view.accounts
    );
    assert!(
        service
            .configure_account(
                "acct_state",
                TurnStateAccountUpdate {
                    maintenance: None,
                    revision: 1,
                    fingerprint_convergence: false,
                    takeover: true,
                },
                &mutation()
            )
            .await
            .is_err()
    );
    store.fail.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        service
            .configure_account(
                "acct_state",
                TurnStateAccountUpdate {
                    maintenance: None,
                    revision: view.revision,
                    fingerprint_convergence: false,
                    takeover: true,
                },
                &mutation()
            )
            .await
            .is_err()
    );
    assert_eq!(service.view().await.unwrap().accounts, view.accounts);
    store
        .fail
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let updated = set_account(second.turn_state_service().unwrap().as_ref(), false, true).await;
    assert_eq!(service.view().await.unwrap().accounts, updated.accounts);
}

#[tokio::test]
async fn http_takeover_marks_actual_source_and_does_not_invalidate_on_312() {
    let server = MockServer::start().await;
    let candidate = token(10, 42);
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .insert_header("x-codex-turn-state", candidate.as_str())
                .set_body_string(COMPLETED_SESSION_SSE),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    execute(
        &bundle,
        operation(Some("client-before-opt-in"), false),
        "acct_state",
    )
    .await;
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 0);
    set_account(service.as_ref(), false, true).await;
    let observed = execute(&bundle, operation(None, false), "acct_state").await;
    let view = service.view().await.unwrap();
    assert_eq!(view.targets[0].candidate_count, 1, "{observed:?} {view:?}");
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let metadata = execute(
        &bundle,
        operation(Some("client-is-replaced"), false),
        "acct_state",
    )
    .await;
    assert_eq!(metadata["turnState"], candidate);
    assert_eq!(metadata["turnStateSource"], "managed");
    assert_eq!(metadata["turnStateSentSource"], "managed");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].headers["x-codex-turn-state"], candidate);
    let body = zstd::stream::decode_all(requests[0].body.as_slice()).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["input"],
        "unchanged user input"
    );
    // 同一 blob 回到别的账号时剥离；绝不拿 A 的池替 B。
    execute(&bundle, operation(Some(&candidate), false), "acct_other").await;
    assert!(
        server.received_requests().await.unwrap()[1]
            .headers
            .get("x-codex-turn-state")
            .is_none()
    );
    server.reset().await;
    let observed_312 = token(11, 17);
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", observed_312.as_str())
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let metadata = execute(&bundle, operation(None, false), "acct_state").await;
    assert_eq!(metadata["turnStateSource"], "response");
    assert_eq!(metadata["turnState"], observed_312);
    assert_eq!(metadata["turnStateSent"], candidate);
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 1);
    // 保存策略不会让密钥/token 明文落进文档。
    set_account(service.as_ref(), false, true).await;
    let sealed = store.load().await.unwrap().1.unwrap();
    assert!(
        !sealed
            .windows(candidate.len())
            .any(|bytes| bytes == candidate.as_bytes())
    );
    set_account(service.as_ref(), false, false).await;
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 0);
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    execute(
        &bundle,
        operation(Some("client-after-disable"), false),
        "acct_state",
    )
    .await;
    assert_eq!(
        server.received_requests().await.unwrap()[0].headers["x-codex-turn-state"],
        "client-after-disable"
    );
}

#[tokio::test]
async fn websocket_takeover_with_convergence_sends_candidate_in_frame_not_handshake() {
    let http = MockServer::start().await;
    let candidate = token(10, 3);
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", candidate.as_str())
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&http)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = http.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let first = bundle(&config.config, &accounts, &store).await;
    let service = first.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    set_account(service.as_ref(), true, true).await;
    execute(&first, operation(None, false), "acct_state").await;
    set_account(service.as_ref(), true, true).await; // persist collected candidate
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.config.api.base_url = format!("http://{}", listener.local_addr().unwrap());
    let second = bundle(&config.config, &accounts, &store).await;
    assert_eq!(
        second
            .turn_state_service()
            .unwrap()
            .view()
            .await
            .unwrap()
            .targets[0]
            .candidate_count,
        1
    );
    let expected = candidate.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws =
            crate::transport::accept_codex_test_websocket_with(socket, |request, _response| {
                assert!(request.headers().get("x-codex-turn-state").is_none());
                assert_eq!(
                    request.headers()["x-client-request-id"],
                    request.headers()["thread-id"]
                );
            })
            .await;
        let frame = ws.next().await.unwrap().unwrap();
        let body: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(body["client_metadata"]["x-codex-turn-state"], expected);
        ws.send(Message::Text(json!({"type":"response.completed","response":{"id":"resp_ws","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}).to_string().into())).await.unwrap();
    });
    let metadata = execute(&second, operation(Some("client-ws"), true), "acct_state").await;
    assert_eq!(metadata["turnStateSentSource"], "managed");
    assert_eq!(metadata["turnStateSource"], "managed");
    assert_eq!(metadata["turnStateSent"], candidate);
    server.await.unwrap();
}

#[tokio::test]
async fn natural_candidates_are_bounded_and_policy_change_revokes_them() {
    let server = MockServer::start().await;
    let counter = Arc::new(std::sync::atomic::AtomicU8::new(1));
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let salt = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(200)
                .insert_header(
                    "x-codex-turn-state",
                    token(if salt.is_multiple_of(2) { 12 } else { 10 }, salt),
                )
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream")
                .set_delay(Duration::from_millis(300))
        })
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    set_account(service.as_ref(), false, true).await;
    futures::future::join_all(
        (0..5).map(|_| execute(&bundle, operation(None, false), "acct_state")),
    )
    .await;
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 3);
    let changed = set_account(service.as_ref(), true, true).await;
    assert_eq!(changed.targets[0].candidate_count, 0);
    assert!(changed.accounts[0].takeover);
}

#[tokio::test]
async fn in_flight_response_cannot_repopulate_a_pool_after_fingerprint_change() {
    let server = MockServer::start().await;
    let sent = Arc::new(tokio::sync::Notify::new());
    let signal = sent.clone();
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 12))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream")
                .set_delay(Duration::from_millis(300))
        })
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    set_account(service.as_ref(), false, true).await;
    let (metadata, ()) = tokio::join!(
        execute(&bundle, operation(None, false), "acct_state"),
        async {
            tokio::time::timeout(Duration::from_secs(5), sent.notified())
                .await
                .unwrap();
            set_account(service.as_ref(), true, true).await;
        }
    );
    assert_eq!(metadata["turnStateSource"], "response");
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 0);
}
