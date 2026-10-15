//! 单行 `model_requests`、账号重试与下游提交屏障测试。

use std::collections::{BTreeSet, VecDeque};
use std::num::NonZeroU32;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::channel::oneshot;
use futures::executor::block_on;

use gateway_core::account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy};
use gateway_core::engine::continuation::{
    ContinuationBinding, NativeContinuationPin, PreviousResponseId,
};
use gateway_core::engine::execution::gateway_error_from_engine;
use gateway_core::engine::provider::{
    Provider, ProviderCallMetadata, ProviderCatalogGeneration, ProviderModelCapabilities,
    ProviderRegistry, ProviderRequest, ProviderStream,
};
use gateway_core::engine::{
    AttemptContext, AttemptCoordinator, AttemptRecord, AttemptTransport, CommitRequirement,
    ContinuationAttempt, EngineError, ExecutionOutcome, ExecutionStore, GatewayEngine,
    IntermediateFailure, ModelRequestFinalization, ModelRequestId, NewModelRequest,
    ProviderAttemptOutcome, RecoveryReport,
};
use gateway_core::error::{
    ClientVisibleUpstreamError, ClientVisibleUpstreamResponse, ContinuationFailure,
    ContinuationRecoveryDisposition, GatewayErrorKind, OpaqueUpstreamValue, ProviderError,
    ProviderErrorKind, StoreError, StoreErrorKind,
};
use gateway_core::event::{
    ContentItem, ContentKind, GatewayEvent, ProtocolWireEvent, ProviderEvent,
    ProviderResponseObservation, ProviderResponseTimings, ResponseMeta, ToolCallDelta,
    UpstreamHttpVersion, WebSocketPoolKind,
};
use gateway_core::lifecycle::CancellationToken;
use gateway_core::metering::{CalculatedCost, CostSource, ProviderReportedCost, Usage};
use gateway_core::operation::{GenerateRequest, Operation, ProtocolPayload, ProviderSessionState};
use gateway_core::policy::ClientApiKeyId;
use gateway_core::routing::{
    AccountRoutingSnapshot, ClientRoutingScope, ConfigRevision, FrozenAccountScope,
    ModelCapabilities, ProviderKind, ProviderModel, PublicModelId, RoutingContext, RoutingPlan,
    RuntimeAccount, RuntimeAccountDirectory, RuntimeSnapshot, UpstreamModelId,
};
use gateway_core::upstream::{UpstreamSendState, UpstreamTransport};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FinalState {
    error_kind: Option<GatewayErrorKind>,
    diagnostic_trace_json: Option<String>,
    outcome: ExecutionOutcome,
    send_state: UpstreamSendState,
    attempt_count: u32,
    committed: bool,
    client_status_code: Option<u16>,
    total_tokens: Option<u64>,
    image_input_tokens: Option<u64>,
    image_output_tokens: Option<u64>,
    image_generation_succeeded: Option<bool>,
    provider_error_code: Option<String>,
    retry_after_ms: Option<u64>,
    latency_ms: Option<u64>,
    client_response_id: Option<String>,
    upstream_response_id: Option<String>,
    upstream_transport: Option<String>,
    http_version: Option<String>,
    websocket_pool: Option<String>,
    service_tier: Option<String>,
    upstream_response_model: Option<String>,
    upstream_request_id: Option<String>,
    upstream_status_code: Option<u16>,
    transport_decision_wait_ms: Option<u64>,
    connect_ms: Option<u64>,
    headers_ms: Option<u64>,
    first_event_ms: Option<u64>,
    first_reasoning_ms: Option<u64>,
    first_text_ms: Option<u64>,
    first_token_ms: Option<u64>,
    provider_processing_ms: Option<u64>,
    provider_metadata_json: Option<String>,
    cost_source: CostSource,
    cost_ticks: Option<u128>,
}

#[derive(Default)]
struct StoreState {
    created: usize,
    attempts: Vec<AttemptRecord>,
    send_states: Vec<UpstreamSendState>,
    commits: usize,
    committed_statuses: Vec<Option<u16>>,
    recorded_statuses: Vec<u16>,
    intermediate_failures: usize,
    intermediate_status_codes: Vec<Option<u16>>,
    intermediate_request_ids: Vec<Option<String>>,
    finalizations: Vec<FinalState>,
}

#[derive(Default)]
struct FakeStore {
    recovery_leases: Arc<AtomicUsize>,
    state: Mutex<StoreState>,
    failures: BTreeSet<StoreWriteFailure>,
    create_gate: Mutex<Option<oneshot::Receiver<()>>>,
    finalize_gate: Mutex<Option<oneshot::Receiver<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StoreWriteFailure {
    Create,
    Commit,
    Finalize,
}

impl FakeStore {
    fn failing(failure: StoreWriteFailure) -> Self {
        Self {
            failures: BTreeSet::from([failure]),
            ..Self::default()
        }
    }

    fn fails(&self, failure: StoreWriteFailure) -> Result<(), StoreError> {
        if self.failures.contains(&failure) {
            Err(StoreError::new(StoreErrorKind::Unavailable))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl ExecutionStore for FakeStore {
    fn maintain_request(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: Option<std::time::SystemTime>,
        _: std::time::Duration,
    ) -> Box<dyn gateway_core::engine::ExecutionRecoveryLeaseGuard> {
        self.recovery_leases.fetch_add(1, Ordering::SeqCst);
        Box::new(TestExecutionRecoveryLease(self.recovery_leases.clone()))
    }

    async fn create_model_request(&self, _request: NewModelRequest) -> Result<(), StoreError> {
        self.fails(StoreWriteFailure::Create)?;
        self.state.lock().expect("store lock").created += 1;
        let gate = self.create_gate.lock().expect("create gate lock").take();
        if let Some(gate) = gate {
            gate.await.expect("create released");
        }
        Ok(())
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), StoreError> {
        self.state
            .lock()
            .expect("store lock")
            .attempts
            .push(attempt);
        Ok(())
    }

    async fn mark_send_state(
        &self,
        _request_id: &ModelRequestId,
        state: UpstreamSendState,
    ) -> Result<(), StoreError> {
        self.state
            .lock()
            .expect("store lock")
            .send_states
            .push(state);
        Ok(())
    }

    async fn mark_downstream_committed(
        &self,
        _request_id: &ModelRequestId,
        _committed_at: SystemTime,
        client_status_code: Option<u16>,
    ) -> Result<(), StoreError> {
        self.fails(StoreWriteFailure::Commit)?;
        let mut state = self.state.lock().expect("store lock");
        state.commits += 1;
        state.committed_statuses.push(client_status_code);
        Ok(())
    }

    async fn record_client_status(
        &self,
        _request_id: &ModelRequestId,
        client_status_code: u16,
    ) -> Result<(), StoreError> {
        self.state
            .lock()
            .expect("store lock")
            .recorded_statuses
            .push(client_status_code);
        Ok(())
    }

    async fn record_intermediate_failure(
        &self,
        failure: IntermediateFailure,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().expect("store lock");
        state.intermediate_failures += 1;
        state
            .intermediate_status_codes
            .push(failure.upstream_status_code);
        state
            .intermediate_request_ids
            .push(failure.upstream_request_id);
        Ok(())
    }

    async fn finalize_model_request(
        &self,
        finalization: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.fails(StoreWriteFailure::Finalize)?;
        self.state
            .lock()
            .expect("store lock")
            .finalizations
            .push(FinalState {
                error_kind: finalization.error.as_ref().map(|error| error.kind()),
                diagnostic_trace_json: finalization.diagnostic_trace_json,
                outcome: finalization.outcome,
                send_state: finalization.send_state,
                attempt_count: finalization.attempt_count,
                committed: finalization.downstream_committed_at.is_some(),
                client_status_code: finalization.client_status_code,
                total_tokens: finalization.usage.total_tokens,
                image_input_tokens: finalization.usage.image_input_tokens,
                image_output_tokens: finalization.usage.image_output_tokens,
                image_generation_succeeded: finalization.image_generation_succeeded,
                provider_error_code: finalization.provider_error_code,
                retry_after_ms: finalization.retry_after_ms,
                latency_ms: finalization.timings.latency_ms,
                client_response_id: finalization.client_response_id,
                upstream_response_id: finalization.upstream_response_id,
                upstream_transport: finalization.upstream_transport,
                http_version: finalization.http_version,
                websocket_pool: finalization.websocket_pool,
                service_tier: finalization.service_tier,
                upstream_response_model: finalization.upstream_response_model,
                upstream_request_id: finalization.upstream_request_id,
                upstream_status_code: finalization.upstream_status_code,
                transport_decision_wait_ms: finalization.timings.transport_decision_wait_ms,
                connect_ms: finalization.timings.connect_ms,
                headers_ms: finalization.timings.headers_ms,
                first_event_ms: finalization.timings.first_event_ms,
                first_reasoning_ms: finalization.timings.first_reasoning_ms,
                first_text_ms: finalization.timings.first_text_ms,
                first_token_ms: finalization.timings.first_token_ms,
                provider_processing_ms: finalization.timings.provider_processing_ms,
                provider_metadata_json: finalization.provider_metadata_json,
                cost_source: finalization.cost.source(),
                cost_ticks: finalization
                    .cost
                    .total()
                    .map(|total| total.amount().scaled()),
            });
        let gate = self
            .finalize_gate
            .lock()
            .expect("finalize gate lock")
            .take();
        if let Some(gate) = gate {
            gate.await.expect("finalize released");
        }
        Ok(())
    }

    async fn recover_expired(&self, _now: SystemTime) -> Result<RecoveryReport, StoreError> {
        Ok(RecoveryReport::default())
    }
}

enum Script {
    DelayedStream {
        account_id: &'static str,
        items: Vec<Result<GatewayEvent, ProviderError>>,
    },
    Stream {
        account_id: &'static str,
        items: Vec<Result<GatewayEvent, ProviderError>>,
    },
    /// 产出 `items` 后永久悬挂的流；用于逼出会话级 deadline。
    HangingStream {
        account_id: &'static str,
        items: Vec<Result<GatewayEvent, ProviderError>>,
    },
    ObservedStream {
        account_id: &'static str,
        items: Vec<Result<ProviderEvent, ProviderError>>,
    },
    Error(ProviderError),
    Pending,
}

struct ScriptedProvider {
    preparations: AtomicUsize,
    scripts: Mutex<VecDeque<Script>>,
    contexts: Mutex<Vec<AttemptContext>>,
    operations: Mutex<Vec<Operation>>,
    released_leases: Arc<AtomicUsize>,
}

struct TrackedLease(Arc<AtomicUsize>);

impl Drop for TrackedLease {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl ScriptedProvider {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            preparations: AtomicUsize::new(0),
            scripts: Mutex::new(scripts.into()),
            contexts: Mutex::new(Vec::new()),
            operations: Mutex::new(Vec::new()),
            released_leases: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    fn prepare_operation(
        &self,
        operation: Operation,
        _candidate: &gateway_core::routing::ProviderCandidate,
        mode: gateway_core::account::FastMode,
    ) -> Operation {
        if mode == gateway_core::account::FastMode::Default {
            return operation;
        }
        self.preparations.fetch_add(1, Ordering::SeqCst);
        let Operation::Generate(request) = operation else {
            panic!("generate expected");
        };
        let mut body = request.protocol_payload().body().clone();
        body.insert("service_tier".into(), json!("priority"));
        Operation::Generate(request.with_protocol_body(body))
    }

    fn name(&self) -> &'static str {
        "openai"
    }

    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }

    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.operations
            .lock()
            .expect("operations lock")
            .push(request.operation().clone());
        self.contexts.lock().expect("contexts lock").push(context);
        let script = self
            .scripts
            .lock()
            .expect("scripts lock")
            .pop_front()
            .expect("one script per provider call");
        if matches!(&script, Script::DelayedStream { .. }) {
            futures_timer::Delay::new(Duration::from_millis(10)).await;
        }
        match script {
            Script::Error(error) => Err(error),
            Script::Pending => futures::future::pending().await,
            Script::Stream { account_id, items } | Script::DelayedStream { account_id, items } => {
                let candidate = request.candidate();
                let metadata = ProviderCallMetadata::new(
                    candidate.provider().clone(),
                    candidate
                        .upstream_model()
                        .cloned()
                        .expect("model route candidate"),
                    ProviderAccountId::new(account_id).expect("account id"),
                    UpstreamTransport::new("http_sse").expect("transport"),
                )
                .with_upstream_request_id(OpaqueUpstreamValue::new("upstream-request"));
                Ok(ProviderStream::new(
                    metadata,
                    Box::pin(futures::stream::iter(
                        items.into_iter().map(canonical_provider_event),
                    )),
                    TrackedLease(Arc::clone(&self.released_leases)),
                ))
            }
            Script::HangingStream { account_id, items } => {
                let candidate = request.candidate();
                let metadata = ProviderCallMetadata::new(
                    candidate.provider().clone(),
                    candidate
                        .upstream_model()
                        .cloned()
                        .expect("model route candidate"),
                    ProviderAccountId::new(account_id).expect("account id"),
                    UpstreamTransport::new("http_sse").expect("transport"),
                );
                Ok(ProviderStream::new(
                    metadata,
                    Box::pin(
                        futures::stream::iter(items.into_iter().map(canonical_provider_event))
                            .chain(futures::stream::pending()),
                    ),
                    (),
                ))
            }
            Script::ObservedStream { account_id, items } => {
                let candidate = request.candidate();
                let metadata = ProviderCallMetadata::new(
                    candidate.provider().clone(),
                    candidate
                        .upstream_model()
                        .cloned()
                        .expect("model route candidate"),
                    ProviderAccountId::new(account_id).expect("account id"),
                    UpstreamTransport::new("websocket").expect("planned transport"),
                );
                Ok(ProviderStream::new(
                    metadata,
                    Box::pin(futures::stream::iter(items)),
                    (),
                ))
            }
        }
    }
}

fn generate_operation() -> Operation {
    let body = json!({
        "model": "gpt-5",
        "input": [{"type": "message", "role": "user", "content": "hello"}],
    });
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().expect("request object").clone())
            .expect("OpenAI payload"),
    ))
}

fn image_generate_operation() -> Operation {
    let body = json!({
        "model": "gpt-5",
        "input": [{"type": "message", "role": "user", "content": "draw"}],
        "tools": [{"type": "image_generation"}],
    });
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().expect("request object").clone())
            .expect("OpenAI payload"),
    ))
}

fn complete_stream(total_tokens: Option<u64>) -> Vec<Result<GatewayEvent, ProviderError>> {
    let mut events = vec![Ok(GatewayEvent::Started(ResponseMeta::new(
        "response-1",
        "gpt-5",
    )))];
    if total_tokens.is_some() {
        events.push(Ok(GatewayEvent::Usage(Usage {
            total_tokens,
            ..Usage::new()
        })));
    }
    events.push(Ok(GatewayEvent::Completed(ResponseMeta::new(
        "response-1",
        "gpt-5",
    ))));
    events
}

fn canonical_provider_event(
    event: Result<GatewayEvent, ProviderError>,
) -> Result<ProviderEvent, ProviderError> {
    event.map(ProviderEvent::canonical)
}

fn atomic_response_failed(response_id: &str, marker: &str) -> ProviderError {
    let started = ProviderEvent::canonical_with_wire(
        vec![GatewayEvent::Started(ResponseMeta::new(
            response_id,
            "gpt-5",
        ))],
        ProtocolWireEvent::json(
            "openai",
            Some("response.created".to_owned()),
            json!({
                "type": "response.created",
                "response": {"id": response_id, "model": "gpt-5", "status": "in_progress"}
            }),
        )
        .expect("started wire"),
    );
    let failed = ProviderEvent::wire(
        ProtocolWireEvent::json(
            "openai",
            Some("response.failed".to_owned()),
            json!({
                "type": "response.failed",
                "response": {
                    "id": response_id,
                    "status": "failed",
                    "error": {"code": "rate_limit_exceeded", "message": marker}
                }
            }),
        )
        .expect("failed wire"),
    );
    ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_status(429)
        .with_replay_safe()
        .with_atomic_client_events(vec![started, failed])
}

fn bare_atomic_response_failed(response_id: &str) -> ProviderError {
    let failed = ProviderEvent::canonical_with_wire(
        vec![GatewayEvent::Started(ResponseMeta::new(
            response_id,
            "fallback-model",
        ))],
        ProtocolWireEvent::json(
            "openai",
            Some("response.failed".to_owned()),
            json!({
                "type": "response.failed",
                "response": {
                    "id": response_id,
                    "status": "failed",
                    "error": {"code": "rate_limit_exceeded", "message": "bare failure"}
                }
            }),
        )
        .expect("bare failed wire"),
    );
    ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_status(429)
        .with_replay_safe()
        .with_atomic_client_events(vec![failed])
}

fn atomic_history_unavailable(response_id: &str) -> ProviderError {
    let failed = ProviderEvent::wire(
        ProtocolWireEvent::json(
            "openai",
            Some("response.failed".to_owned()),
            json!({
                "type": "response.failed",
                "response": {
                    "id": response_id,
                    "status": "failed",
                    "error": {
                        "code": "previous_response_not_found",
                        "message": "Previous response was not found. Retrying the full request."
                    }
                }
            }),
        )
        .expect("history unavailable wire"),
    );
    ProviderError::new(
        ProviderErrorKind::ContinuationRecoveryRequired,
        UpstreamSendState::Sent,
    )
    .with_status(400)
    .with_continuation_failure(ContinuationFailure::HistoryUnavailable)
    .with_atomic_client_events(vec![failed])
}

fn image_stream(image_output_tokens: Option<u64>) -> Vec<Result<GatewayEvent, ProviderError>> {
    let mut usage = Usage::new();
    usage.input_tokens = Some(12);
    usage.output_tokens = Some(5);
    usage.image_input_tokens = Some(31);
    usage.image_output_tokens = image_output_tokens;
    usage.total_tokens = Some(17);
    vec![
        Ok(GatewayEvent::Started(ResponseMeta::new(
            "response-image",
            "gpt-5",
        ))),
        Ok(GatewayEvent::Usage(usage)),
        Ok(GatewayEvent::Completed(ResponseMeta::new(
            "response-image",
            "gpt-5",
        ))),
    ]
}

fn plan(operation: &Operation) -> RoutingPlan {
    plan_with_policy(
        operation,
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(2).expect("account concurrency"),
            Duration::from_millis(50),
        ),
    )
}

fn plan_with_policy(
    operation: &Operation,
    account_selection_policy: AccountSelectionPolicy,
) -> RoutingPlan {
    let provider = ProviderKind::new("openai").expect("provider");
    let public_model = PublicModelId::new("gpt-5").expect("public model");
    let capabilities = ModelCapabilities::new(BTreeSet::from([operation.kind()]), Some(32_000))
        .with_upstream_feature_validation();
    let directory = Arc::new(RuntimeAccountDirectory::new(
        [
            "acct_failed",
            "acct_first",
            "acct_image",
            "acct_observation_mismatch",
            "acct_observed",
            "acct_one",
            "acct_only",
            "acct_other",
            "acct_required",
            "acct_second",
            "acct_tool",
            "acct_two",
            "acct_wrong",
        ]
        .into_iter()
        .map(|id| {
            (
                ProviderAccountId::new(id).expect("account"),
                RuntimeAccount::new(provider.clone(), BTreeSet::new()),
            )
        })
        .collect(),
    ));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).expect("config revision"),
        account_selection_policy,
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider.clone(),
            UpstreamModelId::new("gpt-5").expect("upstream model"),
            capabilities,
        )],
        Vec::new(),
    )
    .expect("snapshot")
    .with_account_directory(Arc::clone(&directory));
    snapshot
        .plan(
            &public_model,
            operation,
            Arc::new(FrozenAccountScope::new(
                directory,
                ClientRoutingScope::all_accounts(),
            )),
            &RoutingContext {
                required_provider: Some(provider),
                ..RoutingContext::default()
            },
        )
        .expect("routing plan")
}

fn model_request(operation: &Operation, deadline: SystemTime) -> NewModelRequest {
    let client_key = ClientApiKeyId::new("key_client_1").expect("client key id");
    NewModelRequest {
        parent_request_id: None,
        initiating_plugin_instance_id: None,
        budget_scope: None,
        admission_decision_ms: None,
        id: ModelRequestId::new("req_core_1").expect("request id"),
        client_api_key_id: Some(client_key.clone()),
        client_api_key_ref: client_key,
        config_revision: ConfigRevision::new(1).expect("config revision"),
        routing: AccountRoutingSnapshot::all(),
        protocol: "openai".to_owned(),
        operation: operation.kind(),
        endpoint: "responses".to_owned(),
        client_transport: "http_sse".to_owned(),
        requested_model: Some(PublicModelId::new("gpt-5").expect("model")),
        client_ip: Some("127.0.0.1".parse().expect("client IP")),
        user_agent: Some("gateway-core-test".to_owned()),
        reasoning_effort: Some("medium".to_owned()),
        reasoning_preset: Some("balanced".to_owned()),
        request_kind: Some("responses".to_owned()),
        subagent_kind: None,
        compact: false,
        continuation: Default::default(),
        image_generation_requested: operation.image_generation_requested(),
        started_at: SystemTime::now(),
        deadline_at: deadline.into(),
    }
}

fn coordinator(
    scripts: Vec<Script>,
) -> (
    AttemptCoordinator<FakeStore>,
    Arc<FakeStore>,
    Arc<ScriptedProvider>,
) {
    let store = Arc::new(FakeStore::default());
    coordinator_with_store(scripts, store)
}

fn coordinator_with_store(
    scripts: Vec<Script>,
    store: Arc<FakeStore>,
) -> (
    AttemptCoordinator<FakeStore>,
    Arc<FakeStore>,
    Arc<ScriptedProvider>,
) {
    let provider = Arc::new(ScriptedProvider::new(scripts));
    let mut registry = ProviderRegistry::builder();
    registry
        .register(provider.clone())
        .expect("register provider");
    let engine = GatewayEngine::new(store.clone(), registry.build());
    (AttemptCoordinator::new(engine), store, provider)
}

fn terminal_non_idempotent_failure(
    items: Vec<Result<GatewayEvent, ProviderError>>,
    continuation: Option<ContinuationBinding>,
) -> (Arc<FakeStore>, Arc<ScriptedProvider>) {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items,
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        continuation,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted()).expect_err("failure must be terminal");
    assert!(matches!(
        error,
        gateway_core::engine::EngineError::Provider(_)
    ));
    (store, provider)
}

#[test]
fn success_updates_one_model_request_and_persists_usage() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: complete_stream(Some(12)),
    }]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let events = block_on(session.collect_uncommitted()).expect("collect response");
    assert_eq!(events.len(), 3);
    let started_id = match &events[0].canonical_facts()[0] {
        GatewayEvent::Started(metadata) => metadata.response_id(),
        event => panic!("unexpected first event: {event:?}"),
    };
    let completed_id = match &events[2].canonical_facts()[0] {
        GatewayEvent::Completed(metadata) => metadata.response_id(),
        event => panic!("unexpected final event: {event:?}"),
    };
    assert_eq!(started_id, "response-1");
    assert_eq!(started_id, completed_id);
    assert!(!session.is_finalized());
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    assert!(session.is_finalized());
    assert_eq!(
        session.provider_attempt_outcomes(),
        &[ProviderAttemptOutcome::Succeeded {
            provider_kind: ProviderKind::new("openai").expect("provider"),
        }]
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.created, 1);
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.commits, 1);
    assert_eq!(state.committed_statuses, vec![Some(200)]);
    assert_eq!(state.intermediate_failures, 0);
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(finalization.send_state, UpstreamSendState::Sent);
    assert_eq!(finalization.attempt_count, 1);
    assert!(finalization.committed);
    assert_eq!(finalization.client_status_code, Some(200));
    assert_eq!(finalization.total_tokens, Some(12));
    assert!(finalization.latency_ms.is_some());
    assert_eq!(finalization.client_response_id.as_deref(), Some(started_id));
    assert_eq!(
        finalization.upstream_response_id.as_deref(),
        Some("response-1")
    );
}

#[test]
fn observation_store_failures_never_block_successful_delivery() {
    for failure in [
        StoreWriteFailure::Create,
        StoreWriteFailure::Commit,
        StoreWriteFailure::Finalize,
    ] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let store = Arc::new(FakeStore::failing(failure));
        let (coordinator, _, provider) = coordinator_with_store(
            vec![Script::Stream {
                account_id: "acct_one",
                items: complete_stream(None),
            }],
            store,
        );
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .expect("start execution");

        let events = block_on(session.collect_uncommitted())
            .unwrap_or_else(|error| panic!("{failure:?} blocked collection: {error}"));
        assert_eq!(events.len(), 2);
        block_on(session.commit_downstream(Some(200)))
            .unwrap_or_else(|error| panic!("{failure:?} blocked commit: {error}"));
        assert!(session.is_finalized());
        assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    }
}

#[test]
fn terminal_provider_error_preserves_request_local_upstream_response() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let raw_body = Bytes::from_static(b"{\"upstream\":\"unchanged\"}\x00");
    let provider_error =
        ProviderError::new(ProviderErrorKind::InvalidRequest, UpstreamSendState::Sent)
            .with_client_visible_upstream_response(ClientVisibleUpstreamResponse::new(
                422,
                Some(b"application/problem+json".to_vec()),
                raw_body.clone(),
            ));
    let (coordinator, _, _) = coordinator(vec![Script::Error(provider_error)]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let error = block_on(session.collect_uncommitted()).expect_err("provider rejection");
    let EngineError::Provider(error) = error else {
        panic!("expected provider error");
    };
    let response = error
        .client_visible_upstream_response()
        .expect("request-local upstream response");

    assert_eq!(response.status(), 422);
    assert_eq!(response.body(), &raw_body);
}

#[test]
fn success_preserves_opaque_upstream_response_id_and_builds_native_pin() {
    let response_id = format!("resp_{}\0opaque", "x".repeat(4_096));
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                response_id.clone(),
                "gpt-5",
            ))),
            Ok(GatewayEvent::Completed(ResponseMeta::new(
                response_id.clone(),
                "gpt-5",
            ))),
        ],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("collect opaque response");
    let provider_state = ProviderSessionState::new("openai", Map::new()).expect("provider state");
    let pin = session
        .native_continuation_pin(&provider_state)
        .expect("native continuation pin");
    assert_eq!(pin.previous_response_id().as_str(), response_id);
    assert_eq!(pin.upstream_response_id().as_str(), response_id);

    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");
    assert_eq!(
        state.finalizations[0].client_response_id,
        Some(response_id.clone())
    );
    assert_eq!(
        state.finalizations[0].upstream_response_id,
        Some(response_id)
    );
}

#[test]
fn canonical_identity_change_and_empty_terminal_id_are_observational() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                "response-created",
                "gpt-5",
            ))),
            Ok(GatewayEvent::Completed(ResponseMeta::new("", "gpt-5"))),
        ],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("identity observation cannot fail delivery");
    let provider_state = ProviderSessionState::new("openai", Map::new()).expect("provider state");
    let pin = session
        .native_continuation_pin(&provider_state)
        .expect("empty opaque response ID remains recordable");
    assert_eq!(pin.previous_response_id().as_str(), "");
    assert_eq!(pin.upstream_response_id().as_str(), "");

    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");
    assert_eq!(
        state.finalizations[0].client_response_id.as_deref(),
        Some("")
    );
    assert_eq!(
        state.finalizations[0].upstream_response_id.as_deref(),
        Some("")
    );
}

fn finalized_image_request(image_output_tokens: Option<u64>) -> FinalState {
    let operation = image_generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_image",
        items: image_stream(image_output_tokens),
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start image execution");
    block_on(session.collect_uncommitted()).expect("collect image response");
    block_on(session.commit_downstream(Some(200))).expect("commit image response");

    store.state.lock().expect("store lock").finalizations[0].clone()
}

#[test]
fn image_request_with_output_should_persist_success_and_image_tokens() {
    let finalization = finalized_image_request(Some(9));

    assert_eq!(
        (
            finalization.image_generation_succeeded,
            finalization.image_input_tokens,
            finalization.image_output_tokens,
        ),
        (Some(true), Some(31), Some(9))
    );
}

#[test]
fn image_request_without_output_should_persist_failure() {
    let finalization = finalized_image_request(None);

    assert_eq!(finalization.image_generation_succeeded, Some(false));
}

#[test]
fn failed_image_request_should_persist_failure() {
    let operation = image_generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_image",
        items: vec![Err(ProviderError::new(
            ProviderErrorKind::Protocol,
            UpstreamSendState::Sent,
        ))],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start image execution");
    block_on(session.collect_uncommitted()).expect_err("image request should fail");
    let state = store.state.lock().expect("store lock");

    assert_eq!(
        state.finalizations[0].image_generation_succeeded,
        Some(false)
    );
}

#[test]
fn response_observation_is_persisted_but_never_delivered() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let observation = ProviderResponseObservation::new(
        UpstreamTransport::new("http_sse").expect("actual transport"),
    )
    .with_http_version(UpstreamHttpVersion::Http2)
    .with_websocket_pool(WebSocketPoolKind::New)
    .with_status_code(200)
    .with_request_id(OpaqueUpstreamValue::new("upstream-observed"))
    .with_upstream_response_model_if_valid("gpt-returned")
    .try_with_service_tier("priority")
    .expect("service tier")
    .with_timings(ProviderResponseTimings {
        transport_decision_wait_ms: Some(7),
        connect_ms: Some(11),
        headers_ms: Some(13),
        first_event_ms: Some(17),
        ..ProviderResponseTimings::default()
    });
    let mut items = vec![Ok(ProviderEvent::observation(observation))];
    items.extend(
        complete_stream(Some(21))
            .into_iter()
            .map(canonical_provider_event),
    );
    let (coordinator, store, _) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_observed",
        items,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let events = block_on(session.collect_uncommitted()).expect("collect response");
    assert_eq!(events.len(), 3);
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let state = store.state.lock().expect("store lock");
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.upstream_transport.as_deref(), Some("http_sse"));
    assert_eq!(finalization.http_version.as_deref(), Some("HTTP/2"));
    assert_eq!(finalization.websocket_pool.as_deref(), Some("new"));
    assert_eq!(finalization.service_tier.as_deref(), Some("priority"));
    assert_eq!(
        finalization.upstream_response_model.as_deref(),
        Some("gpt-returned")
    );
    assert_eq!(
        finalization.upstream_request_id.as_deref(),
        Some("upstream-observed")
    );
    assert_eq!(finalization.upstream_status_code, Some(200));
    assert_eq!(finalization.transport_decision_wait_ms, Some(7));
    assert_eq!(finalization.connect_ms, Some(11));
    assert_eq!(finalization.headers_ms, Some(13));
    assert_eq!(finalization.first_event_ms, Some(17));
}

#[test]
fn mismatched_response_observation_should_not_interrupt_client_events() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let first = ProviderEvent::observation(ProviderResponseObservation::new(
        UpstreamTransport::new("websocket").expect("first transport"),
    ));
    let mismatched = ProviderEvent::observation(ProviderResponseObservation::new(
        UpstreamTransport::new("http_sse").expect("mismatched transport"),
    ));
    let mut items = vec![Ok(first), Ok(mismatched)];
    items.extend(
        complete_stream(None)
            .into_iter()
            .map(canonical_provider_event),
    );
    let (coordinator, store, _) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_observation_mismatch",
        items,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let events = block_on(session.collect_uncommitted()).expect("client events survive");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    assert_eq!(events.len(), 2);
    assert_eq!(
        store.state.lock().expect("store lock").finalizations[0]
            .upstream_transport
            .as_deref(),
        Some("websocket")
    );
}

#[test]
fn response_observation_should_persist_provider_metadata_and_output_timings() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let metadata = gateway_core::event::ProviderResponseMetadata::new(
        serde_json::json!({"effectiveModel": "gpt-test", "firstTokenMs": 23}).to_string(),
    )
    .expect("provider metadata object");
    let observation = ProviderResponseObservation::new(
        UpstreamTransport::new("websocket").expect("actual transport"),
    )
    .with_timings(ProviderResponseTimings {
        first_event_ms: Some(11),
        first_reasoning_ms: Some(17),
        first_text_ms: Some(19),
        first_token_ms: Some(17),
        provider_processing_ms: Some(5),
        ..ProviderResponseTimings::default()
    })
    .with_provider_metadata(metadata);
    let mut items = vec![Ok(ProviderEvent::observation(observation))];
    items.extend(
        complete_stream(Some(21))
            .into_iter()
            .map(canonical_provider_event),
    );
    let (coordinator, store, _) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_observed",
        items,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let state = store.state.lock().expect("store lock");
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.first_event_ms, Some(11));
    assert_eq!(finalization.first_reasoning_ms, Some(17));
    assert_eq!(finalization.first_text_ms, Some(19));
    assert_eq!(finalization.first_token_ms, Some(17));
    assert_eq!(finalization.provider_processing_ms, Some(5));
    assert_eq!(
        finalization.provider_metadata_json.as_deref(),
        Some("{\"effectiveModel\":\"gpt-test\",\"firstTokenMs\":23}")
    );
}

#[test]
fn empty_tool_call_delta_should_not_preempt_provider_first_token_timing() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let transport = UpstreamTransport::new("websocket").expect("actual transport");
    let initial_observation = ProviderResponseObservation::new(transport.clone());
    let output_observation =
        ProviderResponseObservation::new(transport).with_timings(ProviderResponseTimings {
            first_token_ms: Some(37),
            ..ProviderResponseTimings::default()
        });
    let items = vec![
        Ok(ProviderEvent::observation(initial_observation)),
        Ok(ProviderEvent::canonical(GatewayEvent::Started(
            ResponseMeta::new("response-tool", "gpt-5"),
        ))),
        Ok(ProviderEvent::canonical(GatewayEvent::ContentAdded(
            ContentItem::new(0, ContentKind::ToolCall),
        ))),
        Ok(ProviderEvent::canonical(GatewayEvent::ToolCallDelta(
            ToolCallDelta {
                content_index: 0,
                call_id: "call-tool".to_owned(),
                name: Some("apply_patch".to_owned()),
                arguments_delta: String::new(),
            },
        ))),
        Ok(ProviderEvent::observation(output_observation)),
        Ok(ProviderEvent::canonical(GatewayEvent::Completed(
            ResponseMeta::new("response-tool", "gpt-5"),
        ))),
    ];
    let (coordinator, store, _) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_tool",
        items,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let state = store.state.lock().expect("store lock");
    assert_eq!(state.finalizations[0].first_token_ms, Some(37));
}

#[test]
fn unknown_wire_event_before_response_identity_is_discarded_with_retried_attempt() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let unknown = ProtocolWireEvent::json(
        "openai",
        Some("response.future_event".to_owned()),
        serde_json::json!({"type": "response.future_event", "opaque": true}),
    )
    .expect("wire event");
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![
                Ok(ProviderEvent::wire(unknown)),
                Err(
                    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("retry response");
    block_on(session.commit_downstream(Some(200))).expect("commit winning response");

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    assert!(provider.scripts.lock().expect("scripts lock").is_empty());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn discarded_attempt_observation_does_not_leak_into_retry_result() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let first_observation = ProviderResponseObservation::new(
        UpstreamTransport::new("websocket").expect("first transport"),
    )
    .with_http_version(UpstreamHttpVersion::Http11)
    .with_status_code(503)
    .with_request_id(OpaqueUpstreamValue::new("discarded-request"))
    .with_upstream_response_model_if_valid("discarded-model")
    .with_timings(ProviderResponseTimings {
        transport_decision_wait_ms: Some(7),
        connect_ms: Some(11),
        headers_ms: Some(13),
        first_event_ms: Some(987_654),
        provider_processing_ms: Some(41),
        ..ProviderResponseTimings::default()
    });
    let second_observation = ProviderResponseObservation::new(
        UpstreamTransport::new("http_sse").expect("second transport"),
    )
    .with_http_version(UpstreamHttpVersion::Http2)
    .with_status_code(200)
    .with_request_id(OpaqueUpstreamValue::new("winning-request"));
    let mut second_items = vec![Ok(ProviderEvent::observation(second_observation))];
    second_items.extend(
        complete_stream(None)
            .into_iter()
            .map(canonical_provider_event),
    );
    let (coordinator, store, _) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![
                Ok(ProviderEvent::observation(first_observation)),
                Err(
                    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                        .with_replay_safe()
                        .with_diagnostic(
                            gateway_core::error::ProviderDiagnostic::new(
                                "response event idle timeout after 300s",
                            )
                            .with_classification("receive", "receive_idle_timeout"),
                        ),
                ),
            ],
        },
        Script::ObservedStream {
            account_id: "acct_second",
            items: second_items,
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("retry response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let state = store.state.lock().expect("store lock");
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.upstream_response_model, None);
    let trace: Value =
        serde_json::from_str(finalization.diagnostic_trace_json.as_deref().unwrap()).unwrap();
    let events = trace["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["stage"] == "attempt.failed" && event["attemptIndex"] == 1)
    );
    assert!(
        events
            .iter()
            .any(|event| event["stage"] == "attempt.started" && event["attemptIndex"] == 2)
    );
    let failed = events
        .iter()
        .find(|event| event["stage"] == "attempt.failed")
        .unwrap();
    assert_eq!(
        failed["data"]["diagnostic"],
        serde_json::json!({
            "stage": "receive", "code": "receive_idle_timeout", "message": "response event idle timeout after 300s"
        })
    );
    assert_eq!(events.last().unwrap()["stage"], "request.finished");

    assert_eq!(finalization.upstream_transport.as_deref(), Some("http_sse"));
    assert_eq!(finalization.http_version.as_deref(), Some("HTTP/2"));
    assert_eq!(
        finalization.upstream_request_id.as_deref(),
        Some("winning-request")
    );
    assert_eq!(finalization.upstream_status_code, Some(200));
    assert_eq!(finalization.transport_decision_wait_ms, None);
    assert_eq!(finalization.connect_ms, None);
    assert_eq!(finalization.headers_ms, None);
    assert_eq!(finalization.provider_processing_ms, None);
    assert!(
        finalization
            .first_event_ms
            .is_some_and(|elapsed| elapsed < 987_654)
    );
    assert_eq!(state.intermediate_status_codes, vec![Some(503)]);
    assert_eq!(
        state.intermediate_request_ids,
        vec![Some("discarded-request".to_owned())]
    );
}

#[test]
fn websocket_success_keeps_client_http_status_absent() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: complete_stream(None),
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(None)).expect("commit WebSocket response");

    let state = store.state.lock().expect("store lock");
    assert_eq!(state.committed_statuses, vec![None]);
    assert_eq!(state.finalizations[0].client_status_code, None);
}

#[test]
fn success_without_response_observation_persists_no_upstream_status() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: complete_stream(None),
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let state = store.state.lock().expect("store lock");
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
    assert_eq!(state.finalizations[0].upstream_status_code, None);
}

#[test]
fn missing_attempt_is_classified_internal_not_no_available_provider() {
    assert_eq!(
        gateway_error_from_engine(&EngineError::EmptyRoutingPlan).kind(),
        GatewayErrorKind::NoAvailableProvider
    );
    assert_eq!(
        gateway_error_from_engine(&EngineError::NoActiveAttempt).kind(),
        GatewayErrorKind::Internal
    );
}

#[test]
fn authenticated_native_continuation_reaches_every_attempt_context() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: complete_stream(None),
    }]);
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-secret-id"),
        PreviousResponseId::new("provider-native-id"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        ProviderAccountId::new("acct_one").expect("account"),
    );

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");
    let _events = block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(
        contexts[0]
            .continuation()
            .expect("continuation")
            .pinned()
            .expect("pinned continuation")
            .previous_response_id()
            .as_str(),
        "previous-secret-id"
    );
    assert_eq!(
        contexts[0]
            .continuation()
            .expect("continuation")
            .pinned()
            .expect("pinned continuation")
            .upstream_response_id()
            .as_str(),
        "provider-native-id"
    );
    let owner = contexts[0]
        .account_state_owner()
        .expect("continuation account-state owner");
    assert!(owner.matches(
        &ProviderKind::new("openai").expect("provider"),
        &ProviderAccountId::new("acct_one").expect("account"),
    ));
}

#[test]
fn streaming_native_continuation_delivers_started_event_before_later_output() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: complete_stream(None),
    }]);
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-response"),
        PreviousResponseId::new("upstream-response"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        ProviderAccountId::new("acct_one").expect("account"),
    );
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");

    let first = block_on(session.next_event())
        .expect("first event")
        .expect("started event");
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    let events = first.into_provider_events();
    assert!(matches!(
        events.as_slice(),
        [event]
            if matches!(event.canonical_facts(), [GatewayEvent::Started(_)])
    ));

    block_on(session.commit_downstream(Some(200))).expect("commit started event");
    while block_on(session.next_event())
        .expect("remaining event")
        .is_some()
    {}
}

#[test]
fn native_continuation_uses_provider_defined_owner_then_cross_account_recovery() {
    let Operation::Generate(generate) = generate_operation() else {
        panic!("generate operation");
    };
    let mut payload = Map::new();
    payload.insert("transcript".to_owned(), Value::Array(Vec::new()));
    let operation = Operation::Generate(generate.with_provider_session_state(
        ProviderSessionState::new("openai", payload).expect("provider session state"),
    ));
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_one",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "response-native",
                    "gpt-5",
                ))),
                Err(
                    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                        .with_continuation_failure(ContinuationFailure::HistoryUnavailable)
                        .with_continuation_recovery_disposition(
                            ContinuationRecoveryDisposition::ProviderReplayAllowed,
                        )
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_one",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Transport,
                UpstreamSendState::NotSent,
            )
            .with_replay_safe())],
        },
        Script::Stream {
            account_id: "acct_two",
            items: complete_stream(None),
        },
    ]);
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-secret-id"),
        PreviousResponseId::new("provider-native-id"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        ProviderAccountId::new("acct_one").expect("account"),
    );

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("provider-defined recovery succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 3);
    assert_eq!(
        contexts[0].continuation_attempt(),
        ContinuationAttempt::Native
    );
    assert_eq!(
        contexts[1].continuation_attempt(),
        ContinuationAttempt::ReplayOwner
    );
    assert_eq!(
        contexts[2].continuation_attempt(),
        ContinuationAttempt::ReplayAny
    );
    assert!(
        contexts[2]
            .excluded_accounts()
            .contains(&ProviderAccountId::new("acct_one").expect("account"))
    );
}

#[test]
fn native_continuation_client_replay_required_is_terminal() {
    for (kind, send_state) in [
        (
            ProviderErrorKind::ContinuationRecoveryRequired,
            UpstreamSendState::NotSent,
        ),
        (
            ProviderErrorKind::QuotaExhausted,
            UpstreamSendState::NotSent,
        ),
        (ProviderErrorKind::QuotaExhausted, UpstreamSendState::Sent),
    ] {
        let Operation::Generate(generate) = generate_operation() else {
            panic!("generate operation");
        };
        let operation = Operation::Generate(generate.with_provider_session_state(
            ProviderSessionState::new("openai", Map::new()).expect("provider session state"),
        ));
        let route_plan = plan(&operation);
        let (coordinator, store, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_one",
                items: vec![Err(ProviderError::new(kind, send_state)
                    .with_replay_safe()
                    .with_continuation_failure(ContinuationFailure::HistoryUnavailable)
                    .with_continuation_recovery_disposition(
                        ContinuationRecoveryDisposition::ClientReplayRequired,
                    ))],
            },
            Script::Stream {
                account_id: "acct_one",
                items: complete_stream(None),
            },
        ]);
        let continuation = NativeContinuationPin::new(
            PreviousResponseId::new("previous-secret-id"),
            PreviousResponseId::new("provider-native-id"),
            ClientApiKeyId::new("key_client_1").expect("client key"),
            ProviderKind::new("openai").expect("provider"),
            ProviderAccountId::new("acct_one").expect("account"),
        );

        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            Some(ContinuationBinding::Pinned(continuation)),
            CancellationToken::new(),
        ))
        .expect("start execution");
        let error = block_on(session.collect_uncommitted())
            .expect_err("client replay requirement must stop proxy recovery");

        assert!(matches!(error, EngineError::Provider(_)));
        let contexts = provider.contexts.lock().expect("contexts lock");
        assert_eq!(contexts.len(), 1);
        assert_eq!(
            contexts[0].continuation_attempt(),
            ContinuationAttempt::Native
        );
        let state = store.state.lock().expect("store lock");
        assert_eq!(state.intermediate_failures, 0);
        assert_eq!(state.finalizations[0].attempt_count, 1);
        assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
        assert_eq!(state.finalizations[0].send_state, send_state);
    }
}

#[test]
fn native_continuation_exact_retry_keeps_scope_and_account() {
    let Operation::Generate(generate) = generate_operation() else {
        panic!("generate operation");
    };
    let operation = Operation::Generate(generate.with_provider_session_state(
        ProviderSessionState::new("openai", Map::new()).expect("provider session state"),
    ));
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_one",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Unavailable,
                UpstreamSendState::NotSent,
            )
            .with_continuation_failure(ContinuationFailure::Busy)
            .with_continuation_recovery_disposition(
                ContinuationRecoveryDisposition::RetryExactConnection,
            ))],
        },
        Script::Stream {
            account_id: "acct_one",
            items: complete_stream(None),
        },
    ]);
    let account = ProviderAccountId::new("acct_one").expect("account");
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-secret-id"),
        PreviousResponseId::new("provider-native-id"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        account.clone(),
    );

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("exact continuation retry succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 2);
    assert!(
        contexts
            .iter()
            .all(|context| context.continuation_attempt() == ContinuationAttempt::Native)
    );
    assert_eq!(contexts[1].required_account(), Some(&account));
}

#[test]
fn unavailable_native_continuation_uses_provider_recovery_with_the_configured_account_policy() {
    let Operation::Generate(generate) = generate_operation() else {
        panic!("generate operation");
    };
    let operation = Operation::Generate(generate.with_provider_session_state(
        ProviderSessionState::new("openai", Map::new()).expect("provider session state"),
    ));
    let policy = AccountSelectionPolicy::new(
        RotationStrategy::RoundRobin,
        NonZeroU32::new(2).expect("account concurrency"),
        Duration::from_millis(50),
    );
    let route_plan = plan_with_policy(&operation, policy);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Error(ProviderError::new(
            ProviderErrorKind::NoEligibleAccount,
            UpstreamSendState::NotSent,
        )),
        Script::Stream {
            account_id: "acct_two",
            items: complete_stream(None),
        },
    ]);
    let original = ProviderAccountId::new("acct_one").expect("account");
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-response"),
        PreviousResponseId::new("upstream-response"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        original.clone(),
    );

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("provider-defined recovery succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 2);
    assert_eq!(
        contexts[0].continuation_attempt(),
        ContinuationAttempt::Native
    );
    assert_eq!(
        contexts[1].continuation_attempt(),
        ContinuationAttempt::ReplayAny
    );
    assert_eq!(
        contexts[1].account_selection_policy().strategy(),
        RotationStrategy::RoundRobin
    );
    assert!(contexts[1].excluded_accounts().contains(&original));
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn continuation_history_failure_without_replay_proof_is_delivered_without_hidden_retry() {
    let Operation::Generate(generate) = generate_operation() else {
        panic!("generate operation");
    };
    let operation = Operation::Generate(generate.with_provider_session_state(
        ProviderSessionState::new("openai", Map::new()).expect("provider session state"),
    ));
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_one",
            items: vec![Err(atomic_history_unavailable("response-history-missing"))],
        },
        Script::Stream {
            account_id: "acct_two",
            items: complete_stream(None),
        },
    ]);
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-response"),
        PreviousResponseId::new("upstream-response"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        ProviderAccountId::new("acct_one").expect("account"),
    );
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        Some(ContinuationBinding::Pinned(continuation)),
        CancellationToken::new(),
    ))
    .expect("start execution");

    let terminal = block_on(session.next_event())
        .expect("history failure remains deliverable")
        .expect("terminal history failure batch");
    assert_eq!(
        terminal.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    let events = terminal.into_provider_events();
    let failure = events
        .iter()
        .filter_map(ProviderEvent::wire_event)
        .find(|wire| wire.event_type() == Some("response.failed"))
        .expect("raw response.failed event");
    assert_eq!(
        failure
            .data()
            .pointer("/response/error/code")
            .and_then(Value::as_str),
        Some("previous_response_not_found")
    );
    block_on(session.commit_downstream(Some(200))).expect("commit terminal history failure");
    assert!(matches!(
        block_on(session.next_event()),
        Err(EngineError::Provider(_))
    ));

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 1);
    assert_eq!(
        contexts[0].continuation_attempt(),
        ContinuationAttempt::Native
    );
}

#[test]
fn diagnostic_account_reaches_provider_and_matching_metadata_succeeds() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_required",
        items: complete_stream(None),
    }]);
    let required = ProviderAccountId::new("acct_required").expect("account id");
    let mut session = block_on(coordinator.start_diagnostic(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        required.clone(),
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("required account succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].required_account(), Some(&required));
    assert!(contexts[0].is_diagnostic_required_account());
}

#[test]
fn provider_metadata_for_another_account_fails_closed() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_wrong",
            items: complete_stream(None),
        },
        Script::Stream {
            account_id: "acct_required",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        Some(ProviderAccountId::new("acct_required").expect("account id")),
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("different account metadata must fail closed");

    assert!(matches!(
        error,
        gateway_core::engine::EngineError::RequiredAccountMismatch
    ));
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    assert_eq!(provider.scripts.lock().expect("scripts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert!(state.attempts.is_empty());
    assert_eq!(state.created, 1);
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.finalizations[0].attempt_count, 0);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
    assert_eq!(
        state.finalizations[0].error_kind,
        Some(GatewayErrorKind::Internal)
    );
}

#[test]
fn required_account_disables_account_retry_after_stream_creation() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_required",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Unavailable,
                UpstreamSendState::NotSent,
            )
            .with_replay_safe())],
        },
        Script::Stream {
            account_id: "acct_other",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        Some(ProviderAccountId::new("acct_required").expect("account id")),
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("required account must not rotate after failure");

    assert!(matches!(
        error,
        gateway_core::engine::EngineError::Provider(_)
    ));
    assert_eq!(
        session.provider_attempt_outcomes(),
        &[ProviderAttemptOutcome::Failed {
            provider_kind: ProviderKind::new("openai").expect("provider"),
            error_kind: ProviderErrorKind::Unavailable,
        }]
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    assert_eq!(provider.scripts.lock().expect("scripts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
}

#[test]
fn latest_provider_reported_cost_is_persisted_as_known_usd_total() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let events = vec![
        Ok(GatewayEvent::Started(ResponseMeta::new(
            "native-cost",
            "gpt-5",
        ))),
        Ok(GatewayEvent::ProviderCost(
            ProviderReportedCost::from_usd_ticks(10).expect("first cost"),
        )),
        Ok(GatewayEvent::ProviderCost(
            ProviderReportedCost::from_usd_ticks(25).expect("latest cost"),
        )),
        Ok(GatewayEvent::Completed(ResponseMeta::new(
            "native-cost",
            "gpt-5",
        ))),
    ];
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: events,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");
    assert_eq!(
        state.finalizations[0].cost_source,
        CostSource::ProviderReported
    );
    assert_eq!(state.finalizations[0].cost_ticks, Some(25));
}

#[test]
fn calculated_cost_is_persisted_when_provider_does_not_report_cost() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let events = vec![
        Ok(GatewayEvent::Started(ResponseMeta::new(
            "calculated-cost",
            "grok-4.5",
        ))),
        Ok(GatewayEvent::CalculatedCost(
            CalculatedCost::from_usd_ticks(123).expect("calculated cost"),
        )),
        Ok(GatewayEvent::Completed(ResponseMeta::new(
            "calculated-cost",
            "grok-4.5",
        ))),
    ];
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: events,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");

    assert_eq!(state.finalizations[0].cost_source, CostSource::Calculated);
    assert_eq!(state.finalizations[0].cost_ticks, Some(123));
}

#[test]
fn provider_reported_cost_should_not_be_replaced_by_calculated_cost() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let events = vec![
        Ok(GatewayEvent::Started(ResponseMeta::new(
            "reported-cost",
            "grok-4.5",
        ))),
        Ok(GatewayEvent::CalculatedCost(
            CalculatedCost::from_usd_ticks(10).expect("first calculated cost"),
        )),
        Ok(GatewayEvent::ProviderCost(
            ProviderReportedCost::from_usd_ticks(25).expect("provider cost"),
        )),
        Ok(GatewayEvent::CalculatedCost(
            CalculatedCost::from_usd_ticks(999).expect("later calculated cost"),
        )),
        Ok(GatewayEvent::Completed(ResponseMeta::new(
            "reported-cost",
            "grok-4.5",
        ))),
    ];
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: events,
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("collect response");
    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");

    assert_eq!(
        (
            state.finalizations[0].cost_source,
            state.finalizations[0].cost_ticks
        ),
        (CostSource::ProviderReported, Some(25))
    );
}

#[test]
fn discarded_attempt_cost_never_leaks_into_retry_result() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "discarded",
                    "gpt-5",
                ))),
                Ok(GatewayEvent::CalculatedCost(
                    CalculatedCost::from_usd_ticks(888).expect("discarded calculated cost"),
                )),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(999).expect("discarded cost"),
                )),
                Err(
                    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("retry succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.finalizations[0].cost_source, CostSource::Unavailable);
    assert_eq!(state.finalizations[0].cost_ticks, None);
    assert_eq!(
        session.budget_charge().amount_usd.scaled(),
        999,
        "missing final usage cannot erase a charged retry"
    );
}

#[test]
fn shared_budget_retains_itemized_retry_fees_and_leaves_unknown_fees_unclassified() {
    use gateway_core::metering::{
        CalculatedCostAmounts, CalculatedCostBreakdown, CalculatedCostRates, CurrencyCode, Decimal,
        ImageCostBreakdown, Money,
    };

    let money =
        |amount: &str| Money::new(amount.parse().unwrap(), CurrencyCode::new("USD").unwrap());
    let rates = || CalculatedCostRates::new(money("0"), money("0"), money("0"), money("0"));
    let first = CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            money("2"),
            money("3"),
            money("1"),
            money("1"),
            money("12"),
            money("12"),
        ),
        rates(),
        None,
        100,
    )
    .with_image(ImageCostBreakdown {
        input_tokens: 10,
        cached_tokens: 1,
        input_amount: money("1"),
        cache_read_amount: money("1"),
        input_price_per_million: money("0"),
        cache_read_price_per_million: money("0"),
    })
    .calculated_cost();
    let last = CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            money("0.5"),
            money("1"),
            money("0.25"),
            money("0.25"),
            money("2"),
            money("2"),
        ),
        rates(),
        None,
        100,
    )
    .calculated_cost();
    let retry = || {
        Err(
            ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                .with_replay_safe(),
        )
    };
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "itemized-retry",
                    "gpt-5",
                ))),
                Ok(GatewayEvent::CalculatedCost(first.clone())),
                Ok(GatewayEvent::CalculatedCost(first)),
                retry(),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "unknown-retry",
                    "gpt-5",
                ))),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(40_000_000_000).unwrap(),
                )),
                retry(),
            ],
        },
        Script::Stream {
            account_id: "acct_one",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new("winning", "gpt-5"))),
                Ok(GatewayEvent::CalculatedCost(last)),
                Ok(GatewayEvent::Completed(ResponseMeta::new(
                    "winning", "gpt-5",
                ))),
            ],
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("retry succeeds");
    let charge = session.budget_charge();
    assert_eq!(charge.amount_usd.canonical(), "18");
    assert_eq!(
        charge.components_usd.map(Decimal::canonical),
        ["3.5", "4", "2.25", "1.25"]
    );
    block_on(session.commit_downstream(Some(200))).expect("commit response");
    assert_eq!(session.budget_charge().amount_usd, charge.amount_usd);
    assert_eq!(
        session.budget_charge().components_usd,
        charge.components_usd
    );
    let state = store.state.lock().unwrap();
    assert_eq!(state.finalizations[0].cost_ticks, Some(20_000_000_000));
}

#[test]
fn key_budget_accumulates_only_observed_retry_costs() {
    for (first_cost, first_send, expected) in [
        (Some(999), UpstreamSendState::Sent, 1122),
        (
            Some(gateway_core::metering::Decimal::MAX.scaled()),
            UpstreamSendState::Sent,
            gateway_core::metering::Decimal::MAX.scaled(),
        ),
        (None, UpstreamSendState::Sent, 123),
        (None, UpstreamSendState::NotSent, 123),
    ] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let mut first = Vec::new();
        if first_send != UpstreamSendState::NotSent {
            first.push(Ok(GatewayEvent::Started(ResponseMeta::new(
                "resp_first",
                "gpt-5",
            ))));
        }
        if let Some(cost) = first_cost {
            first.push(Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(cost).unwrap(),
            )));
        }
        first.push(Err(ProviderError::new(
            ProviderErrorKind::Unavailable,
            first_send,
        )
        .with_replay_safe()));
        let (coordinator, _, _) = coordinator(vec![
            Script::Stream {
                account_id: "acct_first",
                items: first,
            },
            Script::Stream {
                account_id: "acct_second",
                items: vec![
                    Ok(GatewayEvent::Started(ResponseMeta::new(
                        "resp_done",
                        "gpt-5",
                    ))),
                    Ok(GatewayEvent::ProviderCost(
                        ProviderReportedCost::from_usd_ticks(123).unwrap(),
                    )),
                    Ok(GatewayEvent::Completed(ResponseMeta::new(
                        "resp_done",
                        "gpt-5",
                    ))),
                ],
            },
        ]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        block_on(session.collect_uncommitted()).unwrap();
        block_on(session.commit_downstream(Some(200))).unwrap();
        let charge = session.budget_charge();
        assert_eq!(charge.amount_usd.scaled(), expected);
        assert_eq!(
            session.budget_charge().completed_at,
            charge.completed_at,
            "final settlement time stays stable"
        );
    }
}

#[test]
fn key_budget_settles_post_send_failures_from_observed_cost_without_replaying() {
    for send_state in [UpstreamSendState::Sent, UpstreamSendState::Ambiguous] {
        for cost in [None, Some(123)] {
            for committed in [false, true] {
                let operation = generate_operation();
                let route_plan = plan(&operation);
                let mut items = vec![Ok(GatewayEvent::Started(ResponseMeta::new(
                    "response-interrupted",
                    "gpt-5",
                )))];
                if let Some(cost) = cost {
                    items.push(Ok(GatewayEvent::ProviderCost(
                        ProviderReportedCost::from_usd_ticks(cost).unwrap(),
                    )));
                }
                items.push(Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    send_state,
                )));
                let (coordinator, _, provider) = coordinator(vec![Script::Stream {
                    account_id: "acct_first",
                    items,
                }]);
                let mut session = block_on(coordinator.start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                ))
                .unwrap();
                if committed {
                    block_on(session.next_event()).unwrap().unwrap();
                    block_on(session.commit_downstream(Some(200))).unwrap();
                    loop {
                        match block_on(session.next_event()) {
                            Ok(Some(_)) => {}
                            Err(EngineError::Provider(_)) => break,
                            other => panic!("expected terminal transport error, got {other:?}"),
                        }
                    }
                } else {
                    assert!(matches!(
                        block_on(session.collect_uncommitted()),
                        Err(EngineError::Provider(_))
                    ));
                }
                assert!(session.is_finalized());
                assert_eq!(
                    session.budget_charge().amount_usd.scaled(),
                    cost.unwrap_or(0),
                    "send={send_state:?}, committed={committed}, cost={cost:?}"
                );
                assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            }
        }
    }
}

#[test]
fn pre_commit_failure_excludes_account_and_retries_same_target() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "discarded-response",
                    "gpt-5",
                ))),
                Err(
                    ProviderError::new(ProviderErrorKind::Unauthorized, UpstreamSendState::Sent)
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let events = block_on(session.collect_uncommitted()).expect("second account succeeds");
    assert_eq!(events.len(), 2);
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 2);
    assert!(contexts[0].account_state_owner().is_none());
    assert!(
        contexts[1]
            .account_state_owner()
            .is_some_and(|owner| owner.matches(
                &ProviderKind::new("openai").expect("provider"),
                &ProviderAccountId::new("acct_first").expect("account"),
            ))
    );
    assert!(
        contexts[1]
            .excluded_accounts()
            .contains(&ProviderAccountId::new("acct_first").expect("account id"))
    );
    assert_eq!(
        contexts[1].account_selection_policy().strategy(),
        RotationStrategy::Smart
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.created, 1);
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn recovered_credential_retries_the_same_account_exactly_once() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Unauthorized,
                UpstreamSendState::Sent,
            )
            .with_status(401)
            .with_replay_safe()
            .with_same_account_retry())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("recovered account succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    let account = ProviderAccountId::new("acct_first").expect("account");
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].required_account(), Some(&account));
    assert!(!contexts[1].excluded_accounts().contains(&account));
    assert!(contexts[1].credential_recovery_attempted());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn retryable_error_after_credential_recovery_switches_account_instead_of_terminating() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Unauthorized,
                UpstreamSendState::Sent,
            )
            .with_status(401)
            .with_replay_safe()
            .with_same_account_retry())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::RateLimited,
                UpstreamSendState::Sent,
            )
            .with_status(429)
            .with_replay_safe())],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("third attempt succeeds on another account");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    let first = ProviderAccountId::new("acct_first").expect("account");
    assert_eq!(contexts.len(), 3);
    assert_eq!(contexts[1].required_account(), Some(&first));
    assert!(contexts[1].credential_recovery_attempted());
    // recovery 钉账号只绑定 replay attempt；replay 上的 429 之后必须能换号。
    assert_eq!(contexts[2].required_account(), None);
    assert!(contexts[2].excluded_accounts().contains(&first));
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 3);
    assert_eq!(state.intermediate_failures, 2);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn non_idempotent_explicit_429_rejection_rotates_account_before_output() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::RateLimited,
                UpstreamSendState::Sent,
            )
            .with_status(429)
            .with_replay_safe())],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("second account succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 2);
    assert!(
        contexts[1]
            .excluded_accounts()
            .contains(&ProviderAccountId::new("acct_first").expect("account id"))
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn rate_limited_account_exhaustion_survives_a_later_empty_selection() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::RateLimited,
                UpstreamSendState::Sent,
            )
            .with_status(429)
            .with_upstream_code(OpaqueUpstreamValue::new("rate_limit_exceeded"))
            .with_retry_after(Duration::from_secs(30))
            .with_replay_safe())],
        },
        Script::Error(ProviderError::new(
            ProviderErrorKind::NoEligibleAccount,
            UpstreamSendState::NotSent,
        )),
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("exhausted account must remain a rate-limit response");
    let gateway_core::engine::EngineError::Provider(error) = error else {
        panic!("expected provider error")
    };

    assert_eq!(error.kind(), ProviderErrorKind::RateLimited);
    assert_eq!(error.upstream_status(), Some(429));
    assert_eq!(
        error.upstream_code().map(OpaqueUpstreamValue::as_str),
        Some("rate_limit_exceeded")
    );
    assert_eq!(error.retry_after(), Some(Duration::from_secs(30)));
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.created, 1);
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations.len(), 1);
    // attempt-1 已把 sent 落库；attempt-2 空选路终态不得降级回 not_sent。
    assert_eq!(state.finalizations[0].send_state, UpstreamSendState::Sent);
    assert_eq!(state.finalizations[0].upstream_status_code, Some(429));
    assert_eq!(
        state.finalizations[0].provider_error_code.as_deref(),
        Some("rate_limit_exceeded")
    );
}

#[test]
fn non_idempotent_sent_failure_without_provider_proof_is_not_retried() {
    let (store, provider) = terminal_non_idempotent_failure(
        vec![Err(ProviderError::new(
            ProviderErrorKind::RateLimited,
            UpstreamSendState::Sent,
        ))],
        None,
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 0);
}

#[test]
fn explicit_429_with_ambiguous_send_state_is_not_retried() {
    let (store, provider) = terminal_non_idempotent_failure(
        vec![Err(ProviderError::new(
            ProviderErrorKind::RateLimited,
            UpstreamSendState::Ambiguous,
        )
        .with_status(429)
        .with_replay_safe())],
        None,
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::Ambiguous
    );
    assert_eq!(state.intermediate_failures, 0);
}

#[test]
fn explicit_429_after_structural_event_should_retry_before_commit() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "response-started",
                    "gpt-5",
                ))),
                Err(
                    ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
                        .with_status(429)
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    block_on(session.collect_uncommitted()).expect("second account succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit response");

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn atomic_response_failed_should_rotate_account_before_stream_commit() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![Err(atomic_response_failed(
                "response-first-failed",
                "first account failure",
            ))],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let first = block_on(session.next_event())
        .expect("second account should produce the first delivery")
        .expect("started event");
    let delivered = first.into_provider_events();
    assert!(delivered.iter().flat_map(ProviderEvent::canonical_facts).any(
        |event| matches!(event, GatewayEvent::Started(meta) if meta.response_id() == "response-1")
    ));
    assert!(
        !delivered
            .iter()
            .filter_map(ProviderEvent::wire_event)
            .any(|wire| {
                wire.data().pointer("/response/id").and_then(Value::as_str)
                    == Some("response-first-failed")
            })
    );
    block_on(session.commit_downstream(Some(200))).expect("commit second account");
    while block_on(session.next_event())
        .expect("successful stream")
        .is_some()
    {}

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.commits, 1);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn bare_atomic_response_failed_should_rotate_before_the_first_delivery() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![Err(bare_atomic_response_failed("response-bare-failed"))],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let first = block_on(session.next_event())
        .expect("second account should replace the bare failure")
        .expect("second account started event");
    assert!(first
        .into_provider_events()
        .iter()
        .flat_map(ProviderEvent::canonical_facts)
        .any(|event| matches!(event, GatewayEvent::Started(meta) if meta.response_id() == "response-1")));
    block_on(session.commit_downstream(Some(200))).expect("commit second account");
    while block_on(session.next_event())
        .expect("successful stream")
        .is_some()
    {}

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn exhausted_atomic_response_failed_should_deliver_only_the_last_failure_once() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let scripts = (1..=route_plan.max_attempts().get())
        .map(|attempt| {
            let response_id = format!("response-attempt-{attempt}-failed");
            let marker = if attempt == route_plan.max_attempts().get() {
                "last upstream failure".to_owned()
            } else {
                format!("discarded failure {attempt}")
            };
            Script::ObservedStream {
                account_id: "acct_failed",
                items: vec![Err(atomic_response_failed(&response_id, &marker))],
            }
        })
        .collect::<Vec<_>>();
    let expected_attempts = route_plan.max_attempts().get();
    let expected_last_response = format!("response-attempt-{expected_attempts}-failed");
    let (coordinator, store, provider) = coordinator(scripts);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let terminal = block_on(session.next_event())
        .expect("last failure remains deliverable")
        .expect("terminal failure batch");
    assert_eq!(
        terminal.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    let events = terminal.into_provider_events();
    let failures = events
        .iter()
        .filter_map(ProviderEvent::wire_event)
        .filter(|wire| wire.event_type() == Some("response.failed"))
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures[0]
            .data()
            .pointer("/response/id")
            .and_then(Value::as_str),
        Some(expected_last_response.as_str())
    );
    assert!(
        failures[0]
            .data()
            .to_string()
            .contains("last upstream failure")
    );
    block_on(session.commit_downstream(Some(200))).expect("commit original terminal failure");
    let error = block_on(session.next_event()).expect_err("typed failure finalizes the request");
    assert!(matches!(error, EngineError::Provider(_)));
    assert!(session.is_finalized());

    assert_eq!(
        provider.contexts.lock().expect("contexts lock").len(),
        usize::try_from(expected_attempts).expect("attempt count fits usize")
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(
        state.intermediate_failures,
        usize::try_from(expected_attempts - 1).expect("attempt count fits usize")
    );
    assert_eq!(state.commits, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert!(state.finalizations[0].committed);
}

#[test]
fn empty_selection_after_atomic_failure_should_deliver_the_last_upstream_wire() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let mut failure = bare_atomic_response_failed("response-only-failed");
    let mut events = failure.take_atomic_client_events();
    events.push(ProviderEvent::canonical(GatewayEvent::ProviderCost(
        ProviderReportedCost::from_usd_ticks(500).unwrap(),
    )));
    let failure = failure.with_atomic_client_events(events);
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_only",
            items: vec![Err(failure)],
        },
        Script::Error(ProviderError::new(
            ProviderErrorKind::NoEligibleAccount,
            UpstreamSendState::NotSent,
        )),
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let terminal = block_on(session.next_event())
        .expect("last account failure remains deliverable")
        .expect("terminal failure batch");
    let events = terminal.into_provider_events();
    let failure = events
        .iter()
        .filter_map(ProviderEvent::wire_event)
        .find(|wire| wire.event_type() == Some("response.failed"))
        .expect("last upstream response.failed");
    assert_eq!(
        failure
            .data()
            .pointer("/response/id")
            .and_then(Value::as_str),
        Some("response-only-failed")
    );
    block_on(session.commit_downstream(Some(200))).expect("commit terminal failure");
    assert!(matches!(
        block_on(session.next_event()),
        Err(EngineError::Provider(_))
    ));

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    assert_eq!(
        session.budget_charge().amount_usd.scaled(),
        500,
        "restoring a discarded failure must not charge its usage twice"
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert_eq!(
        state.finalizations[0].client_response_id.as_deref(),
        Some("response-only-failed")
    );
    assert_eq!(
        state.finalizations[0].upstream_response_id.as_deref(),
        Some("response-only-failed")
    );
}

#[test]
fn atomic_response_failed_keeps_collect_uncommitted_error_semantics() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_required",
        items: vec![Err(atomic_response_failed(
            "response-required-failed",
            "required account failure",
        ))],
    }]);
    let required = ProviderAccountId::new("acct_required").expect("required account");
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        Some(required),
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let error = block_on(session.collect_uncommitted())
        .expect_err("non-streaming collection must keep returning the typed failure");
    assert!(matches!(error, EngineError::Provider(_)));
    assert!(session.is_finalized());
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.commits, 0);
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
}

#[test]
fn native_continuation_explicit_429_is_not_retried() {
    let continuation = NativeContinuationPin::new(
        PreviousResponseId::new("previous-secret-id"),
        PreviousResponseId::new("provider-native-id"),
        ClientApiKeyId::new("key_client_1").expect("client key"),
        ProviderKind::new("openai").expect("provider"),
        ProviderAccountId::new("acct_first").expect("account"),
    );
    let (store, provider) = terminal_non_idempotent_failure(
        vec![Err(ProviderError::new(
            ProviderErrorKind::RateLimited,
            UpstreamSendState::Sent,
        )
        .with_status(429)
        .with_replay_safe())],
        Some(ContinuationBinding::Pinned(continuation)),
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 0);
}

#[test]
fn external_continuation_explicit_429_is_not_retried() {
    let opaque_response_id = format!("resp_{}\0opaque", "x".repeat(257));
    let continuation = ContinuationBinding::External(PreviousResponseId::new(opaque_response_id));
    let (store, provider) = terminal_non_idempotent_failure(
        vec![Err(ProviderError::new(
            ProviderErrorKind::RateLimited,
            UpstreamSendState::Sent,
        )
        .with_status(429)
        .with_replay_safe())],
        Some(continuation),
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    assert!(
        provider.contexts.lock().expect("contexts lock")[0]
            .account_state_owner()
            .is_none()
    );
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 0);
}

#[test]
fn ambiguous_send_state_stops_retry() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Transport,
                UpstreamSendState::Ambiguous,
            )
            .with_replay_safe())],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error =
        block_on(session.collect_uncommitted()).expect_err("ambiguous send cannot be replayed");

    assert!(matches!(
        error,
        gateway_core::engine::EngineError::Provider(_)
    ));
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.intermediate_failures, 0);
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.outcome, ExecutionOutcome::Failed);
    assert_eq!(finalization.send_state, UpstreamSendState::Ambiguous);
    assert_eq!(session.budget_charge().amount_usd.scaled(), 0);
    assert_eq!(finalization.attempt_count, 1);
    assert!(!finalization.committed);
    assert!(finalization.latency_ms.is_some());
}

#[test]
fn ambiguous_pre_delivery_retry_marker_does_not_rotate_account() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let checkpoint = ProviderSessionState::new(
        "openai",
        Map::from_iter([("checkpoint".to_owned(), json!("opaque-state"))]),
    )
    .expect("provider checkpoint");
    let mut checkpoint_event = ProviderEvent::observation(ProviderResponseObservation::new(
        UpstreamTransport::new("websocket").expect("transport"),
    ));
    checkpoint_event.attach_session_update(checkpoint);
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![
                Ok(checkpoint_event),
                Err(
                    ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous)
                        .with_pre_delivery_retry(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("ambiguous payload state must suppress provider-requested replay");

    assert!(matches!(error, EngineError::Provider(_)));
    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].attempt_count, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
}

#[test]
fn ambiguous_transport_retry_is_rejected_even_with_provider_session_state() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let checkpoint = ProviderSessionState::new(
        "openai",
        Map::from_iter([("checkpoint".to_owned(), json!("opaque-state"))]),
    )
    .expect("provider checkpoint");
    let mut checkpoint_event = ProviderEvent::observation(ProviderResponseObservation::new(
        UpstreamTransport::new("websocket").expect("transport"),
    ));
    checkpoint_event.attach_session_update(checkpoint);
    let retry_index = NonZeroU32::new(1).expect("retry index");
    let (coordinator, store, provider) = coordinator(vec![
        Script::ObservedStream {
            account_id: "acct_first",
            items: vec![
                Ok(checkpoint_event),
                Err(
                    ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous)
                        .with_replay_safe()
                        .with_pre_delivery_transport_retry(retry_index, Duration::ZERO),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error =
        block_on(session.collect_uncommitted()).expect_err("ambiguous send cannot be replayed");
    assert!(matches!(error, EngineError::Provider(_)));
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].attempt_count, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
}

#[test]
fn pre_delivery_transport_fallback_retries_the_same_account_with_fallback_transport() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Transport,
                UpstreamSendState::NotSent,
            )
            .with_pre_delivery_transport_fallback())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("same-account fallback succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit winning response");

    let original = ProviderAccountId::new("acct_first").expect("account id");
    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].transport(), AttemptTransport::Default);
    assert_eq!(contexts[1].transport(), AttemptTransport::Fallback);
    assert_eq!(contexts[1].required_account(), Some(&original));
    assert!(!contexts[1].excluded_accounts().contains(&original));
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn provider_owned_transport_retries_keep_the_same_account_until_fallback() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let retry_one = NonZeroU32::new(1).expect("non-zero retry index");
    let retry_two = NonZeroU32::new(2).expect("non-zero retry index");
    let transport_error =
        || ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(
                transport_error().with_pre_delivery_transport_retry(retry_one, Duration::ZERO)
            )],
        },
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(
                transport_error().with_pre_delivery_transport_retry(retry_two, Duration::ZERO)
            )],
        },
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(transport_error().with_pre_delivery_transport_fallback())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("fallback succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit winning response");

    let original = ProviderAccountId::new("acct_first").expect("account id");
    let contexts = provider.contexts.lock().expect("contexts lock");
    assert_eq!(contexts.len(), 4);
    assert_eq!(contexts[0].transport(), AttemptTransport::Default);
    assert_eq!(contexts[1].transport(), AttemptTransport::Retry(retry_one));
    assert_eq!(contexts[2].transport(), AttemptTransport::Retry(retry_two));
    assert_eq!(contexts[3].transport(), AttemptTransport::Fallback);
    for context in contexts.iter().skip(1) {
        assert_eq!(context.required_account(), Some(&original));
        assert!(!context.excluded_accounts().contains(&original));
    }
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 3);
    assert_eq!(state.finalizations[0].attempt_count, 4);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

fn transient_rejection() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::UpstreamCapacityUnavailable,
        UpstreamSendState::Sent,
    )
    .with_replay_safe()
    .with_transient_retry(
        NonZeroU32::new(3).expect("retry budget"),
        Duration::ZERO,
        Duration::ZERO,
    )
}

#[test]
fn server_retry_advice_should_override_local_backoff_and_transport_fallback() {
    let advised_delay = Duration::from_millis(120);
    for rejection in [
        transient_rejection().with_transient_retry(
            NonZeroU32::MIN,
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent)
            .with_pre_delivery_transport_fallback(),
    ] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, _, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_first",
                items: vec![Err(rejection.with_retry_after(advised_delay))],
            },
            Script::Stream {
                account_id: "acct_first",
                items: complete_stream(None),
            },
        ]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        let started = std::time::Instant::now();
        let mut collection = Box::pin(session.collect_uncommitted());
        assert!(block_on(async { futures::poll!(&mut collection) }).is_pending());
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        block_on(collection).unwrap();
        assert!(started.elapsed() >= advised_delay);
        let contexts = provider.contexts.lock().unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(
            contexts[1].required_account(),
            Some(&ProviderAccountId::new("acct_first").unwrap())
        );
    }
}

#[test]
fn zero_server_retry_advice_should_override_nonzero_local_backoff() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, _) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(transient_rejection()
                .with_transient_retry(
                    NonZeroU32::MIN,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                )
                .with_retry_after(Duration::ZERO))],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let mut collection = Box::pin(session.collect_uncommitted());
    assert!(matches!(
        block_on(async { futures::poll!(&mut collection) }),
        std::task::Poll::Ready(Ok(_))
    ));
}

#[test]
fn terminal_rejection_should_prevent_not_sent_rotation_and_explicit_transport_recovery() {
    for fallback in [false, true] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let mut error =
            ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
                .with_replay_safe()
                .with_retry_prohibited();
        if fallback {
            error = error.with_pre_delivery_transport_fallback();
        }
        assert!(error.clone().retry_is_prohibited());
        let (coordinator, _, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: vec![Err(error)],
        }]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        assert!(matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Provider(_))
        ));
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
    }
}

#[test]
fn transient_retries_back_off_on_the_same_account_then_rotate_with_a_fresh_budget() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let mut scripts = (0..4)
        .map(|_| Script::Stream {
            account_id: "acct_first",
            items: vec![Err(transient_rejection())],
        })
        .collect::<Vec<_>>();
    scripts.push(Script::Stream {
        account_id: "acct_second",
        items: vec![Err(transient_rejection())],
    });
    scripts.push(Script::Stream {
        account_id: "acct_second",
        items: complete_stream(None),
    });
    let (coordinator, store, provider) = coordinator(scripts);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("second account succeeds");
    block_on(session.commit_downstream(Some(200))).expect("commit");
    let first = ProviderAccountId::new("acct_first").expect("account");
    let second = ProviderAccountId::new("acct_second").expect("account");
    let contexts = provider.contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 6);
    for context in &contexts[1..4] {
        assert_eq!(context.required_account(), Some(&first));
        assert!(!context.excluded_accounts().contains(&first));
    }
    assert_eq!(contexts[4].required_account(), None);
    assert!(contexts[4].excluded_accounts().contains(&first));
    assert_eq!(contexts[5].required_account(), Some(&second));
    assert!(!contexts[5].excluded_accounts().contains(&second));
    assert!(
        contexts
            .iter()
            .all(|context| context.transport() == AttemptTransport::Default)
    );
    assert_eq!(
        store.state.lock().expect("store").finalizations[0].attempt_count,
        6
    );
}

#[test]
fn transient_retries_consume_the_total_routing_budget() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let max_attempts = route_plan.max_attempts();
    let scripts = (0..max_attempts.get())
        .map(|_| Script::Stream {
            account_id: "acct_first",
            items: vec![Err(transient_rejection().with_transient_retry(
                max_attempts,
                Duration::ZERO,
                Duration::ZERO,
            ))],
        })
        .collect();
    let (coordinator, store, provider) = coordinator(scripts);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Provider(_))
    ));
    assert_eq!(
        provider.contexts.lock().expect("contexts").len(),
        max_attempts.get() as usize
    );
    assert_eq!(
        store.state.lock().expect("store").finalizations[0].attempt_count,
        max_attempts.get()
    );
}

#[test]
fn transient_retry_intent_does_not_supply_replay_proof_or_override_ambiguous_send() {
    for (send_state, prove_safe) in [
        (UpstreamSendState::Sent, false),
        (UpstreamSendState::Ambiguous, true),
    ] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let mut error = ProviderError::new(ProviderErrorKind::Unavailable, send_state)
            .with_transient_retry(NonZeroU32::MIN, Duration::ZERO, Duration::ZERO);
        if prove_safe {
            error = error.with_replay_safe();
        }
        let (coordinator, _, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: vec![Err(error)],
        }]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .expect("start execution");
        assert!(matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Provider(_))
        ));
        assert_eq!(provider.contexts.lock().expect("contexts").len(), 1);
    }
}

#[test]
fn transient_backoff_obeys_request_deadline() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_first",
        items: vec![Err(transient_rejection().with_transient_retry(
            NonZeroU32::MIN,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ))],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_millis(100)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Deadline)
    ));
    assert_eq!(provider.contexts.lock().expect("contexts").len(), 1);
}

#[test]
fn transient_retry_keeps_http_fallback_for_the_pinned_account() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Transport,
                UpstreamSendState::NotSent,
            )
            .with_pre_delivery_transport_fallback())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(transient_rejection())],
        },
        Script::Stream {
            account_id: "acct_first",
            items: complete_stream(None),
        },
    ]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    block_on(session.collect_uncommitted()).expect("fallback succeeds after backoff");
    let contexts = provider.contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 3);
    assert_eq!(contexts[2].transport(), AttemptTransport::Fallback);
}

#[test]
fn transient_retry_is_suppressed_for_required_accounts_and_committed_output() {
    for committed in [false, true] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let mut items = Vec::new();
        if committed {
            items.push(Ok(GatewayEvent::Started(ResponseMeta::new(
                "response-visible",
                "gpt-5",
            ))));
        }
        items.push(Err(transient_rejection()));
        let (coordinator, _, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items,
        }]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            (!committed).then(|| ProviderAccountId::new("acct_first").expect("account")),
            None,
            CancellationToken::new(),
        ))
        .expect("start execution");
        if committed {
            block_on(session.next_event())
                .expect("first event")
                .expect("visible event");
            block_on(session.commit_downstream(Some(200))).expect("commit output");
        }
        assert!(matches!(
            block_on(session.next_event()),
            Err(EngineError::Provider(_))
        ));
        assert_eq!(provider.contexts.lock().expect("contexts").len(), 1);
    }
}

#[test]
fn transient_backoff_can_be_cancelled_before_the_next_attempt() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_first",
        items: vec![Err(transient_rejection().with_transient_retry(
            NonZeroU32::MIN,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ))],
    }]);
    let cancellation = CancellationToken::new();
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        cancellation.clone(),
    ))
    .expect("start execution");
    let cancel = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        cancellation.cancel();
    });
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Cancelled)
    ));
    cancel.join().expect("cancel task");
    assert_eq!(provider.contexts.lock().expect("contexts").len(), 1);
}

#[test]
fn final_capacity_exhaustion_returns_the_last_retryable_upstream_failure() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let raw_body = Bytes::from_static(b"{\"error\":\"last upstream body\"}\x00");
    let upstream_error =
        ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent)
            .with_pre_delivery_retry()
            .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                "message too big",
                Some("1009".to_owned()),
                Some("websocket_close_error".to_owned()),
            ))
            .with_client_visible_upstream_response(ClientVisibleUpstreamResponse::new(
                502,
                Some(b"application/json".to_vec()),
                raw_body.clone(),
            ));
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(upstream_error)],
        },
        Script::Error(ProviderError::new(
            ProviderErrorKind::AccountCapacityUnavailable,
            UpstreamSendState::NotSent,
        )),
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("empty selection must return the retained upstream failure");
    let EngineError::Provider(error) = error else {
        panic!("expected retained provider error");
    };

    assert_eq!(error.kind(), ProviderErrorKind::Transport);
    let detail = error
        .client_visible_upstream_error()
        .expect("structured upstream detail");
    assert_eq!(detail.message(), "message too big");
    assert_eq!(detail.code(), Some("1009"));
    assert_eq!(detail.error_type(), Some("websocket_close_error"));
    let response = error
        .client_visible_upstream_response()
        .expect("original upstream response");
    assert_eq!(response.status(), 502);
    assert_eq!(response.body(), &raw_body);

    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
}

#[test]
fn provider_infrastructure_failure_does_not_restore_the_last_upstream_failure() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let upstream_error =
        ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent)
            .with_pre_delivery_retry()
            .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                "upstream marker",
                Some("upstream_marker".to_owned()),
                Some("upstream_error".to_owned()),
            ))
            .with_client_visible_upstream_response(ClientVisibleUpstreamResponse::new(
                502,
                Some(b"application/json".to_vec()),
                Bytes::from_static(b"{\"error\":\"upstream marker\"}"),
            ));
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![Err(upstream_error)],
        },
        Script::Error(ProviderError::new(
            ProviderErrorKind::ProviderInfrastructureUnavailable,
            UpstreamSendState::NotSent,
        )),
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("provider infrastructure failure must remain terminal");
    let EngineError::Provider(error) = error else {
        panic!("expected provider error");
    };

    assert_eq!(
        error.kind(),
        ProviderErrorKind::ProviderInfrastructureUnavailable
    );
    assert!(error.client_visible_upstream_error().is_none());
    assert!(error.client_visible_upstream_response().is_none());
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 2);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
}

#[test]
fn pre_delivery_retry_marker_is_ignored_after_downstream_commit() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "response-visible",
                    "gpt-5",
                ))),
                Err(
                    ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous)
                        .with_pre_delivery_transport_fallback(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let first = block_on(session.next_event())
        .expect("first event")
        .expect("visible event");
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    block_on(session.commit_downstream(Some(200))).expect("commit first event");
    let error = block_on(session.next_event()).expect_err("committed output cannot be replayed");

    assert!(matches!(error, EngineError::Provider(_)));
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert!(state.finalizations[0].committed);
}

#[test]
fn structural_event_before_replay_safe_failure_should_switch_account_before_commit() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![
        Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "response-1",
                    "gpt-5",
                ))),
                Err(
                    ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Sent)
                        .with_replay_safe(),
                ),
            ],
        },
        Script::Stream {
            account_id: "acct_second",
            items: complete_stream(None),
        },
    ]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let first = block_on(session.next_event())
        .expect("first event")
        .expect("started event");
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    assert_eq!(first.into_provider_events().len(), 1);
    block_on(session.commit_downstream(Some(200))).expect("commit first event");
    block_on(session.next_event()).expect_err("committed stream failure must not be replayed");
    assert_eq!(
        session.provider_attempt_outcomes(),
        &[ProviderAttemptOutcome::Failed {
            provider_kind: ProviderKind::new("openai").expect("provider"),
            error_kind: ProviderErrorKind::Transport,
        }]
    );
    assert_eq!(provider.contexts.lock().expect("contexts lock").len(), 1);
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.commits, 1);
    assert_eq!(state.intermediate_failures, 0);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert!(state.finalizations[0].committed);
    assert_eq!(state.finalizations[0].client_status_code, Some(200));
}

#[test]
fn cancellation_before_pending_delivery_commit_reaches_terminal_state() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_first",
        items: complete_stream(None),
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let first = block_on(session.next_event())
        .expect("first event")
        .expect("started event");
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );

    block_on(session.cancel_and_finalize()).expect("cancel finalization");

    assert!(session.is_finalized());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.commits, 0);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Cancelled);
    assert!(!state.finalizations[0].committed);
    assert_eq!(session.budget_charge().amount_usd.scaled(), 0);
}

#[test]
fn no_eligible_account_before_stream_records_failure_without_fabricating_an_attempt() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Error(ProviderError::new(
        ProviderErrorKind::NoEligibleAccount,
        UpstreamSendState::NotSent,
    ))]);

    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let error = block_on(session.collect_uncommitted())
        .expect_err("provider failed before returning metadata");

    assert!(matches!(
        error,
        gateway_core::engine::EngineError::Provider(ref error)
            if error.kind() == ProviderErrorKind::NoEligibleAccount
    ));
    assert!(session.provider_attempt_outcomes().is_empty());
    assert!(session.is_finalized());
    block_on(session.record_client_status(503)).expect("HTTP error status");
    block_on(session.cancel_and_finalize()).expect("repeated finalization");
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.created, 1);
    assert!(state.attempts.is_empty());
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.recorded_statuses, [503]);
    let finalization = &state.finalizations[0];
    assert_eq!(finalization.outcome, ExecutionOutcome::Failed);
    assert_eq!(finalization.attempt_count, 0);
    assert_eq!(finalization.send_state, UpstreamSendState::NotSent);
    assert_eq!(
        finalization.error_kind,
        Some(GatewayErrorKind::NoAvailableProvider)
    );
    assert_eq!(finalization.upstream_transport, None);
    assert_eq!(finalization.total_tokens, None);
    assert_eq!(finalization.cost_ticks, None);
    assert_eq!(session.budget_charge().amount_usd.scaled(), 0);
    let trace: Value =
        serde_json::from_str(finalization.diagnostic_trace_json.as_deref().unwrap()).unwrap();
    let events = trace["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["stage"] == "attempt.failed")
    );
    assert_eq!(events.last().unwrap()["stage"], "request.finished");
}

#[test]
fn expired_deadline_finalizes_without_calling_provider() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, provider) = coordinator(vec![]);
    let deadline = SystemTime::now() - Duration::from_secs(1);
    let mut request = model_request(&operation, deadline);
    request.started_at = deadline - Duration::from_secs(30);

    let error = match block_on(coordinator.start(
        request,
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    )) {
        Ok(_) => panic!("deadline already elapsed"),
        Err(error) => error,
    };

    assert!(matches!(error, gateway_core::engine::EngineError::Deadline));
    assert!(provider.contexts.lock().expect("contexts lock").is_empty());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.created, 1);
    assert!(state.attempts.is_empty());
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.finalizations[0].attempt_count, 0);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
    assert_eq!(
        state.finalizations[0].error_kind,
        Some(GatewayErrorKind::Timeout)
    );
}

#[test]
fn preparation_failures_preserve_the_error_when_observation_writes_fail() {
    for failure in [
        None,
        Some(StoreWriteFailure::Create),
        Some(StoreWriteFailure::Finalize),
    ] {
        let operation = generate_operation();
        let store = Arc::new(failure.map_or_else(FakeStore::default, FakeStore::failing));
        let (coordinator, store, provider) = coordinator_with_store(
            vec![Script::Error(
                ProviderError::new(
                    ProviderErrorKind::ProviderInfrastructureUnavailable,
                    UpstreamSendState::NotSent,
                )
                .with_upstream_request_id(OpaqueUpstreamValue::new("prepare-request-id")),
            )],
            store,
        );
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation.clone(),
            plan(&operation),
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();

        assert!(matches!(
            block_on(session.next_event()),
            Err(EngineError::Provider(ref error))
                if error.kind() == ProviderErrorKind::ProviderInfrastructureUnavailable
        ));
        assert!(session.is_finalized());
        let charge = session.budget_charge();
        block_on(session.cancel_and_finalize()).unwrap();
        assert!(block_on(session.next_event()).unwrap().is_none());
        assert_eq!(session.budget_charge().completed_at, charge.completed_at);
        assert_eq!(charge.amount_usd.scaled(), 0);
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        let state = store.state.lock().unwrap();
        assert!(state.attempts.is_empty());
        assert_eq!(
            state.created,
            usize::from(failure != Some(StoreWriteFailure::Create))
        );
        assert_eq!(state.finalizations.len(), usize::from(failure.is_none()));
        if failure.is_none() {
            assert_eq!(
                state.finalizations[0].upstream_request_id.as_deref(),
                Some("prepare-request-id")
            );
        }
    }
}

#[test]
fn interrupted_early_finalization_resumes_the_same_write_and_keeps_the_original_failure() {
    block_on(async {
        for create in [true, false] {
            for detach in [true, false] {
                let (release, gate) = oneshot::channel();
                let store = Arc::new(if create {
                    FakeStore {
                        create_gate: Mutex::new(Some(gate)),
                        ..FakeStore::default()
                    }
                } else {
                    FakeStore {
                        finalize_gate: Mutex::new(Some(gate)),
                        ..FakeStore::default()
                    }
                });
                let operation = generate_operation();
                let (coordinator, store, provider) = coordinator_with_store(
                    vec![Script::Error(ProviderError::new(
                        ProviderErrorKind::NoEligibleAccount,
                        UpstreamSendState::NotSent,
                    ))],
                    store,
                );
                let mut session = coordinator
                    .start(
                        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                        operation.clone(),
                        plan(&operation),
                        None,
                        None,
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                for _ in 0..2 {
                    let mut next = Box::pin(session.next_event());
                    assert!(futures::poll!(next.as_mut()).is_pending());
                    drop(next);
                    assert!(!session.is_finalized());
                    assert_eq!(store.recovery_leases.load(Ordering::SeqCst), 1);
                    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
                }
                let completed_at = session.budget_charge().completed_at;
                release
                    .send(())
                    .expect("original write must still be alive");
                if detach {
                    session.cancel_and_finalize().await.unwrap();
                } else {
                    assert!(session.next_event().await.unwrap().is_none());
                }
                assert!(session.is_finalized());
                assert_eq!(store.recovery_leases.load(Ordering::SeqCst), 0);
                session.cancel_and_finalize().await.unwrap();
                assert_eq!(session.budget_charge().completed_at, completed_at);
                let state = store.state.lock().unwrap();
                assert_eq!(state.created, 1);
                assert!(state.attempts.is_empty());
                assert_eq!(state.finalizations.len(), 1);
                assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
                assert_eq!(
                    state.finalizations[0].error_kind,
                    Some(GatewayErrorKind::NoAvailableProvider)
                );
                let trace: Value = serde_json::from_str(
                    state.finalizations[0]
                        .diagnostic_trace_json
                        .as_deref()
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    trace["events"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|event| event["stage"] == "request.finished")
                        .count(),
                    1
                );
            }
        }
    });
}

#[test]
fn cancellation_before_or_during_preparation_records_a_zero_attempt_terminal_request() {
    block_on(async {
        for before_start in [true, false] {
            let operation = generate_operation();
            let (coordinator, store, provider) = coordinator(vec![Script::Pending]);
            let cancellation = CancellationToken::new();
            if before_start {
                cancellation.cancel();
            }
            let result = coordinator
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation.clone(),
                    plan(&operation),
                    None,
                    None,
                    cancellation.clone(),
                )
                .await;
            if before_start {
                assert!(matches!(result, Err(EngineError::Cancelled)));
                assert!(provider.contexts.lock().unwrap().is_empty());
            } else {
                let mut session = result.unwrap();
                let mut next = Box::pin(session.next_event());
                assert!(futures::poll!(next.as_mut()).is_pending());
                cancellation.cancel();
                assert!(matches!(next.await, Err(EngineError::Cancelled)));
                assert!(session.is_finalized());
                session.cancel_and_finalize().await.unwrap();
                assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            }
            let state = store.state.lock().unwrap();
            assert_eq!(state.created, 1);
            assert!(state.attempts.is_empty());
            assert_eq!(state.finalizations.len(), 1);
            assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Cancelled);
            assert_eq!(
                state.finalizations[0].send_state,
                UpstreamSendState::NotSent
            );
            assert_eq!(state.finalizations[0].attempt_count, 0);
        }
    });
}

#[test]
fn deadline_during_preparation_records_failure_without_an_upstream_attempt() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![Script::Pending]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_millis(300)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.next_event()),
        Err(EngineError::Deadline)
    ));
    assert!(session.provider_attempt_outcomes().is_empty());
    assert!(session.is_finalized());
    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
    let state = store.state.lock().unwrap();
    assert_eq!(state.created, 1);
    assert!(state.attempts.is_empty());
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(
        state.finalizations[0].error_kind,
        Some(GatewayErrorKind::Timeout)
    );
    assert_eq!(state.finalizations[0].attempt_count, 0);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
}

#[test]
fn pre_stream_send_facts_cannot_be_rewritten_as_a_zero_attempt_not_sent_request() {
    for send_state in [UpstreamSendState::Sent, UpstreamSendState::Ambiguous] {
        let operation = generate_operation();
        let (coordinator, store, _) = coordinator(vec![Script::Error(ProviderError::new(
            ProviderErrorKind::Transport,
            send_state,
        ))]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation.clone(),
            plan(&operation),
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        assert!(matches!(
            block_on(session.next_event()),
            Err(EngineError::Provider(ref error)) if error.send_state() == send_state
        ));
        assert!(session.is_finalized());
        block_on(session.cancel_and_finalize()).unwrap();
        let state = store.state.lock().unwrap();
        assert_eq!(state.created, 0);
        assert!(state.attempts.is_empty());
        assert!(state.finalizations.is_empty());
    }
}

#[test]
fn failed_first_attempt_write_is_not_replaced_with_a_fabricated_zero_attempt_record() {
    let operation = generate_operation();
    let (coordinator, store, _) = coordinator_with_store(
        vec![Script::Stream {
            account_id: "acct_first",
            items: vec![Err(ProviderError::new(
                ProviderErrorKind::Transport,
                UpstreamSendState::Sent,
            ))],
        }],
        Arc::new(FakeStore::failing(StoreWriteFailure::Create)),
    );
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.next_event()),
        Err(EngineError::Provider(_))
    ));
    assert!(session.is_finalized());
    let state = store.state.lock().unwrap();
    assert_eq!(state.created, 0);
    assert!(state.finalizations.is_empty());
}

#[test]
fn interrupted_first_attempt_write_never_restarts_selection_or_creates_a_zero_attempt_request() {
    block_on(async {
        for cancel in [true, false] {
            let (release, gate) = oneshot::channel();
            let operation = generate_operation();
            let (coordinator, store, provider) = coordinator_with_store(
                vec![Script::Stream {
                    account_id: "acct_first",
                    items: complete_stream(None),
                }],
                Arc::new(FakeStore {
                    create_gate: Mutex::new(Some(gate)),
                    ..FakeStore::default()
                }),
            );
            let mut session = coordinator
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation.clone(),
                    plan(&operation),
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut next = Box::pin(session.next_event());
            assert!(futures::poll!(next.as_mut()).is_pending());
            drop(next);
            assert_eq!(store.state.lock().unwrap().created, 1);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 0);
            // 首写属于可丢弃观测；丢失返回确认后不臆测入库成功，也不能退回零次重新创建。
            assert!(release.send(()).is_err());
            if cancel {
                session.cancel_and_finalize().await.unwrap();
            } else {
                session.collect_uncommitted().await.unwrap();
                session.commit_downstream(Some(200)).await.unwrap();
            }
            assert!(session.is_finalized());
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            let trace = session.trace().snapshot().unwrap();
            let events = trace["events"].as_array().unwrap();
            assert_eq!(events.last().unwrap()["data"]["attempts"], 1);
            let state = store.state.lock().unwrap();
            assert_eq!(state.created, 1);
            assert!(state.finalizations.is_empty());
            drop(state);
            drop(session);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
        }
    });
}

#[test]
fn rejected_cold_stream_releases_its_lease_before_waiting_for_observation_writes() {
    block_on(async {
        let (release, gate) = oneshot::channel();
        let operation = generate_operation();
        let (coordinator, store, provider) = coordinator_with_store(
            vec![Script::Stream {
                account_id: "acct_wrong",
                items: complete_stream(None),
            }],
            Arc::new(FakeStore {
                create_gate: Mutex::new(Some(gate)),
                ..FakeStore::default()
            }),
        );
        let mut session = coordinator
            .start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation.clone(),
                plan(&operation),
                Some(ProviderAccountId::new("acct_required").unwrap()),
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut next = Box::pin(session.next_event());
        assert!(futures::poll!(next.as_mut()).is_pending());
        assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        assert!(matches!(
            next.await,
            Err(EngineError::RequiredAccountMismatch)
        ));
        let state = store.state.lock().unwrap();
        assert_eq!(state.created, 1);
        assert!(state.attempts.is_empty());
        assert_eq!(state.finalizations[0].attempt_count, 0);
    });
}

#[test]
fn charged_terminal_write_resumes_without_replacing_success_failure_or_send_facts() {
    block_on(async {
        for failed in [true, false] {
            for cancel in [true, false] {
                let (release, gate) = oneshot::channel();
                let operation = generate_operation();
                let mut items = vec![
                    Ok(GatewayEvent::Started(ResponseMeta::new(
                        "charged-response",
                        "gpt-5",
                    ))),
                    Ok(GatewayEvent::ProviderCost(
                        ProviderReportedCost::from_usd_ticks(25).unwrap(),
                    )),
                ];
                items.push(if failed {
                    Err(ProviderError::new(
                        ProviderErrorKind::Unavailable,
                        UpstreamSendState::Sent,
                    ))
                } else {
                    Ok(GatewayEvent::Completed(ResponseMeta::new(
                        "charged-response",
                        "gpt-5",
                    )))
                });
                let (coordinator, store, provider) = coordinator_with_store(
                    vec![Script::Stream {
                        account_id: "acct_first",
                        items,
                    }],
                    Arc::new(FakeStore {
                        finalize_gate: Mutex::new(Some(gate)),
                        ..FakeStore::default()
                    }),
                );
                let mut session = coordinator
                    .start(
                        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                        operation.clone(),
                        plan(&operation),
                        None,
                        None,
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                if failed {
                    let mut collect = Box::pin(session.collect_uncommitted());
                    assert!(futures::poll!(collect.as_mut()).is_pending());
                } else {
                    session.collect_uncommitted().await.unwrap();
                    let mut commit = Box::pin(session.commit_downstream(Some(200)));
                    assert!(futures::poll!(commit.as_mut()).is_pending());
                }
                assert!(!session.is_finalized());
                let charge = session.budget_charge();
                assert_eq!(charge.amount_usd.scaled(), 25);
                release
                    .send(())
                    .expect("the original finalization still owns its gate");
                if cancel {
                    session.cancel_and_finalize().await.unwrap();
                } else {
                    assert!(session.next_event().await.unwrap().is_none());
                }
                assert!(session.is_finalized());
                assert_eq!(session.budget_charge().amount_usd, charge.amount_usd);
                assert_eq!(session.budget_charge().completed_at, charge.completed_at);
                assert_eq!(provider.contexts.lock().unwrap().len(), 1);
                let state = store.state.lock().unwrap();
                assert_eq!(state.created, 1);
                assert_eq!(state.attempts.len(), 1);
                assert_eq!(state.finalizations.len(), 1);
                let finalization = &state.finalizations[0];
                assert_eq!(finalization.attempt_count, 1);
                assert_eq!(finalization.send_state, UpstreamSendState::Sent);
                assert_eq!(finalization.cost_ticks, Some(25));
                assert_eq!(
                    finalization.outcome,
                    if failed {
                        ExecutionOutcome::Failed
                    } else {
                        ExecutionOutcome::Succeeded
                    }
                );
            }
        }
    });
}

#[test]
fn deadline_after_commit_is_incomplete_without_upstream_failure() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::HangingStream {
        account_id: "acct_first",
        items: vec![Ok(GatewayEvent::Started(ResponseMeta::new(
            "response-1",
            "gpt-5",
        )))],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_millis(300)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");
    let first = block_on(session.next_event())
        .expect("first event")
        .expect("started event");
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    block_on(session.commit_downstream(Some(200))).expect("commit started event");

    let error = block_on(session.next_event()).expect_err("deadline elapses mid-stream");

    assert!(matches!(error, EngineError::Deadline));
    // 网关自身请求预算到期不是上游超时；已在交付中的长流集中到期
    // 不得作为 provider Timeout 计入熔断。
    assert!(session.provider_attempt_outcomes().is_empty());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert!(state.finalizations[0].committed);
}

#[test]
fn deadline_before_first_event_records_no_upstream_failure() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::HangingStream {
        account_id: "acct_first",
        items: Vec::new(),
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_millis(300)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .expect("start execution");

    let error = block_on(session.next_event()).expect_err("deadline elapses before first event");

    assert!(matches!(error, EngineError::Deadline));
    assert!(session.provider_attempt_outcomes().is_empty());
    let state = store.state.lock().expect("store lock");
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Failed);
    assert!(!state.finalizations[0].committed);
}

#[test]
fn connection_retry_requires_not_sent_and_keeps_the_same_account() {
    for sent in [UpstreamSendState::NotSent, UpstreamSendState::Ambiguous] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, store, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_first",
                items: vec![Err(ProviderError::new(ProviderErrorKind::Transport, sent)
                    .with_connection_retry(AttemptTransport::Fallback))],
            },
            Script::Stream {
                account_id: "acct_first",
                items: complete_stream(None),
            },
        ]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        let result = block_on(session.collect_uncommitted());
        let contexts = provider.contexts.lock().unwrap();
        if sent == UpstreamSendState::NotSent {
            result.unwrap();
            block_on(session.commit_downstream(Some(200))).unwrap();
            assert_eq!(contexts.len(), 2);
            assert_eq!(contexts[1].transport(), AttemptTransport::Fallback);
            assert_eq!(
                contexts[1].required_account().unwrap().as_str(),
                "acct_first"
            );
            assert!(contexts[1].excluded_accounts().is_empty());
            // Every attempt observes the same budget object.
            contexts[0].connection_budget().complete();
            assert!(contexts[1].connection_budget().begin().unwrap().is_none());
            assert_eq!(store.state.lock().unwrap().intermediate_failures, 1);
        } else {
            assert!(result.is_err());
            assert_eq!(contexts.len(), 1);
            assert_eq!(store.state.lock().unwrap().intermediate_failures, 0);
        }
    }
}

#[derive(Clone, Copy)]
enum AttemptMiddlewareMode {
    OverrideFast,
    Pass,
    RewriteFacts,
    FailAfterRead,
    ShortCircuit,
    ShortCircuitAfterRetry,
    EarlyEnd,
    RequireImage,
    FailWhileStalled,
    ForgeCheckpoint,
    ResumeRead,
    HoldError,
    WaitBeforeNext,
    ObservationInterrupted,
}
struct TestAttemptMiddleware {
    mode: AttemptMiddlewareMode,
    store: Arc<FakeStore>,
    contexts: Arc<Mutex<Vec<AttemptContext>>>,
}
impl gateway_core::engine::attempt_middleware::AttemptMiddleware for TestAttemptMiddleware {
    fn invoke(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
        next: gateway_core::engine::attempt_middleware::AttemptNext,
    ) -> futures::future::BoxFuture<
        'static,
        Result<
            gateway_core::engine::attempt_middleware::AttemptOutput,
            gateway_core::engine::middleware::MiddlewareError,
        >,
    > {
        use gateway_core::engine::middleware::MiddlewareError;
        let mode = self.mode;
        let store = self.store.clone();
        self.contexts.lock().unwrap().push(context.clone());
        let future: futures::future::BoxFuture<
            'static,
            Result<gateway_core::engine::attempt_middleware::AttemptEvents, MiddlewareError>,
        > = Box::pin(async move {
            if matches!(mode, AttemptMiddlewareMode::ShortCircuit)
                || (matches!(mode, AttemptMiddlewareMode::ShortCircuitAfterRetry)
                    && context.attempt_index().get() > 1)
            {
                return Ok(Box::pin(futures::stream::iter(
                    complete_stream(None)
                        .into_iter()
                        .map(|value| Ok(ProviderEvent::canonical(value.unwrap()))),
                ))
                    as gateway_core::engine::attempt_middleware::AttemptEvents);
            }
            if matches!(mode, AttemptMiddlewareMode::WaitBeforeNext) {
                futures_timer::Delay::new(Duration::from_millis(10)).await;
            }
            let operation = if matches!(mode, AttemptMiddlewareMode::OverrideFast) {
                assert_eq!(
                    context.fast_mode(),
                    gateway_core::account::FastMode::Enabled
                );
                let Operation::Generate(request) = request.operation() else {
                    panic!("generate expected");
                };
                assert_eq!(
                    request.protocol_payload().body()["service_tier"],
                    "priority"
                );
                let mut body = request.protocol_payload().body().clone();
                body.insert("service_tier".into(), json!("flex"));
                Operation::Generate(request.clone().with_protocol_body(body))
            } else if matches!(mode, AttemptMiddlewareMode::RequireImage) {
                image_generate_operation()
            } else {
                request.operation().clone()
            };
            let mut source = next.run(operation).await?;
            assert_eq!(source.metadata().provider(), request.candidate().provider());
            if !matches!(mode, AttemptMiddlewareMode::ObservationInterrupted) {
                assert_eq!(
                    store.state.lock().unwrap().attempts.len(),
                    context.attempt_index().get() as usize,
                    "cold attempt must be registered before plugin reads"
                );
            }
            if matches!(mode, AttemptMiddlewareMode::FailWhileStalled) {
                source.next_event().await?;
                source.next_event().await?;
                let read = Box::pin(source.next_event());
                let timeout = Box::pin(futures_timer::Delay::new(Duration::from_millis(10)));
                assert!(matches!(
                    futures::future::select(read, timeout).await,
                    futures::future::Either::Right(_)
                ));
                return Err(MiddlewareError::Fault);
            }
            if matches!(mode, AttemptMiddlewareMode::ResumeRead) {
                let mut read = Box::pin(source.next_event());
                assert!(futures::poll!(read.as_mut()).is_pending());
                drop(read);
                return Ok(source.into_events());
            }
            if matches!(mode, AttemptMiddlewareMode::HoldError) {
                while source.next_event().await.is_ok() {}
                return futures::future::pending().await;
            }
            if matches!(
                mode,
                AttemptMiddlewareMode::Pass
                    | AttemptMiddlewareMode::ShortCircuitAfterRetry
                    | AttemptMiddlewareMode::WaitBeforeNext
                    | AttemptMiddlewareMode::ObservationInterrupted
            ) {
                return Ok(source.into_events());
            }
            if matches!(mode, AttemptMiddlewareMode::EarlyEnd) {
                return Ok(Box::pin(futures::stream::empty())
                    as gateway_core::engine::attempt_middleware::AttemptEvents);
            }
            while source.next_event().await?.is_some() {}
            if matches!(mode, AttemptMiddlewareMode::FailAfterRead) {
                return Err(MiddlewareError::Fault);
            }
            let mut projected = vec![
                Ok(ProviderEvent::canonical(GatewayEvent::Started(
                    ResponseMeta::new("forged-response", "forged-model"),
                ))),
                Ok(ProviderEvent::canonical(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(999).unwrap(),
                ))),
                Ok(ProviderEvent::canonical(GatewayEvent::Completed(
                    ResponseMeta::new("forged-response", "forged-model"),
                ))),
            ];
            if matches!(mode, AttemptMiddlewareMode::ForgeCheckpoint) {
                projected[0].as_mut().unwrap().attach_session_update(
                    ProviderSessionState::new(
                        "openai",
                        json!({"forged":true}).as_object().unwrap().clone(),
                    )
                    .unwrap(),
                );
            }
            Ok(Box::pin(futures::stream::iter(projected))
                as gateway_core::engine::attempt_middleware::AttemptEvents)
        });
        Box::pin(async move { future.await.map(Into::into) })
    }
}
fn costed_attempt() -> Script {
    Script::Stream {
        account_id: "acct_one",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                "original", "gpt-5",
            ))),
            Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(123).unwrap(),
            )),
            Ok(GatewayEvent::Completed(ResponseMeta::new(
                "original", "gpt-5",
            ))),
        ],
    }
}
fn attempt_hook(mode: AttemptMiddlewareMode, store: &Arc<FakeStore>) -> Arc<TestAttemptMiddleware> {
    Arc::new(TestAttemptMiddleware {
        mode,
        store: store.clone(),
        contexts: Arc::new(Mutex::new(vec![])),
    })
}
#[test]
fn attempt_middleware_observes_original_cost_before_rewriting_display_facts() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::RewriteFacts, &store);
    let deadline = SystemTime::now() + Duration::from_secs(30);
    let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
    let mut session = block_on(coordinator.start(
        model_request(&operation, deadline),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let output = block_on(session.collect_uncommitted()).unwrap();
    assert!(output.iter().any(|event| event.canonical_facts().iter().any(|fact| matches!(fact, GatewayEvent::ProviderCost(cost) if cost.total().amount().scaled() == 999))));
    block_on(session.commit_downstream(Some(200))).unwrap();
    let state = store.state.lock().unwrap();
    assert_eq!(state.commits, 1);
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.finalizations[0].cost_ticks, Some(123));
    assert_eq!(
        state.finalizations[0].upstream_response_id.as_deref(),
        Some("original")
    );
    assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
    assert_eq!(hook.contexts.lock().unwrap()[0].deadline(), deadline.into());
    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
}
#[test]
fn attempt_middleware_failure_after_read_keeps_cost_and_does_not_retry() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![costed_attempt(), costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::FailAfterRead, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Middleware)
    ));
    assert!(session.is_finalized());
    assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
    assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
    let state = store.state.lock().unwrap();
    assert_eq!(state.commits, 0);
    assert_eq!(state.finalizations[0].cost_ticks, Some(123));
    assert_eq!(state.finalizations[0].send_state, UpstreamSendState::Sent);
}
#[test]
fn attempt_middleware_cannot_expand_routed_capabilities() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::RequireImage, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Middleware)
    ));
    assert!(provider.contexts.lock().unwrap().is_empty());
    assert_eq!(
        store.state.lock().unwrap().finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
}
#[test]
fn attempt_middleware_short_circuit_never_selects_or_sends_a_provider() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![]);
    let hook = attempt_hook(AttemptMiddlewareMode::ShortCircuit, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert_eq!(block_on(session.collect_uncommitted()).unwrap().len(), 2);
    block_on(session.commit_downstream(Some(200))).unwrap();
    assert!(provider.contexts.lock().unwrap().is_empty());
    let state = store.state.lock().unwrap();
    assert!(state.attempts.is_empty());
    assert_eq!(state.finalizations[0].attempt_count, 0);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}
#[test]
fn attempt_middleware_must_finish_original_source_before_claiming_success() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::EarlyEnd, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::Middleware)
    ));
    assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
    assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
    assert_eq!(
        store.state.lock().unwrap().finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
}
#[test]
fn attempt_middleware_pass_through_uses_original_delivery_barrier() {
    let operation = generate_operation();
    let (coordinator, store, _) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::Pass, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let first = block_on(session.next_event()).unwrap().unwrap();
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    assert!(matches!(
        block_on(session.next_event()),
        Err(EngineError::DownstreamCommitRequired)
    ));
    block_on(session.commit_downstream(Some(200))).unwrap();
    while block_on(session.next_event()).unwrap().is_some() {}
    assert_eq!(
        store.state.lock().unwrap().finalizations[0].cost_ticks,
        Some(123)
    );
}

#[test]
fn attempt_middleware_is_recreated_for_each_core_retry_and_keeps_accumulated_costs() {
    let operation = generate_operation();
    let first = Script::Stream {
        account_id: "acct_first",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new("first", "gpt-5"))),
            Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(999).unwrap(),
            )),
            Err(
                ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                    .with_replay_safe(),
            ),
        ],
    };
    let (coordinator, store, provider) = coordinator(vec![first, costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::Pass, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let output = block_on(session.collect_uncommitted()).unwrap();
    assert_eq!(output.len(), 3);
    block_on(session.commit_downstream(Some(200))).unwrap();
    assert_eq!(session.budget_charge().amount_usd.scaled(), 1122);
    assert_eq!(provider.contexts.lock().unwrap().len(), 2);
    let contexts = hook.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].attempt_index().get(), 1);
    assert_eq!(contexts[1].attempt_index().get(), 2);
    assert_eq!(contexts[0].request_id(), contexts[1].request_id());
    assert_eq!(contexts[0].deadline(), contexts[1].deadline());
    let state = store.state.lock().unwrap();
    assert_eq!(state.commits, 1);
    assert_eq!(state.finalizations[0].attempt_count, 2);
    assert_eq!(state.finalizations[0].cost_ticks, Some(123));
}
#[test]
fn attempt_middleware_cannot_read_a_provider_outside_the_frozen_scope() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let Script::Stream { items, .. } = costed_attempt() else {
        unreachable!()
    };
    let (coordinator, store, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_outside_scope",
        items,
    }]);
    let hook = attempt_hook(AttemptMiddlewareMode::Pass, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(matches!(
        block_on(session.collect_uncommitted()),
        Err(EngineError::AccountOutsideClientScope)
    ));
    assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
    let state = store.state.lock().unwrap();
    assert!(state.attempts.is_empty());
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
    assert_eq!(state.finalizations[0].cost_ticks, None);
}

#[test]
fn attempt_middleware_can_fail_while_the_original_upstream_read_is_stalled() {
    let operation = generate_operation();
    let (coordinator, store, _) = coordinator(vec![Script::HangingStream {
        account_id: "acct_one",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                "original", "gpt-5",
            ))),
            Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(123).unwrap(),
            )),
        ],
    }]);
    let hook = attempt_hook(AttemptMiddlewareMode::FailWhileStalled, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(2)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert!(
        matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Middleware)
        ),
        "plugin failure must win over root deadline"
    );
    assert!(session.is_finalized());
    assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
    assert!(
        session.provider_attempt_outcomes().is_empty(),
        "plugin failure cannot penalize provider health"
    );
}
#[test]
fn attempt_middleware_projection_cannot_forge_a_continuation_checkpoint() {
    let operation = generate_operation();
    let (coordinator, store, _) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::ForgeCheckpoint, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let events = block_on(session.collect_uncommitted()).unwrap();
    assert!(events.iter().all(|event| event.session_update().is_none()));
    block_on(session.commit_downstream(Some(200))).unwrap();
    assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
}

#[test]
fn attempt_middleware_preserves_original_checkpoint_when_display_forges_one() {
    let original = ProviderSessionState::new(
        "openai",
        json!({"original":true}).as_object().unwrap().clone(),
    )
    .unwrap();
    let mut completed = ProviderEvent::canonical(GatewayEvent::Completed(ResponseMeta::new(
        "original", "gpt-5",
    )));
    completed.attach_session_update(original.clone());
    let operation = generate_operation();
    let (coordinator, store, _) = coordinator(vec![Script::ObservedStream {
        account_id: "acct_one",
        items: vec![
            Ok(ProviderEvent::canonical(GatewayEvent::Started(
                ResponseMeta::new("original", "gpt-5"),
            ))),
            Ok(completed),
        ],
    }]);
    let hook = attempt_hook(AttemptMiddlewareMode::ForgeCheckpoint, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let events = block_on(session.collect_uncommitted()).unwrap();
    let checkpoints: Vec<_> = events
        .iter()
        .filter_map(ProviderEvent::session_update)
        .collect();
    assert_eq!(checkpoints, vec![&original]);
    assert!(session.native_continuation_pin(checkpoints[0]).is_some());
    block_on(session.commit_downstream(Some(200))).unwrap();
}
#[test]
fn attempt_middleware_resumes_a_dropped_read_without_losing_or_duplicating_facts() {
    let operation = generate_operation();
    let (coordinator, store, provider) = coordinator(vec![costed_attempt()]);
    let hook = attempt_hook(AttemptMiddlewareMode::ResumeRead, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert_eq!(block_on(session.collect_uncommitted()).unwrap().len(), 3);
    block_on(session.commit_downstream(Some(200))).unwrap();
    assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
    assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
}
#[test]
fn attempt_middleware_live_cancel_keeps_cost_while_source_is_stalled() {
    block_on(async {
        let operation = generate_operation();
        let (coordinator, store, provider) = coordinator(vec![Script::HangingStream {
            account_id: "acct_one",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "original", "gpt-5",
                ))),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(123).unwrap(),
                )),
            ],
        }]);
        let hook = attempt_hook(AttemptMiddlewareMode::Pass, &store);
        let coordinator = coordinator.with_attempt_middleware(Some(hook));
        let cancellation = CancellationToken::new();
        let mut session = coordinator
            .start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation.clone(),
                plan(&operation),
                None,
                None,
                cancellation.clone(),
            )
            .await
            .unwrap();
        let collect = Box::pin(session.collect_uncommitted());
        let timer = Box::pin(futures_timer::Delay::new(Duration::from_millis(10)));
        let futures::future::Either::Right((_, collect)) =
            futures::future::select(collect, timer).await
        else {
            panic!("source should be stalled")
        };
        cancellation.cancel();
        assert!(matches!(collect.await, Err(EngineError::Cancelled)));
        session.cancel_and_finalize().await.unwrap();
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        assert!(session.provider_attempt_outcomes().is_empty());
        let state = store.state.lock().unwrap();
        assert_eq!(state.finalizations.len(), 1);
        assert_eq!(state.finalizations[0].cost_ticks, Some(123));
    });
}
#[test]
fn attempt_middleware_cancel_cannot_erase_atomic_error_receipts() {
    block_on(async {
        for explicit_cleanup in [false, true] {
            let operation = generate_operation();
            let error = ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
                .with_atomic_client_events(vec![
                    ProviderEvent::canonical(GatewayEvent::Started(ResponseMeta::new(
                        "original-error",
                        "gpt-5",
                    ))),
                    ProviderEvent::canonical(GatewayEvent::ProviderCost(
                        ProviderReportedCost::from_usd_ticks(123).unwrap(),
                    )),
                ]);
            let (coordinator, store, provider) = coordinator(vec![Script::ObservedStream {
                account_id: "acct_one",
                items: vec![Err(error)],
            }]);
            let hook = attempt_hook(AttemptMiddlewareMode::HoldError, &store);
            let coordinator = coordinator.with_attempt_middleware(Some(hook));
            let cancellation = CancellationToken::new();
            let mut session = coordinator
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation.clone(),
                    plan(&operation),
                    None,
                    None,
                    cancellation.clone(),
                )
                .await
                .unwrap();
            let collect = Box::pin(session.collect_uncommitted());
            let timer = Box::pin(futures_timer::Delay::new(Duration::from_millis(10)));
            let futures::future::Either::Right((_, collect)) =
                futures::future::select(collect, timer).await
            else {
                panic!("middleware should retain original error")
            };
            if explicit_cleanup {
                drop(collect);
                session.cancel_and_finalize().await.unwrap();
            } else {
                cancellation.cancel();
                assert!(matches!(collect.await, Err(EngineError::Cancelled)));
            }
            session.cancel_and_finalize().await.unwrap();
            assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            assert_eq!(session.provider_attempt_outcomes().len(), 1);
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations.len(), 1);
            assert_eq!(state.finalizations[0].cost_ticks, Some(123));
            assert_eq!(state.finalizations[0].send_state, UpstreamSendState::Sent);
            assert_eq!(
                state.finalizations[0].upstream_response_id.as_deref(),
                Some("original-error")
            );
        }
    });
}

#[test]
fn attempt_middleware_resumes_preparation_without_reinvoking_the_plugin_or_provider() {
    block_on(async {
        for before_next in [true, false] {
            let operation = generate_operation();
            let script = if before_next {
                costed_attempt()
            } else {
                Script::DelayedStream {
                    account_id: "acct_one",
                    items: complete_stream(None),
                }
            };
            let (coordinator, store, provider) = coordinator(vec![script]);
            let hook = attempt_hook(
                if before_next {
                    AttemptMiddlewareMode::WaitBeforeNext
                } else {
                    AttemptMiddlewareMode::Pass
                },
                &store,
            );
            let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
            let mut session = coordinator
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation.clone(),
                    plan(&operation),
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut first = Box::pin(session.next_event());
            assert!(futures::poll!(first.as_mut()).is_pending());
            drop(first);
            let events = session.collect_uncommitted().await.unwrap();
            assert_eq!(events.len(), if before_next { 3 } else { 2 });
            session.commit_downstream(Some(200)).await.unwrap();
            assert_eq!(hook.contexts.lock().unwrap().len(), 1);
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            assert_eq!(store.state.lock().unwrap().created, 1);
            assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        }
    });
}
#[test]
fn attempt_middleware_resumes_validated_cold_source_after_interrupted_observation() {
    block_on(async {
        let operation = generate_operation();
        let (release, gate) = oneshot::channel();
        let (coordinator, store, provider) = coordinator_with_store(
            vec![costed_attempt()],
            Arc::new(FakeStore {
                create_gate: Mutex::new(Some(gate)),
                ..FakeStore::default()
            }),
        );
        let hook = attempt_hook(AttemptMiddlewareMode::ObservationInterrupted, &store);
        let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
        let mut session = coordinator
            .start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation.clone(),
                plan(&operation),
                None,
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut first = Box::pin(session.next_event());
        assert!(futures::poll!(first.as_mut()).is_pending());
        drop(first);
        // Preserve the existing best-effort registration contract.
        assert!(release.send(()).is_err());
        assert_eq!(session.collect_uncommitted().await.unwrap().len(), 3);
        session.commit_downstream(Some(200)).await.unwrap();
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
        assert_eq!(hook.contexts.lock().unwrap().len(), 1);
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        assert_eq!(store.state.lock().unwrap().created, 1);
        assert!(store.state.lock().unwrap().finalizations.is_empty());
    });
}

#[test]
fn attempt_middleware_short_circuit_after_unsent_retry_keeps_not_sent() {
    let operation = generate_operation();
    let error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::NotSent)
        .with_status(429);
    let (coordinator, store, provider) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: vec![Err(error)],
    }]);
    let hook = attempt_hook(AttemptMiddlewareMode::ShortCircuitAfterRetry, &store);
    let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation.clone(),
        plan(&operation),
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    assert_eq!(block_on(session.collect_uncommitted()).unwrap().len(), 2);
    block_on(session.commit_downstream(Some(200))).unwrap();
    assert_eq!(hook.contexts.lock().unwrap().len(), 2);
    assert_eq!(provider.contexts.lock().unwrap().len(), 1);
    let state = store.state.lock().unwrap();
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.finalizations[0].attempt_count, 1);
    assert_eq!(
        state.finalizations[0].send_state,
        UpstreamSendState::NotSent
    );
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
}

#[test]
fn fast_defaults_precede_attempt_middleware_on_every_retry_and_do_not_overwrite_it() {
    for with_middleware in [false, true] {
        let operation = generate_operation();
        let (coordinator, store, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_one",
                items: vec![Err(ProviderError::new(
                    ProviderErrorKind::RateLimited,
                    UpstreamSendState::NotSent,
                )
                .with_status(429))],
            },
            costed_attempt(),
        ]);
        let coordinator = if with_middleware {
            coordinator.with_attempt_middleware(Some(attempt_hook(
                AttemptMiddlewareMode::OverrideFast,
                &store,
            )))
        } else {
            coordinator
        };
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation.clone(),
            plan(&operation).with_fast_mode(gateway_core::account::FastMode::Enabled),
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        block_on(session.collect_uncommitted()).unwrap();
        block_on(session.commit_downstream(Some(200))).unwrap();
        assert_eq!(provider.preparations.load(Ordering::SeqCst), 2);
        let operations = provider.operations.lock().unwrap();
        assert_eq!(operations.len(), 2);
        for operation in operations.iter() {
            let Operation::Generate(request) = operation else {
                panic!("generate");
            };
            assert_eq!(
                request.protocol_payload().body()["service_tier"],
                if with_middleware { "flex" } else { "priority" }
            );
        }
        assert!(
            provider
                .contexts
                .lock()
                .unwrap()
                .iter()
                .all(|context| context.fast_mode() == gateway_core::account::FastMode::Enabled)
        );
        assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
    }
}

struct TestExecutionRecoveryLease(Arc<AtomicUsize>);
impl Drop for TestExecutionRecoveryLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl gateway_core::engine::ExecutionRecoveryLeaseGuard for TestExecutionRecoveryLease {}

#[test]
fn dropping_unpolled_session_stops_recovery_maintenance() {
    block_on(async {
        let operation = generate_operation();
        let (coordinator, store, _) =
            coordinator_with_store(vec![], Arc::new(FakeStore::default()));
        let session = coordinator
            .start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation.clone(),
                plan(&operation),
                None,
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(store.recovery_leases.load(Ordering::SeqCst), 1);
        drop(session);
        assert_eq!(store.recovery_leases.load(Ordering::SeqCst), 0);
        assert_eq!(store.state.lock().unwrap().created, 0);
    });
}

#[test]
fn unlimited_execution_still_cancels_pending_preparation_and_streams_once() {
    block_on(async {
        for script in [
            Script::Pending,
            Script::HangingStream {
                account_id: "acct_first",
                items: vec![],
            },
        ] {
            let operation = generate_operation();
            let (coordinator, store, provider) = coordinator(vec![script]);
            let cancellation = CancellationToken::new();
            let mut request = model_request(&operation, SystemTime::now());
            request.started_at = SystemTime::now() - Duration::from_secs(601);
            request.deadline_at = gateway_core::lifecycle::Deadline::default();
            let mut session = coordinator
                .start(
                    request,
                    operation.clone(),
                    plan(&operation),
                    None,
                    None,
                    cancellation.clone(),
                )
                .await
                .unwrap();
            let mut next = Box::pin(session.next_event());
            assert!(futures::poll!(next.as_mut()).is_pending());
            futures_timer::Delay::new(Duration::from_millis(20)).await;
            assert!(futures::poll!(next.as_mut()).is_pending());
            assert_eq!(provider.contexts.lock().unwrap()[0].deadline().at(), None);
            cancellation.cancel();
            assert!(matches!(next.await, Err(EngineError::Cancelled)));
            session.cancel_and_finalize().await.unwrap();
            assert!(session.is_finalized());
            assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        }
    });
}

mod retry_policy {
    use super::*;
    use futures::future::BoxFuture;
    use gateway_core::engine::retry_policy::{
        RetryDecision, RetryInput, RetryPolicy, RetryPolicyFault, RetryResult,
    };

    struct Policy {
        decision: RetryDecision,
        fault: bool,
        global: CancellationToken,
        consultation: CancellationToken,
        inputs: Mutex<Vec<RetryInput>>,
        gate: Mutex<Option<oneshot::Receiver<()>>>,
    }
    impl Policy {
        fn new(decision: RetryDecision) -> Arc<Self> {
            Arc::new(Self {
                decision,
                fault: false,
                global: CancellationToken::new(),
                consultation: CancellationToken::new(),
                inputs: Mutex::new(vec![]),
                gate: Mutex::new(None),
            })
        }
    }
    impl RetryPolicy for Policy {
        fn cancellation(&self) -> CancellationToken {
            self.global.clone()
        }
        fn decide_retry(
            &self,
            input: RetryInput,
        ) -> BoxFuture<'static, Result<RetryResult, RetryPolicyFault>> {
            self.inputs.lock().unwrap().push(input);
            let gate = self.gate.lock().unwrap().take();
            let decision = self.decision;
            let fault = self.fault;
            let revocation = self.consultation.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                if fault {
                    Err(RetryPolicyFault)
                } else {
                    Ok(RetryResult {
                        decision,
                        revocation,
                    })
                }
            })
        }
    }
    fn failure_items(error: ProviderError) -> Vec<Result<GatewayEvent, ProviderError>> {
        vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                "response-1",
                "gpt-5",
            ))),
            Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(123).unwrap(),
            )),
            Err(error),
        ]
    }
    fn safe_failure() -> ProviderError {
        ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
            .with_status(429)
            .with_replay_safe()
    }

    #[test]
    fn stop_preserves_original_atomic_failure_fee_and_single_finalization() {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let mut error = atomic_response_failed("original-policy-failure", "original-wire-marker");
        let mut events = error.take_atomic_client_events();
        events.insert(
            1,
            ProviderEvent::canonical(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(123).unwrap(),
            )),
        );
        let (coordinator, store, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: vec![Err(error.with_atomic_client_events(events))],
        }]);
        let policy = Policy::new(RetryDecision::Stop);
        let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        let batch = block_on(session.next_event()).unwrap().unwrap();
        let wire = batch
            .into_provider_events()
            .iter()
            .filter_map(ProviderEvent::wire_event)
            .map(|wire| wire.data().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(wire.contains("original-policy-failure"));
        assert!(wire.contains("original-wire-marker"));
        block_on(session.commit_downstream(Some(429))).unwrap();
        assert!(matches!(
            block_on(session.next_event()),
            Err(EngineError::Provider(_))
        ));
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
        drop(session);
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
        let state = store.state.lock().unwrap();
        assert_eq!(state.finalizations.len(), 1);
        assert_eq!(state.finalizations[0].cost_ticks, Some(123));
        assert_eq!(state.finalizations[0].send_state, UpstreamSendState::Sent);
        assert_eq!(state.intermediate_failures, 0);
        assert_eq!(
            policy.inputs.lock().unwrap()[0].upstream_status(),
            Some(429)
        );
    }

    #[test]
    fn delegate_retry_and_policy_fault_continue_only_original_recovery() {
        for (decision, fault) in [
            (RetryDecision::Delegate, false),
            (RetryDecision::Retry, false),
            (RetryDecision::Stop, true),
        ] {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let budget = route_plan.max_attempts().get();
            let (coordinator, store, provider) = coordinator(vec![
                Script::Stream {
                    account_id: "acct_first",
                    items: failure_items(safe_failure()),
                },
                Script::Stream {
                    account_id: "acct_second",
                    items: complete_stream(None),
                },
            ]);
            let mut policy = Policy::new(decision);
            Arc::get_mut(&mut policy).unwrap().fault = fault;
            let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation,
                route_plan,
                None,
                None,
                CancellationToken::new(),
            ))
            .unwrap();
            block_on(session.collect_uncommitted()).unwrap();
            block_on(session.commit_downstream(Some(200))).unwrap();
            assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
            drop(session);
            let inputs = policy.inputs.lock().unwrap();
            assert_eq!(inputs.len(), 1);
            let input = &inputs[0];
            assert_eq!(input.request_id().as_str(), "req_core_1");
            assert_eq!(input.client_key_id().as_str(), "key_client_1");
            assert_eq!(input.attempt_index().get(), 1);
            assert_eq!(input.provider().as_str(), "openai");
            assert_eq!(input.model(), Some("gpt-5"));
            assert_eq!(input.error_kind(), ProviderErrorKind::RateLimited);
            assert_eq!(input.send_state(), UpstreamSendState::Sent);
            assert_eq!(input.remaining_routing_attempts(), budget - 1);
            assert!(input.retry_allowed());
            assert!(input.excluded_instances().is_empty());
            assert_eq!(provider.contexts.lock().unwrap().len(), 2);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 2);
            assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        }
    }

    #[test]
    fn retry_cannot_override_ambiguous_unproved_prohibited_or_hard_pinned_failure() {
        for (error, required) in [
            (
                ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent),
                None,
            ),
            (
                ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Ambiguous)
                    .with_replay_safe(),
                None,
            ),
            (
                ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::NotSent)
                    .with_retry_prohibited(),
                None,
            ),
            (
                safe_failure(),
                Some(ProviderAccountId::new("acct_first").unwrap()),
            ),
        ] {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let (coordinator, store, provider) = coordinator(vec![Script::Stream {
                account_id: "acct_first",
                items: vec![Err(error)],
            }]);
            let policy = Policy::new(RetryDecision::Retry);
            let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
                model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                operation,
                route_plan,
                required,
                None,
                CancellationToken::new(),
            ))
            .unwrap();
            assert!(matches!(
                block_on(session.collect_uncommitted()),
                Err(EngineError::Provider(_))
            ));
            assert!(!policy.inputs.lock().unwrap()[0].retry_allowed());
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        }
    }

    #[test]
    fn retry_cannot_override_delivery_commit_or_routing_budget() {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, store, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: vec![
                Ok(GatewayEvent::Started(ResponseMeta::new(
                    "committed",
                    "gpt-5",
                ))),
                Err(safe_failure()),
            ],
        }]);
        let policy = Policy::new(RetryDecision::Retry);
        let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        block_on(session.next_event()).unwrap().unwrap();
        block_on(session.commit_downstream(Some(200))).unwrap();
        assert!(matches!(
            block_on(session.next_event()),
            Err(EngineError::Provider(_))
        ));
        assert!(!policy.inputs.lock().unwrap()[0].retry_allowed());
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);

        let operation = generate_operation();
        let route_plan = plan(&operation);
        let budget = route_plan.max_attempts().get();
        let (coordinator, store, provider) = super::coordinator(
            (0..budget)
                .map(|_| Script::Stream {
                    account_id: "acct_first",
                    items: vec![Err(safe_failure())],
                })
                .collect(),
        );
        let policy = Policy::new(RetryDecision::Retry);
        let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        assert!(matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Provider(_))
        ));
        assert!(
            !policy
                .inputs
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .retry_allowed()
        );
        assert_eq!(
            policy
                .inputs
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .remaining_routing_attempts(),
            0
        );
        assert_eq!(provider.contexts.lock().unwrap().len(), budget as usize);
        assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
    }

    #[test]
    fn dropping_a_read_keeps_one_policy_call_original_lease_and_recovery() {
        block_on(async {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let (coordinator, store, provider) = coordinator(vec![
                Script::Stream {
                    account_id: "acct_first",
                    items: failure_items(safe_failure()),
                },
                Script::Stream {
                    account_id: "acct_second",
                    items: complete_stream(None),
                },
            ]);
            let policy = Policy::new(RetryDecision::Retry);
            let hook = attempt_hook(AttemptMiddlewareMode::Pass, &store);
            let coordinator = coordinator.with_attempt_middleware(Some(hook.clone()));
            let (send, receive) = oneshot::channel();
            *policy.gate.lock().unwrap() = Some(receive);
            let mut session = coordinator
                .with_retry_policy(Some(policy.clone()))
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut read = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut read).is_pending());
            assert_eq!(policy.inputs.lock().unwrap().len(), 1);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 0);
            drop(read);
            let mut resumed = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut resumed).is_pending());
            assert_eq!(policy.inputs.lock().unwrap().len(), 1);
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            send.send(()).unwrap();
            resumed.await.unwrap();
            session.commit_downstream(Some(200)).await.unwrap();
            assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
            drop(session);
            assert_eq!(provider.contexts.lock().unwrap().len(), 2);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 2);
            assert_eq!(hook.contexts.lock().unwrap().len(), 2);
            assert_eq!(store.state.lock().unwrap().finalizations.len(), 1);
        });
    }

    #[test]
    fn cancellation_and_deadline_while_policy_waits_preserve_cost_and_release_once() {
        block_on(async {
            for deadline in [false, true] {
                let operation = generate_operation();
                let route_plan = plan(&operation);
                let (coordinator, store, provider) = coordinator(vec![Script::Stream {
                    account_id: "acct_first",
                    items: failure_items(safe_failure()),
                }]);
                let policy = Policy::new(RetryDecision::Retry);
                let (_send, receive) = oneshot::channel();
                *policy.gate.lock().unwrap() = Some(receive);
                let cancellation = CancellationToken::new();
                let mut session = coordinator
                    .with_retry_policy(Some(policy.clone()))
                    .start(
                        model_request(
                            &operation,
                            SystemTime::now()
                                + if deadline {
                                    Duration::from_millis(40)
                                } else {
                                    Duration::from_secs(30)
                                },
                        ),
                        operation,
                        route_plan,
                        None,
                        None,
                        cancellation.clone(),
                    )
                    .await
                    .unwrap();
                let mut read = Box::pin(session.collect_uncommitted());
                assert!(futures::poll!(&mut read).is_pending());
                if !deadline {
                    cancellation.cancel();
                }
                let result = read.await;
                assert!(matches!(
                    (deadline, result),
                    (true, Err(EngineError::Deadline)) | (false, Err(EngineError::Cancelled))
                ));
                session.cancel_and_finalize().await.unwrap();
                assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
                drop(session);
                assert_eq!(provider.contexts.lock().unwrap().len(), 1);
                assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
                assert_eq!(store.recovery_leases.load(Ordering::SeqCst), 0);
                let state = store.state.lock().unwrap();
                assert_eq!(state.finalizations.len(), 1);
                assert_eq!(state.finalizations[0].cost_ticks, Some(123));
                assert_eq!(state.finalizations[0].send_state, UpstreamSendState::Sent);
            }
        });
    }

    #[test]
    fn explicit_finalize_after_abandoning_policy_wait_preserves_original_receipts() {
        block_on(async {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let (coordinator, store, provider) = coordinator(vec![Script::Stream {
                account_id: "acct_first",
                items: failure_items(safe_failure()),
            }]);
            let policy = Policy::new(RetryDecision::Retry);
            let (_send, receive) = oneshot::channel();
            *policy.gate.lock().unwrap() = Some(receive);
            let mut session = coordinator
                .with_retry_policy(Some(policy))
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut read = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut read).is_pending());
            drop(read);
            session.cancel_and_finalize().await.unwrap();
            assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
            drop(session);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1);
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations.len(), 1);
            assert_eq!(state.finalizations[0].cost_ticks, Some(123));
        });
    }

    #[test]
    fn successful_consultation_revocation_cancels_later_attempt_and_keeps_both_fees() {
        block_on(async {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let (coordinator, store, provider) = coordinator(vec![
                Script::Stream {
                    account_id: "acct_first",
                    items: failure_items(safe_failure()),
                },
                Script::HangingStream {
                    account_id: "acct_second",
                    items: vec![
                        Ok(GatewayEvent::Started(ResponseMeta::new("second", "gpt-5"))),
                        Ok(GatewayEvent::ProviderCost(
                            ProviderReportedCost::from_usd_ticks(200).unwrap(),
                        )),
                    ],
                },
            ]);
            let policy = Policy::new(RetryDecision::Retry);
            let mut session = coordinator
                .with_retry_policy(Some(policy.clone()))
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut read = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut read).is_pending());
            assert_eq!(provider.contexts.lock().unwrap().len(), 2);
            policy.consultation.cancel();
            assert!(matches!(read.await, Err(EngineError::Cancelled)));
            assert_eq!(session.budget_charge().amount_usd.scaled(), 323);
            drop(session);
            assert_eq!(provider.released_leases.load(Ordering::SeqCst), 1); // hanging fixture has no tracked guard
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations.len(), 1);
            assert_eq!(state.finalizations[0].cost_ticks, Some(200));
        });
    }

    #[test]
    fn retry_does_not_shorten_server_backoff_or_change_pinned_transport() {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, _, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_first",
                items: vec![Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    UpstreamSendState::NotSent,
                )
                .with_replay_safe()
                .with_pre_delivery_transport_fallback()
                .with_retry_after(Duration::from_millis(100)))],
            },
            Script::Stream {
                account_id: "acct_first",
                items: complete_stream(None),
            },
        ]);
        let policy = Policy::new(RetryDecision::Retry);
        let mut session = block_on(coordinator.with_retry_policy(Some(policy)).start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        let began = std::time::Instant::now();
        block_on(session.collect_uncommitted()).unwrap();
        assert!(began.elapsed() >= Duration::from_millis(100));
        let contexts = provider.contexts.lock().unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(
            contexts[1].required_account(),
            Some(&ProviderAccountId::new("acct_first").unwrap())
        );
        assert_eq!(contexts[1].transport(), AttemptTransport::Fallback);
    }

    #[test]
    fn unbounded_root_still_delegates_after_the_policy_wait_limit() {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, _, provider) = coordinator(vec![
            Script::Stream {
                account_id: "acct_first",
                items: vec![Err(safe_failure())],
            },
            Script::Stream {
                account_id: "acct_second",
                items: complete_stream(None),
            },
        ]);
        let policy = Policy::new(RetryDecision::Stop);
        let (_send, receive) = oneshot::channel();
        *policy.gate.lock().unwrap() = Some(receive);
        let mut request = model_request(&operation, SystemTime::now() + Duration::from_secs(30));
        request.deadline_at = Default::default();
        let mut session = block_on(coordinator.with_retry_policy(Some(policy.clone())).start(
            request,
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        let began = std::time::Instant::now();
        block_on(session.collect_uncommitted()).unwrap();
        assert!(began.elapsed() >= Duration::from_secs(2));
        assert!(began.elapsed() < Duration::from_secs(8));
        assert_eq!(provider.contexts.lock().unwrap().len(), 2);
        assert!(policy.inputs.lock().unwrap()[0].deadline().at().is_none());
    }

    #[test]
    fn dropping_an_early_failure_wait_does_not_repeat_provider_preparation() {
        block_on(async {
            let operation = generate_operation();
            let route_plan = plan(&operation);
            let (coordinator, store, provider) =
                coordinator(vec![Script::Error(ProviderError::new(
                    ProviderErrorKind::NoEligibleAccount,
                    UpstreamSendState::NotSent,
                ))]);
            let policy = Policy::new(RetryDecision::Retry);
            let (send, receive) = oneshot::channel();
            *policy.gate.lock().unwrap() = Some(receive);
            let mut session = coordinator
                .with_retry_policy(Some(policy.clone()))
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let mut read = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut read).is_pending());
            drop(read);
            let mut resumed = Box::pin(session.collect_uncommitted());
            assert!(futures::poll!(&mut resumed).is_pending());
            assert_eq!(policy.inputs.lock().unwrap().len(), 1);
            assert!(!policy.inputs.lock().unwrap()[0].retry_allowed());
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            send.send(()).unwrap();
            assert!(
                matches!(resumed.await,Err(EngineError::Provider(error)) if error.kind()==ProviderErrorKind::NoEligibleAccount)
            );
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations.len(), 1);
            assert_eq!(state.finalizations[0].attempt_count, 0);
            assert_eq!(
                state.finalizations[0].send_state,
                UpstreamSendState::NotSent
            );
        });
    }

    #[test]
    fn fixed_account_diagnostics_do_not_consult_plugin_retry_policies() {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, _, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: vec![Err(safe_failure())],
        }]);
        let policy = Policy::new(RetryDecision::Stop);
        let mut session = block_on(
            coordinator
                .with_retry_policy(Some(policy.clone()))
                .start_diagnostic(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    ProviderAccountId::new("acct_first").unwrap(),
                    None,
                    CancellationToken::new(),
                ),
        )
        .unwrap();
        assert!(matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Provider(_))
        ));
        assert!(policy.inputs.lock().unwrap().is_empty());
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
    }

    #[test]
    fn cancellation_racing_a_policy_fault_wins_over_delegation() {
        struct CancelOnFault;
        impl RetryPolicy for CancelOnFault {
            fn cancellation(&self) -> CancellationToken {
                CancellationToken::new()
            }
            fn decide_retry(
                &self,
                input: RetryInput,
            ) -> BoxFuture<'static, Result<RetryResult, RetryPolicyFault>> {
                Box::pin(async move {
                    input.cancellation().cancel();
                    Err(RetryPolicyFault)
                })
            }
        }
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let (coordinator, store, provider) = coordinator(vec![Script::Stream {
            account_id: "acct_first",
            items: failure_items(safe_failure()),
        }]);
        let mut session = block_on(
            coordinator
                .with_retry_policy(Some(Arc::new(CancelOnFault)))
                .start(
                    model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
                    operation,
                    route_plan,
                    None,
                    None,
                    CancellationToken::new(),
                ),
        )
        .unwrap();
        assert!(matches!(
            block_on(session.collect_uncommitted()),
            Err(EngineError::Cancelled)
        ));
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
        drop(session);
        assert_eq!(provider.contexts.lock().unwrap().len(), 1);
        let state = store.state.lock().unwrap();
        assert_eq!(state.finalizations.len(), 1);
        assert_eq!(
            state.finalizations[0].error_kind,
            Some(GatewayErrorKind::Cancelled)
        );
    }
}

#[test]
fn extension_session_owner_keeps_connection_local_pins_out_of_persisted_scope() {
    use gateway_core::{
        engine::continuation::NativeContinuationScope, operation::ExtensionSessionOwner,
    };
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, _, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: vec![
            Ok(GatewayEvent::Started(ResponseMeta::new(
                "adapter-response",
                "gpt-5",
            ))),
            Ok(GatewayEvent::Completed(ResponseMeta::new(
                "adapter-response",
                "gpt-5",
            ))),
        ],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    block_on(session.collect_uncommitted()).unwrap();
    for connection_local in [false, true] {
        let state = ProviderSessionState::new("openai", Map::new())
            .unwrap()
            .with_extension_owner(ExtensionSessionOwner {
                instance_id: "instance".into(),
                contribution_id: "contribution".into(),
                adapter_id: "adapter".into(),
                generation: 4,
                incarnation: "incarnation".into(),
                connection_local,
            });
        let pin = session.native_continuation_pin(&state).unwrap();
        assert_eq!(
            pin.scope(),
            if connection_local {
                NativeContinuationScope::ConnectionLocal
            } else {
                NativeContinuationScope::Persisted
            }
        );
        assert_eq!(pin.session_state(), Some(&state));
        assert_eq!(pin.account().as_str(), "acct_one");
        assert_eq!(pin.upstream_response_id().as_str(), "adapter-response");
    }
}

#[test]
fn replayable_prelude_and_transport_commit_allow_retry_but_semantic_output_does_not() {
    for replayable in [true, false] {
        let operation = generate_operation();
        let route_plan = plan(&operation);
        let prelude =
            ProviderEvent::canonical(GatewayEvent::Started(ResponseMeta::new("old", "gpt-5")));
        let prelude = if replayable {
            prelude.with_replayable_prelude()
        } else {
            prelude
        };
        let (coordinator, store, provider) = coordinator(vec![
            Script::ObservedStream {
                account_id: "acct_one",
                items: vec![
                    Ok(prelude),
                    Err(
                        ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Sent)
                            .with_replay_safe(),
                    ),
                ],
            },
            Script::Stream {
                account_id: "acct_two",
                items: complete_stream(None),
            },
        ]);
        let mut session = block_on(coordinator.start(
            model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
            operation,
            route_plan,
            None,
            None,
            CancellationToken::new(),
        ))
        .unwrap();
        session.response_commit().record(Some(200)); // 首个事件之前的 SSE 注释。
        assert!(block_on(session.next_event()).unwrap().is_some());
        block_on(session.commit_downstream(Some(200))).unwrap();
        if replayable {
            while block_on(session.next_event()).unwrap().is_some() {}
            assert_eq!(provider.contexts.lock().unwrap().len(), 2);
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Succeeded);
            assert_eq!(state.finalizations[0].client_status_code, Some(200));
        } else {
            assert!(block_on(session.next_event()).is_err());
            assert_eq!(provider.contexts.lock().unwrap().len(), 1);
            let state = store.state.lock().unwrap();
            assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
        }
    }
}

#[test]
fn heartbeat_then_upstream_failure_preserves_http_status_and_incomplete_outcome() {
    let operation = generate_operation();
    let route_plan = plan(&operation);
    let (coordinator, store, _) = coordinator(vec![Script::Stream {
        account_id: "acct_one",
        items: vec![Err(ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            UpstreamSendState::Sent,
        ))],
    }]);
    let mut session = block_on(coordinator.start(
        model_request(&operation, SystemTime::now() + Duration::from_secs(30)),
        operation,
        route_plan,
        None,
        None,
        CancellationToken::new(),
    ))
    .unwrap();
    let response_commit = session.response_commit();
    response_commit.record(Some(200));
    assert!(block_on(session.next_event()).is_err());
    block_on(session.record_client_status(400)).unwrap();
    let state = store.state.lock().unwrap();
    assert_eq!(state.finalizations.len(), 1);
    assert_eq!(state.finalizations[0].outcome, ExecutionOutcome::Incomplete);
    assert_eq!(state.finalizations[0].client_status_code, Some(200));
    assert!(state.finalizations[0].committed);
    assert_eq!(
        state.commits, 0,
        "heartbeat is not a business delivery acknowledgment"
    );
}
