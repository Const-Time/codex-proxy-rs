use crate::support::{MemoryStore, actor, archive, update};
use async_trait::async_trait;
use futures::{StreamExt as _, future::BoxFuture};
use gateway_admin::ports::plugins::{PluginRuntime, PluginService};
use gateway_admin::{
    ClientKeyService,
    model::{AdminError, MutationContext, Revision, client_keys::*},
    plugins::PluginManager,
};
use gateway_core::{
    account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy},
    engine::{admission::*, budget::*, continuation::*, execution::*, provider::*, *},
    error::{GatewayError, ProviderError, ProviderErrorKind, StoreError},
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ResponseMeta},
    lifecycle::CancellationToken,
    metering::ProviderReportedCost,
    operation::{Operation, OperationKind, RawJsonPayload},
    policy::{ClientApiKeyId, ClientPolicy, PlaintextClientApiKey, RateLimits},
    routing::*,
    runtime::RuntimeSnapshotHandle,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

#[derive(Default)]
pub struct Core {
    pub catalogs: AtomicUsize,
    pub catalog_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub active: AtomicUsize,
    pub starts: AtomicUsize,
    pub polled: Arc<AtomicUsize>,
    pub commits: AtomicUsize,
    pub requests: Mutex<Vec<NewModelRequest>>,
    pub attempts: Mutex<Vec<AttemptRecord>>,
    pub bodies: Mutex<Vec<Value>>,
    pub selected: Mutex<Vec<ProviderAccountId>>,
    pub leases: Arc<Mutex<BTreeMap<ProviderAccountId, usize>>>,
    pub selection_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub selection_waiters: Arc<AtomicUsize>,
    pub finalizations: Mutex<Vec<ModelRequestFinalization>>,
    pub charges: Mutex<Vec<ClientBudgetCharge>>,
    pub continuation: Mutex<Option<NativeContinuationPin>>,
    pub admit_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub commit_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub settle_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
struct Lease;
struct SelectionWait(Arc<AtomicUsize>);
impl Drop for SelectionWait {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct AccountLease {
    account: ProviderAccountId,
    leases: Arc<Mutex<BTreeMap<ProviderAccountId, usize>>>,
}
impl Drop for AccountLease {
    fn drop(&mut self) {
        let mut leases = self.leases.lock().unwrap();
        let active = leases.get_mut(&self.account).unwrap();
        *active -= 1;
        if *active == 0 {
            leases.remove(&self.account);
        }
    }
}
impl ExecutionRecoveryLeaseGuard for Lease {}
impl ClientAdmissionLeaseGuard for Lease {}
#[async_trait]
impl ExecutionStore for Core {
    fn maintain_request(
        &self,
        _: &ModelRequestId,
        _: Option<SystemTime>,
        _: Duration,
    ) -> Box<dyn ExecutionRecoveryLeaseGuard> {
        Box::new(Lease)
    }
    async fn create_model_request(&self, request: NewModelRequest) -> Result<(), StoreError> {
        self.requests.lock().unwrap().push(request);
        Ok(())
    }
    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), StoreError> {
        self.attempts.lock().unwrap().push(attempt);
        Ok(())
    }
    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let gate = self.commit_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.await.unwrap();
        }
        Ok(())
    }
    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        Ok(())
    }
    async fn finalize_model_request(
        &self,
        row: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.finalizations.lock().unwrap().push(row);
        Ok(())
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        Ok(RecoveryReport::default())
    }
}
impl ClientAdmissionPort for Core {
    fn maintain(
        &self,
        _: &ClientApiKeyId,
        _: &ModelRequestId,
        _: Duration,
        _: CancellationToken,
    ) -> Box<dyn ClientAdmissionLeaseGuard> {
        Box::new(Lease)
    }
    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_add(1, Ordering::SeqCst);
            let gate = self.admit_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.unwrap();
            }
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            assert!(self.active.fetch_sub(1, Ordering::SeqCst) > 0);
            Ok(true)
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}
impl ClientBudgetPort for Core {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<AccountGroupId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            Ok(ClientBudgetScope {
                user_id: "alice".into(),
                group_id: AccountGroupId::new("grp_00000000000000000000000000000001").unwrap(),
            })
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            let gate = self.settle_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.unwrap();
            }
            assert!(self.active.load(Ordering::SeqCst) > 0);
            self.charges.lock().unwrap().push(charge);
            Ok(())
        })
    }
}
impl NativeContinuationPort for Core {
    fn resolve<'a>(
        &'a self,
        key: &'a ClientApiKeyId,
        previous: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async move {
            Ok(self
                .continuation
                .lock()
                .unwrap()
                .as_ref()
                .filter(|pin| pin.matches_client(key) && pin.previous_response_id() == previous)
                .cloned())
        })
    }
    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { Ok(()) })
    }
}
impl ClientApiKeyUsageSink for Core {
    fn record_used(&self, _: &ClientApiKeyId) {}
}
#[async_trait]
impl Provider for Core {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(vec![])
    }
    async fn query_client_model_catalog(
        &self,
        scope: &FrozenAccountScope,
        protocol: &str,
        _: &str,
    ) -> Result<Option<Vec<ProviderModelDescriptor>>, ProviderCatalogUnavailable> {
        self.catalogs.fetch_add(1, Ordering::SeqCst);
        let gate = self.catalog_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.await.unwrap();
        }
        assert!(scope.allows(&ProviderAccountId::new("acct_allowed").unwrap()));
        assert_eq!(protocol, "openai");
        Ok(Some(vec![ProviderModelDescriptor {
            model: UpstreamModelId::new("gpt-native").unwrap(),
            payload: RawJsonPayload::new(
                "openai",
                bytes::Bytes::from_static(br#"{"id":"gpt-native"}"#),
            )
            .unwrap(),
        }]))
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let gate = self.selection_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            self.selection_waiters.fetch_add(1, Ordering::SeqCst);
            let _wait = SelectionWait(self.selection_waiters.clone());
            gate.await.unwrap();
        }
        if let Operation::Generate(generate) = request.operation() {
            self.bodies
                .lock()
                .unwrap()
                .push(generate.protocol_payload().body().clone().into());
        }
        assert_eq!(context.client_api_key_ref().as_str(), "key_alice");
        let account = context.required_account().cloned().unwrap_or_else(|| {
            [
                "acct_allowed",
                "acct_alternative",
                "acct_third",
                "acct_fallback",
            ]
            .into_iter()
            .map(|id| ProviderAccountId::new(id).unwrap())
            .find(|id| {
                context.account_scope().unwrap().allows(id)
                    && !context.excluded_accounts().contains(id)
                    && context
                        .account_scope()
                        .unwrap()
                        .directory()
                        .account(id)
                        .unwrap()
                        .provider_kind()
                        == request.candidate().provider()
            })
            .expect("Core must preserve at least one eligible account")
        });
        assert!(context.account_scope().unwrap().allows(&account));
        assert!(!context.excluded_accounts().contains(&account));
        self.selected.lock().unwrap().push(account.clone());
        let lease_account = account.clone();
        let metadata = match request.candidate().upstream_model() {
            Some(model) => ProviderCallMetadata::new(
                request.candidate().provider().clone(),
                model.clone(),
                account,
                UpstreamTransport::new("http").unwrap(),
            ),
            None => ProviderCallMetadata::for_provider_endpoint(
                request.candidate().provider().clone(),
                account,
                UpstreamTransport::new("http").unwrap(),
            ),
        };
        let mode = match request.operation() {
            Operation::Generate(g) => g
                .protocol_payload()
                .body()
                .get("failure")
                .and_then(Value::as_str)
                .or_else(|| {
                    g.protocol_payload()
                        .body()
                        .get("mode")
                        .and_then(Value::as_str)
                })
                .unwrap_or_default(),
            _ => "",
        };
        if request.candidate().provider().as_str() == "openai" {
            let kind = match mode {
                "fail-no-account" => Some(ProviderErrorKind::NoEligibleAccount),
                "fail-capacity" => Some(ProviderErrorKind::AccountCapacityUnavailable),
                "fail-quota" => Some(ProviderErrorKind::QuotaExhausted),
                "fail-infrastructure" => Some(ProviderErrorKind::ProviderInfrastructureUnavailable),
                "fail-native-selection"
                    if context.continuation_attempt() == ContinuationAttempt::Native =>
                {
                    Some(ProviderErrorKind::NoEligibleAccount)
                }
                _ => None,
            };
            if let Some(kind) = kind {
                return Err(ProviderError::new(kind, UpstreamSendState::NotSent));
            }
        }
        if let Some(send_state) = match mode {
            "fail-not-sent" => Some(UpstreamSendState::NotSent),
            "fail-sent" => Some(UpstreamSendState::Sent),
            "fail-ambiguous" => Some(UpstreamSendState::Ambiguous),
            _ => None,
        } {
            return Err(
                ProviderError::new(ProviderErrorKind::InvalidRequest, send_state).with_status(400),
            );
        }
        let meta = if matches!(request.operation(), Operation::Generate(_)) {
            ResponseMeta::new(context.request_id().as_str(), "gpt-native")
        } else {
            ResponseMeta::for_provider_endpoint(context.request_id().as_str())
        };
        let response =
            json!({"id":meta.response_id(),"model":"gpt-native","output":[],"status":"completed"});
        let start = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Started(meta.clone())],
            ProtocolWireEvent::json(
                "openai",
                Some("response.created".into()),
                json!({"type":"response.created","response":response}),
            )
            .unwrap(),
        );
        let wire = if matches!(request.operation(), Operation::Generate(_)) {
            ProtocolWireEvent::json(
                "openai",
                Some("response.completed".into()),
                json!({"type":"response.completed","response":response}),
            )
            .unwrap()
        } else {
            ProtocolWireEvent::raw_json(
                "openai",
                bytes::Bytes::from_static(br#"{"result":"fixture"}"#),
            )
            .unwrap()
        };
        let events: Vec<Result<ProviderEvent, ProviderError>> = vec![
            Ok(start),
            Ok(ProviderEvent::canonical_with_wire(
                vec![GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(123).unwrap(),
                )],
                ProtocolWireEvent::raw_sse(
                    "openai",
                    bytes::Bytes::from_static(b": fixture cost\n\n"),
                )
                .unwrap(),
            )),
            Ok(ProviderEvent::canonical_with_wire(
                vec![GatewayEvent::Completed(meta)],
                wire,
            )),
        ];
        let polled = self.polled.clone();
        let stream: EventStream = if mode == "stall" {
            Box::pin(
                futures::stream::iter(events.into_iter().take(2)).chain(futures::stream::pending()),
            )
        } else {
            Box::pin(futures::stream::iter(events))
        };
        Ok(ProviderStream::new(
            metadata,
            stream.inspect(move |_| {
                polled.fetch_add(1, Ordering::SeqCst);
            }),
            {
                *self
                    .leases
                    .lock()
                    .unwrap()
                    .entry(lease_account.clone())
                    .or_default() += 1;
                AccountLease {
                    account: lease_account,
                    leases: self.leases.clone(),
                }
            },
        ))
    }
}
struct FallbackProvider(Arc<Core>);
#[async_trait]
impl Provider for FallbackProvider {
    fn name(&self) -> &'static str {
        "xai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(vec![])
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.0.execute(request, context).await
    }
}
fn key_record(id: &str, enabled: bool) -> ClientKeyRecord {
    ClientKeyRecord {
        active_concurrency: None,
        id: ClientApiKeyId::new(id).unwrap(),
        name: id.into(),
        label: None,
        groups: vec![],
        provider_kinds: vec![ProviderKind::new("openai").unwrap()],
        prefix: "sk_fixture".into(),
        enabled,
        limits: RateLimits::unlimited(),
        budget: Default::default(),
        last_used_at: None,
        created_at: "2026-10-01T00:00:00Z".parse().unwrap(),
        updated_at: "2026-10-01T00:00:00Z".parse().unwrap(),
    }
}
pub struct Keys;
#[async_trait]
impl ClientKeyService for Keys {
    async fn list(
        &self,
        owner: &str,
        query: ClientKeyListQuery,
    ) -> Result<ClientKeyPage, AdminError> {
        if owner != "alice" {
            return Err(AdminError::not_found("caller"));
        }
        let mut items = vec![
            key_record("key_alice", true),
            key_record("key_disabled", false),
        ];
        if let Some(cursor) = query.cursor {
            items.retain(|k| k.id > cursor.id);
        }
        let total = 2;
        let next_cursor = if items.len() > query.page_size.get() as usize {
            items.truncate(query.page_size.get() as usize);
            items.last().map(|k| ClientKeyCursor {
                sort: query.sort,
                value: ClientKeyCursorValue::Name(k.name.clone()),
                id: k.id.clone(),
            })
        } else {
            None
        };
        Ok(ClientKeyPage {
            config_revision: Revision::new(1).unwrap(),
            items,
            total,
            next_cursor,
        })
    }
    async fn reveal(
        &self,
        owner: &str,
        id: &ClientApiKeyId,
    ) -> Result<ClientKeySecret, AdminError> {
        if owner != "alice" || !matches!(id.as_str(), "key_alice" | "key_disabled") {
            return Err(AdminError::not_found("Key"));
        }
        Ok(ClientKeySecret::new(
            key_record(id.as_str(), id.as_str() != "key_disabled"),
            if id.as_str() == "key_alice" {
                "sk_native_fixture"
            } else {
                "sk_disabled_fixture"
            },
        ))
    }
    async fn create(
        &self,
        _: &MutationContext,
        _: CreateClientKey,
    ) -> Result<CreatedClientKey, AdminError> {
        Err(AdminError::invalid("unused"))
    }
    async fn update(
        &self,
        _: &MutationContext,
        _: UpdateClientKey,
    ) -> Result<ClientKeyMutation, AdminError> {
        Err(AdminError::invalid("unused"))
    }
    async fn set_enabled(
        &self,
        _: &MutationContext,
        _: SetClientKeyEnabled,
    ) -> Result<ClientKeyMutation, AdminError> {
        Err(AdminError::invalid("unused"))
    }
    async fn delete(
        &self,
        _: &MutationContext,
        _: DeleteClientKey,
    ) -> Result<ClientKeyMutation, AdminError> {
        Err(AdminError::invalid("unused"))
    }
}
pub fn snapshot(revision: u64, enabled: bool) -> RuntimeSnapshot {
    snapshot_with_accounts(revision, enabled, &["acct_allowed"])
}
fn snapshot_with_accounts(revision: u64, enabled: bool, accounts: &[&str]) -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").unwrap();
    let mut providers = vec![provider.clone()];
    if accounts.contains(&"acct_fallback") {
        providers.push(ProviderKind::new("xai").unwrap());
    }
    let directory = Arc::new(RuntimeAccountDirectory::new(
        accounts
            .iter()
            .map(|id| {
                (
                    ProviderAccountId::new(*id).unwrap(),
                    RuntimeAccount::new(
                        if *id == "acct_fallback" {
                            ProviderKind::new("xai").unwrap()
                        } else {
                            provider.clone()
                        },
                        BTreeSet::new(),
                    ),
                )
            })
            .collect(),
    ));
    let scope = Arc::new(FrozenAccountScope::new(
        directory.clone(),
        ClientRoutingScope::all_accounts(),
    ));
    RuntimeSnapshot::new(
        ConfigRevision::new(revision).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(1).unwrap(),
            Duration::ZERO,
        ),
        providers.clone(),
        providers
            .into_iter()
            .map(|provider| {
                ProviderModel::new(
                    provider,
                    UpstreamModelId::new("gpt-native").unwrap(),
                    ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
                        .with_upstream_feature_validation(),
                )
            })
            .collect(),
        vec![
            ClientPolicy::new(
                ClientApiKeyId::new("key_alice").unwrap(),
                PlaintextClientApiKey::new("sk_native_fixture").unwrap(),
                scope.clone(),
                enabled,
                RateLimits::unlimited(),
            ),
            ClientPolicy::new(
                ClientApiKeyId::new("key_disabled").unwrap(),
                PlaintextClientApiKey::new("sk_disabled_fixture").unwrap(),
                scope,
                false,
                RateLimits::unlimited(),
            ),
        ],
    )
    .unwrap()
    .with_account_directory(directory)
}
pub struct Fixture {
    pub core: Arc<Core>,
    pub execution: Arc<DefaultExecutionService>,
    pub snapshot: RuntimeSnapshotHandle,
    pub manager: Arc<PluginManager>,
    pub id: String,
    _cache: tempfile::TempDir,
}
impl Fixture {
    pub async fn new() -> Self {
        Self::build(false, &["acct_allowed"]).await
    }
    pub async fn with_middleware() -> Self {
        Self::build(true, &["acct_allowed"]).await
    }
    pub async fn with_accounts(accounts: &[&str]) -> Self {
        Self::build(true, accounts).await
    }
    async fn build(middleware: bool, accounts: &[&str]) -> Self {
        let core = Arc::new(Core::default());
        let mut providers: Vec<Arc<dyn Provider>> = vec![core.clone()];
        if accounts.contains(&"acct_fallback") {
            providers.push(Arc::new(FallbackProvider(core.clone())));
        }
        let snapshot = RuntimeSnapshotHandle::new(snapshot_with_accounts(1, true, accounts));
        let execution = Arc::new(
            DefaultExecutionService::new(
                snapshot.clone(),
                core.clone(),
                ProviderRegistry::new(providers).unwrap(),
                core.clone(),
                core.clone(),
                core.clone(),
            )
            .with_budget(core.clone()),
        );
        let store = Arc::new(MemoryStore::default());
        let cache = tempfile::tempdir().unwrap();
        let manager = Arc::new_cyclic(|plugins| {
            let engine = PluginEngine::new(
                cache.path().into(),
                "3.11.2".parse().unwrap(),
                store.clone(),
            )
            .with_models(
                Arc::new(gateway_api::openai::middleware::PluginModelExecution::new(
                    execution.clone(),
                    plugins.clone(),
                )),
                Arc::new(Keys),
            );
            let runtime: Arc<dyn PluginRuntime> = if middleware {
                Arc::new(crate::middleware::plan::KernelRuntime(engine))
            } else {
                Arc::new(engine)
            };
            PluginManager::new(store, runtime)
        });
        let artifact = manager
            .upload(archive("1.0.0", middleware), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        if middleware {
            command.middleware = Some(gateway_admin::model::plugins::PluginMiddlewareBinding {
                order: 0,
                mounts: vec![gateway_admin::model::plugins::PluginMiddlewareMount::Request],
                scope: gateway_admin::model::plugins::PluginExecutionScope::All {},
            });
        }
        let installed = manager.update(None, command, &actor()).await.unwrap();
        Self {
            core,
            execution,
            snapshot,
            manager,
            id: installed.id,
            _cache: cache,
        }
    }
    pub async fn call(&self, input: Value) -> Value {
        self.call_as(
            input,
            gateway_admin::model::MutationActor::AdminSession {
                admin_user_id: "alice".into(),
            },
        )
        .await
    }
    pub async fn start(&self, prepared: PreparedRootExecution) -> StartedExecution {
        self.execution
            .start_prepared(
                prepared,
                PreparedExecutionRequest {
                    public_model: PublicModelId::new("gpt-native").unwrap(),
                    operation: Operation::Generate(
                        gateway_core::operation::GenerateRequest::from_protocol_payload(
                            gateway_core::operation::ProtocolPayload::json_object(
                                "openai",
                                serde_json::from_value(
                                    json!({"model":"gpt-native","input":"parent"}),
                                )
                                .unwrap(),
                            )
                            .unwrap(),
                        ),
                    ),
                    metadata: ExecutionRequestMetadata {
                        protocol: "openai".into(),
                        endpoint: "/v1/responses".into(),
                        transport: ClientTransport::HttpJson,
                        stream: false,
                        client_ip: None,
                        user_agent: None,
                        previous_response_id: None,
                    },
                },
            )
            .await
            .unwrap()
    }
    pub async fn install_layers(&self) -> String {
        let artifact = self
            .manager
            .upload(archive("1.0.0", true), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        command.middleware = Some(gateway_admin::model::plugins::PluginMiddlewareBinding {
            order: 1,
            mounts: vec![
                gateway_admin::model::plugins::PluginMiddlewareMount::Request,
                gateway_admin::model::plugins::PluginMiddlewareMount::Attempt,
            ],
            scope: gateway_admin::model::plugins::PluginExecutionScope::All {},
        });
        self.manager
            .update(None, command, &actor())
            .await
            .unwrap()
            .id
    }
    pub async fn call_as(
        &self,
        mut input: Value,
        actor: gateway_admin::model::MutationActor,
    ) -> Value {
        input["operation"] = json!("models");
        let mut request = crate::support::request("models");
        request.actor.actor = actor;
        request.body = serde_json::to_vec(&input).unwrap();
        let response = self.manager.handle(&self.id, 1, request).await.unwrap();
        serde_json::from_slice(&response.body).unwrap()
    }
    pub async fn clean(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.core.active.load(Ordering::SeqCst) == 0
                    && self.core.charges.lock().unwrap().len() == count
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all native Core sessions must settle and release");
    }
}
