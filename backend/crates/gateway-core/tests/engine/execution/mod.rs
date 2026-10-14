use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use gateway_core::engine::budget::{ClientBudgetCharge, ClientBudgetError, ClientBudgetPort};
use gateway_core::error::GatewayError;
mod affinity;

#[derive(Default)]
pub(super) struct Admissions {
    pub(super) renewals: Arc<AtomicUsize>,
    pub(super) active: Arc<AtomicBool>,
    pub(super) limits: Mutex<Vec<RateLimits>>,
    pub(super) release_gate: Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) releases: AtomicUsize,
}

#[test]
fn extension_graph_idle_wait_tracks_original_http_permits_and_does_not_reset_call_budget() {
    use gateway_core::engine::nested::ExtensionCallScope;
    block_on(async {
        let root = ExtensionCallScope::default();
        assert!(root.is_root());
        root.wait_until_idle().await;
        let child = root.fork_http("graph-idle".into()).unwrap();
        assert!(!child.is_root());
        let held = child.clone();
        let mut wait = Box::pin(root.wait_until_idle());
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(child);
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(held);
        wait.await;
        for _ in 1..64 {
            drop(root.fork_http("graph-idle".into()).unwrap());
        }
        root.wait_until_idle().await;
        assert!(root.fork_http("graph-idle".into()).is_err());
    });
}

#[test]
fn extension_graph_idle_wait_wakes_all_waiters_when_release_races_registration() {
    use gateway_core::engine::nested::ExtensionCallScope;
    for _ in 0..128 {
        let root = ExtensionCallScope::default();
        let child = root.fork_http("graph-race".into()).unwrap();
        let dropper = std::thread::spawn(move || drop(child));
        block_on(async {
            let wait = futures::future::join(root.wait_until_idle(), root.wait_until_idle());
            futures::pin_mut!(wait);
            let timeout = futures_timer::Delay::new(Duration::from_secs(2));
            futures::pin_mut!(timeout);
            assert!(matches!(
                futures::future::select(wait, timeout).await,
                futures::future::Either::Left(_)
            ));
        });
        dropper.join().unwrap();
    }
}

impl ClientAdmissionPort for Admissions {
    fn maintain(
        &self,
        _: &gateway_core::policy::ClientApiKeyId,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::Duration,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Box<dyn gateway_core::engine::admission::ClientAdmissionLeaseGuard> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        Box::new(CountingAdmissionLease(self.renewals.clone()))
    }

    fn admit(
        &self,
        request: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async move {
            self.limits.lock().unwrap().push(request.limits);
            assert!(!self.active.swap(true, Ordering::SeqCst));
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            assert_eq!(
                self.renewals.load(Ordering::SeqCst),
                0,
                "stop renewal before release"
            );
            self.releases.fetch_add(1, Ordering::SeqCst);
            let gate = self.release_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.expect("release gate");
            }
            Ok(self.active.swap(false, Ordering::SeqCst))
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

#[derive(Default)]
pub(super) struct Budget {
    pub(super) reject: bool,
    pub(super) active: Arc<AtomicBool>,
    pub(super) charges: Mutex<Vec<ClientBudgetCharge>>,
    pub(super) settlement_gate: Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) settlements: AtomicUsize,
    pub(super) fail_settlement: bool,
}

impl ClientBudgetPort for Budget {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<gateway_core::routing::AccountGroupId>,
    ) -> BoxFuture<'_, Result<gateway_core::engine::budget::ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            assert!(self.active.load(Ordering::SeqCst));
            if self.reject {
                Err(
                    GatewayError::new(GatewayErrorKind::RateLimited, "budget exhausted")
                        .with_client_code("key_daily_budget_exceeded"),
                )
            } else {
                Ok(gateway_core::engine::budget::ClientBudgetScope {
                    user_id: "user-1".into(),
                    group_id: gateway_core::routing::AccountGroupId::new(
                        "grp_00000000000000000000000000000001",
                    )
                    .unwrap(),
                })
            }
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            self.settlements.fetch_add(1, Ordering::SeqCst);
            assert!(
                self.active.load(Ordering::SeqCst),
                "settle before releasing the concurrent request slot"
            );
            let gate = self.settlement_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.expect("settlement gate");
            }
            assert!(self.active.load(Ordering::SeqCst));
            self.charges.lock().unwrap().push(charge);
            if self.fail_settlement {
                Err(ClientBudgetError)
            } else {
                Ok(())
            }
        })
    }
}

fn service(admissions: Arc<Admissions>, budget: Arc<Budget>) -> DefaultExecutionService {
    DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
    .with_budget(budget)
}

pub(super) fn request(
    service: &DefaultExecutionService,
    transport: ClientTransport,
) -> StartExecution {
    StartExecution {
        client: service.authenticate("sk_start_test").unwrap(),
        public_model: PublicModelId::new("gpt-start").unwrap(),
        operation: start_operation(),
        metadata: ExecutionRequestMetadata {
            protocol: "openai".to_owned(),
            endpoint: "/v1/responses".to_owned(),
            transport,
            stream: transport != ClientTransport::HttpJson,
            client_ip: None,
            user_agent: None,
            previous_response_id: None,
        },
    }
}

struct SchedulingPolicy {
    mode: &'static str,
    revocation: gateway_core::lifecycle::CancellationToken,
    observed: Mutex<Vec<gateway_core::engine::account_scheduling::AccountScheduleInput>>,
}
impl gateway_core::engine::account_scheduling::AccountScheduler for SchedulingPolicy {
    fn cancellation(&self) -> gateway_core::lifecycle::CancellationToken {
        gateway_core::lifecycle::CancellationToken::new()
    }
    fn schedule_account(
        &self,
        input: gateway_core::engine::account_scheduling::AccountScheduleInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::account_scheduling::AccountScheduleResult,
            gateway_core::engine::account_scheduling::AccountSchedulingFault,
        >,
    > {
        use gateway_core::engine::account_scheduling::*;
        self.observed.lock().unwrap().push(input);
        let mode = self.mode;
        let revocation = self.revocation.clone();
        Box::pin(async move {
            if mode == "fault" {
                return Err(AccountSchedulingFault);
            }
            Ok(AccountScheduleResult {
                revocation,
                decision: match mode {
                    "reject" => AccountScheduleDecision::Reject,
                    "foreign" => AccountScheduleDecision::Pick(
                        ProviderAccountId::new("acct_foreign").unwrap(),
                    ),
                    "pick" | "hard" => AccountScheduleDecision::Pick(
                        ProviderAccountId::new("acct_second").unwrap(),
                    ),
                    _ => AccountScheduleDecision::Delegate,
                },
            })
        })
    }
}
struct SchedulingProvider {
    mode: &'static str,
    outcomes: Arc<
        Mutex<Vec<Result<String, gateway_core::engine::account_scheduling::AccountPolicyError>>>,
    >,
}
#[async_trait]
impl Provider for SchedulingProvider {
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
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        use gateway_core::account::*;
        let now = SystemTime::now();
        let candidates = ["acct_start", "acct_second", "acct_foreign"]
            .into_iter()
            .map(|id| AccountCandidate {
                account: ProviderAccount::new(
                    ProviderAccountId::new(id).unwrap(),
                    ProviderKind::new("openai").unwrap(),
                    id.into(),
                    Some(id.into()),
                    "oauth".into(),
                    CredentialRevision::new(1).unwrap(),
                    Some(if self.mode == "stale" {
                        now - Duration::from_secs(1)
                    } else {
                        now + Duration::from_secs(3600)
                    }),
                )
                .with_account_facts(
                    true,
                    CredentialState::Ready,
                    QuotaState::unknown(),
                    None,
                    None,
                ),
                signals: AccountRuntimeSignals {
                    in_flight: u32::from(self.mode == "busy"),
                    last_started_at: None,
                    quota_reset_at: None,
                    quota_remaining_rank: Some(7500),
                    rate_limited_until: None,
                    failure_rate_basis_points: Some(300),
                    first_output_latency_ms: Some(250),
                },
            })
            .collect::<Vec<_>>();
        let required = (self.mode == "hard").then(|| ProviderAccountId::new("acct_start").unwrap());
        let selection_context = AccountSelectionContext {
            policy: context.account_selection_policy(),
            now: if self.mode == "stale" {
                now - Duration::from_secs(2)
            } else {
                now
            },
            excluded_accounts: context.excluded_accounts().clone(),
            preferred_account: Some(ProviderAccountId::new("acct_start").unwrap()),
            preferred_account_overrides_weight: true,
            round_robin_cursor: 0,
            eligibility: AccountEligibilityPolicy::Enforce,
            account_scope: context.account_scope().cloned(),
        };
        let result = context
            .select_account(
                request.candidate().provider(),
                request.candidate().upstream_model().map(|m| m.as_str()),
                &candidates,
                &selection_context,
                required.as_ref(),
            )
            .await;
        self.outcomes.lock().unwrap().push(
            result
                .as_ref()
                .map(|selection| {
                    selection.map_or("none".into(), |s| {
                        s.candidate().account.id().as_str().into()
                    })
                })
                .map_err(|e| *e),
        );
        let selection = result
            .map_err(|e| e.into_provider_error())?
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::NoEligibleAccount,
                    UpstreamSendState::NotSent,
                )
            })?;
        let metadata = ProviderCallMetadata::new(
            request.candidate().provider().clone(),
            request.candidate().upstream_model().unwrap().clone(),
            selection.candidate().account.id().clone(),
            UpstreamTransport::new("http").unwrap(),
        );
        let response = ResponseMeta::new("resp_schedule", "gpt-start");
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(vec![
                Ok(GatewayEvent::Started(response.clone()).into()),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(known_charge().scaled()).unwrap(),
                )
                .into()),
                Ok(GatewayEvent::Completed(response).into()),
            ]),
            (),
        ))
    }
}

type SchedulingFixture = (
    DefaultExecutionService,
    Arc<Admissions>,
    Arc<Budget>,
    Arc<SchedulingPolicy>,
    Arc<Mutex<Vec<Result<String, gateway_core::engine::account_scheduling::AccountPolicyError>>>>,
);
struct SchedulingFallback(&'static str);
#[async_trait]
impl Provider for SchedulingFallback {
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
        _: ProviderRequest,
        _: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        assert!(
            matches!(self.0, "busy" | "stale"),
            "a policy rejection/fault must not delegate to another Provider: {}",
            self.0
        );
        Err(ProviderError::new(
            ProviderErrorKind::NoEligibleAccount,
            UpstreamSendState::NotSent,
        ))
    }
}
fn scheduling_fixture(mode: &'static str) -> SchedulingFixture {
    let admissions = Arc::new(Admissions::default());
    let budget = Arc::new(Budget {
        active: admissions.active.clone(),
        ..Default::default()
    });
    let policy = Arc::new(SchedulingPolicy {
        mode,
        revocation: gateway_core::lifecycle::CancellationToken::new(),
        observed: Mutex::new(vec![]),
    });
    let outcomes = Arc::new(Mutex::new(vec![]));
    let provider = ProviderKind::new("openai").unwrap();
    let xai = ProviderKind::new("xai").unwrap();
    let directory = ["acct_start", "acct_second"]
        .into_iter()
        .map(|id| {
            (
                ProviderAccountId::new(id).unwrap(),
                RuntimeAccount::new(provider.clone(), BTreeSet::new()),
            )
        })
        .chain([(
            ProviderAccountId::new("acct_xai").unwrap(),
            RuntimeAccount::new(xai.clone(), BTreeSet::new()),
        )])
        .collect();
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(directory)),
        ClientRoutingScope::all_accounts(),
    ));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).unwrap(),
            Duration::from_millis(1),
        ),
        vec![provider.clone(), xai.clone()],
        [provider, xai]
            .into_iter()
            .map(|p| {
                ProviderModel::new(
                    p,
                    UpstreamModelId::new("gpt-start").unwrap(),
                    ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), Some(16_000)),
                )
            })
            .collect(),
        vec![ClientPolicy::new(
            ClientApiKeyId::new("key_start_test").unwrap(),
            PlaintextClientApiKey::new("sk_start_test").unwrap(),
            scope,
            true,
            RateLimits::unlimited(),
        )],
    )
    .unwrap();
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(snapshot),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::new([
            Arc::new(SchedulingProvider {
                mode,
                outcomes: outcomes.clone(),
            }) as Arc<dyn Provider>,
            Arc::new(SchedulingFallback(mode)) as Arc<dyn Provider>,
        ])
        .unwrap(),
        admissions.clone(),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
    .with_budget(budget.clone());
    (service, admissions, budget, policy, outcomes)
}

#[test]
fn account_scheduling_projects_core_scope_preserves_hard_pins_and_rechecks_expiry() {
    block_on(async {
        use gateway_core::engine::{
            account_scheduling::AccountPolicyError, execution::PreparedExecutionRequest,
        };
        for mode in [
            "pick", "delegate", "foreign", "hard", "stale", "reject", "fault", "busy",
        ] {
            let (service, admissions, budget, policy, outcomes) = scheduling_fixture(mode);
            let r = request(&service, ClientTransport::HttpJson);
            let prepared = service
                .prepare_execution(r.client)
                .unwrap()
                .with_account_scheduler(policy.clone());
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: r.public_model,
                        operation: r.operation,
                        metadata: r.metadata,
                    },
                )
                .await
                .unwrap();
            if matches!(mode, "pick" | "delegate") {
                consume_charged_prefix(started.session.as_mut()).await;
            } else {
                assert!(started.session.next_event().await.is_err(), "{mode}");
            }
            started.session.detach_finalize().await;
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
            let observed = policy.observed.lock().unwrap();
            if mode == "busy" {
                assert!(observed.is_empty());
            } else {
                assert_eq!(
                    observed.len(),
                    1,
                    "policy failure must not be retried: {mode}"
                );
                let input = &observed[0];
                assert_eq!(input.client_key_id().as_str(), "key_start_test");
                assert_eq!(input.model(), Some("gpt-start"));
                assert_eq!(input.attempt_index().get(), 1);
                let ids = input
                    .candidates()
                    .iter()
                    .map(|c| c.account_id.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    ids,
                    if mode == "hard" {
                        vec!["acct_start"]
                    } else {
                        vec!["acct_start", "acct_second"]
                    }
                );
                assert_eq!(input.candidates()[0].maximum_concurrency, 1);
                assert_eq!(input.candidates()[0].quota_remaining_rank, Some(7500));
                assert_eq!(input.candidates()[0].failure_rate_basis_points, Some(300));
            }
            let outcomes = outcomes.lock().unwrap();
            let expected = match mode {
                "foreign" | "hard" | "fault" => Err(AccountPolicyError::Fault),
                "reject" => Err(AccountPolicyError::Rejected),
                "stale" => Err(AccountPolicyError::StaleCandidate),
                "pick" => Ok("acct_second".to_owned()),
                "delegate" => Ok("acct_start".to_owned()),
                _ => Ok("none".to_owned()),
            };
            assert_eq!(&outcomes[..], &[expected], "{mode}");
            assert_eq!(
                budget.charges.lock().unwrap()[0].amount_usd,
                if matches!(mode, "pick" | "delegate") {
                    known_charge()
                } else {
                    Decimal::ZERO
                }
            );
        }
    });
}

#[test]
fn consulted_account_policy_revocation_reaches_original_execution_after_selection() {
    block_on(async {
        use gateway_core::engine::execution::PreparedExecutionRequest;
        let (service, admissions, budget, policy, _) = scheduling_fixture("pick");
        let r = request(&service, ClientTransport::HttpSse);
        let prepared = service
            .prepare_execution(r.client)
            .unwrap()
            .with_account_scheduler(policy.clone());
        let cancellation = prepared.cancellation();
        let mut started = service
            .start_prepared(
                prepared,
                PreparedExecutionRequest {
                    public_model: r.public_model,
                    operation: r.operation,
                    metadata: r.metadata,
                },
            )
            .await
            .unwrap();
        consume_charged_prefix(started.session.as_mut()).await;
        assert_eq!(policy.observed.lock().unwrap().len(), 1);
        assert!(!cancellation.is_cancelled());
        policy.revocation.cancel();
        assert!(
            cancellation.is_cancelled(),
            "existing API/root clones must observe the consulted generation"
        );
        assert!(matches!(
            started.session.next_event().await,
            Err(gateway_core::engine::EngineError::Cancelled)
        ));
        started.session.detach_finalize().await;
        assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
        assert_eq!(budget.charges.lock().unwrap()[0].amount_usd, known_charge());
    });
}

#[test]
fn routing_policy_preserves_requested_model_and_revalidates_original_capability_limits() {
    use gateway_core::engine::{execution::PreparedExecutionRequest, model_routing::*};
    use gateway_core::lifecycle::CancellationToken;
    struct Router(AtomicUsize);
    impl ModelRouter for Router {
        fn cancellation(&self) -> CancellationToken {
            CancellationToken::new()
        }
        fn route_model(
            &self,
            input: ModelRouteInput,
        ) -> BoxFuture<'static, Result<ModelRouteResult, ModelRoutingFault>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(input.requested_model().as_str(), "gpt-alias");
            assert_eq!(input.client_key_id().as_str(), "key_start_test");
            assert_eq!(
                input.available_providers(),
                &BTreeSet::from([ProviderKind::new("openai").unwrap()])
            );
            Box::pin(async {
                Ok(ModelRouteResult {
                    decision: ModelRouteDecision::Route {
                        provider: None,
                        model: Some(PublicModelId::new("gpt-start").unwrap()),
                    },
                    revocation: CancellationToken::new(),
                })
            })
        }
    }
    block_on(async {
        for max_tokens in [20_001, 128] {
            let admissions = Arc::new(Admissions::default());
            let store = Arc::new(TrackingExecutionStore::default());
            let router = Arc::new(Router(AtomicUsize::new(0)));
            let service = DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(start_snapshot()),
                store.clone(),
                ProviderRegistry::new([
                    Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>
                ])
                .unwrap(),
                admissions.clone(),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            );
            let request = request(&service, ClientTransport::HttpJson);
            let body = json!({"model":"gpt-alias","input":"original input","max_output_tokens":max_tokens});
            let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body.as_object().unwrap().clone()).unwrap(),
            ));
            let prepared = service
                .prepare_execution(request.client)
                .unwrap()
                .with_model_router(router.clone());
            let result = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: PublicModelId::new("gpt-alias").unwrap(),
                        operation,
                        metadata: request.metadata,
                    },
                )
                .await;
            assert_eq!(router.0.load(Ordering::SeqCst), 1);
            if max_tokens > 16_000 {
                assert!(
                    matches!(result, Err(e) if e.kind() == GatewayErrorKind::NoAvailableProvider)
                );
                assert!(admissions.limits.lock().unwrap().is_empty());
                assert!(store.requests.lock().unwrap().is_empty());
            } else {
                let mut started = result.unwrap();
                consume_charged_prefix(started.session.as_mut()).await;
                started.session.detach_finalize().await;
                assert_eq!(
                    store.requests.lock().unwrap()[0]
                        .requested_model
                        .as_ref()
                        .unwrap()
                        .as_str(),
                    "gpt-alias"
                );
                assert_eq!(
                    store.attempts.lock().unwrap()[0]
                        .upstream_model_id
                        .as_ref()
                        .unwrap()
                        .as_str(),
                    "gpt-start"
                );
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
            }
        }
    });
}

#[test]
fn preparing_middleware_execution_is_lazy_and_preserves_one_identity_and_deadline() {
    use gateway_core::engine::execution::PreparedExecutionRequest;
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let store = Arc::new(TrackingExecutionStore::default());
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(start_snapshot()),
            store.clone(),
            ProviderRegistry::new([Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>])
                .unwrap(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let request = request(&service, ClientTransport::HttpSse);
        let prepared = service.prepare_execution(request.client).unwrap();
        let id = prepared.request_id().clone();
        let start = prepared.started_at();
        let deadline = prepared.deadline_at();
        assert_eq!(deadline.at(), None);
        assert_eq!(
            prepared.client().policy().key_id().as_str(),
            "key_start_test"
        );
        assert!(admissions.limits.lock().unwrap().is_empty());
        assert!(!store.touched.load(Ordering::SeqCst));
        let mut started = service
            .start_prepared(
                prepared,
                PreparedExecutionRequest {
                    public_model: request.public_model,
                    operation: request.operation,
                    metadata: request.metadata,
                },
            )
            .await
            .unwrap();
        assert_eq!(started.request_id, id);
        assert_eq!(started.created_at, start);
        consume_charged_prefix(started.session.as_mut()).await;
        {
            let rows = store.requests.lock().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, id);
            assert_eq!(rows[0].started_at, start);
            assert_eq!(rows[0].deadline_at, deadline);
        }
        started.session.detach_finalize().await;
        assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn prepared_authority_revocation_blocks_start_without_admission_or_ledger_writes() {
    use gateway_core::engine::execution::PreparedExecutionRequest;
    for cause in ["key", "parent", "request"] {
        let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
        let admissions = Arc::new(Admissions::default());
        let store = Arc::new(TrackingExecutionStore::default());
        let service = DefaultExecutionService::new(
            snapshots.clone(),
            store.clone(),
            ProviderRegistry::default(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let parent = gateway_core::lifecycle::CancellationToken::new();
        let client = service
            .authenticate_bound_key(
                &ClientApiKeyId::new("key_start_test").unwrap(),
                parent.clone(),
            )
            .unwrap();
        let request = request(&service, ClientTransport::WebSocket);
        let prepared = service.prepare_execution(client).unwrap();
        match cause {
            "key" => {
                snapshots.publish(start_snapshot_with_policy(
                    2,
                    false,
                    RateLimits::unlimited(),
                ));
            }
            "parent" => parent.cancel(),
            _ => prepared.cancellation().cancel(),
        }
        let result = block_on(service.start_prepared(
            prepared,
            PreparedExecutionRequest {
                public_model: request.public_model,
                operation: request.operation,
                metadata: request.metadata,
            },
        ));
        assert!(
            matches!(result,Err(e) if e.kind()==if cause=="request" {GatewayErrorKind::Cancelled}else{GatewayErrorKind::Unauthorized})
        );
        assert!(admissions.limits.lock().unwrap().is_empty());
        assert!(!store.touched.load(Ordering::SeqCst));
        if cause != "parent" {
            assert!(!parent.is_cancelled());
        }
    }
}

#[test]
fn prepared_cancellation_reaches_active_execution_and_settles_only_the_original_charge() {
    use gateway_core::engine::execution::PreparedExecutionRequest;
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let service = charged_service(admissions.clone(), Some(budget.clone()), false);
            let parent = gateway_core::lifecycle::CancellationToken::new();
            let client = service
                .authenticate_bound_key(
                    &ClientApiKeyId::new("key_start_test").unwrap(),
                    parent.clone(),
                )
                .unwrap();
            let request = request(&service, transport);
            let prepared = service.prepare_execution(client).unwrap();
            let cancellation = prepared.cancellation();
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: request.public_model,
                        operation: request.operation,
                        metadata: request.metadata,
                    },
                )
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            cancellation.cancel();
            assert!(matches!(
                started.session.next_event().await,
                Err(gateway_core::engine::EngineError::Cancelled)
            ));
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
            assert!(!parent.is_cancelled());
        }
    });
}

#[test]
fn budget_rejection_releases_client_concurrency_without_creating_a_charge() {
    let admissions = Arc::new(Admissions::default());
    let budget = Arc::new(Budget {
        reject: true,
        active: admissions.active.clone(),
        charges: Mutex::default(),
        ..Default::default()
    });
    let service = service(admissions.clone(), budget.clone());
    for transport in [
        ClientTransport::HttpJson,
        ClientTransport::HttpSse,
        ClientTransport::WebSocket,
    ] {
        let result = block_on(service.start(request(&service, transport)));
        assert!(
            matches!(result, Err(error) if error.client_error_code() == Some("key_daily_budget_exceeded"))
        );
        assert!(!admissions.active.load(Ordering::SeqCst));
        assert!(budget.charges.lock().unwrap().is_empty());
    }
}

#[test]
fn reused_client_uses_updated_limits_for_each_execution() {
    let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
    let admissions = Arc::new(Admissions::default());
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        admissions.clone(),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let client = service.authenticate("sk_start_test").unwrap();
    let limited = RateLimits {
        max_concurrency: 1,
        requests_per_minute: 1,
    };
    for (revision, limits) in [(2, limited), (3, RateLimits::unlimited())] {
        snapshots.publish(start_snapshot_with_policy(revision, true, limits));
        let mut next = request(&service, ClientTransport::WebSocket);
        next.client = client.clone();
        let started = block_on(service.start(next)).expect("new execution");
        block_on(started.session.detach_finalize());
    }
    assert_eq!(
        *admissions.limits.lock().unwrap(),
        vec![limited, RateLimits::unlimited()]
    );
}

#[derive(Default)]
struct SharedUserAdmissions {
    renewals: Arc<AtomicUsize>,
    active: Mutex<BTreeMap<ClientApiKeyId, BTreeSet<ModelRequestId>>>,
    counts: Mutex<BTreeMap<ClientApiKeyId, u64>>,
}

impl ClientAdmissionPort for SharedUserAdmissions {
    fn maintain(
        &self,
        _: &gateway_core::policy::ClientApiKeyId,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::Duration,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Box<dyn gateway_core::engine::admission::ClientAdmissionLeaseGuard> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        Box::new(CountingAdmissionLease(self.renewals.clone()))
    }

    fn admit(
        &self,
        request: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async move {
            use gateway_core::engine::admission::ClientAdmissionRejection;
            let mut active = self.active.lock().unwrap();
            let ids = active.entry(request.client_api_key_id.clone()).or_default();
            if request.limits.max_concurrency > 0
                && ids.len() as u64 >= request.limits.max_concurrency
            {
                return Ok(ClientAdmissionDecision::Rejected(
                    ClientAdmissionRejection::ConcurrencyLimited,
                ));
            }
            let mut counts = self.counts.lock().unwrap();
            let count = counts.entry(request.client_api_key_id).or_default();
            if request.limits.requests_per_minute > 0
                && *count >= request.limits.requests_per_minute
            {
                return Ok(ClientAdmissionDecision::Rejected(
                    ClientAdmissionRejection::RateLimited,
                ));
            }
            *count += 1;
            ids.insert(request.model_request_id);
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        scope: &'a ClientApiKeyId,
        request: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async move {
            Ok(self
                .active
                .lock()
                .unwrap()
                .entry(scope.clone())
                .or_default()
                .remove(request))
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

#[test]
fn user_concurrency_and_rpm_are_shared_across_distinct_keys_and_release_both_scopes() {
    for limits in [
        RateLimits {
            max_concurrency: 1,
            requests_per_minute: 0,
        },
        RateLimits {
            max_concurrency: 0,
            requests_per_minute: 1,
        },
    ] {
        let provider = ProviderKind::new("openai").unwrap();
        let clients = ["a", "b", "other"].map(|id| {
            ClientPolicy::new(
                ClientApiKeyId::new(format!("key_{id}")).unwrap(),
                PlaintextClientApiKey::new(format!("sk_{id}")).unwrap(),
                account_scope(&provider, "acct_start"),
                true,
                RateLimits::unlimited(),
            )
            .with_user_admission(Some((
                ClientApiKeyId::user_admission(if id == "other" { "bob" } else { "alice" }),
                limits,
            )))
        });
        let admissions = Arc::new(SharedUserAdmissions::default());
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(start_snapshot_with_clients(1, clients.to_vec())),
            Arc::new(TrackingExecutionStore::default()),
            ProviderRegistry::default(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let next = |key: &str| StartExecution {
            client: service.authenticate(key).unwrap(),
            public_model: PublicModelId::new("gpt-start").unwrap(),
            operation: start_operation(),
            metadata: ExecutionRequestMetadata {
                protocol: "openai".into(),
                endpoint: "/v1/responses".into(),
                transport: ClientTransport::HttpSse,
                stream: true,
                client_ip: None,
                user_agent: None,
                previous_response_id: None,
            },
        };
        let first = block_on(service.start(next("sk_a"))).unwrap();
        assert_eq!(admissions.renewals.load(Ordering::SeqCst), 2);
        if limits.requests_per_minute > 0 {
            block_on(first.session.detach_finalize());
        } else {
            assert!(
                matches!(block_on(service.start(next("sk_b"))), Err(error) if error.kind() == GatewayErrorKind::RateLimited && error.client_error_code() == Some("user_concurrency_limited"))
            );
            let other = block_on(service.start(next("sk_other"))).unwrap();
            assert_eq!(admissions.renewals.load(Ordering::SeqCst), 4);
            block_on(other.session.detach_finalize());
            assert_eq!(admissions.renewals.load(Ordering::SeqCst), 2);
            block_on(first.session.detach_finalize());
            assert_eq!(admissions.renewals.load(Ordering::SeqCst), 0);
            let after_release = block_on(service.start(next("sk_b"))).unwrap();
            block_on(after_release.session.detach_finalize());
        }
        if limits.requests_per_minute > 0 {
            assert!(
                matches!(block_on(service.start(next("sk_b"))), Err(error) if error.kind() == GatewayErrorKind::RateLimited && error.client_error_code() == Some("user_rate_limited"))
            );
            let other = block_on(service.start(next("sk_other"))).unwrap();
            block_on(other.session.detach_finalize());
        }
        assert!(
            admissions
                .active
                .lock()
                .unwrap()
                .values()
                .all(BTreeSet::is_empty)
        );
    }
}

#[test]
fn reused_client_cannot_start_after_key_disable_or_snapshot_suspension() {
    for suspend in [false, true] {
        let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
        let admissions = Arc::new(Admissions::default());
        let service = DefaultExecutionService::new(
            snapshots.clone(),
            Arc::new(TrackingExecutionStore::default()),
            ProviderRegistry::default(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let next = request(&service, ClientTransport::WebSocket);
        if suspend {
            snapshots.suspend();
        } else {
            snapshots.publish(start_snapshot_with_policy(
                2,
                false,
                RateLimits::unlimited(),
            ));
        }
        let result = block_on(service.start(next));
        let expected = if suspend {
            GatewayErrorKind::Internal
        } else {
            GatewayErrorKind::Unauthorized
        };
        assert!(matches!(result, Err(error) if error.kind() == expected));
        assert!(admissions.limits.lock().unwrap().is_empty());
    }
}

#[test]
fn cancellation_and_pre_send_failure_settle_zero_and_release_concurrency_for_all_transports() {
    let admissions = Arc::new(Admissions::default());
    let budget = Arc::new(Budget {
        reject: false,
        active: admissions.active.clone(),
        charges: Mutex::default(),
        ..Default::default()
    });
    let service = service(admissions.clone(), budget.clone());
    for transport in [
        ClientTransport::HttpJson,
        ClientTransport::HttpSse,
        ClientTransport::WebSocket,
    ] {
        for detach in [true, false] {
            let mut started = block_on(service.start(request(&service, transport))).unwrap();
            assert!(admissions.active.load(Ordering::SeqCst));
            if detach {
                block_on(started.session.detach_finalize());
            } else {
                assert!(block_on(started.session.next_event()).is_err());
                block_on(started.session.detach_finalize());
            }
            assert!(!admissions.active.load(Ordering::SeqCst));
        }
    }
    let charges = budget.charges.lock().unwrap();
    assert_eq!(
        charges.len(),
        6,
        "one settlement per request, including detached finalized sessions"
    );
    assert!(
        charges
            .iter()
            .all(|charge| charge.amount_usd == gateway_core::metering::Decimal::ZERO)
    );
}

fn early_failure_service(
    store: Arc<TrackingExecutionStore>,
    admissions: Arc<Admissions>,
    budget: Arc<Budget>,
) -> DefaultExecutionService {
    DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        store,
        ProviderRegistry::new([Arc::new(LocalFailingProvider) as Arc<dyn Provider>]).unwrap(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
    .with_budget(budget)
}

fn assert_early_failure_recorded(
    store: &TrackingExecutionStore,
    request_id: &ModelRequestId,
    transport: ClientTransport,
) {
    assert_eq!(store.creates.load(Ordering::SeqCst), 1);
    assert_eq!(store.finalizes.load(Ordering::SeqCst), 1);
    assert!(store.attempts.lock().unwrap().is_empty());
    let requests = store.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.id, *request_id);
    assert_eq!(request.client_api_key_ref.as_str(), "key_start_test");
    assert_eq!(
        request.client_api_key_id.as_ref(),
        Some(&request.client_api_key_ref)
    );
    assert_eq!(request.protocol, "openai");
    assert_eq!(request.endpoint, "/v1/responses");
    assert_eq!(request.client_transport, transport.as_str());
    assert_eq!(request.operation, OperationKind::Generate);
    assert_eq!(
        request.requested_model.as_ref().map(PublicModelId::as_str),
        Some("gpt-start")
    );
    let finalizations = store.finalizations.lock().unwrap();
    assert_eq!(finalizations.len(), 1);
    let finalization = &finalizations[0];
    assert_eq!(finalization.request_id, *request_id);
    assert_eq!(finalization.outcome, ExecutionOutcome::Failed);
    assert_eq!(finalization.send_state, UpstreamSendState::NotSent);
    assert_eq!(finalization.attempt_count, 0);
    assert!(finalization.downstream_committed_at.is_none());
    assert!(finalization.upstream_request_id.is_none());
    assert!(finalization.upstream_response_id.is_none());
    assert!(finalization.upstream_status_code.is_none());
    assert!(finalization.upstream_transport.is_none());
    assert!(finalization.http_version.is_none());
    assert!(finalization.websocket_pool.is_none());
    assert!(finalization.provider_metadata_json.is_none());
    assert_eq!(
        finalization
            .error
            .as_ref()
            .expect("original failure")
            .kind(),
        GatewayErrorKind::Unsupported,
        "detached cancellation must not replace the original provider failure"
    );
    assert_eq!(finalization.usage, Default::default());
    assert!(finalization.cost.total().is_none());
}

fn assert_zero_cleanup_completed(
    admissions: &Admissions,
    budget: &Budget,
    store: &TrackingExecutionStore,
    request_id: &ModelRequestId,
) {
    assert!(!admissions.active.load(Ordering::SeqCst));
    assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
    assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
    let charges = budget.charges.lock().unwrap();
    assert_eq!(charges.len(), 1);
    assert_eq!(charges[0].request_id, *request_id);
    assert_eq!(charges[0].key_id.as_str(), "key_start_test");
    assert_eq!(charges[0].amount_usd, Decimal::ZERO);
    assert_eq!(
        charges[0].completed_at,
        store.finalizations.lock().unwrap()[0].completed_at,
        "settlement must retain the original failure completion time"
    );
}

#[test]
fn early_provider_failure_records_request_and_settles_zero_for_all_transports() {
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let store = Arc::new(TrackingExecutionStore::default());
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let service = early_failure_service(store.clone(), admissions.clone(), budget.clone());
            let mut started = service.start(request(&service, transport)).await.unwrap();
            assert!(admissions.active.load(Ordering::SeqCst));
            assert_eq!(store.creates.load(Ordering::SeqCst), 0);
            let error = if transport == ClientTransport::HttpJson {
                started.session.collect_uncommitted().await.unwrap_err()
            } else {
                started.session.next_event().await.unwrap_err()
            };
            assert!(matches!(
                error,
                EngineError::Provider(error)
                    if error.kind() == ProviderErrorKind::Unsupported
                        && error.send_state() == UpstreamSendState::NotSent
            ));
            assert!(started.session.is_finalized());
            assert_early_failure_recorded(&store, &started.request_id, transport);
            assert_zero_cleanup_completed(&admissions, &budget, &store, &started.request_id);
            assert!(started.session.next_event().await.unwrap().is_none());
            started.session.detach_finalize().await;
            assert_early_failure_recorded(&store, &started.request_id, transport);
            assert_zero_cleanup_completed(&admissions, &budget, &store, &started.request_id);
        }
    });
}

#[test]
fn detached_early_failure_resumes_cancelled_store_write_and_settles_once_for_all_transports() {
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            for suspend_create in [true, false] {
                let (complete_write, write_gate) = oneshot::channel();
                let store = Arc::new(TrackingExecutionStore::default());
                if suspend_create {
                    *store.create_gate.lock().unwrap() = Some(write_gate);
                } else {
                    *store.finalize_gate.lock().unwrap() = Some(write_gate);
                }
                let (complete_release, release_gate) = oneshot::channel();
                let admissions = Arc::new(Admissions {
                    release_gate: Mutex::new(Some(release_gate)),
                    ..Default::default()
                });
                let (complete_settlement, settlement_gate) = oneshot::channel();
                let budget = Arc::new(Budget {
                    active: admissions.active.clone(),
                    settlement_gate: Mutex::new(Some(settlement_gate)),
                    ..Default::default()
                });
                let service =
                    early_failure_service(store.clone(), admissions.clone(), budget.clone());
                let mut started = service.start(request(&service, transport)).await.unwrap();
                let mut next = started.session.next_event();
                assert!(futures::poll!(next.as_mut()).is_pending());
                drop(next);
                assert!(!started.session.is_finalized());
                assert_eq!(store.creates.load(Ordering::SeqCst), 1);
                assert_eq!(
                    store.finalizes.load(Ordering::SeqCst),
                    usize::from(!suspend_create)
                );
                assert_eq!(
                    store.requests.lock().unwrap().len(),
                    usize::from(!suspend_create)
                );
                assert!(store.finalizations.lock().unwrap().is_empty());
                assert_eq!(budget.settlements.load(Ordering::SeqCst), 0);
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 0);
                assert_eq!(admissions.renewals.load(Ordering::SeqCst), 1);
                assert!(admissions.active.load(Ordering::SeqCst));

                let mut detached = started.session.detach_finalize();
                assert!(futures::poll!(detached.as_mut()).is_pending());
                assert_eq!(store.creates.load(Ordering::SeqCst), 1);
                assert_eq!(
                    store.finalizes.load(Ordering::SeqCst),
                    usize::from(!suspend_create)
                );
                // receiver 已从 Store 替身取走；只有延续原 future 才能继续接收此信号。
                complete_write
                    .send(())
                    .expect("detached cleanup retains the original store write");
                assert!(futures::poll!(detached.as_mut()).is_pending());
                assert_early_failure_recorded(&store, &started.request_id, transport);
                assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
                assert!(budget.charges.lock().unwrap().is_empty());
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 0);
                assert_eq!(admissions.renewals.load(Ordering::SeqCst), 1);
                assert!(admissions.active.load(Ordering::SeqCst));

                complete_settlement.send(()).expect("original settlement");
                assert!(futures::poll!(detached.as_mut()).is_pending());
                assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
                {
                    let charges = budget.charges.lock().unwrap();
                    assert_eq!(charges.len(), 1);
                    assert_eq!(charges[0].amount_usd, Decimal::ZERO);
                }
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
                assert!(admissions.active.load(Ordering::SeqCst));
                complete_release
                    .send(())
                    .expect("original admission release");
                detached.await;
                assert_early_failure_recorded(&store, &started.request_id, transport);
                assert_zero_cleanup_completed(&admissions, &budget, &store, &started.request_id);
            }
        }
    });
}

pub(super) struct ChargedProvider {
    pub(super) fail: bool,
}

pub(super) fn known_charge() -> Decimal {
    "1.25".parse().unwrap()
}

#[async_trait]
impl Provider for ChargedProvider {
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
        _: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let candidate = request.candidate();
        let metadata = ProviderCallMetadata::new(
            candidate.provider().clone(),
            candidate.upstream_model().unwrap().clone(),
            ProviderAccountId::new("acct_start").unwrap(),
            UpstreamTransport::new("websocket").unwrap(),
        );
        let response = ResponseMeta::new("resp_cleanup", "gpt-start");
        let terminal = if self.fail {
            Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                UpstreamSendState::Sent,
            ))
        } else {
            Ok(GatewayEvent::Completed(response.clone()).into())
        };
        let events: Vec<Result<ProviderEvent, ProviderError>> = vec![
            Ok(GatewayEvent::Started(response).into()),
            Ok(GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(known_charge().scaled()).unwrap(),
            )
            .into()),
            terminal,
        ];
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(events),
            (),
        ))
    }
}

fn charged_service(
    admissions: Arc<Admissions>,
    budget: Option<Arc<Budget>>,
    fail: bool,
) -> DefaultExecutionService {
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::new([Arc::new(ChargedProvider { fail }) as Arc<dyn Provider>]).unwrap(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    match budget {
        Some(budget) => service.with_budget(budget),
        None => service,
    }
}

pub(super) async fn consume_charged_prefix(session: &mut dyn ExecutionSession) {
    let first = session.next_event().await.unwrap().unwrap();
    assert_eq!(
        first.commit_requirement(),
        CommitRequirement::CommitBeforeDelivery
    );
    session.commit_downstream(None).await.unwrap();
    let cost = session.next_event().await.unwrap().unwrap();
    assert_eq!(
        cost.commit_requirement(),
        CommitRequirement::AlreadyCommitted
    );
}

pub(super) fn assert_cleanup_completed(
    admissions: &Admissions,
    budget: &Budget,
    request_id: &ModelRequestId,
) {
    assert!(!admissions.active.load(Ordering::SeqCst));
    assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
    assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
    let charges = budget.charges.lock().unwrap();
    assert_eq!(charges.len(), 1);
    assert_eq!(charges[0].request_id, *request_id);
    assert_eq!(charges[0].key_id.as_str(), "key_start_test");
    assert_eq!(charges[0].amount_usd, known_charge());
}

#[test]
fn normal_completion_settles_known_charge_once_for_all_transports() {
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let service = charged_service(admissions.clone(), Some(budget.clone()), false);
            let mut started = service.start(request(&service, transport)).await.unwrap();
            if transport == ClientTransport::HttpJson {
                assert_eq!(
                    started.session.collect_uncommitted().await.unwrap().len(),
                    3
                );
                assert!(!started.session.is_finalized());
                started.session.commit_downstream(Some(200)).await.unwrap();
            } else {
                consume_charged_prefix(started.session.as_mut()).await;
                assert!(started.session.next_event().await.unwrap().is_some());
            }
            assert!(started.session.next_event().await.unwrap().is_none());
            assert!(started.session.is_finalized());
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
        }
    });
}

#[test]
fn settlement_survives_cancelled_event_wait_and_resumes_without_restart() {
    block_on(async {
        for fail in [false, true] {
            let admissions = Arc::new(Admissions::default());
            let (release, gate) = oneshot::channel();
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                settlement_gate: Mutex::new(Some(gate)),
                ..Default::default()
            });
            let service = charged_service(admissions.clone(), Some(budget.clone()), fail);
            let mut started = service
                .start(request(&service, ClientTransport::WebSocket))
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            if !fail {
                assert!(started.session.next_event().await.unwrap().is_some());
            }
            for _ in 0..2 {
                let mut terminal = started.session.next_event();
                assert!(futures::poll!(terminal.as_mut()).is_pending());
                drop(terminal);
                assert!(!started.session.is_finalized());
                assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
                assert!(budget.charges.lock().unwrap().is_empty());
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 0);
                assert_eq!(admissions.renewals.load(Ordering::SeqCst), 1);
            }
            release
                .send(())
                .expect("the original settlement is still alive");
            assert!(started.session.next_event().await.unwrap().is_none());
            assert!(started.session.is_finalized());
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
        }
    });
}

#[test]
fn detached_cleanup_settles_cancelled_execution_and_resumes_existing_settlement() {
    block_on(async {
        for cancel_event_wait in [false, true] {
            let admissions = Arc::new(Admissions::default());
            let (release, gate) = oneshot::channel();
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                settlement_gate: Mutex::new(Some(gate)),
                ..Default::default()
            });
            let service = charged_service(admissions.clone(), Some(budget.clone()), false);
            let mut started = service
                .start(request(&service, ClientTransport::WebSocket))
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            if cancel_event_wait {
                started.session.cancel();
                let mut terminal = started.session.next_event();
                assert!(futures::poll!(terminal.as_mut()).is_pending());
                drop(terminal);
                assert!(!started.session.is_finalized());
            }
            let mut detached = started.session.detach_finalize();
            assert!(futures::poll!(detached.as_mut()).is_pending());
            assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 0);
            assert_eq!(admissions.renewals.load(Ordering::SeqCst), 1);
            release
                .send(())
                .expect("detached cleanup retains settlement");
            detached.await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
        }
    });
}

#[test]
fn cancelled_release_resumes_without_repeating_settlement_with_or_without_budget() {
    block_on(async {
        for with_budget in [false, true] {
            let (release, gate) = oneshot::channel();
            let admissions = Arc::new(Admissions {
                release_gate: Mutex::new(Some(gate)),
                ..Default::default()
            });
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let service = charged_service(
                admissions.clone(),
                with_budget.then(|| budget.clone()),
                false,
            );
            let mut started = service
                .start(request(&service, ClientTransport::HttpSse))
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            assert!(started.session.next_event().await.unwrap().is_some());
            let mut terminal = started.session.next_event();
            assert!(futures::poll!(terminal.as_mut()).is_pending());
            drop(terminal);
            assert!(!started.session.is_finalized());
            assert!(admissions.active.load(Ordering::SeqCst));
            assert_eq!(
                budget.settlements.load(Ordering::SeqCst),
                usize::from(with_budget)
            );
            let mut detached = started.session.detach_finalize();
            assert!(futures::poll!(detached.as_mut()).is_pending());
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
            release
                .send(())
                .expect("the original release is still alive");
            detached.await;
            if with_budget {
                assert_cleanup_completed(&admissions, &budget, &started.request_id);
            } else {
                assert!(!admissions.active.load(Ordering::SeqCst));
                assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
                assert!(budget.charges.lock().unwrap().is_empty());
            }
        }
    });
}

#[test]
fn cancelled_buffered_commit_keeps_settlement_for_detached_cleanup() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let (release, gate) = oneshot::channel();
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            settlement_gate: Mutex::new(Some(gate)),
            ..Default::default()
        });
        let service = charged_service(admissions.clone(), Some(budget.clone()), false);
        let mut started = service
            .start(request(&service, ClientTransport::HttpJson))
            .await
            .unwrap();
        assert_eq!(
            started.session.collect_uncommitted().await.unwrap().len(),
            3
        );
        let mut commit = started.session.commit_downstream(Some(200));
        assert!(futures::poll!(commit.as_mut()).is_pending());
        drop(commit);
        assert!(!started.session.is_finalized());
        release
            .send(())
            .expect("buffered commit retains settlement");
        started.session.detach_finalize().await;
        assert_cleanup_completed(&admissions, &budget, &started.request_id);
    });
}

#[test]
fn settlement_failure_keeps_provider_error_and_releases_concurrency_once() {
    block_on(async {
        for fail_settlement in [false, true] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                fail_settlement,
                ..Default::default()
            });
            let service = charged_service(admissions.clone(), Some(budget.clone()), true);
            let mut started = service
                .start(request(&service, ClientTransport::WebSocket))
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            assert!(matches!(
                started.session.next_event().await,
                Err(EngineError::Provider(error)) if error.kind() == ProviderErrorKind::InvalidRequest
            ));
            assert!(started.session.is_finalized());
            started.session.detach_finalize().await;
            // Store 端口已接管精确费用后，结算错误不能改写 Provider 错误或触发第二次结算。
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
        }
    });
}

use async_trait::async_trait;
use bytes::Bytes;
use futures::{channel::oneshot, executor::block_on, future::BoxFuture};
use gateway_core::account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy};
use gateway_core::engine::admission::{
    ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort, ClientAdmissionRecovery,
    ClientAdmissionRequest, ClientAdmissionRestoreResult,
};
use gateway_core::engine::continuation::{
    NativeContinuationPin, NativeContinuationPort, NativeContinuationStoreError, PreviousResponseId,
};
use gateway_core::engine::execution::{
    ClientApiKeyUsageSink, ClientTransport, DefaultExecutionService, ExecutionRequestMetadata,
    ExecutionService, ExecutionSession, StartExecution, StartProviderExecution,
};
use gateway_core::engine::probe::{AccountProbe, AccountProbeErrorSource, AccountProbeRequest};
use gateway_core::engine::provider::{
    Provider, ProviderCallMetadata, ProviderRegistry, ProviderRequest, ProviderRequestObservation,
    ProviderStream,
};
use gateway_core::engine::{
    AttemptContext, AttemptRecord, CommitRequirement, EngineError, ExecutionOutcome,
    ExecutionStore, IntermediateFailure, ModelRequestFinalization, ModelRequestId, NewModelRequest,
    ProbeFailure, RecoveryReport,
};
use gateway_core::error::{
    ClientVisibleUpstreamResponse, GatewayErrorKind, ProviderError, ProviderErrorKind, StoreError,
    StoreErrorKind,
};
use gateway_core::event::{GatewayEvent, ProviderEvent, ResponseMeta};
use gateway_core::metering::{Decimal, ProviderReportedCost};
use gateway_core::operation::{
    GenerateRequest, ImageRequest, ImageRequestKind, Operation, OperationKind, ProtocolPayload,
    RawJsonPayload,
};
use gateway_core::policy::{ClientApiKeyId, ClientPolicy, PlaintextClientApiKey, RateLimits};
use gateway_core::routing::{
    ClientRoutingScope, ConfigRevision, FrozenAccountScope, ModelCapabilities,
    ProviderCatalogGeneration, ProviderKind, ProviderModel, ProviderModelCapabilities,
    PublicModelId, RuntimeAccount, RuntimeAccountDirectory, RuntimeSnapshot, UpstreamModelId,
};
use gateway_core::runtime::RuntimeSnapshotHandle;
use gateway_core::upstream::{UpstreamSendState, UpstreamTransport};
use serde_json::json;

#[test]
fn account_probe_should_not_write_to_the_persistent_execution_store() {
    let store = Arc::new(TrackingExecutionStore::default());
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(probe_snapshot()),
        store.clone(),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );

    let error = block_on(service.probe(AccountProbeRequest {
        account_id: ProviderAccountId::new("acct_probe").expect("account ID"),
        provider_kind: ProviderKind::new("openai").expect("provider kind"),
        upstream_model: UpstreamModelId::new("gpt-probe").expect("model ID"),
        operation: probe_operation(),
    }))
    .expect_err("empty Provider registry should stop the probe after it starts");

    assert_eq!(error.kind(), GatewayErrorKind::NoAvailableProvider);
    assert_eq!(error.source(), AccountProbeErrorSource::Gateway);
    assert_eq!(error.send_state(), None);
    assert!(!store.touched.load(Ordering::SeqCst));
}

#[test]
fn probe_failures_should_be_observable_without_a_model_request_row() {
    let store = Arc::new(TrackingExecutionStore::default());
    let providers = ProviderRegistry::new([Arc::new(FailingProvider) as Arc<dyn Provider>])
        .expect("provider registry");
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(probe_snapshot()),
        store.clone(),
        providers,
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );

    let error = block_on(service.probe(AccountProbeRequest {
        account_id: ProviderAccountId::new("acct_probe").expect("account ID"),
        provider_kind: ProviderKind::new("openai").expect("provider kind"),
        upstream_model: UpstreamModelId::new("gpt-probe").expect("model ID"),
        operation: probe_operation(),
    }))
    .expect_err("the provider rejects every probe");

    assert_eq!(error.kind(), GatewayErrorKind::UpstreamUnavailable);
    assert_eq!(error.source(), AccountProbeErrorSource::Upstream);
    assert_eq!(error.send_state(), Some(UpstreamSendState::NotSent));
    let upstream = error
        .upstream_response()
        .expect("probe must preserve its request-local upstream response");
    assert_eq!(upstream.status(), 502);
    assert_eq!(
        upstream.content_type(),
        Some(b"application/json".as_slice())
    );
    assert_eq!(
        upstream.body(),
        &Bytes::from_static(br#"{"error":{"message":"source upstream failure"}}"#),
    );
    assert!(!format!("{error:?}").contains("source upstream failure"));
    assert!(!store.touched.load(Ordering::SeqCst));
    assert_eq!(store.probe_failures(), vec!["transport".to_owned()]);
}

#[test]
fn provider_local_probe_failure_should_remain_distinct_from_upstream() {
    let store = Arc::new(TrackingExecutionStore::default());
    let providers = ProviderRegistry::new([Arc::new(LocalFailingProvider) as Arc<dyn Provider>])
        .expect("provider registry");
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(probe_snapshot()),
        store,
        providers,
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );

    let error = block_on(service.probe(AccountProbeRequest {
        account_id: ProviderAccountId::new("acct_probe").expect("account ID"),
        provider_kind: ProviderKind::new("openai").expect("provider kind"),
        upstream_model: UpstreamModelId::new("gpt-probe").expect("model ID"),
        operation: probe_operation(),
    }))
    .expect_err("the Provider rejects the probe before sending it");

    assert_eq!(error.source(), AccountProbeErrorSource::Provider);
    assert_eq!(error.send_state(), Some(UpstreamSendState::NotSent));
    assert!(error.upstream_response().is_none());
}

#[test]
fn probe_observation_store_failure_preserves_the_provider_error() {
    let store = Arc::new(TrackingExecutionStore {
        fail_probe_observation: AtomicBool::new(true),
        ..TrackingExecutionStore::default()
    });
    let providers = ProviderRegistry::new([Arc::new(FailingProvider) as Arc<dyn Provider>])
        .expect("provider registry");
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(probe_snapshot()),
        store.clone(),
        providers,
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );

    let error = block_on(service.probe(AccountProbeRequest {
        account_id: ProviderAccountId::new("acct_probe").expect("account ID"),
        provider_kind: ProviderKind::new("openai").expect("provider kind"),
        upstream_model: UpstreamModelId::new("gpt-probe").expect("model ID"),
        operation: probe_operation(),
    }))
    .expect_err("the provider error must survive observation failure");

    assert_eq!(error.kind(), GatewayErrorKind::UpstreamUnavailable);
    assert!(!store.touched.load(Ordering::SeqCst));
    assert!(store.probe_failures().is_empty());
}

struct FailingProvider;

#[async_trait]
impl Provider for FailingProvider {
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
        _request: ProviderRequest,
        _context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        Err(
            ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent)
                .with_client_visible_upstream_response(ClientVisibleUpstreamResponse::new(
                    502,
                    Some(b"application/json".to_vec()),
                    Bytes::from_static(br#"{"error":{"message":"source upstream failure"}}"#),
                )),
        )
    }
}

struct LocalFailingProvider;

#[async_trait]
impl Provider for LocalFailingProvider {
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
        _: ProviderRequest,
        _: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            UpstreamSendState::NotSent,
        ))
    }
}

struct ColdFailingProvider {
    requested_model: Option<PublicModelId>,
}

#[async_trait]
impl Provider for ColdFailingProvider {
    fn name(&self) -> &'static str {
        "openai"
    }

    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }

    fn request_observation(&self, _: &Operation, _: &ClientApiKeyId) -> ProviderRequestObservation {
        ProviderRequestObservation {
            requested_model: self.requested_model.clone(),
            ..Default::default()
        }
    }

    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        _: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let metadata = ProviderCallMetadata::for_provider_endpoint(
            request.candidate().provider().clone(),
            ProviderAccountId::new("acct_usage").expect("account"),
            UpstreamTransport::new("http_json").expect("transport"),
        );
        Ok(ProviderStream::new(
            metadata,
            futures::stream::once(async {
                Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    UpstreamSendState::NotSent,
                ))
            }),
            (),
        ))
    }
}

#[test]
fn successful_authentication_should_record_client_key_usage() {
    let usage = Arc::new(RecordingClientApiKeyUsage::default());
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(client_snapshot()),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        usage.clone(),
    );

    service
        .authenticate("sk_usage_test")
        .expect("successful authentication");

    assert_eq!(usage.recorded(), vec!["key_usage_test".to_owned()]);
}

#[test]
fn provider_endpoint_should_persist_its_real_v1_endpoint() {
    for prepared in [false, true] {
        assert_provider_endpoint_observation(None, prepared);
        assert_provider_endpoint_observation(Some("gpt-image-2"), prepared);
    }
}

fn assert_provider_endpoint_observation(model: Option<&str>, prepare: bool) {
    let store = Arc::new(TrackingExecutionStore::default());
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(client_snapshot()),
        store.clone(),
        ProviderRegistry::new([Arc::new(ColdFailingProvider {
            requested_model: model.map(|model| PublicModelId::new(model).expect("model")),
        }) as Arc<dyn Provider>])
        .expect("provider registry"),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let client = service
        .authenticate("sk_usage_test")
        .expect("authenticated client");
    let operation = Operation::GenerateImage(ImageRequest::from_raw_json(
        ImageRequestKind::Generation,
        RawJsonPayload::new(
            "openai",
            Bytes::from_static(br#"{"model":"gpt-image-2","prompt":"hello"}"#),
        )
        .expect("image payload"),
    ));

    let request = StartProviderExecution {
        client,
        provider: ProviderKind::new("openai").expect("provider"),
        operation,
        metadata: ExecutionRequestMetadata {
            protocol: "openai".to_owned(),
            endpoint: "/v1/images/generations".to_owned(),
            transport: ClientTransport::HttpJson,
            stream: false,
            client_ip: None,
            user_agent: None,
            previous_response_id: None,
        },
    };
    let prepared = prepare.then(|| service.prepare_execution(request.client.clone()).unwrap());
    let expected_id = prepared.as_ref().map(|p| p.request_id().clone());
    let mut started = match prepared {
        Some(prepared) => block_on(service.start_prepared_provider_endpoint(
            prepared,
            request.provider,
            request.operation,
            request.metadata,
        )),
        None => block_on(service.start_provider_endpoint(request)),
    }
    .expect("provider endpoint starts without a text catalog entry");
    if let Some(id) = expected_id {
        assert_eq!(started.request_id, id);
    }

    let error = block_on(started.session.collect_uncommitted())
        .expect_err("the cold provider stops execution after persistence");
    assert_eq!(
        store.model_request_endpoints(),
        vec!["/v1/images/generations".to_owned()],
        "execution error: {error:?}"
    );
    assert_eq!(store.requested_models(), vec![model.map(str::to_owned)]);
    let upstream_models = store.upstream_models();
    assert!(!upstream_models.is_empty());
    assert!(upstream_models.iter().all(Option::is_none));
}

#[test]
fn known_catalog_should_reject_a_model_that_the_provider_did_not_publish() {
    let store = Arc::new(TrackingExecutionStore::default());
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        store.clone(),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let client = service
        .authenticate("sk_start_test")
        .expect("authenticated client");
    let model = "gpt-future-not-in-catalog";

    let result = block_on(service.start(StartExecution {
        client,
        public_model: PublicModelId::from_client_wire(model).expect("client model"),
        operation: start_operation_for_model(model),
        metadata: ExecutionRequestMetadata {
            protocol: "openai".to_owned(),
            endpoint: "/v1/responses".to_owned(),
            transport: ClientTransport::HttpJson,
            stream: false,
            client_ip: None,
            user_agent: None,
            previous_response_id: None,
        },
    }));
    let Err(error) = result else {
        panic!("a known provider catalog must be authoritative for model availability");
    };

    assert_eq!(
        (error.kind(), error.client_message()),
        (
            GatewayErrorKind::ModelNotFound,
            "the requested model was not found in the provider catalogs available to this API key; check the model name",
        )
    );
    let rejections = store.entry_rejections.lock().unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].error.kind(), GatewayErrorKind::ModelNotFound);
    assert_eq!(
        rejections[0].requested_model.as_ref().map(|m| m.as_str()),
        Some(model)
    );
    assert_eq!(rejections[0].metadata.endpoint, "/v1/responses");
    assert!(store.requests.lock().unwrap().is_empty());
}

#[test]
fn continuation_owned_by_another_client_api_key_should_fail_closed() {
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(RejectedContinuation::OwnershipMismatch),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let client = service
        .authenticate("sk_start_test")
        .expect("authenticated client");

    let result = block_on(service.start(StartExecution {
        client,
        public_model: PublicModelId::new("gpt-start").expect("public model"),
        operation: start_operation(),
        metadata: execution_metadata_with_continuation(),
    }));
    let Err(error) = result else {
        panic!("cross-client continuation must be rejected");
    };

    assert_eq!(error.kind(), GatewayErrorKind::PolicyDenied);
}

#[test]
fn invalid_continuation_record_should_not_be_forwarded_as_an_external_handle() {
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(RejectedContinuation::InvalidData),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let client = service
        .authenticate("sk_start_test")
        .expect("authenticated client");

    let result = block_on(service.start(StartExecution {
        client,
        public_model: PublicModelId::new("gpt-start").expect("public model"),
        operation: start_operation(),
        metadata: execution_metadata_with_continuation(),
    }));
    let Err(error) = result else {
        panic!("invalid continuation state must be rejected");
    };

    assert_eq!(error.kind(), GatewayErrorKind::Internal);
}

fn execution_metadata_with_continuation() -> ExecutionRequestMetadata {
    ExecutionRequestMetadata {
        protocol: "openai".to_owned(),
        endpoint: "/v1/responses".to_owned(),
        transport: ClientTransport::HttpJson,
        stream: false,
        client_ip: None,
        user_agent: None,
        previous_response_id: Some(PreviousResponseId::new("response-private")),
    }
}

#[derive(Default)]
pub(super) struct RecordingClientApiKeyUsage {
    ids: Mutex<Vec<String>>,
}

impl RecordingClientApiKeyUsage {
    fn recorded(&self) -> Vec<String> {
        self.ids.lock().expect("recorded API Key IDs").clone()
    }
}

impl ClientApiKeyUsageSink for RecordingClientApiKeyUsage {
    fn record_used(&self, key_id: &ClientApiKeyId) {
        self.ids
            .lock()
            .expect("recorded API Key IDs")
            .push(key_id.as_str().to_owned());
    }
}

#[derive(Default)]
pub(super) struct TrackingExecutionStore {
    pub(super) touched: AtomicBool,
    pub(super) probe_failures: Mutex<Vec<String>>,
    pub(super) entry_rejections: Mutex<Vec<gateway_core::engine::EntryRejection>>,
    pub(super) requests: Mutex<Vec<NewModelRequest>>,
    pub(super) attempts: Mutex<Vec<AttemptRecord>>,
    pub(super) finalizations: Mutex<Vec<ModelRequestFinalization>>,
    pub(super) create_gate: Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) finalize_gate: Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) creates: AtomicUsize,
    pub(super) finalizes: AtomicUsize,
    pub(super) fail_probe_observation: AtomicBool,
}

impl TrackingExecutionStore {
    fn touch(&self) {
        self.touched.store(true, Ordering::SeqCst);
    }

    fn probe_failures(&self) -> Vec<String> {
        self.probe_failures
            .lock()
            .expect("probe failures lock")
            .clone()
    }

    fn model_request_endpoints(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("model requests lock")
            .iter()
            .map(|request| request.endpoint.clone())
            .collect()
    }

    fn requested_models(&self) -> Vec<Option<String>> {
        self.requests
            .lock()
            .expect("model requests lock")
            .iter()
            .map(|request| {
                request
                    .requested_model
                    .as_ref()
                    .map(|model| model.as_str().to_owned())
            })
            .collect()
    }

    fn upstream_models(&self) -> Vec<Option<String>> {
        self.attempts
            .lock()
            .expect("attempts lock")
            .iter()
            .map(|attempt| {
                attempt
                    .upstream_model_id
                    .as_ref()
                    .map(|model| model.as_str().to_owned())
            })
            .collect()
    }
}

#[async_trait]
impl ExecutionStore for TrackingExecutionStore {
    fn maintain_request(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: Option<std::time::SystemTime>,
        _: std::time::Duration,
    ) -> Box<dyn gateway_core::engine::ExecutionRecoveryLeaseGuard> {
        Box::new(TestExecutionRecoveryLease)
    }

    async fn record_entry_rejection(
        &self,
        rejection: gateway_core::engine::EntryRejection,
    ) -> Result<(), StoreError> {
        self.entry_rejections.lock().unwrap().push(rejection);
        Ok(())
    }

    async fn create_model_request(&self, request: NewModelRequest) -> Result<(), StoreError> {
        self.touch();
        self.creates.fetch_add(1, Ordering::SeqCst);
        let gate = self.create_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.await.expect("create model request gate");
        }
        self.requests
            .lock()
            .expect("model requests lock")
            .push(request);
        Ok(())
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), StoreError> {
        self.touch();
        self.attempts.lock().expect("attempts lock").push(attempt);
        Ok(())
    }

    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        self.touch();
        Ok(())
    }

    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        self.touch();
        Ok(())
    }

    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        self.touch();
        Ok(())
    }

    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        self.touch();
        Ok(())
    }

    async fn record_probe_failure(&self, failure: ProbeFailure) -> Result<(), StoreError> {
        if self.fail_probe_observation.load(Ordering::SeqCst) {
            return Err(StoreError::new(StoreErrorKind::Unavailable));
        }
        self.probe_failures
            .lock()
            .expect("probe failures lock")
            .push(failure.error.kind().as_str().to_owned());
        Ok(())
    }

    async fn finalize_model_request(
        &self,
        finalization: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.touch();
        self.finalizes.fetch_add(1, Ordering::SeqCst);
        let gate = self.finalize_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.await.expect("finalize model request gate");
        }
        self.finalizations
            .lock()
            .expect("finalizations lock")
            .push(finalization);
        Ok(())
    }

    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        self.touch();
        Ok(RecoveryReport::default())
    }
}

struct UnusedAdmissions;

impl ClientAdmissionPort for UnusedAdmissions {
    fn maintain(
        &self,
        _: &gateway_core::policy::ClientApiKeyId,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::Duration,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Box<dyn gateway_core::engine::admission::ClientAdmissionLeaseGuard> {
        Box::new(TestAdmissionLease)
    }

    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionDecision::Granted) })
    }

    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async { Ok(true) })
    }

    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

pub(super) struct UnusedContinuation;

impl NativeContinuationPort for UnusedContinuation {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

enum RejectedContinuation {
    OwnershipMismatch,
    InvalidData,
}

impl NativeContinuationPort for RejectedContinuation {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async move {
            Err(match self {
                Self::OwnershipMismatch => NativeContinuationStoreError::ownership_mismatch(),
                Self::InvalidData => NativeContinuationStoreError::invalid_data("invalid record"),
            })
        })
    }

    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

fn account_scope(provider: &ProviderKind, account_id: &str) -> Arc<FrozenAccountScope> {
    Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([(
            ProviderAccountId::new(account_id).expect("account ID"),
            RuntimeAccount::new(provider.clone(), BTreeSet::new()),
        )]))),
        ClientRoutingScope::all_accounts(),
    ))
}

fn probe_snapshot() -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").expect("provider kind");
    let capabilities =
        ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), Some(16_000));
    RuntimeSnapshot::new(
        ConfigRevision::new(1).expect("config revision"),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).expect("concurrency"),
            Duration::from_millis(1),
        ),
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider,
            UpstreamModelId::new("gpt-probe").expect("model ID"),
            capabilities,
        )],
        Vec::new(),
    )
    .expect("probe snapshot")
}

fn client_snapshot() -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").expect("provider kind");
    RuntimeSnapshot::new(
        ConfigRevision::new(1).expect("config revision"),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).expect("concurrency"),
            Duration::from_millis(1),
        ),
        vec![provider.clone()],
        Vec::new(),
        vec![ClientPolicy::new(
            ClientApiKeyId::new("key_usage_test").expect("client API key ID"),
            PlaintextClientApiKey::new("sk_usage_test").expect("plaintext client API key"),
            account_scope(&provider, "acct_usage"),
            true,
            RateLimits::unlimited(),
        )],
    )
    .expect("client snapshot")
}

pub(super) fn start_snapshot() -> RuntimeSnapshot {
    start_snapshot_with_policy(1, true, RateLimits::unlimited())
}

pub(super) fn start_snapshot_with_policy(
    revision: u64,
    enabled: bool,
    limits: RateLimits,
) -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").expect("provider kind");
    start_snapshot_with_clients(
        revision,
        vec![ClientPolicy::new(
            ClientApiKeyId::new("key_start_test").unwrap(),
            PlaintextClientApiKey::new("sk_start_test").unwrap(),
            account_scope(&provider, "acct_start"),
            enabled,
            limits,
        )],
    )
}

pub(super) fn start_snapshot_with_clients(
    revision: u64,
    clients: Vec<ClientPolicy>,
) -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").expect("provider kind");
    let capabilities =
        ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), Some(16_000));
    RuntimeSnapshot::new(
        ConfigRevision::new(revision).expect("config revision"),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).expect("concurrency"),
            Duration::from_millis(1),
        ),
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider,
            UpstreamModelId::new("gpt-start").expect("model ID"),
            capabilities,
        )],
        clients,
    )
    .expect("start snapshot")
}

fn probe_operation() -> Operation {
    let body = json!({
        "model": "gpt-probe",
        "input": [{"type": "message", "role": "user", "content": "ping"}],
    });
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().expect("request object").clone())
            .expect("OpenAI payload"),
    ))
}

fn start_operation() -> Operation {
    start_operation_for_model("gpt-start")
}

fn start_operation_for_model(model: &str) -> Operation {
    let body = json!({
        "model": model,
        "input": [{"type": "message", "role": "user", "content": "ping"}],
    });
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().expect("request object").clone())
            .expect("OpenAI payload"),
    ))
}

#[test]
fn host_bound_identity_requires_an_enabled_key_and_never_reuses_revoked_authorization() {
    let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
    let admissions = Arc::new(Admissions::default());
    let usage = Arc::new(RecordingClientApiKeyUsage::default());
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        admissions.clone(),
        Arc::new(UnusedContinuation),
        usage,
    );
    let revocation = gateway_core::lifecycle::CancellationToken::new();
    let key = ClientApiKeyId::new("key_start_test").unwrap();
    assert!(
        service
            .authenticate_bound_key(&ClientApiKeyId::new("missing").unwrap(), revocation.clone())
            .is_err()
    );
    let client = service
        .authenticate_bound_key(&key, revocation.clone())
        .unwrap();
    assert_eq!(client.policy().key_id(), &key);
    let access = service.client_access(&client).unwrap();
    revocation.cancel();
    assert!(access.cancellation().is_cancelled());
    assert!(service.authenticate_bound_key(&key, revocation).is_err());
    let mut r = request(&service, ClientTransport::WebSocket);
    r.client = client;
    assert!(
        matches!(block_on(service.start(r)),Err(e) if e.kind()==GatewayErrorKind::Unauthorized)
    );
    assert!(admissions.limits.lock().unwrap().is_empty());
    let fresh = gateway_core::lifecycle::CancellationToken::new();
    snapshots.publish(start_snapshot_with_policy(
        2,
        false,
        RateLimits::unlimited(),
    ));
    assert!(service.authenticate_bound_key(&key, fresh.clone()).is_err());
    snapshots.suspend();
    assert_eq!(
        service.authenticate_bound_key(&key, fresh).unwrap_err(),
        gateway_core::engine::execution::ClientAuthenticationError::SnapshotUnavailable
    );
}

#[test]
fn delegated_builtin_authentication_cannot_upgrade_an_old_secret_to_a_rotated_policy() {
    struct RotateOnUse(RuntimeSnapshotHandle, AtomicBool);
    impl ClientApiKeyUsageSink for RotateOnUse {
        fn record_used(&self, _: &ClientApiKeyId) {
            if self.1.swap(false, Ordering::SeqCst) {
                self.0.publish(start_snapshot_with_clients(
                    2,
                    vec![ClientPolicy::new(
                        ClientApiKeyId::new("key_start_test").unwrap(),
                        PlaintextClientApiKey::new("sk_rotated").unwrap(),
                        account_scope(&ProviderKind::new("openai").unwrap(), "acct_start"),
                        true,
                        RateLimits::unlimited(),
                    )],
                ));
            }
        }
    }
    let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(Admissions::default()),
        Arc::new(UnusedContinuation),
        Arc::new(RotateOnUse(snapshots, AtomicBool::new(true))),
    );
    let epoch = gateway_core::lifecycle::CancellationToken::new();
    assert!(
        service
            .authenticate_revocable("sk_start_test", epoch.clone())
            .is_err()
    );
    let current = service
        .authenticate_revocable("sk_rotated", epoch.clone())
        .unwrap();
    assert_eq!(
        current.policy().plaintext_key().expose_for_auth(),
        "sk_rotated"
    );
    epoch.cancel();
    assert!(
        current
            .bound_access()
            .unwrap()
            .cancellation()
            .is_cancelled()
    );
    assert!(service.authenticate_revocable("sk_rotated", epoch).is_err());
}
#[test]
fn bound_execution_revocation_preserves_charges_and_cancelling_one_request_preserves_parent() {
    block_on(async {
        for policy_revocation in [false, true] {
            for transport in [
                ClientTransport::HttpJson,
                ClientTransport::HttpSse,
                ClientTransport::WebSocket,
            ] {
                let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
                let admissions = Arc::new(Admissions::default());
                let budget = Arc::new(Budget {
                    active: admissions.active.clone(),
                    ..Default::default()
                });
                let service = DefaultExecutionService::new(
                    snapshots.clone(),
                    Arc::new(TrackingExecutionStore::default()),
                    ProviderRegistry::new([
                        Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>
                    ])
                    .unwrap(),
                    admissions.clone(),
                    Arc::new(UnusedContinuation),
                    Arc::new(RecordingClientApiKeyUsage::default()),
                )
                .with_budget(budget.clone());
                let parent = gateway_core::lifecycle::CancellationToken::new();
                let client = service
                    .authenticate_bound_key(
                        &ClientApiKeyId::new("key_start_test").unwrap(),
                        parent.clone(),
                    )
                    .unwrap();
                let mut next = request(&service, transport);
                next.client = client;
                let mut started = service.start(next).await.unwrap();
                consume_charged_prefix(started.session.as_mut()).await;
                if policy_revocation {
                    // The start consumed the only client; the live session must retain its guard.
                    snapshots.publish(start_snapshot_with_policy(
                        2,
                        false,
                        RateLimits::unlimited(),
                    ));
                } else {
                    parent.cancel();
                }
                assert!(matches!(
                    started.session.next_event().await,
                    Err(gateway_core::engine::EngineError::Cancelled)
                ));
                started.session.detach_finalize().await;
                assert_cleanup_completed(&admissions, &budget, &started.request_id);
                if policy_revocation {
                    assert!(!parent.is_cancelled());
                }
            }
        }
        let admissions = Arc::new(Admissions::default());
        let service = charged_service(admissions.clone(), None, false);
        let parent = gateway_core::lifecycle::CancellationToken::new();
        let client = service
            .authenticate_bound_key(
                &ClientApiKeyId::new("key_start_test").unwrap(),
                parent.clone(),
            )
            .unwrap();
        for _ in 0..2 {
            let mut next = request(&service, ClientTransport::WebSocket);
            next.client = client.clone();
            let started = service.start(next).await.unwrap();
            started.session.cancel();
            started.session.detach_finalize().await;
            assert!(!parent.is_cancelled());
            assert!(!admissions.active.load(Ordering::SeqCst));
        }
    });
}

#[test]
fn prepared_runtime_settings_route_the_request_without_mutating_identity_or_other_requests() {
    use gateway_core::engine::execution::PreparedExecutionRequest;
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
            let service = DefaultExecutionService::new(
                snapshots.clone(),
                store.clone(),
                ProviderRegistry::new([
                    Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>
                ])
                .unwrap(),
                admissions.clone(),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            )
            .with_budget(budget.clone());
            let input = request(&service, transport);
            let prepared = service.prepare_execution(input.client).unwrap();
            let request_id = prepared.request_id().clone();
            let start = prepared.started_at();
            let deadline = prepared.deadline_at();
            let policy = prepared.client().policy().clone();
            let original = prepared.client().snapshot().clone();
            let replacement = prepared
                .settings()
                .values()
                .clone()
                .with_model_mappings(BTreeMap::from([("alias".into(), "gpt-start".into())]));
            let settings = prepared
                .settings()
                .replace(replacement, "mapping-plugin")
                .unwrap();
            let prepared = prepared.with_settings(settings).unwrap();
            assert_eq!(prepared.client().policy(), &policy);
            assert!(Arc::ptr_eq(prepared.client().snapshot(), &original));
            assert_eq!(prepared.request_id(), &request_id);
            assert_eq!(prepared.started_at(), start);
            assert_eq!(prepared.deadline_at(), deadline);
            assert_eq!(snapshots.acquire().unwrap().mapped_model("alias"), "alias");
            assert_eq!(
                service
                    .prepare_execution(service.authenticate("sk_start_test").unwrap())
                    .unwrap()
                    .settings()
                    .snapshot()
                    .mapped_model("alias"),
                "alias"
            );
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: PublicModelId::new("alias").unwrap(),
                        operation: start_operation_for_model("alias"),
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            consume_charged_prefix(started.session.as_mut()).await;
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &request_id);
            let rows = store.requests.lock().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, request_id);
            assert_eq!(rows[0].started_at, start);
            assert_eq!(rows[0].deadline_at, deadline);
            assert_eq!(rows[0].client_api_key_ref, policy.key_id().clone());
        }
    });
}

#[test]
fn prepared_runtime_settings_reject_foreign_baselines_and_preserve_revocation() {
    for cause in ["foreign", "key", "cancel", "timeout", "key_scope"] {
        let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
        let admissions = Arc::new(Admissions::default());
        let store = Arc::new(TrackingExecutionStore::default());
        let service = DefaultExecutionService::new(
            snapshots.clone(),
            store.clone(),
            ProviderRegistry::default(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let prepared = service
            .prepare_execution(service.authenticate("sk_start_test").unwrap())
            .unwrap();
        let settings = if cause == "foreign" {
            gateway_core::routing::RequestSettings::new(Arc::new(start_snapshot()))
        } else {
            prepared.settings().clone()
        };
        let settings = if cause == "timeout" {
            settings.with_execution(prepared.client().policy(), Some(0))
        } else if cause == "key_scope" {
            let other = ClientPolicy::new(
                ClientApiKeyId::new("other").unwrap(),
                PlaintextClientApiKey::new("sk_other").unwrap(),
                prepared.client().policy().account_scope().clone(),
                true,
                RateLimits::unlimited(),
            );
            settings.with_execution(&other, Some(600_000))
        } else {
            settings
        };
        if cause == "key" {
            snapshots.publish(start_snapshot_with_policy(
                2,
                false,
                RateLimits::unlimited(),
            ));
        }
        if cause == "cancel" {
            prepared.cancellation().cancel();
        }
        let result = prepared.with_settings(settings);
        assert!(
            matches!(result,Err(e) if e.kind()==match cause { "foreign" | "key_scope"=>GatewayErrorKind::InvalidRequest, "timeout"=>GatewayErrorKind::Timeout, "key"=>GatewayErrorKind::Unauthorized, _=>GatewayErrorKind::Cancelled })
        );
        assert!(admissions.limits.lock().unwrap().is_empty());
        assert!(!store.touched.load(Ordering::SeqCst));
    }
}

struct ExecutionSettingsProbe {
    observed: Mutex<Vec<gateway_core::account::FastMode>>,
}
#[async_trait]
impl Provider for ExecutionSettingsProbe {
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
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.observed.lock().unwrap().push(context.fast_mode());
        ChargedProvider { fail: false }
            .execute(request, context)
            .await
    }
}
#[test]
fn prepared_execution_passes_fast_and_key_limits_without_changing_the_authenticated_policy() {
    use gateway_core::{account::FastMode, engine::execution::PreparedExecutionRequest};
    block_on(async {
        for mode in [FastMode::Default, FastMode::Enabled, FastMode::Disabled] {
            for transport in [
                ClientTransport::HttpJson,
                ClientTransport::HttpSse,
                ClientTransport::WebSocket,
            ] {
                let admissions = Arc::new(Admissions::default());
                let budget = Arc::new(Budget {
                    active: admissions.active.clone(),
                    ..Default::default()
                });
                let provider = Arc::new(ExecutionSettingsProbe {
                    observed: Mutex::default(),
                });
                let service = DefaultExecutionService::new(
                    RuntimeSnapshotHandle::new(start_snapshot()),
                    Arc::new(TrackingExecutionStore::default()),
                    ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                    admissions.clone(),
                    Arc::new(UnusedContinuation),
                    Arc::new(RecordingClientApiKeyUsage::default()),
                )
                .with_budget(budget.clone());
                let input = request(&service, transport);
                let prepared = service.prepare_execution(input.client).unwrap();
                let policy = prepared.client().policy().clone();
                let mut values = prepared.settings().execution_values().unwrap();
                values.fast_mode = mode;
                values.client_limits = RateLimits {
                    max_concurrency: 3,
                    requests_per_minute: 13,
                };
                let changed = prepared
                    .settings()
                    .replace_execution(&values, "settings")
                    .unwrap();
                let prepared = prepared.with_settings(changed).unwrap();
                assert_eq!(prepared.client().policy(), &policy);
                let mut started = service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata,
                        },
                    )
                    .await
                    .unwrap();
                consume_charged_prefix(started.session.as_mut()).await;
                started.session.detach_finalize().await;
                assert_cleanup_completed(&admissions, &budget, &started.request_id);
                assert_eq!(*provider.observed.lock().unwrap(), [mode]);
                assert_eq!(*admissions.limits.lock().unwrap(), [values.client_limits]);
                assert_eq!(
                    service.authenticate("sk_start_test").unwrap().policy(),
                    &policy
                );
            }
        }
    });
}

#[test]
fn execution_limit_overrides_do_not_remove_the_shared_user_admission_limit() {
    use gateway_core::engine::execution::PreparedExecutionRequest;
    block_on(async {
        let provider = ProviderKind::new("openai").unwrap();
        let clients = ["a", "b"].map(|id| {
            ClientPolicy::new(
                ClientApiKeyId::new(format!("key_{id}")).unwrap(),
                PlaintextClientApiKey::new(format!("sk_{id}")).unwrap(),
                account_scope(&provider, "acct_start"),
                true,
                RateLimits {
                    max_concurrency: 2,
                    requests_per_minute: 0,
                },
            )
            .with_user_admission(Some((
                ClientApiKeyId::user_admission("alice"),
                RateLimits {
                    max_concurrency: 1,
                    requests_per_minute: 0,
                },
            )))
        });
        let admissions = Arc::new(SharedUserAdmissions::default());
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(start_snapshot_with_clients(1, clients.to_vec())),
            Arc::new(TrackingExecutionStore::default()),
            ProviderRegistry::default(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        );
        let mut sessions = Vec::new();
        for (index, key) in ["sk_a", "sk_b"].into_iter().enumerate() {
            let prepared = service
                .prepare_execution(service.authenticate(key).unwrap())
                .unwrap();
            let mut values = prepared.settings().execution_values().unwrap();
            values.client_limits = RateLimits::unlimited();
            let settings = prepared
                .settings()
                .replace_execution(&values, "unlimited")
                .unwrap();
            let result = service
                .start_prepared(
                    prepared.with_settings(settings).unwrap(),
                    PreparedExecutionRequest {
                        public_model: PublicModelId::new("gpt-start").unwrap(),
                        operation: start_operation(),
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
                .await;
            if index == 0 {
                sessions.push(result.unwrap());
            } else {
                assert!(matches!(result,Err(e) if e.kind()==GatewayErrorKind::RateLimited));
            }
        }
        for started in sessions {
            started.session.detach_finalize().await;
        }
        assert!(
            admissions
                .active
                .lock()
                .unwrap()
                .values()
                .all(BTreeSet::is_empty)
        );
    });
}

struct TestAdmissionLease;
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for TestAdmissionLease {}

struct CountingAdmissionLease(Arc<AtomicUsize>);
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for CountingAdmissionLease {}
impl Drop for CountingAdmissionLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct TestExecutionRecoveryLease;
impl gateway_core::engine::ExecutionRecoveryLeaseGuard for TestExecutionRecoveryLease {}

#[test]
fn timeout_overrides_use_original_start_and_can_remove_the_total_deadline() {
    let admissions = Arc::new(Admissions::default());
    let service = service(admissions.clone(), Arc::new(Budget::default()));
    let prepared = service
        .prepare_execution(service.authenticate("sk_start_test").unwrap())
        .unwrap();
    let start = prepared.started_at();
    let id = prepared.request_id().clone();
    assert_eq!(prepared.deadline_at().at(), None);
    let mut values = prepared.settings().execution_values().unwrap();
    values.timeout_ms = Some(2000);
    let settings = prepared
        .settings()
        .replace_execution(&values, "finite")
        .unwrap();
    let prepared = prepared.with_settings(settings).unwrap();
    assert_eq!(
        prepared.deadline_at().at(),
        Some(start + Duration::from_secs(2))
    );
    std::thread::sleep(Duration::from_millis(20));
    values.timeout_ms = Some(4000);
    let settings = prepared
        .settings()
        .replace_execution(&values, "later")
        .unwrap();
    let prepared = prepared.with_settings(settings).unwrap();
    assert_eq!(
        prepared.deadline_at().at(),
        Some(start + Duration::from_secs(4))
    );
    values.timeout_ms = None;
    let settings = prepared
        .settings()
        .replace_execution(&values, "unlimited")
        .unwrap();
    let prepared = prepared.with_settings(settings).unwrap();
    assert_eq!(prepared.deadline_at().at(), None);
    assert_eq!(prepared.started_at(), start);
    assert_eq!(prepared.request_id(), &id);
    values.timeout_ms = Some(1);
    let settings = prepared
        .settings()
        .replace_execution(&values, "elapsed")
        .unwrap();
    assert!(
        matches!(prepared.with_settings(settings), Err(e) if e.kind() == GatewayErrorKind::Timeout)
    );
    assert!(admissions.limits.lock().unwrap().is_empty());
}

use gateway_core::{
    account::FastMode,
    engine::{execution::PreparedExecutionRequest, middleware::MiddlewareRequest},
};

pub(super) fn prepare(
    service: &DefaultExecutionService,
) -> gateway_core::engine::execution::PreparedRootExecution {
    service
        .prepare_execution(service.authenticate("sk_start_test").unwrap())
        .unwrap()
}

pub(super) fn assert_failure<T>(result: Result<T, GatewayError>, kind: GatewayErrorKind) {
    match result {
        Err(error) => assert_eq!(error.kind(), kind),
        Ok(_) => panic!("nested preparation should fail with {kind:?}"),
    }
}

#[test]
fn explicit_model_targets_cannot_escape_key_scope_or_wait_on_the_parent_account() {
    use gateway_core::engine::nested::ModelExecutionTarget;
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    for target in [
        ModelExecutionTarget {
            provider: Some(ProviderKind::new("xai").unwrap()),
            ..Default::default()
        },
        ModelExecutionTarget {
            account: Some(ProviderAccountId::new("acct_other").unwrap()),
            ..Default::default()
        },
    ] {
        assert_failure(
            prepare(&service).with_model_target(target),
            GatewayErrorKind::PolicyDenied,
        );
    }
    let account = ProviderAccountId::new("acct_start").unwrap();
    assert_failure(
        prepare(&service).with_model_target(ModelExecutionTarget {
            account: Some(account.clone()),
            parent_account: Some(account),
            ..Default::default()
        }),
        GatewayErrorKind::NoAvailableProvider,
    );
}
#[test]
fn an_excluded_parent_account_with_no_alternative_fails_before_admission() {
    use gateway_core::engine::nested::ModelExecutionTarget;
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let service = service(admissions.clone(), Arc::new(Budget::default()));
        let prepared = prepare(&service)
            .with_model_target(ModelExecutionTarget {
                parent_account: Some(ProviderAccountId::new("acct_start").unwrap()),
                ..Default::default()
            })
            .unwrap();
        let input = request(&service, ClientTransport::HttpJson);
        assert_failure(
            service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await,
            GatewayErrorKind::NoAvailableProvider,
        );
        assert!(admissions.limits.lock().unwrap().is_empty());
    });
}
#[test]
fn standalone_plugin_calls_share_limits_and_have_no_fictitious_parent_request() {
    use gateway_core::engine::nested::ExtensionCallScope;
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let graph = ExtensionCallScope::default();
    let cancellation = gateway_core::lifecycle::CancellationToken::new();
    for _ in 0..64 {
        let prepared = service
            .prepare_plugin_execution(
                service.authenticate("sk_start_test").unwrap(),
                "plugin-standalone".into(),
                graph.clone(),
                cancellation.clone(),
            )
            .unwrap();
        let authority = prepared.nested_context();
        assert!(authority.scope().parent_request_id().is_none());
        assert!(authority.scope().initiating_plugin_instance_id().is_none());
        assert!(
            authority
                .scope()
                .excluded_instances()
                .contains("plugin-standalone")
        );
    }
    assert_failure(
        service.prepare_plugin_execution(
            service.authenticate("sk_start_test").unwrap(),
            "plugin-standalone".into(),
            graph,
            cancellation,
        ),
        GatewayErrorKind::RateLimited,
    );
}

#[test]
fn nested_preparation_is_lazy_inherits_settings_and_bounds_its_deadline() {
    let admissions = Arc::new(Admissions::default());
    let service = service(admissions.clone(), Arc::new(Budget::default()));
    let parent = prepare(&service);
    let mut values = parent.settings().execution_values().unwrap();
    values.fast_mode = FastMode::Disabled;
    values.client_limits = RateLimits {
        max_concurrency: 3,
        requests_per_minute: 17,
    };
    values.timeout_ms = Some(60_000);
    let changed = parent
        .settings()
        .replace_execution(&values, "plugin-a")
        .unwrap();
    let parent = parent.with_settings(changed).unwrap();
    let child = service
        .prepare_nested(parent.nested_context(), "plugin-a".into(), None)
        .unwrap();
    assert_ne!(child.request_id(), parent.request_id());
    assert_eq!(child.client().policy(), parent.client().policy());
    assert_eq!(child.settings().execution_values().unwrap(), values);
    assert_eq!(child.deadline_at(), parent.deadline_at());
    assert_eq!(
        child.nested_context().scope().parent_request_id(),
        Some(parent.request_id())
    );
    assert_eq!(
        child
            .nested_context()
            .scope()
            .initiating_plugin_instance_id(),
        Some("plugin-a")
    );
    assert_eq!(
        child.nested_context().scope().excluded_instances(),
        &BTreeSet::from(["plugin-a".into()])
    );
    let mut unbounded = values;
    unbounded.timeout_ms = None;
    let settings = child
        .settings()
        .replace_execution(&unbounded, "plugin-b")
        .unwrap();
    let child = child.with_settings(settings).unwrap();
    assert_eq!(
        child.deadline_at(),
        parent.deadline_at(),
        "a child cannot remove the parent deadline"
    );
    assert!(admissions.limits.lock().unwrap().is_empty());
    parent.cancellation().cancel();
    assert!(child.cancellation().is_cancelled());
    assert_failure(
        service.prepare_nested(child.nested_context(), "plugin-b".into(), None),
        GatewayErrorKind::Cancelled,
    );
}

#[test]
fn http_children_adopt_model_ancestry_and_keep_transport_permits_after_preparation_is_dropped() {
    use gateway_core::engine::middleware::http::Context;
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let parent = prepare(&service);
    let mut values = parent.settings().execution_values().unwrap();
    values.fast_mode = FastMode::Disabled;
    values.timeout_ms = Some(60_000);
    let settings = parent
        .settings()
        .replace_execution(&values, "parent-settings")
        .unwrap();
    let parent = parent.with_settings(settings).unwrap();
    let root = Context::from_execution(parent.nested_context());
    let mut held = vec![];
    for _ in 0..16 {
        let context = root.child("http-plugin".into()).unwrap();
        let child = service
            .prepare_http_execution(
                service.authenticate("sk_start_test").unwrap(),
                context.clone(),
            )
            .unwrap();
        assert_eq!(child.settings().execution_values().unwrap(), values);
        assert_eq!(child.deadline_at(), parent.deadline_at());
        assert_eq!(
            child.nested_context().scope().parent_request_id(),
            Some(parent.request_id())
        );
        assert_eq!(
            child
                .nested_context()
                .scope()
                .initiating_plugin_instance_id(),
            Some("http-plugin")
        );
        drop(child);
        held.push(context);
    }
    assert!(root.child("http-plugin".into()).is_err());
    held.pop();
    let child = root.child("http-plugin".into()).unwrap();
    let grandchild = child.child("another-http-plugin".into());
    assert!(
        grandchild.is_err(),
        "all sixteen shared graph slots remain held"
    );
    held.pop();
    let grandchild = child.child("another-http-plugin".into()).unwrap();
    let adopted = service
        .prepare_http_execution(service.authenticate("sk_start_test").unwrap(), grandchild)
        .unwrap();
    assert_eq!(
        adopted.nested_context().scope().parent_request_id(),
        Some(parent.request_id())
    );
    assert_eq!(
        adopted.nested_context().scope().excluded_instances().len(),
        2
    );
    parent.cancellation().cancel();
    assert!(adopted.cancellation().is_cancelled());
    assert_failure(
        service.prepare_http_execution(service.authenticate("sk_start_test").unwrap(), child),
        GatewayErrorKind::Cancelled,
    );
}

#[test]
fn http_model_parent_must_match_the_reserved_graph_and_cannot_outlive_execution() {
    use gateway_core::engine::middleware::http::Context;
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let first = prepare(&service);
    let second = prepare(&service);
    let context = Context::from_execution(first.nested_context())
        .child("plugin-http".into())
        .unwrap();
    let forged = Context {
        model_parent: Some(second.nested_context()),
        ..context.clone()
    };
    assert_failure(
        service.prepare_http_execution(service.authenticate("sk_start_test").unwrap(), forged),
        GatewayErrorKind::InvalidRequest,
    );
    drop(first);
    assert_failure(
        service.prepare_http_execution(service.authenticate("sk_start_test").unwrap(), context),
        GatewayErrorKind::Cancelled,
    );
}

#[test]
fn choosing_another_key_resets_execution_overrides_but_keeps_runtime_sources() {
    let provider = ProviderKind::new("openai").unwrap();
    let first = start_snapshot().client_policies().next().unwrap().clone();
    let second = ClientPolicy::new(
        ClientApiKeyId::new("key_other").unwrap(),
        PlaintextClientApiKey::new("sk_other").unwrap(),
        account_scope(&provider, "acct_start"),
        true,
        RateLimits {
            max_concurrency: 2,
            requests_per_minute: 31,
        },
    );
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot_with_clients(1, vec![first, second.clone()])),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(Admissions::default()),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let parent = prepare(&service);
    let mut values = parent.settings().execution_values().unwrap();
    values.fast_mode = FastMode::Enabled;
    values.client_limits = RateLimits {
        max_concurrency: 7,
        requests_per_minute: 99,
    };
    values.timeout_ms = Some(60_000);
    let mut runtime = serde_json::to_value(&values.runtime).unwrap();
    runtime["max_concurrent_per_account"] = json!(5);
    values.runtime = serde_json::from_value(runtime).unwrap();
    let settings = parent
        .settings()
        .replace_execution(&values, "plugin-a")
        .unwrap();
    let parent = parent.with_settings(settings).unwrap();
    let other = service.authenticate("sk_other").unwrap();
    let http_child = service
        .prepare_http_execution(
            other.clone(),
            gateway_core::engine::middleware::http::Context::from_execution(
                parent.nested_context(),
            )
            .child("plugin-a".into())
            .unwrap(),
        )
        .unwrap();
    let native_child = service
        .prepare_nested(parent.nested_context(), "plugin-a".into(), Some(other))
        .unwrap();
    for child in [http_child, native_child] {
        let actual = child.settings().execution_values().unwrap();
        assert_eq!(actual.fast_mode, FastMode::Default);
        assert_eq!(actual.client_limits, second.limits());
        assert_eq!(actual.timeout_ms, None);
        assert_eq!(
            serde_json::to_value(&actual.runtime).unwrap()["max_concurrent_per_account"],
            5
        );
        assert_eq!(
            child.settings().inspect()["overrides"]["max_concurrent_per_account"]["instance_id"],
            "plugin-a"
        );
        assert_eq!(
            child.settings().inspect()["execution"]["fast_mode"],
            serde_json::Value::Null
        );
        assert_eq!(child.deadline_at(), parent.deadline_at());
        assert_eq!(child.client().policy().key_id(), second.key_id());
    }
}

#[test]
fn retaining_authority_does_not_keep_parent_alive_or_pin_graph_capacity() {
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let parent = prepare(&service);
    let authority = parent.nested_context();
    let mut retained = Vec::new();
    for _ in 0..64 {
        let child = service
            .prepare_nested(authority.clone(), "plugin-a".into(), None)
            .unwrap();
        retained.push(child.nested_context());
        drop(child);
    }
    assert!(
        retained
            .iter()
            .all(|context| context.cancellation().is_cancelled())
    );
    assert_failure(
        service.prepare_nested(authority.clone(), "plugin-a".into(), None),
        GatewayErrorKind::RateLimited,
    );
    drop(parent);
    assert!(authority.cancellation().is_cancelled());
    assert_failure(
        service.prepare_nested(authority, "plugin-a".into(), None),
        GatewayErrorKind::Cancelled,
    );
    let fresh_root = prepare(&service);
    assert!(
        service
            .prepare_nested(fresh_root.nested_context(), "plugin-a".into(), None)
            .is_ok()
    );
}

#[test]
fn graph_limits_bound_active_children_depth_and_ancestor_recursion() {
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let root = prepare(&service);
    for invalid in ["", "plugin\nforged"] {
        assert_failure(
            service.prepare_nested(root.nested_context(), invalid.into(), None),
            GatewayErrorKind::InvalidRequest,
        );
    }
    let mut children = Vec::new();
    for _ in 0..16 {
        children.push(
            service
                .prepare_nested(root.nested_context(), "plugin-a".into(), None)
                .unwrap(),
        );
    }
    assert_failure(
        service.prepare_nested(root.nested_context(), "plugin-a".into(), None),
        GatewayErrorKind::RateLimited,
    );
    let context = children.pop().unwrap().nested_context();
    assert!(context.cancellation().is_cancelled());
    assert!(
        service
            .prepare_nested(root.nested_context(), "plugin-a".into(), None)
            .is_ok()
    );
    children.clear();
    let mut ancestors = vec![root];
    for depth in 0..8 {
        let previous = ancestors.last().unwrap().nested_context();
        let child = service
            .prepare_nested(previous, format!("depth-{depth}"), None)
            .unwrap();
        ancestors.push(child);
    }
    assert_failure(
        service.prepare_nested(ancestors[1].nested_context(), "depth-0".into(), None),
        GatewayErrorKind::Unsupported,
    );
    assert_failure(
        service.prepare_nested(
            ancestors.last().unwrap().nested_context(),
            "depth-8".into(),
            None,
        ),
        GatewayErrorKind::Unsupported,
    );
}

#[test]
fn revoking_the_parent_key_rejects_children_even_with_an_explicit_live_other_key() {
    let other = ClientPolicy::new(
        ClientApiKeyId::new("key_other").unwrap(),
        PlaintextClientApiKey::new("sk_other").unwrap(),
        account_scope(&ProviderKind::new("openai").unwrap(), "acct_start"),
        true,
        RateLimits::unlimited(),
    );
    let first = start_snapshot().client_policies().next().unwrap().clone();
    let snapshots =
        RuntimeSnapshotHandle::new(start_snapshot_with_clients(1, vec![first, other.clone()]));
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(Admissions::default()),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let parent = prepare(&service);
    let context = parent.nested_context();
    snapshots.publish(start_snapshot_with_clients(2, vec![other]));
    let live_other = service.authenticate("sk_other").unwrap();
    assert_failure(
        service.prepare_nested(context, "plugin-a".into(), Some(live_other)),
        GatewayErrorKind::Unauthorized,
    );
    let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
    let service = DefaultExecutionService::new(
        snapshots,
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(Admissions::default()),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let parent = prepare(&service);
    let revocation = gateway_core::lifecycle::CancellationToken::new();
    let authority = parent.nested_context().with_revocation(revocation.clone());
    revocation.cancel();
    assert_failure(
        service.prepare_nested(
            authority,
            "plugin-a".into(),
            Some(service.authenticate("sk_start_test").unwrap()),
        ),
        GatewayErrorKind::Cancelled,
    );
}

#[test]
fn middleware_body_replacement_preserves_authority_and_uses_current_settings() {
    let service = service(Arc::new(Admissions::default()), Arc::new(Budget::default()));
    let parent = prepare(&service);
    let authority = parent.nested_context();
    let request = MiddlewareRequest::new("openai", vec![], bytes::Bytes::from_static(b"{}"))
        .with_execution_context(authority)
        .with_settings(parent.settings().clone());
    let mut values = parent.settings().execution_values().unwrap();
    values.fast_mode = FastMode::Disabled;
    let settings = parent
        .settings()
        .replace_execution(&values, "plugin-a")
        .unwrap();
    let request = request
        .replace_parts(
            "openai".into(),
            vec![],
            bytes::Bytes::from_static(b"{\"input\":\"changed\"}"),
            None,
        )
        .unwrap()
        .with_settings(settings);
    let child = service
        .prepare_nested(
            request.nested_context().unwrap().unwrap(),
            "plugin-a".into(),
            None,
        )
        .unwrap();
    assert_eq!(
        child.settings().execution_values().unwrap().fast_mode,
        FastMode::Disabled
    );
    assert_eq!(
        parent.settings().execution_values().unwrap().fast_mode,
        FastMode::Default
    );
    let foreign = prepare(&service);
    let foreign_settings = foreign.settings().clone();
    // Two executions on the same Key/snapshot may exchange values, but not opaque ancestry.
    let request = request.with_settings(foreign_settings);
    assert_eq!(
        request.nested_context().unwrap().unwrap().request_id(),
        parent.request_id()
    );
}

#[test]
fn child_and_parent_settle_distinct_calls_once_and_child_authority_expires_after_cleanup() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(start_snapshot()),
            store.clone(),
            ProviderRegistry::new([Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>])
                .unwrap(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        )
        .with_budget(budget.clone());
        let parent = prepare(&service);
        let parent_id = parent.request_id().clone();
        let child = service
            .prepare_nested(parent.nested_context(), "plugin-a".into(), None)
            .unwrap();
        let child_authority = child.nested_context();
        let child_id = child.request_id().clone();
        for prepared in [child, parent] {
            let input = request(&service, ClientTransport::HttpJson);
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                started.session.collect_uncommitted().await.unwrap().len(),
                3
            );
            started.session.commit_downstream(Some(200)).await.unwrap();
            assert!(started.session.next_event().await.unwrap().is_none());
            assert!(started.session.is_finalized());
            // Keeping a finalized session and its context cannot start new descendants.
            assert_failure(
                service.prepare_nested(child_authority.clone(), "plugin-b".into(), None),
                GatewayErrorKind::Cancelled,
            );
            started.session.detach_finalize().await;
        }
        let rows = store.requests.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, child_id);
        assert_eq!(rows[0].parent_request_id, Some(parent_id.clone()));
        assert_eq!(
            rows[0].initiating_plugin_instance_id.as_deref(),
            Some("plugin-a")
        );
        assert_eq!(rows[1].id, parent_id);
        assert_eq!(rows[1].parent_request_id, None);
        let charges = budget.charges.lock().unwrap();
        assert_eq!(charges.len(), 2);
        assert_eq!(charges[0].request_id, child_id);
        assert_eq!(charges[1].request_id, parent_id);
        assert!(
            charges
                .iter()
                .all(|charge| charge.amount_usd == known_charge())
        );
        assert_eq!(admissions.releases.load(Ordering::SeqCst), 2);
        assert!(!admissions.active.load(Ordering::SeqCst));
    });
}

struct NestedRetryProbe {
    retry: AtomicBool,
    calls: AtomicUsize,
    delay: Duration,
}
#[async_trait]
impl Provider for NestedRetryProbe {
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
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.retry.load(Ordering::SeqCst) && call == 0 {
            let candidate = request.candidate();
            let metadata = ProviderCallMetadata::new(
                candidate.provider().clone(),
                candidate.upstream_model().unwrap().clone(),
                ProviderAccountId::new("acct_start").unwrap(),
                UpstreamTransport::new("websocket").unwrap(),
            );
            let error =
                ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::NotSent)
                    .with_replay_safe()
                    .with_pre_delivery_transport_retry(
                        std::num::NonZeroU32::new(1).unwrap(),
                        self.delay,
                    );
            Ok(ProviderStream::new(
                metadata,
                futures::stream::iter([Err(error)]),
                (),
            ))
        } else {
            ChargedProvider { fail: false }
                .execute(request, context)
                .await
        }
    }
}

#[test]
fn observed_child_effects_prevent_parent_replay_before_child_settlement() {
    block_on(async {
        for child_effects in [false, true] {
            let provider = Arc::new(NestedRetryProbe {
                retry: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let service = DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(start_snapshot()),
                store,
                ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                Arc::new(UnusedAdmissions),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            );
            let parent = prepare(&service);
            let mut child = if child_effects {
                let prepared = service
                    .prepare_nested(parent.nested_context(), "plugin-a".into(), None)
                    .unwrap();
                let cancellation = prepared.cancellation();
                let input = request(&service, ClientTransport::HttpSse);
                let mut child = service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata,
                        },
                    )
                    .await
                    .unwrap();
                assert!(child.session.next_event().await.unwrap().is_some());
                assert!(!child.session.is_finalized());
                Some((child, cancellation))
            } else {
                None
            };
            provider.calls.store(0, Ordering::SeqCst);
            provider.retry.store(true, Ordering::SeqCst);
            let input = request(&service, ClientTransport::HttpJson);
            let mut started = service
                .start_prepared(
                    parent,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            let result = started.session.collect_uncommitted().await;
            if child_effects {
                assert!(matches!(result, Err(EngineError::Provider(_))));
                assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            } else {
                assert_eq!(result.unwrap().len(), 3);
                assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
                started.session.commit_downstream(Some(200)).await.unwrap();
            }
            started.session.detach_finalize().await;
            if let Some((child, cancellation)) = child.take() {
                assert!(cancellation.is_cancelled());
                child.session.detach_finalize().await;
            }
        }
    });
}

#[test]
fn child_effects_observed_during_parent_backoff_prevent_the_next_attempt() {
    block_on(async {
        for child_effects in [false, true] {
            let provider = Arc::new(NestedRetryProbe {
                retry: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                delay: Duration::from_secs(1),
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let service = DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(start_snapshot()),
                store.clone(),
                ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                Arc::new(UnusedAdmissions),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            );
            let parent = prepare(&service);
            let prepared = service
                .prepare_nested(parent.nested_context(), "plugin-a".into(), None)
                .unwrap();
            let cancellation = prepared.cancellation();
            let input = request(&service, ClientTransport::HttpSse);
            let mut child = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            let input = request(&service, ClientTransport::HttpJson);
            let mut started = service
                .start_prepared(
                    parent,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            let mut collecting = Box::pin(started.session.collect_uncommitted());
            assert!(futures::poll!(&mut collecting).is_pending());
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                1,
                "parent must be waiting in the retry backoff"
            );
            if child_effects {
                assert!(child.session.next_event().await.unwrap().is_some());
                assert!(!child.session.is_finalized());
            }
            let result = collecting.await;
            if child_effects {
                assert!(
                    matches!(result, Err(EngineError::Provider(_))),
                    "a late child effect must retain the original parent failure: {result:?}"
                );
            } else {
                assert_eq!(result.unwrap().len(), 3);
                started.session.commit_downstream(Some(200)).await.unwrap();
            }
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                2,
                "a sent child must replace, not add to, the parent retry"
            );
            started.session.detach_finalize().await;
            assert!(cancellation.is_cancelled());
            child.session.detach_finalize().await;
            let finalizations = store.finalizations.lock().unwrap();
            assert_eq!(finalizations.len(), 2);
            assert_eq!(
                finalizations
                    .iter()
                    .find(|f| f.request_id == started.request_id)
                    .unwrap()
                    .attempt_count,
                if child_effects { 1 } else { 2 }
            );
        }
    });
}

#[test]
fn http_dispatch_effects_in_the_adopted_scope_prevent_replaying_the_parent_model_request() {
    block_on(async {
        for dispatched in [false, true] {
            let provider = Arc::new(NestedRetryProbe {
                retry: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let service = DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(start_snapshot()),
                store.clone(),
                ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                Arc::new(UnusedAdmissions),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            );
            let scope = gateway_core::engine::nested::ExtensionCallScope::default();
            if dispatched {
                scope.observe_http_dispatch();
            }
            let input = request(&service, ClientTransport::HttpJson);
            let prepared = service
                .prepare_http_execution(
                    input.client,
                    gateway_core::engine::middleware::http::Context {
                        request_id: "ingress".into(),
                        call_id: "call".into(),
                        parent_call_id: None,
                        call_scope: scope,
                        model_parent: None,
                        cancellation: Default::default(),
                        deadline_at: Default::default(),
                    },
                )
                .unwrap();
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            let result = started.session.collect_uncommitted().await;
            if dispatched {
                assert!(matches!(result, Err(EngineError::Provider(_))));
            } else {
                result.unwrap();
                started.session.commit_downstream(Some(200)).await.unwrap();
            }
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                if dispatched { 1 } else { 2 }
            );
            started.session.detach_finalize().await;
            let records = store.finalizations.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].attempt_count, if dispatched { 1 } else { 2 });
        }
    });
}

mod retry_policy {
    use super::*;
    use gateway_core::engine::retry_policy::{
        RetryDecision, RetryInput, RetryPolicy, RetryPolicyFault, RetryResult,
    };

    struct Policy {
        decision: RetryDecision,
        inputs: Mutex<Vec<RetryInput>>,
        gate: Mutex<Option<oneshot::Receiver<()>>>,
        revocation: gateway_core::lifecycle::CancellationToken,
    }
    impl Policy {
        fn new(decision: RetryDecision) -> Arc<Self> {
            Arc::new(Self {
                decision,
                inputs: Mutex::new(vec![]),
                gate: Mutex::new(None),
                revocation: Default::default(),
            })
        }
    }
    impl RetryPolicy for Policy {
        fn cancellation(&self) -> gateway_core::lifecycle::CancellationToken {
            self.revocation.clone()
        }
        fn decide_retry(
            &self,
            input: RetryInput,
        ) -> BoxFuture<'static, Result<RetryResult, RetryPolicyFault>> {
            self.inputs.lock().unwrap().push(input);
            let gate = self.gate.lock().unwrap().take();
            let decision = self.decision;
            let revocation = self.revocation.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                Ok(RetryResult {
                    decision,
                    revocation,
                })
            })
        }
    }

    struct TwoAccountProvider {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl Provider for TwoAccountProvider {
        fn name(&self) -> &'static str {
            "openai"
        }
        fn catalog_generation(&self) -> ProviderCatalogGeneration {
            Default::default()
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
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let account = context.required_account().cloned().unwrap_or_else(|| {
                ProviderAccountId::new(if call == 0 {
                    "acct_start"
                } else {
                    "acct_second"
                })
                .unwrap()
            });
            let metadata = ProviderCallMetadata::new(
                request.candidate().provider().clone(),
                request.candidate().upstream_model().unwrap().clone(),
                account,
                UpstreamTransport::new("websocket").unwrap(),
            );
            let events = if call == 0 {
                vec![Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    UpstreamSendState::NotSent,
                )
                .with_replay_safe()
                .with_pre_delivery_transport_retry(
                    std::num::NonZeroU32::MIN,
                    Duration::ZERO,
                ))]
            } else {
                let response = ResponseMeta::new("retry-complete", "gpt-start");
                vec![
                    Ok(GatewayEvent::Started(response.clone()).into()),
                    Ok(GatewayEvent::ProviderCost(
                        ProviderReportedCost::from_usd_ticks(known_charge().scaled()).unwrap(),
                    )
                    .into()),
                    Ok(GatewayEvent::Completed(response).into()),
                ]
            };
            Ok(ProviderStream::new(
                metadata,
                futures::stream::iter(events),
                (),
            ))
        }
    }
    fn two_account_snapshot() -> RuntimeSnapshot {
        let provider = ProviderKind::new("openai").unwrap();
        let directory = Arc::new(RuntimeAccountDirectory::new(
            ["acct_start", "acct_second"]
                .into_iter()
                .map(|id| {
                    (
                        ProviderAccountId::new(id).unwrap(),
                        RuntimeAccount::new(provider.clone(), BTreeSet::new()),
                    )
                })
                .collect(),
        ));
        start_snapshot_with_clients(
            1,
            vec![ClientPolicy::new(
                ClientApiKeyId::new("key_start_test").unwrap(),
                PlaintextClientApiKey::new("sk_start_test").unwrap(),
                Arc::new(FrozenAccountScope::new(
                    directory,
                    ClientRoutingScope::all_accounts(),
                )),
                true,
                RateLimits::unlimited(),
            )],
        )
    }

    #[test]
    fn prepared_retry_policy_vetoes_early_cross_provider_recovery() {
        block_on(async {
            for stop in [false, true] {
                let (service, admissions, budget, _, _) = scheduling_fixture("busy");
                let input = request(&service, ClientTransport::HttpJson);
                let policy = Policy::new(if stop {
                    RetryDecision::Stop
                } else {
                    RetryDecision::Retry
                });
                let prepared = service
                    .prepare_execution(input.client)
                    .unwrap()
                    .with_retry_policy(policy.clone());
                let mut started = service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata,
                        },
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    started.session.collect_uncommitted().await,
                    Err(EngineError::Provider(_))
                ));
                started.session.detach_finalize().await;
                assert!(!admissions.active.load(Ordering::SeqCst));
                assert_eq!(budget.charges.lock().unwrap().len(), 1);
                let inputs = policy.inputs.lock().unwrap();
                assert_eq!(inputs.len(), if stop { 1 } else { 2 });
                assert!(inputs[0].retry_allowed());
                assert_eq!(inputs[0].provider().as_str(), "openai");
                assert_eq!(inputs[0].client_key_id().as_str(), "key_start_test");
                assert_eq!(inputs[0].send_state(), UpstreamSendState::NotSent);
                assert_eq!(inputs[0].error_kind(), ProviderErrorKind::NoEligibleAccount);
                if !stop {
                    assert_eq!(inputs[1].provider().as_str(), "xai");
                    assert!(!inputs[1].retry_allowed());
                }
            }
        });
    }

    #[test]
    fn child_effects_during_policy_wait_veto_original_parent_recovery() {
        block_on(async {
            for child_effects in [false, true] {
                let provider = Arc::new(TwoAccountProvider {
                    calls: AtomicUsize::new(0),
                });
                let store = Arc::new(TrackingExecutionStore::default());
                let service = DefaultExecutionService::new(
                    RuntimeSnapshotHandle::new(two_account_snapshot()),
                    store.clone(),
                    ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                    Arc::new(UnusedAdmissions),
                    Arc::new(UnusedContinuation),
                    Arc::new(RecordingClientApiKeyUsage::default()),
                );
                let policy = Policy::new(RetryDecision::Retry);
                let (send, receive) = oneshot::channel();
                *policy.gate.lock().unwrap() = Some(receive);
                let parent = prepare(&service).with_retry_policy(policy.clone());
                let prepared = service
                    .prepare_nested(parent.nested_context(), "origin-plugin".into(), None)
                    .unwrap();
                let input = request(&service, ClientTransport::HttpSse);
                let mut child = service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata,
                        },
                    )
                    .await
                    .unwrap();
                let input = request(&service, ClientTransport::HttpJson);
                let mut parent = service
                    .start_prepared(
                        parent,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata,
                        },
                    )
                    .await
                    .unwrap();
                let mut read = Box::pin(parent.session.collect_uncommitted());
                assert!(futures::poll!(&mut read).is_pending());
                assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
                assert!(policy.inputs.lock().unwrap()[0].retry_allowed());
                if child_effects {
                    child.session.next_event().await.unwrap().unwrap();
                }
                send.send(()).unwrap();
                let result = read.await;
                if child_effects {
                    assert!(matches!(result, Err(EngineError::Provider(_))));
                } else {
                    result.unwrap();
                    parent.session.commit_downstream(Some(200)).await.unwrap();
                }
                assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
                parent.session.detach_finalize().await;
                child.session.detach_finalize().await;
                assert_eq!(
                    store
                        .finalizations
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|f| f.request_id == parent.request_id)
                        .unwrap()
                        .attempt_count,
                    if child_effects { 1 } else { 2 }
                );
            }
        });
    }

    #[test]
    fn nested_retry_input_keeps_host_key_and_all_plugin_ancestors() {
        block_on(async {
            let provider = Arc::new(NestedRetryProbe {
                retry: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            });
            let service = DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(start_snapshot()),
                Arc::new(TrackingExecutionStore::default()),
                ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
                Arc::new(UnusedAdmissions),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            );
            let parent = prepare(&service);
            let middle = service
                .prepare_nested(parent.nested_context(), "origin-plugin".into(), None)
                .unwrap();
            let policy = Policy::new(RetryDecision::Retry);
            let nested = service
                .prepare_nested(middle.nested_context(), "middle-plugin".into(), None)
                .unwrap()
                .with_retry_policy(policy.clone());
            let input = request(&service, ClientTransport::HttpJson);
            let mut started = service
                .start_prepared(
                    nested,
                    PreparedExecutionRequest {
                        public_model: input.public_model,
                        operation: input.operation,
                        metadata: input.metadata,
                    },
                )
                .await
                .unwrap();
            started.session.collect_uncommitted().await.unwrap();
            started.session.commit_downstream(Some(200)).await.unwrap();
            started.session.detach_finalize().await;
            let inputs = policy.inputs.lock().unwrap();
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].request_id(), &started.request_id);
            assert_eq!(inputs[0].client_key_id().as_str(), "key_start_test");
            assert_eq!(
                inputs[0].excluded_instances(),
                &BTreeSet::from(["origin-plugin".into(), "middle-plugin".into()])
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        });
    }
}

struct CatalogChargedProvider;
#[async_trait]
impl Provider for CatalogChargedProvider {
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
        _: &gateway_core::routing::FrozenAccountScope,
        _: &str,
        _: &str,
    ) -> Result<
        Option<Vec<gateway_core::routing::ProviderModelDescriptor>>,
        gateway_core::routing::ProviderCatalogUnavailable,
    > {
        Ok(Some(vec![gateway_core::routing::ProviderModelDescriptor {
            model: UpstreamModelId::new("gpt-start").unwrap(),
            payload: gateway_core::operation::RawJsonPayload::new(
                "codex",
                bytes::Bytes::from_static(
                    br#"{"slug":"gpt-start","native_field":{"effort":"high"}}"#,
                ),
            )
            .unwrap(),
        }]))
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        ChargedProvider { fail: false }
            .execute(request, context)
            .await
    }
}
#[test]
fn contributed_alias_native_catalog_and_execution_share_original_scope_and_single_ledger() {
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let base = start_snapshot();
            let catalog = gateway_core::routing::ContributedModelCatalog::new(
                vec![gateway_core::routing::ContributedModelAlias {
                    owner: "catalog-fixture".into(),
                    id: PublicModelId::new("plugin-alias").unwrap(),
                    provider: ProviderKind::new("openai").unwrap(),
                    target: UpstreamModelId::new("gpt-start").unwrap(),
                }],
                gateway_core::lifecycle::CancellationToken::new(),
            );
            let snapshots = RuntimeSnapshotHandle::new(base.clone());
            let service = DefaultExecutionService::new(
                snapshots.clone(),
                store.clone(),
                ProviderRegistry::new([Arc::new(CatalogChargedProvider) as Arc<dyn Provider>])
                    .unwrap(),
                admissions.clone(),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            )
            .with_budget(budget.clone());
            let client = service
                .authenticate("sk_start_test")
                .unwrap()
                .with_model_catalog(catalog)
                .unwrap();
            let alias = PublicModelId::new("plugin-alias").unwrap();
            assert!(service.public_models(&client).contains(&alias));
            let catalog = service
                .client_model_catalog(&client, "codex", "fixture")
                .await
                .unwrap();
            let mut native = None;
            let mut contributed = None;
            for entry in catalog {
                if let gateway_core::routing::PublicModelDescriptor::Native { model, payload } =
                    entry
                {
                    if model.as_str() == "gpt-start" {
                        native = Some(payload);
                    } else if model == alias {
                        contributed = Some(payload);
                    }
                }
            }
            assert_eq!(contributed.as_ref(), native.as_ref());
            assert!(contributed.is_some());
            // A later host snapshot cannot mutate an already prepared execution view.
            let prepared = service.prepare_execution(client.clone()).unwrap();
            snapshots.publish(base);
            assert!(
                !service
                    .contains_public_model(&service.authenticate("sk_start_test").unwrap(), &alias)
            );
            assert!(service.contains_public_model(&client, &alias));
            let metadata = request(&service, transport).metadata;
            let mut started = service
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: alias,
                        operation: start_operation_for_model("plugin-alias"),
                        metadata,
                    },
                )
                .await
                .unwrap();
            let request_id = started.request_id.clone();
            consume_charged_prefix(started.session.as_mut()).await;
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &request_id);
            assert_eq!(store.requested_models(), vec![Some("plugin-alias".into())]);
            assert_eq!(store.upstream_models(), vec![Some("gpt-start".into())]);
            assert_eq!(budget.charges.lock().unwrap().len(), 1);
            assert_eq!(budget.charges.lock().unwrap()[0].amount_usd, known_charge());
        }
    });
}

#[test]
fn contributed_catalog_revalidates_preparation_and_cannot_override_a_request_settings_branch() {
    use gateway_core::{lifecycle::CancellationToken, routing::ContributedModelCatalog};
    let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
    let admissions = Arc::new(Admissions::default());
    let store = Arc::new(TrackingExecutionStore::default());
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        store.clone(),
        ProviderRegistry::new([Arc::new(CatalogChargedProvider) as Arc<dyn Provider>]).unwrap(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    );
    let stop = CancellationToken::new();
    let catalog = ContributedModelCatalog::new(
        vec![gateway_core::routing::ContributedModelAlias {
            owner: "catalog-fixture".into(),
            id: PublicModelId::new("plugin-alias").unwrap(),
            provider: ProviderKind::new("openai").unwrap(),
            target: UpstreamModelId::new("gpt-start").unwrap(),
        }],
        stop.clone(),
    );
    let client = service
        .authenticate("sk_start_test")
        .unwrap()
        .with_model_catalog(catalog.clone())
        .unwrap();
    snapshots.publish(start_snapshot().with_model_mappings(BTreeMap::from([(
        "plugin-alias".into(),
        "gpt-start".into(),
    )])));
    assert!(service.prepare_execution(client.clone()).is_err());
    snapshots.publish(start_snapshot());
    let prepared = service
        .prepare_execution(service.authenticate("sk_start_test").unwrap())
        .unwrap();
    let mut values = prepared.settings().execution_values().unwrap();
    values.runtime = values.runtime.with_model_mappings(BTreeMap::from([(
        "plugin-alias".into(),
        "gpt-start".into(),
    )]));
    let settings = prepared.settings().replace_execution(&values, "rewrite");
    assert!(settings.is_ok());
    let prepared = prepared.with_settings(settings.unwrap()).unwrap();
    assert!(prepared.with_model_catalog(catalog).is_err());
    stop.cancel();
    assert!(service.prepare_execution(client).is_err());
    assert!(store.requested_models().is_empty());
}

mod upstream_adapter {
    use super::*;
    use gateway_core::engine::upstream_adapter::{UpstreamAdapter, UpstreamAdapterPlan};
    use gateway_core::lifecycle::CancellationToken;

    #[derive(Debug)]
    struct Plan {
        cancellation: CancellationToken,
        observed: Mutex<Vec<(String, String, String)>>,
    }
    impl UpstreamAdapterPlan for Plan {
        fn cancellation(&self) -> CancellationToken {
            self.cancellation.clone()
        }
        fn select(
            &self,
            context: &AttemptContext,
            provider: &ProviderKind,
            model: &UpstreamModelId,
        ) -> Result<Option<Arc<dyn UpstreamAdapter>>, ProviderError> {
            self.observed.lock().unwrap().push((
                context.request_id().as_str().to_owned(),
                provider.as_str().to_owned(),
                model.as_str().to_owned(),
            ));
            assert_eq!(context.client_api_key_ref().as_str(), "key_start_test");
            assert_eq!(context.requested_model().unwrap().as_str(), "gpt-start");
            assert_eq!(
                context.client_transport(),
                Some(ClientTransport::HttpSse.as_str())
            );
            assert!(context.nested_context().is_some());
            Ok(None)
        }
    }
    struct DelegatingProvider;
    #[async_trait]
    impl Provider for DelegatingProvider {
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
        async fn execute(
            &self,
            request: ProviderRequest,
            context: AttemptContext,
        ) -> Result<ProviderStream, ProviderError> {
            assert!(
                context
                    .upstream_adapter(
                        request.candidate().provider(),
                        request.candidate().upstream_model().unwrap()
                    )?
                    .is_none()
            );
            ChargedProvider { fail: false }
                .execute(request, context)
                .await
        }
    }
    #[test]
    fn upstream_adapter_plan_reaches_provider_and_preserves_root_settlement() {
        block_on(async {
            for cancel in [false, true] {
                let admissions = Arc::new(Admissions::default());
                let budget = Arc::new(Budget {
                    active: admissions.active.clone(),
                    ..Default::default()
                });
                let store = Arc::new(TrackingExecutionStore::default());
                let service = DefaultExecutionService::new(
                    RuntimeSnapshotHandle::new(start_snapshot()),
                    store.clone(),
                    ProviderRegistry::new([Arc::new(DelegatingProvider) as Arc<dyn Provider>])
                        .unwrap(),
                    admissions.clone(),
                    Arc::new(UnusedContinuation),
                    Arc::new(RecordingClientApiKeyUsage::default()),
                )
                .with_budget(budget.clone());
                let plan = Arc::new(Plan {
                    cancellation: CancellationToken::new(),
                    observed: Mutex::new(vec![]),
                });
                let request = request(&service, ClientTransport::HttpSse);
                let prepared = service
                    .prepare_execution(request.client)
                    .unwrap()
                    .with_upstream_adapters(plan.clone());
                let mut started = service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: request.public_model,
                            operation: request.operation,
                            metadata: request.metadata,
                        },
                    )
                    .await
                    .unwrap();
                consume_charged_prefix(started.session.as_mut()).await;
                assert_eq!(
                    *plan.observed.lock().unwrap(),
                    vec![(
                        started.request_id.as_str().to_owned(),
                        "openai".into(),
                        "gpt-start".into()
                    )]
                );
                if cancel {
                    plan.cancellation.cancel();
                }
                while started
                    .session
                    .next_event()
                    .await
                    .is_ok_and(|event| event.is_some())
                {}
                started.session.detach_finalize().await;
                assert_cleanup_completed(&admissions, &budget, &started.request_id);
                assert_eq!(store.attempts.lock().unwrap().len(), 1);
                assert_eq!(store.finalizations.lock().unwrap().len(), 1);
            }
        });
    }
}
