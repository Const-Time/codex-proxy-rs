// Real HTTP routing, PluginManager publication, Core admission and settlement.
use super::*;
use crate::openai::{
    EmptyWorkerHealth, IgnoredClientApiKeyUsage, TestLifecycle,
    snapshot_with_capabilities_and_accounts,
};
use async_trait::async_trait;
use axum::{body::Body, http::Request};
use futures::StreamExt;
use gateway_admin::{
    model::{AdminError, MutationContext, plugins::*},
    plugins::PluginManager,
    ports::plugins::*,
};
use gateway_core::{
    account::ProviderAccountId,
    engine::{admission::*, budget::*, continuation::*, execution::*, provider::*, *},
    error::{GatewayError, ProviderError, StoreError},
    event::{GatewayEvent, ResponseMeta},
    metering::{Decimal, ProviderReportedCost},
    operation::Operation,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, ProviderModelCapabilities},
    runtime::RuntimeSnapshotHandle,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use std::{sync::atomic::AtomicUsize, time::SystemTime};
use tower::ServiceExt;

type AdminResult<T> = Result<T, AdminError>;
struct Store {
    records: Vec<PluginRecord>,
}
fn artifact() -> PluginArtifact {
    PluginArtifact {
        info: PluginArtifactInfo {
            sha256: "a".repeat(64),
            plugin_id: "fixture".into(),
            version: "1".into(),
            display_name: "fixture".into(),
            manifest: json!({}),
            unsupported_capabilities: vec![],
            version_warning: None,
        },
        archive: Arc::from([]),
    }
}
#[async_trait]
impl PluginStore for Store {
    async fn list(&self) -> AdminResult<Vec<PluginRecord>> {
        Ok(self.records.clone())
    }
    async fn artifact(&self, _: &str) -> AdminResult<PluginArtifact> {
        Ok(artifact())
    }
    async fn artifacts(&self) -> AdminResult<Vec<PluginArtifactInfo>> {
        unreachable!()
    }
    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn artifact_info(&self, _: &str) -> AdminResult<PluginArtifactInfo> {
        unreachable!()
    }
    async fn put_artifact(&self, _: PluginArtifact, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn save(&self, _: &PluginRecord, _: u64, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: u64, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn state_get(
        &self,
        _: &PluginStateScope,
        _: &str,
    ) -> AdminResult<Option<PluginStateRecord>> {
        unreachable!()
    }
    async fn state_put(
        &self,
        _: &PluginStateScope,
        _: &str,
        _: Option<u64>,
        _: Option<serde_json::Value>,
    ) -> AdminResult<u64> {
        unreachable!()
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Mode {
    HttpUpgrade,
    HttpUpgradeBadResponse,
    HttpUpgradeDiscard,
    HttpAuthError,
    Rewrite,
    InvalidModel,
    Transport,
    BadResponse,
    Head,
    ShortCircuit,
    ShortCircuitRaw,
    ResponseRewrite,
    ExpandInvalid,
    MessageRewrite,
    MessageIdentity,
    WaitBeforeNext,
    WaitAfterNext,
    ChangeContinuation,
    HandleTools,
    RequireReasoning,
    FalseHandleTools,
    Settings,
    ZeroTimeout,
}
pub(crate) struct Plugin {
    pub(crate) access: PluginAccess,
    pub(crate) mode: Mode,
    pub(crate) seen: Mutex<Vec<MiddlewareContext>>,
    pub(crate) headers: Mutex<Vec<String>>,
    pub(crate) routing_calls: AtomicUsize,
    pub(crate) scheduling_calls: AtomicUsize,
    pub(crate) retry_calls: AtomicUsize,
    pub(crate) http_seen: Mutex<Vec<(String, gateway_core::engine::middleware::http::Context)>>,
}
struct Runtime(Arc<Plugin>);
#[async_trait]
impl PluginRuntime for Runtime {
    fn describe(&self, info: PluginArtifactInfo) -> AdminResult<PluginArtifactInfo> {
        Ok(info)
    }
    async fn inspect(&self, _: Arc<[u8]>) -> AdminResult<PluginArtifact> {
        unreachable!()
    }
    async fn validate(
        &self,
        _: &PluginRecord,
        _: &PluginArtifact,
        _: Option<&PluginArtifact>,
    ) -> AdminResult<()> {
        Ok(())
    }
    async fn prepare(
        &self,
        _: &PluginRecord,
        _: PluginArtifact,
    ) -> AdminResult<Arc<dyn PluginProcess>> {
        Ok(self.0.clone())
    }
}
#[async_trait]
impl PluginProcess for Plugin {
    async fn http_middleware(
        &self,
        context: gateway_core::engine::middleware::http::Context,
        request: gateway_core::engine::middleware::http::Request,
        next: gateway_core::engine::middleware::http::Next,
    ) -> Result<gateway_core::engine::middleware::http::Response, MiddlewareError> {
        crate::middleware::fixture::handle(self, context, request, next).await
    }
    async fn retry_decision(
        &self,
        input: gateway_core::engine::retry_policy::RetryInput,
        timeout: std::time::Duration,
    ) -> Result<
        gateway_core::engine::retry_policy::RetryDecision,
        gateway_core::engine::retry_policy::RetryPolicyFault,
    > {
        assert!(input.retry_allowed());
        assert!(timeout <= std::time::Duration::from_secs(2));
        self.retry_calls.fetch_add(1, Ordering::SeqCst);
        Ok(gateway_core::engine::retry_policy::RetryDecision::Delegate)
    }
    async fn schedule_account(
        &self,
        input: gateway_core::engine::account_scheduling::AccountScheduleInput,
    ) -> Result<
        gateway_core::engine::account_scheduling::AccountScheduleDecision,
        gateway_core::engine::account_scheduling::AccountSchedulingFault,
    > {
        self.scheduling_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(input.candidates().len(), 1);
        assert_eq!(input.candidates()[0].account_id.as_str(), "acct_api_test");
        Ok(
            gateway_core::engine::account_scheduling::AccountScheduleDecision::Pick(
                input.candidates()[0].account_id.clone(),
            ),
        )
    }
    fn access(&self) -> PluginAccess {
        self.access.clone()
    }
    fn status(&self) -> &'static str {
        "ready"
    }
    fn pages(&self) -> Vec<PluginPage> {
        vec![]
    }
    fn resources(&self) -> Vec<PluginResource> {
        vec![]
    }
    fn activate(&self) {}
    fn revoke(&self) {
        self.access.revoke();
    }
    async fn stop(&self) {}
    async fn route_model(
        &self,
        input: gateway_core::engine::model_routing::ModelRouteInput,
    ) -> Result<
        gateway_core::engine::model_routing::ModelRouteDecision,
        gateway_core::engine::model_routing::ModelRoutingFault,
    > {
        use gateway_core::{
            engine::model_routing::ModelRouteDecision, identity::ProviderKind,
            routing::PublicModelId,
        };
        self.routing_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(input.requested_model().as_str(), "model-a");
        assert_eq!(input.client_key_id().as_str(), "key_api_test");
        Ok(ModelRouteDecision::Route {
            provider: Some(ProviderKind::new("openai").unwrap()),
            model: Some(PublicModelId::new("model-b").unwrap()),
        })
    }
    async fn resource(&self, _: &str) -> AdminResult<PluginHttpResponse> {
        unreachable!()
    }
    async fn handle(&self, _: PluginHttpRequest) -> AdminResult<PluginHttpResponse> {
        unreachable!()
    }
    async fn middleware(
        &self,
        context: MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        let websocket = context.transport == ClientTransport::WebSocket;
        let attempt = context.mount == MiddlewareMount::Attempt;
        self.seen.lock().unwrap().push(context);
        *self.headers.lock().unwrap() = request.headers().iter().map(|h| h.name().into()).collect();
        if matches!(self.mode, Mode::WaitBeforeNext) {
            return futures::future::pending().await;
        }
        let request = if !attempt && matches!(self.mode, Mode::Settings | Mode::ZeroTimeout) {
            let settings = request.settings().unwrap();
            let mut values = settings.execution_values().unwrap();
            values.fast_mode = gateway_core::account::FastMode::Enabled;
            let mut runtime = serde_json::to_value(&values.runtime).unwrap();
            runtime["request_interval_ms"] = json!(137);
            values.runtime = serde_json::from_value(runtime).unwrap();
            values.timeout_ms = Some(if matches!(self.mode, Mode::ZeroTimeout) {
                0
            } else {
                60_000
            });
            let changed = settings.replace_execution(&values, "fixture").unwrap();
            request.with_settings(changed)
        } else {
            request
        };
        if attempt && matches!(self.mode, Mode::Settings) {
            let settings = request.settings().unwrap();
            assert_eq!(
                settings.execution_values().unwrap().fast_mode,
                gateway_core::account::FastMode::Enabled
            );
            assert_eq!(
                settings.inspect()["execution"]["fast_mode"]["instance_id"],
                "fixture"
            );
        }
        let original = request.clone();
        let (_, mut headers, body) = request.into_parts();
        let mut body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if !attempt || matches!(self.mode, Mode::InvalidModel) {
            body["model"] = json!(if matches!(self.mode, Mode::InvalidModel) {
                "forbidden-model"
            } else {
                "model-b"
            });
        }
        if matches!(self.mode, Mode::ShortCircuit | Mode::ShortCircuitRaw) {
            return Ok(MiddlewareResponse::new(
                "openai".into(),
                403,
                vec![],
                Box::new(attempt::Synthetic(Some(MiddlewareFrame::new(
                    Bytes::from_static(if matches!(self.mode, Mode::ShortCircuitRaw) {
                        b"denied"
                    } else {
                        b"{\"error\":{\"message\":\"blocked\"}}"
                    }),
                    MiddlewareFraming::JsonDocument,
                    true,
                )))),
            ));
        }
        if !attempt
            && matches!(self.mode, Mode::ChangeContinuation)
            && body.get("previous_response_id").is_some()
        {
            body["previous_response_id"] = json!("resp_other");
        }
        body["input"] = json!("changed input");
        if matches!(self.mode, Mode::Transport) {
            body["stream"] = json!(!websocket);
        }
        for (name, value) in [
            ("authorization", "Bearer forged"),
            ("connection", "x-plugin-hop"),
            ("x-plugin-hop", "hidden"),
            ("chatgpt-account-id", "forged-account"),
        ] {
            headers.push(MiddlewareHeader::new(
                name,
                Bytes::from_static(value.as_bytes()),
            ));
        }
        let declaration = if !attempt {
            match self.mode {
                Mode::HandleTools => {
                    body.as_object_mut().unwrap().remove("tools");
                    body.as_object_mut().unwrap().remove("reasoning");
                    Some(MiddlewareCapabilityDeclaration {
                        handled: [gateway_core::operation::Feature::Tools].into(),
                        required: Default::default(),
                    })
                }
                Mode::RequireReasoning => Some(MiddlewareCapabilityDeclaration {
                    handled: Default::default(),
                    required: [gateway_core::operation::Feature::Reasoning].into(),
                }),
                Mode::FalseHandleTools => Some(MiddlewareCapabilityDeclaration {
                    handled: [gateway_core::operation::Feature::Tools].into(),
                    required: Default::default(),
                }),
                _ => None,
            }
        } else {
            None
        };
        let request = original.replace_parts(
            "openai".into(),
            headers,
            Bytes::from(body.to_string()),
            declaration,
        )?;
        let response = next.run(request).await?;
        if matches!(self.mode, Mode::WaitAfterNext) {
            let _hold_response = response;
            return futures::future::pending().await;
        }
        if matches!(self.mode, Mode::Head) {
            let (protocol, _, mut headers, body, envelope) = response.into_parts();
            headers.push(MiddlewareHeader::new(
                "x-plugin-result",
                Bytes::from_static(b"changed"),
            ));
            headers.push(MiddlewareHeader::new(
                "set-cookie",
                Bytes::from_static(b"must-not-forward"),
            ));
            let mut response = MiddlewareResponse::new(protocol, 201, headers, body);
            if let Some(envelope) = envelope {
                response = response.with_envelope(envelope);
            }
            return Ok(response);
        }
        Ok(rewrite(
            response,
            if matches!(self.mode, Mode::BadResponse) {
                Rewrite::Invalid
            } else if matches!(self.mode, Mode::ResponseRewrite) {
                Rewrite::Json
            } else if matches!(self.mode, Mode::MessageRewrite) {
                Rewrite::MessageJson
            } else if matches!(self.mode, Mode::MessageIdentity) {
                Rewrite::MessageIdentity
            } else if matches!(self.mode, Mode::ExpandInvalid) {
                Rewrite::ExpandInvalidTerminal
            } else {
                Rewrite::Pass
            },
        ))
    }
}
#[derive(Default)]
pub(crate) struct Effects {
    pub(crate) provider_failure: Mutex<Option<fn() -> ProviderError>>,
    retry: AtomicBool,
    pub(crate) active: AtomicUsize,
    pub(crate) calls: AtomicUsize,
    pub(crate) commits: AtomicUsize,
    pub(crate) completion: Mutex<Option<Arc<tokio::sync::Notify>>>,
    pub(crate) charges: Mutex<Vec<ClientBudgetCharge>>,
    pub(crate) finalizations: Mutex<Vec<ModelRequestFinalization>>,
    pub(crate) requests: Mutex<Vec<(String, String, String, gateway_core::lifecycle::Deadline)>>,
    pub(crate) raw_requests: Mutex<Vec<Operation>>,
    pub(crate) revoke_after_start: Mutex<Option<PluginAccess>>,
    pub(crate) operations: Mutex<Vec<Operation>>,
    pub(crate) settings: Mutex<Vec<gateway_core::routing::RequestSettings>>,
    pub(crate) interrupt: AtomicBool,
    pub(crate) state: Mutex<Option<gateway_core::operation::ProviderSessionState>>,
    pub(crate) raw_response: Mutex<Option<Bytes>>,
}
impl ClientAdmissionPort for Effects {
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
        Box::pin(async {
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { unreachable!() })
    }
}
impl ClientBudgetPort for Effects {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<AccountGroupId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            Ok(ClientBudgetScope {
                user_id: "original-user".into(),
                group_id: AccountGroupId::new("grp_00000000000000000000000000000001").unwrap(),
            })
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            self.charges.lock().unwrap().push(charge);
            Ok(())
        })
    }
}
#[async_trait]
impl ExecutionStore for Effects {
    fn maintain_request(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: Option<std::time::SystemTime>,
        _: std::time::Duration,
    ) -> Box<dyn gateway_core::engine::ExecutionRecoveryLeaseGuard> {
        Box::new(TestExecutionRecoveryLease)
    }

    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
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
        value: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.finalizations.lock().unwrap().push(value);
        Ok(())
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        unreachable!()
    }
}
struct Continuations;
impl NativeContinuationPort for Continuations {
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
#[async_trait]
impl Provider for Effects {
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(settings) = context.settings() {
            self.settings.lock().unwrap().push(settings.clone());
        }
        if let Some(failure) = *self.provider_failure.lock().unwrap() {
            return Err(failure());
        }
        self.operations
            .lock()
            .unwrap()
            .push(request.operation().clone());
        let Operation::Generate(generate) = request.operation() else {
            return self.raw_execute(request, context).await;
        };
        let payload = generate.protocol_payload();
        let opaque = payload
            .context()
            .get("opaque_request_headers")
            .unwrap()
            .to_string();
        assert!(
            !opaque.contains("authorization")
                && !opaque.contains("x-plugin-hop")
                && !opaque.contains("chatgpt-account-id"),
            "{opaque}"
        );
        self.requests.lock().unwrap().push((
            context.client_api_key_ref().as_str().into(),
            request
                .candidate()
                .upstream_model()
                .unwrap()
                .as_str()
                .into(),
            payload.body()["input"].as_str().unwrap().into(),
            context.deadline(),
        ));
        use gateway_core::account::*;
        let candidate = AccountCandidate {
            account: ProviderAccount::new(
                ProviderAccountId::new("acct_api_test").unwrap(),
                request.candidate().provider().clone(),
                "test".into(),
                Some("test-upstream".into()),
                "oauth".into(),
                CredentialRevision::new(1).unwrap(),
                None,
            )
            .with_account_facts(
                true,
                CredentialState::Ready,
                QuotaState::unknown(),
                None,
                None,
            ),
            signals: AccountRuntimeSignals {
                in_flight: 0,
                last_started_at: None,
                quota_reset_at: None,
                quota_remaining_rank: None,
                rate_limited_until: None,
                failure_rate_basis_points: None,
                first_output_latency_ms: None,
            },
        };
        let mut candidates = vec![candidate.clone()];
        if self.retry.load(Ordering::SeqCst) {
            candidates.push(AccountCandidate {
                account: ProviderAccount::new(
                    ProviderAccountId::new("acct_api_retry").unwrap(),
                    request.candidate().provider().clone(),
                    "retry".into(),
                    Some("test-upstream".into()),
                    "oauth".into(),
                    CredentialRevision::new(1).unwrap(),
                    None,
                )
                .with_account_facts(
                    true,
                    CredentialState::Ready,
                    QuotaState::unknown(),
                    None,
                    None,
                ),
                signals: candidate.signals,
            });
        }
        let selection = AccountSelectionContext {
            policy: context.account_selection_policy(),
            now: SystemTime::now(),
            excluded_accounts: context.excluded_accounts().clone(),
            preferred_account: self
                .retry
                .load(Ordering::SeqCst)
                .then(|| ProviderAccountId::new("acct_api_test").unwrap()),
            preferred_account_overrides_weight: true,
            round_robin_cursor: 0,
            eligibility: AccountEligibilityPolicy::Enforce,
            account_scope: context.account_scope().cloned(),
        };
        let account = context
            .select_account(
                request.candidate().provider(),
                request.candidate().upstream_model().map(|m| m.as_str()),
                &candidates,
                &selection,
                context.required_account(),
            )
            .await
            .map_err(|e| e.into_provider_error())?
            .ok_or_else(|| {
                ProviderError::new(
                    gateway_core::error::ProviderErrorKind::NoEligibleAccount,
                    UpstreamSendState::NotSent,
                )
            })?
            .candidate()
            .account
            .id()
            .clone();
        let meta = ProviderCallMetadata::new(
            request.candidate().provider().clone(),
            request.candidate().upstream_model().unwrap().clone(),
            account,
            UpstreamTransport::new("http").unwrap(),
        );
        if self.retry.load(Ordering::SeqCst)
            && meta.provider_account_id().as_str() == "acct_api_test"
        {
            return Ok(ProviderStream::new(
                meta,
                futures::stream::iter([Err(ProviderError::new(
                    gateway_core::error::ProviderErrorKind::RateLimited,
                    UpstreamSendState::Sent,
                )
                .with_status(429)
                .with_replay_safe())]),
                (),
            ));
        }
        let created = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Started(ResponseMeta::new(
                "resp_original",
                "model-b",
            ))],
            event("response.created").into_parts().1.unwrap(),
        );
        let mut completed = ProviderEvent::canonical_with_wire(
            vec![
                GatewayEvent::ProviderCost(ProviderReportedCost::from_usd_ticks(25).unwrap()),
                GatewayEvent::Completed(ResponseMeta::new("resp_original", "model-b")),
            ],
            event("response.completed").into_parts().1.unwrap(),
        );
        if let Some(state) = self.state.lock().unwrap().clone() {
            completed.attach_session_update(state);
        }
        let active = if self.interrupt.load(Ordering::SeqCst) {
            Some(
                context
                    .response_control()
                    .unwrap()
                    .activate("resp_original".into())
                    .unwrap(),
            )
        } else {
            None
        };
        let wait = self.completion.lock().unwrap().clone();
        let stream =
            futures::stream::iter([Ok(created)]).chain(futures::stream::once(async move {
                if let Some(active) = active {
                    active.requested().await;
                }
                if let Some(wait) = wait {
                    wait.notified().await;
                }
                Ok(completed)
            }));
        Ok(ProviderStream::new(meta, stream, ()))
    }
}
pub(crate) struct Fixture {
    pub(crate) app: axum::Router,
    pub(crate) manager: Arc<PluginManager>,
    pub(crate) plugin: Arc<Plugin>,
    pub(crate) effects: Arc<Effects>,
    pub(crate) lifecycle: Arc<TestLifecycle>,
}
impl Fixture {
    pub(crate) async fn new(mode: Mode, installed: bool) -> Self {
        Self::with_mounts(
            mode,
            if installed {
                vec![PluginMiddlewareMount::Request]
            } else {
                vec![]
            },
        )
        .await
    }
    pub(crate) async fn with_mounts(mode: Mode, mounts: Vec<PluginMiddlewareMount>) -> Self {
        Self::with_features(mode, mounts, &[]).await
    }
    pub(crate) async fn with_features(
        mode: Mode,
        mounts: Vec<PluginMiddlewareMount>,
        features: &[gateway_core::operation::Feature],
    ) -> Self {
        Self::with_policy(mode, mounts, features, false, false, false).await
    }
    pub(crate) async fn with_router() -> Self {
        Self::with_policy(Mode::Rewrite, vec![], &[], true, false, false).await
    }
    pub(crate) async fn with_scheduler() -> Self {
        Self::with_policy(Mode::Rewrite, vec![], &[], false, true, false).await
    }
    pub(crate) async fn with_retry() -> Self {
        Self::with_policy(Mode::Rewrite, vec![], &[], false, false, true).await
    }
    async fn with_policy(
        mode: Mode,
        mounts: Vec<PluginMiddlewareMount>,
        features: &[gateway_core::operation::Feature],
        routing: bool,
        scheduling: bool,
        retry: bool,
    ) -> Self {
        let plugin = Arc::new(Plugin {
            access: PluginAccess::default(),
            mode,
            seen: Mutex::new(vec![]),
            headers: Mutex::new(vec![]),
            routing_calls: AtomicUsize::new(0),
            scheduling_calls: AtomicUsize::new(0),
            retry_calls: AtomicUsize::new(0),
            http_seen: Mutex::new(vec![]),
        });
        let records = if !mounts.is_empty() || routing || scheduling || retry {
            vec![PluginRecord {
                retry_policy: retry.then_some(PluginRetryPolicyBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                }),
                account_scheduler: scheduling.then_some(PluginAccountSchedulerBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                    failure_policy: PluginPolicyFailurePolicy::Reject,
                }),
                model_router: routing.then_some(PluginModelRouterBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                    failure_policy: PluginPolicyFailurePolicy::Reject,
                }),
                id: "fixture".into(),
                revision: 1,
                name: "fixture".into(),
                artifact_sha256: "a".repeat(64),
                enabled: true,
                configuration: json!({}),
                secrets: Default::default(),
                frontend_authentication: None,
                middleware: (!mounts.is_empty()).then_some(PluginMiddlewareBinding {
                    order: 0,
                    mounts,
                    scope: PluginExecutionScope::All {},
                }),
            }]
        } else {
            vec![]
        };
        let manager = Arc::new(PluginManager::new(
            Arc::new(Store { records }),
            Arc::new(Runtime(plugin.clone())),
        ));
        manager.restore().await.unwrap();
        let effects = Arc::new(Effects::default());
        effects.retry.store(retry, Ordering::SeqCst);
        let execution = Arc::new(
            DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(snapshot_with_capabilities_and_accounts(
                    "sk_middleware",
                    "openai",
                    features
                        .iter()
                        .copied()
                        .chain([gateway_core::operation::Feature::NativeContinuation])
                        .fold(
                            gateway_core::routing::ModelCapabilities::new(
                                [gateway_core::operation::OperationKind::Generate].into(),
                                Some(16000),
                            ),
                            |caps, feature| {
                                caps.with_feature(
                                    feature,
                                    gateway_core::routing::SupportLevel::Native,
                                )
                            },
                        ),
                    if retry {
                        &["acct_api_test", "acct_api_retry"]
                    } else {
                        &["acct_api_test"]
                    },
                )),
                effects.clone(),
                ProviderRegistry::new([effects.clone() as Arc<dyn Provider>]).unwrap(),
                effects.clone(),
                Arc::new(Continuations),
                Arc::new(IgnoredClientApiKeyUsage),
            )
            .with_budget(effects.clone()),
        );
        let admin = crate::admin::AdminTestFixture::new().await;
        let lifecycle = Arc::new(TestLifecycle::default());
        let app = gateway_api::initialize(
            gateway_api::ApiConfig {
                asset_directory: std::env::temp_dir(),
                cors_allowed_origins: vec![],
                request_timeout_seconds: None,
                request_id_header: "x-request-id".into(),
            },
            Arc::new(PostStartExecution {
                inner: execution,
                effects: effects.clone(),
            }),
            admin.services.with_plugins(manager.clone()),
            vec![],
            Arc::new(EmptyWorkerHealth),
            lifecycle.clone(),
        )
        .unwrap()
        .router();
        Self {
            app,
            manager,
            plugin,
            effects,
            lifecycle,
        }
    }
    pub(crate) async fn request(&self, stream: bool) -> axum::response::Response {
        self.app
            .clone()
            .oneshot(
                Request::post("/v1/responses")
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .header("connection", "x-client-hop")
                    .header("x-client-hop", "hidden")
                    .header("x-openai-actor-authorization", "managed")
                    .header("x-custom", "preserved")
                    .body(Body::from(
                        json!({"model":"model-a","input":"original input","stream":stream})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
    pub(crate) async fn settled(&self) {
        for _ in 0..100 {
            if self.effects.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(self.effects.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.effects.finalizations.lock().unwrap().len(), 1);
        let charges = self.effects.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].key_id.as_str(), "key_api_test");
        assert_eq!(charges[0].scope.as_ref().unwrap().user_id, "original-user");
        assert!(charges[0].amount_usd > Decimal::ZERO);
    }
}

impl Effects {
    async fn raw_execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        assert!(request.candidate().upstream_model().is_none());
        assert_eq!(context.client_api_key_ref().as_str(), "key_api_test");
        self.raw_requests
            .lock()
            .unwrap()
            .push(request.operation().clone());
        let meta = ProviderCallMetadata::for_provider_endpoint(
            request.candidate().provider().clone(),
            ProviderAccountId::new("acct_api_test").unwrap(),
            UpstreamTransport::new("http").unwrap(),
        );
        let observation = ProviderEvent::observation(
            gateway_core::event::ProviderResponseObservation::new(
                UpstreamTransport::new("http").unwrap(),
            )
            .with_status_code(202),
        );
        let cost = ProviderEvent::canonical(GatewayEvent::ProviderCost(
            ProviderReportedCost::from_usd_ticks(25).unwrap(),
        ));
        let wire = ProviderEvent::wire(
            ProtocolWireEvent::raw_json(
                "openai",
                self.raw_response
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| Bytes::from_static(b"{ \"data\": [1], \"data\": [2] }\n")),
            )
            .unwrap(),
        );
        let wait = self.completion.lock().unwrap().clone();
        let meta_response = ResponseMeta::for_provider_endpoint(context.request_id().as_str());
        let started = ProviderEvent::canonical(GatewayEvent::Started(meta_response.clone()));
        let completed = ProviderEvent::canonical(GatewayEvent::Completed(meta_response));
        let stream = futures::stream::iter([Ok(observation), Ok(started), Ok(cost)])
            .chain(futures::stream::once(async move {
                if let Some(wait) = wait {
                    wait.notified().await;
                }
                Ok(wire)
            }))
            .chain(futures::stream::iter([Ok(completed)]));
        Ok(ProviderStream::new(meta, stream, ()))
    }
}
impl Fixture {
    pub(crate) async fn raw_request(&self, endpoint: &str) -> axum::response::Response {
        self.app
            .clone()
            .oneshot(
                Request::post(endpoint)
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .header("x-codex-image-turn-id", "image-turn-original")
                    .header("session_id", "session-original")
                    .header("x-custom", "original")
                    .body(Body::from(
                        "{ \"prompt\": \"original\", \"unknown\": [1, 2] } ",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
}

// Place revocation after Core has returned its live session, before API forwarding.
struct PostStartExecution {
    inner: Arc<DefaultExecutionService>,
    effects: Arc<Effects>,
}
impl ExecutionService for PostStartExecution {
    fn request_settings(&self) -> Option<gateway_core::routing::RequestSettings> {
        self.inner.request_settings()
    }
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        self.inner.authenticate(plaintext)
    }
    fn client_access(
        &self,
        client: &AuthenticatedClient,
    ) -> Option<gateway_core::runtime::ClientAccessGuard> {
        self.inner.client_access(client)
    }
    fn public_models(
        &self,
        client: &AuthenticatedClient,
    ) -> Vec<gateway_core::routing::PublicModelId> {
        self.inner.public_models(client)
    }
    fn contains_public_model(
        &self,
        client: &AuthenticatedClient,
        model: &gateway_core::routing::PublicModelId,
    ) -> bool {
        self.inner.contains_public_model(client, model)
    }
    fn prepare_execution(
        &self,
        client: AuthenticatedClient,
    ) -> Result<PreparedRootExecution, GatewayError> {
        self.inner.prepare_execution(client)
    }
    fn start_prepared(
        &self,
        prepared: PreparedRootExecution,
        request: PreparedExecutionRequest,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            let started = self.inner.start_prepared(prepared, request).await?;
            if let Some(access) = self.effects.revoke_after_start.lock().unwrap().take() {
                access.revoke();
            }
            Ok(started)
        })
    }
    fn start_prepared_provider_endpoint(
        &self,
        prepared: PreparedRootExecution,
        provider: gateway_core::routing::ProviderKind,
        operation: Operation,
        metadata: ExecutionRequestMetadata,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.inner
            .start_prepared_provider_endpoint(prepared, provider, operation, metadata)
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.inner.start(request)
    }
    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.inner.start_provider_endpoint(request)
    }
}

struct TestAdmissionLease;
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for TestAdmissionLease {}

struct TestExecutionRecoveryLease;
impl gateway_core::engine::ExecutionRecoveryLeaseGuard for TestExecutionRecoveryLease {}
