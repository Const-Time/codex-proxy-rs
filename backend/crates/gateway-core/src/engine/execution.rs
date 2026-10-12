//! 数据面执行用例：认证、准入、路由、continuation、circuit 与会话生命周期。

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures::{FutureExt, future::BoxFuture, pin_mut, select_biased};
use futures_timer::Delay;
use uuid::Uuid;

use crate::engine::admission::{
    ClientAdmissionDecision, ClientAdmissionPort, ClientAdmissionRejection, ClientAdmissionRequest,
};
use crate::engine::budget::{ClientBudgetCharge, ClientBudgetPort};
use crate::engine::continuation::{
    ContinuationBinding, NativeContinuationPin, NativeContinuationPort,
    NativeContinuationStoreErrorKind, PreviousResponseId,
};
use crate::engine::coordinator::ResponseExecutionSession;
use crate::engine::probe::{
    AccountProbe, AccountProbeError, AccountProbeErrorSource, AccountProbeRequest,
    AccountProbeResult, AccountProbeUpstreamResponse,
};
use crate::engine::provider::ProviderRegistry;
use crate::engine::{
    AttemptCoordinator, AttemptRecord, CoordinatedEvent, EngineError, ExecutionStore,
    GatewayEngine, IntermediateFailure, ModelRequestFinalization, ModelRequestId, NewModelRequest,
    ProbeFailure, ProviderAccountId, RecoveryReport, UpstreamSendState,
};
use crate::error::{GatewayError, GatewayErrorKind, StoreError};
use crate::event::{GatewayEvent, ProviderEvent, ProviderResponseHeader};
use crate::identity::ProviderKind;
use crate::lifecycle::CancellationToken;
use crate::operation::{Operation, ProviderSessionState};
use crate::policy::{ClientApiKeyId, ClientPolicy};
use crate::routing::{
    ProviderCatalogUnavailable, PublicModelDescriptor, PublicModelId, RoutingContext,
    RuntimeSnapshot, UpstreamModelId,
};
use crate::runtime::RuntimeSnapshotHandle;

const MODEL_PROBE_DEADLINE: Duration = Duration::from_secs(10 * 60);
const COORDINATION_TIMEOUT: Duration = Duration::from_millis(100);

fn admission_error(reason: ClientAdmissionRejection, user: bool) -> GatewayError {
    let (code, message) = match (user, reason) {
        (true, ClientAdmissionRejection::ConcurrencyLimited) => (
            "user_concurrency_limited",
            "user concurrent request limit exceeded",
        ),
        (true, ClientAdmissionRejection::RateLimited) => (
            "user_rate_limited",
            "user requests per minute limit exceeded",
        ),
        (false, ClientAdmissionRejection::ConcurrencyLimited) => (
            "client_key_concurrency_limited",
            "client API key concurrent request limit exceeded",
        ),
        (false, ClientAdmissionRejection::RateLimited) => (
            "client_key_rate_limited",
            "client API key requests per minute limit exceeded",
        ),
    };
    GatewayError::new(GatewayErrorKind::RateLimited, message).with_client_code(code)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTransport {
    HttpJson,
    HttpSse,
    WebSocket,
    InternalProbe,
}

impl ClientTransport {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpJson => "http_json",
            Self::HttpSse => "http_sse",
            Self::WebSocket => "websocket",
            Self::InternalProbe => "internal",
        }
    }
}

/// API 解码后交给 Core 的稳定请求元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRequestMetadata {
    pub protocol: String,
    pub endpoint: String,
    pub transport: ClientTransport,
    pub stream: bool,
    pub client_ip: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub previous_response_id: Option<PreviousResponseId>,
}

#[derive(Clone)]
pub struct AuthenticatedClient {
    snapshot: Arc<RuntimeSnapshot>,
    policy: ClientPolicy,
    bound_authorization: Option<crate::runtime::ClientAccessGuard>,
}

impl AuthenticatedClient {
    pub fn bound_access(&self) -> Option<crate::runtime::ClientAccessGuard> {
        self.bound_authorization.clone()
    }
    #[must_use]
    pub const fn snapshot(&self) -> &Arc<RuntimeSnapshot> {
        &self.snapshot
    }

    #[must_use]
    pub const fn policy(&self) -> &ClientPolicy {
        &self.policy
    }
}

impl fmt::Debug for AuthenticatedClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedClient")
            .field("key_id", &self.policy.key_id())
            .field("revision", &self.snapshot.revision())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClientAuthenticationError {
    #[error("client API key is invalid")]
    InvalidKey,
    #[error("runtime snapshot is unavailable")]
    SnapshotUnavailable,
}

/// A single root invocation prepared before middleware runs. This value cannot be
/// cloned or constructed by protocol adapters; consuming it starts the existing
/// execution path without replacing its identity or resetting its deadline.
pub struct PreparedRootExecution {
    model_target: super::nested::ModelExecutionTarget,
    lifetime: Arc<super::nested::ExecutionLifetime>,
    extension_scope: super::nested::ExtensionCallScope,
    parent_deadline: crate::lifecycle::Deadline,
    settings: crate::routing::RequestSettings,
    attempt_middleware: Option<Arc<dyn super::attempt_middleware::AttemptMiddleware>>,
    client: AuthenticatedClient,
    request_id: ModelRequestId,
    started_at: SystemTime,
    deadline_at: crate::lifecycle::Deadline,
    cancellation: CancellationToken,
}

impl PreparedRootExecution {
    pub fn with_model_target(
        mut self,
        mut target: super::nested::ModelExecutionTarget,
    ) -> Result<Self, GatewayError> {
        self.validate()?;
        let scope = self.client.policy.account_scope();
        if target
            .provider
            .as_ref()
            .is_some_and(|provider| !scope.provider_kinds().contains(provider))
            || target.account.as_ref().is_some_and(|account| {
                !scope.allows(account)
                    || target.provider.as_ref().is_some_and(|provider| {
                        scope
                            .directory()
                            .account(account)
                            .is_none_or(|facts| facts.provider_kind() != provider)
                    })
            })
        {
            return Err(GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "requested execution target is outside the Key account scope",
            ));
        }
        if target.account.is_some() && target.account == target.parent_account {
            return Err(GatewayError::new(
                GatewayErrorKind::NoAvailableProvider,
                "nested execution cannot wait on its parent's account lease",
            ));
        }
        if target.provider.is_none()
            && let Some(account) = target.account.as_ref()
        {
            // A fixed account also fixes its Provider; a group's other Providers
            // cannot receive this account ID as a fallback target.
            target.provider = scope
                .directory()
                .account(account)
                .map(|facts| facts.provider_kind().clone());
        }
        self.model_target = target;
        Ok(self)
    }
    pub fn nested_context(&self) -> super::nested::NestedExecutionContext {
        super::nested::NestedExecutionContext {
            lifetime: Arc::downgrade(&self.lifetime),
            client: self.client.clone(),
            settings: self.settings.clone(),
            request_id: self.request_id.clone(),
            started_at: self.started_at,
            parent_deadline: self.parent_deadline,
            deadline: self.deadline_at,
            cancellation: CancellationToken::linked([
                self.cancellation.clone(),
                self.lifetime.cancellation(),
            ]),
            scope: self.extension_scope.clone(),
        }
    }
    pub fn settings(&self) -> &crate::routing::RequestSettings {
        &self.settings
    }

    /// Apply a request branch without accepting another request's authorization or snapshot.
    pub fn with_settings(
        mut self,
        settings: crate::routing::RequestSettings,
    ) -> Result<Self, GatewayError> {
        self.validate()?;
        if !settings.has_baseline(&self.client.snapshot)
            || !settings.is_for(self.client.policy.key_id())
        {
            return Err(GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "request settings do not match the prepared scope",
            ));
        }
        let timeout = settings
            .execution_values()
            .ok_or_else(|| {
                GatewayError::new(
                    GatewayErrorKind::InvalidRequest,
                    "execution settings are unavailable",
                )
            })?
            .timeout_ms;
        self.deadline_at = crate::lifecycle::Deadline::from_timeout(
            self.started_at,
            timeout.map(Duration::from_millis),
        )
        .ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "execution timeout is outside the supported range",
            )
        })?;
        if let Some(parent) = self.parent_deadline.at() {
            self.deadline_at = self.deadline_at.cap(parent);
        }
        self.settings = settings;
        self.validate()?;
        Ok(self)
    }

    pub fn with_attempt_middleware(
        mut self,
        middleware: Arc<dyn super::attempt_middleware::AttemptMiddleware>,
    ) -> Self {
        self.attempt_middleware = Some(middleware);
        self
    }

    pub fn client(&self) -> &AuthenticatedClient {
        &self.client
    }
    pub fn request_id(&self) -> &ModelRequestId {
        &self.request_id
    }
    pub fn started_at(&self) -> SystemTime {
        self.started_at
    }
    pub fn deadline_at(&self) -> crate::lifecycle::Deadline {
        self.deadline_at
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Add host-owned revocation without replacing the prepared identity or deadline.
    pub fn with_revocation(mut self, revocation: CancellationToken) -> Self {
        self.cancellation = CancellationToken::linked([self.cancellation, revocation]);
        self
    }

    fn validate(&self) -> Result<(), GatewayError> {
        if self
            .client
            .bound_authorization
            .as_ref()
            .is_none_or(|access| access.cancellation().is_cancelled())
        {
            return Err(GatewayError::new(
                GatewayErrorKind::Unauthorized,
                "client authorization was revoked",
            ));
        }
        if self.cancellation.is_cancelled() {
            return Err(GatewayError::new(
                GatewayErrorKind::Cancelled,
                "request was cancelled",
            ));
        }
        if self.deadline_at.is_elapsed() {
            return Err(GatewayError::new(
                GatewayErrorKind::Timeout,
                "request deadline elapsed",
            ));
        }
        Ok(())
    }
}

/// Middleware may replace request facts, never the prepared authority.
pub struct PreparedExecutionRequest {
    pub public_model: PublicModelId,
    pub operation: Operation,
    pub metadata: ExecutionRequestMetadata,
}

pub struct StartExecution {
    pub client: AuthenticatedClient,
    pub public_model: PublicModelId,
    pub operation: Operation,
    pub metadata: ExecutionRequestMetadata,
}

/// 启动一个由协议 adapter 明确绑定到 Provider 自有端点的请求。
pub struct StartProviderExecution {
    pub client: AuthenticatedClient,
    pub provider: ProviderKind,
    pub operation: Operation,
    pub metadata: ExecutionRequestMetadata,
}

enum ExecutionTarget {
    Model(PublicModelId),
    ProviderEndpoint(ProviderKind),
}

impl ExecutionTarget {
    fn into_public_model(self) -> Option<PublicModelId> {
        match self {
            Self::Model(model) => Some(model),
            Self::ProviderEndpoint(_) => None,
        }
    }
}

struct PendingStartExecution {
    prepared: PreparedRootExecution,
    target: ExecutionTarget,
    operation: Operation,
    metadata: ExecutionRequestMetadata,
}

pub struct StartedExecution {
    pub request_id: ModelRequestId,
    pub created_at: SystemTime,
    pub stream: bool,
    pub session: Box<dyn ExecutionSession>,
}

pub trait ExecutionSession: Send {
    /// Monotonic execution fact; an implementation without evidence must stay conservative.
    fn send_state(&self) -> UpstreamSendState {
        UpstreamSendState::Ambiguous
    }
    /// Optional active-response control, scoped to this execution only.
    fn response_control(&self) -> Option<super::response_control::ResponseControl> {
        None
    }
    fn trace(&self) -> crate::diagnostics::TraceContext {
        crate::diagnostics::TraceContext::default()
    }
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>>;
    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>>;
    fn response_headers(&self) -> &[ProviderResponseHeader];
    fn middleware_response_head(&self) -> Option<&super::attempt_middleware::AttemptResponseHead> {
        None
    }
    fn response_status_code(&self) -> Option<u16> {
        None
    }
    fn commit_downstream(
        &mut self,
        client_status_code: Option<u16>,
    ) -> BoxFuture<'_, Result<(), EngineError>>;
    fn record_client_status(
        &mut self,
        client_status_code: u16,
    ) -> BoxFuture<'_, Result<(), EngineError>>;
    /// 执行已终结且结算、准入释放均已返回，协议层才可以放弃清理责任。
    /// 结算失败仍由 Store 保留费用重试，释放失败仍按租约 TTL 收敛。
    fn is_finalized(&self) -> bool;
    fn cancel(&self);
    /// 将会话交给宿主持续驱动清理；取消请求事件的等待不会丢弃已开始的结算。
    fn detach_finalize(self: Box<Self>) -> BoxFuture<'static, ()>;
}

pub trait ExecutionService: Send + Sync {
    /// Delegated plugin requests require live revocation support. Missing support fails closed at the caller.
    fn client_access(
        &self,
        _client: &AuthenticatedClient,
    ) -> Option<crate::runtime::ClientAccessGuard> {
        None
    }
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError>;
    /// Keep the exact plaintext-authenticated policy while adding host revocation.
    /// Registering access rechecks that policy; never resolve a newer policy by Key ID here.
    fn authenticate_revocable(
        &self,
        plaintext: &str,
        revocation: CancellationToken,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        let mut client = self.authenticate(plaintext)?;
        let bound = self
            .client_access(&client)
            .ok_or(ClientAuthenticationError::SnapshotUnavailable)?
            .with_revocation(revocation);
        if bound.cancellation().is_cancelled() {
            return Err(ClientAuthenticationError::InvalidKey);
        }
        client.bound_authorization = Some(bound);
        Ok(client)
    }
    /// Trusted host entry after an external principal has been mapped to a local Key.
    /// This is not an API for accepting an untrusted client-supplied Key ID.
    fn authenticate_bound_key(
        &self,
        _key: &ClientApiKeyId,
        _revocation: CancellationToken,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        Err(ClientAuthenticationError::InvalidKey)
    }
    fn public_models(&self, client: &AuthenticatedClient) -> Vec<PublicModelId>;
    fn client_model_catalog<'a>(
        &'a self,
        _client: &'a AuthenticatedClient,
        _protocol: &'a str,
        _client_version: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PublicModelDescriptor>, ProviderCatalogUnavailable>> {
        Box::pin(async { Err(ProviderCatalogUnavailable) })
    }
    fn contains_public_model(&self, client: &AuthenticatedClient, model: &PublicModelId) -> bool;
    /// Freeze current authorization, a single request ID, deadline and cancellation
    /// before invoking extensions. Preparation does not acquire quota or admission.
    fn prepare_execution(
        &self,
        _client: AuthenticatedClient,
    ) -> Result<PreparedRootExecution, GatewayError> {
        Err(GatewayError::new(
            GatewayErrorKind::Unsupported,
            "prepared execution is unavailable",
        ))
    }
    /// A standalone plugin call shares its host RPC's limits and cancellation.
    /// It has no model parent; the management call context retains the instance and caller.
    fn prepare_plugin_execution(
        &self,
        client: AuthenticatedClient,
        initiating_plugin_instance_id: String,
        call_scope: super::nested::ExtensionCallScope,
        call_cancellation: CancellationToken,
    ) -> Result<PreparedRootExecution, GatewayError> {
        let mut prepared = self.prepare_execution(client)?;
        prepared.extension_scope = call_scope.fork_call(None, initiating_plugin_instance_id)?;
        prepared.lifetime.bind_scope(&prepared.extension_scope);
        prepared.cancellation =
            CancellationToken::linked([prepared.cancellation, call_cancellation]);
        prepared.validate()?;
        Ok(prepared)
    }
    /// Host-issued parent context preserves ancestry and cancellation even with an explicit other Key.
    fn prepare_nested(
        &self,
        parent: super::nested::NestedExecutionContext,
        initiating_plugin_instance_id: String,
        selected_client: Option<AuthenticatedClient>,
    ) -> Result<PreparedRootExecution, GatewayError> {
        parent.validate()?;
        let client = selected_client.unwrap_or_else(|| parent.client.clone());
        let mut prepared = self.prepare_execution(client)?;
        prepared.extension_scope = parent
            .scope
            .fork(parent.request_id, initiating_plugin_instance_id)?;
        prepared.lifetime.bind_scope(&prepared.extension_scope);
        prepared.parent_deadline = parent.deadline;
        prepared.cancellation =
            CancellationToken::linked([prepared.cancellation, parent.cancellation]);
        let settings = parent
            .settings
            .rebase(prepared.client.snapshot.clone())
            .map_err(|_| {
                GatewayError::new(
                    GatewayErrorKind::InvalidRequest,
                    "nested settings cannot be rebased",
                )
            })?
            .with_execution(&prepared.client.policy, None);
        prepared.with_settings(settings)
    }
    fn start_prepared(
        &self,
        _prepared: PreparedRootExecution,
        _request: PreparedExecutionRequest,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async {
            Err(GatewayError::new(
                GatewayErrorKind::Unsupported,
                "prepared execution is unavailable",
            ))
        })
    }
    fn start_prepared_provider_endpoint(
        &self,
        _prepared: PreparedRootExecution,
        _provider: ProviderKind,
        _operation: Operation,
        _metadata: ExecutionRequestMetadata,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async {
            Err(GatewayError::new(
                GatewayErrorKind::Unsupported,
                "prepared execution is unavailable",
            ))
        })
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>>;
    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>>;
}

/// 成功认证后的 API Key 使用事实接收器。
///
/// 认证仍是同步快照读取；实现必须自行异步、去重地持久化，不得阻塞客户端请求。
pub trait ClientApiKeyUsageSink: Send + Sync {
    fn record_used(&self, key_id: &ClientApiKeyId);
}

pub struct DefaultExecutionService {
    snapshots: RuntimeSnapshotHandle,
    coordinator: Arc<AttemptCoordinator<dyn ExecutionStore>>,
    probe_coordinator: Arc<AttemptCoordinator<dyn ExecutionStore>>,
    /// probe 自身走 transient store，探测失败仍写入持久 store 的 ops_events。
    observations: Arc<dyn ExecutionStore>,
    providers: ProviderRegistry,
    admissions: Arc<dyn ClientAdmissionPort>,
    continuation: Arc<dyn NativeContinuationPort>,
    client_api_key_usage: Arc<dyn ClientApiKeyUsageSink>,
    budget: Option<Arc<dyn ClientBudgetPort>>,
}

impl DefaultExecutionService {
    #[must_use]
    pub fn new(
        snapshots: RuntimeSnapshotHandle,
        execution: Arc<dyn ExecutionStore>,
        providers: ProviderRegistry,
        admissions: Arc<dyn ClientAdmissionPort>,
        continuation: Arc<dyn NativeContinuationPort>,
        client_api_key_usage: Arc<dyn ClientApiKeyUsageSink>,
    ) -> Self {
        let observations = Arc::clone(&execution);
        let engine = GatewayEngine::<dyn ExecutionStore>::new(execution, providers.clone());
        let transient: Arc<dyn ExecutionStore> = Arc::new(TransientExecutionStore);
        let probe_engine = GatewayEngine::<dyn ExecutionStore>::new(transient, providers.clone());
        Self {
            snapshots,
            coordinator: Arc::new(AttemptCoordinator::new(engine)),
            probe_coordinator: Arc::new(AttemptCoordinator::new(probe_engine)),
            observations,
            providers,
            admissions,
            continuation,
            client_api_key_usage,
            budget: None,
        }
    }

    #[must_use]
    pub fn with_budget(mut self, budget: Arc<dyn ClientBudgetPort>) -> Self {
        self.budget = Some(budget);
        self
    }

    async fn start_inner(&self, request: StartExecution) -> Result<StartedExecution, GatewayError> {
        let StartExecution {
            client,
            public_model,
            operation,
            metadata,
        } = request;
        self.start_prepared_with_target(PendingStartExecution {
            prepared: self.prepare_execution(client)?,
            target: ExecutionTarget::Model(public_model),
            operation,
            metadata,
        })
        .await
    }

    async fn start_provider_endpoint_inner(
        &self,
        request: StartProviderExecution,
    ) -> Result<StartedExecution, GatewayError> {
        let StartProviderExecution {
            client,
            provider,
            operation,
            metadata,
        } = request;
        self.start_prepared_with_target(PendingStartExecution {
            prepared: self.prepare_execution(client)?,
            target: ExecutionTarget::ProviderEndpoint(provider),
            operation,
            metadata,
        })
        .await
    }

    async fn start_prepared_with_target(
        &self,
        request: PendingStartExecution,
    ) -> Result<StartedExecution, GatewayError> {
        request.prepared.validate()?;
        let request_id = request.prepared.request_id.clone();
        let client_key_id = request.prepared.client.policy.key_id().clone();
        let metadata = request.metadata.clone();
        let requested_model = match &request.target {
            ExecutionTarget::Model(model) => Some(model.clone()),
            ExecutionTarget::ProviderEndpoint(_) => None,
        };
        let routing_group_refs = request
            .prepared
            .client
            .policy
            .account_scope()
            .routing_snapshot()
            .groups_snapshot()
            .iter()
            .map(|group| group.id().as_str().to_owned())
            .collect();
        let started = Instant::now();
        let result = self.start_authorized_target(request).await;
        if let Err(error) = &result {
            let observation = self
                .observations
                .record_entry_rejection(super::EntryRejection {
                    request_id,
                    client_key_id,
                    metadata,
                    requested_model,
                    routing_group_refs,
                    error: error.clone(),
                    latency: started.elapsed(),
                })
                .fuse();
            let timeout = Delay::new(COORDINATION_TIMEOUT).fuse();
            pin_mut!(observation, timeout);
            select_biased! {
                result = observation => { if let Err(error) = result { tracing::warn!(%error, "entry rejection observation failed"); } },
                _ = timeout => { tracing::warn!("entry rejection observation timed out"); },
            }
        }
        result
    }

    async fn start_authorized_target(
        &self,
        mut request: PendingStartExecution,
    ) -> Result<StartedExecution, GatewayError> {
        let request_id = request.prepared.request_id.clone();
        let routing_context = RoutingContext {
            required_provider: request.prepared.model_target.provider.clone(),
            ..Default::default()
        };
        let target = &request.prepared.model_target;
        let target_restricted = target.provider.is_some()
            || target.account.is_some()
            || target.parent_account.is_some();
        let account_scope = if target_restricted {
            Arc::new(
                request
                    .prepared
                    .client
                    .policy
                    .account_scope()
                    .execution_target(
                        target.provider.as_ref(),
                        target.account.as_ref(),
                        target.parent_account.as_ref(),
                    ),
            )
        } else {
            Arc::clone(request.prepared.client.policy.account_scope())
        };
        if target_restricted
            && account_scope
                .directory()
                .providers_with_accounts()
                .is_empty()
        {
            return Err(GatewayError::new(
                GatewayErrorKind::NoAvailableProvider,
                "execution target has no authorized available accounts",
            ));
        }
        let plan = match &request.target {
            ExecutionTarget::ProviderEndpoint(provider) => {
                request.prepared.settings.snapshot().plan_provider_endpoint(
                    provider,
                    &request.operation,
                    account_scope,
                    &routing_context,
                )
            }
            ExecutionTarget::Model(public_model) => request.prepared.settings.snapshot().plan(
                public_model,
                &request.operation,
                account_scope,
                &routing_context,
            ),
        }
        .map_err(map_routing_error)?
        .with_fast_mode(
            request
                .prepared
                .settings
                .execution_values()
                .ok_or_else(|| {
                    GatewayError::new(
                        GatewayErrorKind::Internal,
                        "execution settings are unavailable",
                    )
                })?
                .fast_mode,
        );
        let continuation = match request.metadata.previous_response_id.as_ref() {
            Some(previous) => {
                let resolve = self
                    .continuation
                    .resolve(request.prepared.client.policy.key_id(), previous)
                    .fuse();
                let timeout = Delay::new(COORDINATION_TIMEOUT).fuse();
                pin_mut!(resolve, timeout);
                let pin = select_biased! {
                    result = resolve => match result {
                        Ok(pin) => pin,
                        Err(error)
                            if error.kind() == NativeContinuationStoreErrorKind::Unavailable =>
                        {
                            tracing::debug!(
                                request_id = request_id.as_str(),
                                %error,
                                "Continuation affinity 查询失败，退化为外部续接"
                            );
                            None
                        }
                        Err(error)
                            if error.kind()
                                == NativeContinuationStoreErrorKind::OwnershipMismatch =>
                        {
                            return Err(GatewayError::new(
                                GatewayErrorKind::PolicyDenied,
                                "continuation does not belong to this client API key",
                            ));
                        }
                        Err(error) => {
                            tracing::warn!(
                                request_id = request_id.as_str(),
                                %error,
                                "Continuation affinity 记录无效，已拒绝续接"
                            );
                            return Err(GatewayError::new(
                                GatewayErrorKind::Internal,
                                "continuation state is invalid",
                            ));
                        }
                    },
                    _ = timeout => None,
                };
                match pin {
                    Some(pin) if !pin.matches_client(request.prepared.client.policy.key_id()) => {
                        return Err(GatewayError::new(
                            GatewayErrorKind::PolicyDenied,
                            "continuation does not belong to this client API key",
                        ));
                    }
                    Some(pin)
                        if !request
                            .prepared
                            .client
                            .policy
                            .account_scope()
                            .allows(pin.account()) =>
                    {
                        return Err(GatewayError::new(
                            GatewayErrorKind::PolicyDenied,
                            "continuation account is outside the client account scope",
                        ));
                    }
                    Some(pin) if !plan.account_scope().allows(pin.account()) => {
                        return Err(GatewayError::new(
                            GatewayErrorKind::PolicyDenied,
                            "continuation account conflicts with the execution target",
                        ));
                    }
                    Some(pin)
                        if !plan
                            .candidates()
                            .iter()
                            .any(|candidate| candidate.provider() == pin.provider()) =>
                    {
                        return Err(GatewayError::new(
                            GatewayErrorKind::NoAvailableProvider,
                            "continuation provider is not available",
                        ));
                    }
                    Some(pin) => {
                        attach_continuation_session_state(&mut request.operation, &pin);
                        ContinuationBinding::Pinned(pin)
                    }
                    None => ContinuationBinding::External(previous.clone()),
                }
            }
            None => {
                return self.start_without_continuation(request, plan, None).await;
            }
        };
        self.start_without_continuation(request, plan, Some(continuation))
            .await
    }

    async fn start_without_continuation(
        &self,
        request: PendingStartExecution,
        plan: crate::routing::RoutingPlan,
        continuation: Option<ContinuationBinding>,
    ) -> Result<StartedExecution, GatewayError> {
        request.prepared.validate()?;
        let PendingStartExecution {
            prepared,
            target,
            operation,
            metadata,
        } = request;
        let execution_context = prepared.nested_context();
        let PreparedRootExecution {
            model_target,
            lifetime,
            extension_scope,
            parent_deadline: _,
            settings,
            attempt_middleware,
            client,
            request_id,
            started_at,
            deadline_at,
            cancellation,
        } = prepared;
        let request_settings = settings.clone();
        let settings = settings.execution_values().ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::Internal,
                "execution settings are unavailable",
            )
        })?;
        let admission_request = ClientAdmissionRequest {
            model_request_id: request_id.clone(),
            client_api_key_id: client.policy.key_id().clone(),
            lease_ttl: crate::lifecycle::REQUEST_LEASE_TTL,
            limits: settings.client_limits,
        };
        let admission_started_at = Instant::now();
        match self
            .admissions
            .admit(admission_request)
            .await
            .map_err(|_| {
                GatewayError::new(
                    GatewayErrorKind::NoAvailableProvider,
                    "request admission is temporarily unavailable",
                )
            })? {
            ClientAdmissionDecision::Granted => {}
            ClientAdmissionDecision::Rejected(reason) => {
                return Err(admission_error(reason, false));
            }
        }
        let admission_decision_ms = duration_ms(admission_started_at.elapsed());
        let mut admission = AdmissionLease {
            renewals: vec![self.admissions.maintain(
                client.policy.key_id(),
                &request_id,
                crate::lifecycle::REQUEST_LEASE_TTL,
                cancellation.clone(),
            )],
            port: Arc::clone(&self.admissions),
            scopes: vec![client.policy.key_id().clone()],
            model_request_id: request_id.clone(),
        };
        if let Some((scope, limits)) = client.policy.user_admission() {
            let decision = self
                .admissions
                .admit(ClientAdmissionRequest {
                    model_request_id: request_id.clone(),
                    client_api_key_id: scope.clone(),
                    lease_ttl: crate::lifecycle::REQUEST_LEASE_TTL,
                    limits: *limits,
                })
                .await;
            match decision {
                Ok(ClientAdmissionDecision::Granted) => {
                    admission.renewals.push(self.admissions.maintain(
                        scope,
                        &request_id,
                        crate::lifecycle::REQUEST_LEASE_TTL,
                        cancellation.clone(),
                    ));
                    admission.scopes.push(scope.clone());
                }
                Ok(ClientAdmissionDecision::Rejected(reason)) => {
                    admission.release().await;
                    return Err(admission_error(reason, true));
                }
                Err(_) => {
                    admission.release().await;
                    return Err(GatewayError::new(
                        GatewayErrorKind::NoAvailableProvider,
                        "user admission is temporarily unavailable",
                    ));
                }
            }
        }
        let routing = client.policy.account_scope().routing_snapshot();
        let expected_group = if routing.groups_snapshot().len() == 1 {
            Some(routing.groups_snapshot()[0].id().clone())
        } else {
            None
        };
        let budget_scope = if let Some(budget) = &self.budget {
            match budget
                .admit(client.policy.key_id().clone(), expected_group)
                .await
            {
                Ok(scope) => Some(scope),
                Err(error) => {
                    admission.release().await;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let observation = plan
            .candidates()
            .first()
            .map_or_else(Default::default, |candidate| {
                self.providers.request_observation(
                    candidate.provider(),
                    &operation,
                    client.policy.key_id(),
                )
            });
        let new_request = NewModelRequest {
            parent_request_id: extension_scope.parent_request_id().cloned(),
            initiating_plugin_instance_id: extension_scope
                .initiating_plugin_instance_id()
                .map(str::to_owned),
            budget_scope: budget_scope.clone(),
            id: request_id.clone(),
            client_api_key_id: Some(client.policy.key_id().clone()),
            client_api_key_ref: client.policy.key_id().clone(),
            config_revision: plan.config_revision(),
            routing: client.policy.account_scope().routing_snapshot(),
            protocol: metadata.protocol,
            operation: operation.kind(),
            endpoint: metadata.endpoint,
            client_transport: metadata.transport.as_str().to_owned(),
            requested_model: target.into_public_model().or(observation.requested_model),
            client_ip: metadata.client_ip,
            user_agent: metadata.user_agent,
            reasoning_effort: observation.reasoning_effort,
            reasoning_preset: observation.reasoning_preset,
            request_kind: observation.request_kind,
            subagent_kind: observation.subagent_kind,
            compact: observation.compact,
            continuation: observation.continuation,
            image_generation_requested: operation.image_generation_requested(),
            admission_decision_ms: Some(admission_decision_ms),
            started_at,
            deadline_at,
        };
        let core = match self
            .coordinator
            .with_attempt_middleware(attempt_middleware)
            .with_request_settings(request_settings)
            .with_execution_context(execution_context)
            .start(
                new_request,
                operation,
                plan,
                model_target.account,
                continuation,
                cancellation,
            )
            .await
        {
            Ok(core) => core,
            Err(error) => {
                if let Some(budget) = &self.budget {
                    settle_budget(
                        budget.as_ref(),
                        ClientBudgetCharge {
                            scope: budget_scope.clone(),
                            key_id: client.policy.key_id().clone(),
                            request_id: request_id.clone(),
                            amount_usd: crate::metering::Decimal::ZERO,
                            completed_at: SystemTime::now(),
                        },
                    )
                    .await;
                }
                admission.release().await;
                return Err(gateway_error_from_engine(&error));
            }
        };
        Ok(StartedExecution {
            request_id,
            created_at: started_at,
            stream: metadata.stream,
            session: Box::new(DefaultExecutionSession::new(
                core,
                admission,
                Arc::clone(&self.continuation),
                self.budget.clone(),
                client.bound_authorization,
                lifetime,
            )),
        })
    }

    async fn probe_inner(
        &self,
        request: AccountProbeRequest,
    ) -> Result<AccountProbeResult, AccountProbeError> {
        let AccountProbeRequest {
            account_id,
            provider_kind,
            upstream_model,
            operation,
        } = request;
        let observed = ProbeObservation {
            provider_kind: provider_kind.clone(),
            account_id: account_id.clone(),
            upstream_model: upstream_model.clone(),
        };
        let snapshot = self.snapshots.acquire().map_err(|_| {
            GatewayError::new(
                GatewayErrorKind::Internal,
                "runtime snapshot is unavailable",
            )
        })?;
        let public_model =
            PublicModelId::new(upstream_model.as_str().to_owned()).map_err(|_| {
                GatewayError::new(GatewayErrorKind::Unsupported, "requested model is invalid")
            })?;
        let routing_context = RoutingContext {
            required_provider: Some(provider_kind),
            ..RoutingContext::default()
        };
        let plan = snapshot
            .plan(
                &public_model,
                &operation,
                snapshot.all_account_scope(),
                &routing_context,
            )
            .map_err(map_routing_error)?;
        let started_at = SystemTime::now();
        let deadline_at = started_at
            .checked_add(MODEL_PROBE_DEADLINE)
            .map(crate::lifecycle::Deadline::from)
            .ok_or_else(|| {
                GatewayError::new(GatewayErrorKind::Internal, "system clock is invalid")
            })?;
        let request_id = new_request_id()?;
        let actor = ClientApiKeyId::new("admin_connection_test")
            .map_err(|_| GatewayError::new(GatewayErrorKind::Internal, "invalid admin actor"))?;
        let new_request = NewModelRequest {
            parent_request_id: None,
            initiating_plugin_instance_id: None,
            id: request_id,
            budget_scope: None,
            client_api_key_id: None,
            client_api_key_ref: actor,
            config_revision: plan.config_revision(),
            routing: crate::routing::AccountRoutingSnapshot::all(),
            protocol: "admin_connection_test".to_owned(),
            operation: operation.kind(),
            endpoint: "/api/admin/accounts/connection-test".to_owned(),
            client_transport: ClientTransport::InternalProbe.as_str().to_owned(),
            requested_model: Some(public_model),
            client_ip: None,
            user_agent: None,
            reasoning_effort: None,
            reasoning_preset: None,
            request_kind: Some("account_connection_test".to_owned()),
            subagent_kind: None,
            compact: false,
            continuation: Default::default(),
            image_generation_requested: false,
            admission_decision_ms: None,
            started_at,
            deadline_at,
        };
        let mut session = match self
            .probe_coordinator
            .start_diagnostic(
                new_request,
                operation,
                plan,
                account_id,
                None,
                CancellationToken::new(),
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                return Err(self
                    .observe_probe_failure(&observed, started_at, &error)
                    .await);
            }
        };
        let events = session.collect_uncommitted().await;
        let events = match events {
            Ok(events) => events,
            Err(error) => {
                return Err(self
                    .observe_probe_failure(&observed, started_at, &error)
                    .await);
            }
        };
        if let Err(error) = session.commit_downstream(Some(200)).await {
            return Err(self
                .observe_probe_failure(&observed, started_at, &error)
                .await);
        }
        Ok(AccountProbeResult {
            text: events
                .into_iter()
                .flat_map(|event| event.into_parts().0)
                .filter_map(|fact| match fact {
                    GatewayEvent::TextDelta(delta) => Some(delta.text),
                    _ => None,
                })
                .collect(),
        })
    }

    /// 探测失败先记录脱敏分类事实，再把请求局部的原始上游响应交给认证管理端。
    async fn observe_probe_failure(
        &self,
        observed: &ProbeObservation,
        started_at: SystemTime,
        error: &EngineError,
    ) -> AccountProbeError {
        let (source, send_state, upstream_response) = match error {
            EngineError::Provider(provider_error) => {
                let upstream_response = provider_error
                    .client_visible_upstream_response()
                    .map(AccountProbeUpstreamResponse::from_client_response);
                let has_upstream_facts = provider_error.send_state() != UpstreamSendState::NotSent
                    || provider_error.upstream_status().is_some()
                    || provider_error.upstream_code().is_some()
                    || provider_error.client_visible_upstream_error().is_some()
                    || upstream_response.is_some();
                let source = if has_upstream_facts {
                    AccountProbeErrorSource::Upstream
                } else {
                    AccountProbeErrorSource::Provider
                };
                (source, Some(provider_error.send_state()), upstream_response)
            }
            _ => (AccountProbeErrorSource::Gateway, None, None),
        };
        if let EngineError::Provider(provider_error) = error {
            let latency = started_at.elapsed().unwrap_or_default();
            let latency_ms = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);
            tracing::warn!(
                target: "gateway_probe",
                provider_kind = observed.provider_kind.as_str(),
                account_id = observed.account_id.as_str(),
                upstream_model = observed.upstream_model.as_str(),
                failure_kind = provider_error.kind().as_str(),
                send_state = ?provider_error.send_state(),
                upstream_status = ?provider_error.upstream_status(),
                provider_error_code = ?provider_error.upstream_code().map(|code| code.as_str()),
                latency_ms,
                "账号连接测试失败"
            );
            if let Err(store_error) = self
                .observations
                .record_probe_failure(ProbeFailure {
                    provider_kind: observed.provider_kind.clone(),
                    account_id: observed.account_id.clone(),
                    upstream_model_id: observed.upstream_model.clone(),
                    error: provider_error.clone(),
                    latency,
                })
                .await
            {
                tracing::warn!(
                    operation = "record_probe_failure",
                    provider_kind = observed.provider_kind.as_str(),
                    account_id = observed.account_id.as_str(),
                    error_kind = ?store_error.kind(),
                    "账号连接测试观测写入失败，测试结果不受影响"
                );
            }
        }
        AccountProbeError::new(
            gateway_error_from_engine(error),
            source,
            send_state,
            upstream_response,
        )
    }
}

struct ProbeObservation {
    provider_kind: ProviderKind,
    account_id: ProviderAccountId,
    upstream_model: UpstreamModelId,
}

struct TransientExecutionStore;
impl super::ExecutionRecoveryLeaseGuard for TransientExecutionStore {}

#[async_trait::async_trait]
impl ExecutionStore for TransientExecutionStore {
    fn maintain_request(
        &self,
        _: &ModelRequestId,
        _: Option<SystemTime>,
        _: Duration,
    ) -> Box<dyn super::ExecutionRecoveryLeaseGuard> {
        // Internal probes intentionally have no durable model request record.
        Box::new(Self)
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
        Ok(())
    }

    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        Ok(())
    }

    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        Ok(())
    }

    async fn finalize_model_request(&self, _: ModelRequestFinalization) -> Result<(), StoreError> {
        Ok(())
    }

    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        Ok(RecoveryReport::default())
    }
}

impl ExecutionService for DefaultExecutionService {
    fn client_access(
        &self,
        client: &AuthenticatedClient,
    ) -> Option<crate::runtime::ClientAccessGuard> {
        let access = self.snapshots.client_access(client.policy()).ok()?;
        Some(match &client.bound_authorization {
            Some(bound) => access.with_revocation(bound.cancellation()),
            None => access,
        })
    }
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        let snapshot = self
            .snapshots
            .acquire()
            .map_err(|_| ClientAuthenticationError::SnapshotUnavailable)?;
        let policy = snapshot
            .client_policies()
            .filter(|policy| {
                constant_time_equal(plaintext, policy.plaintext_key().expose_for_auth())
            })
            .find(|policy| policy.authorize().is_ok())
            .cloned()
            .ok_or(ClientAuthenticationError::InvalidKey)?;
        self.client_api_key_usage.record_used(policy.key_id());
        Ok(AuthenticatedClient {
            snapshot,
            policy,
            bound_authorization: None,
        })
    }

    fn authenticate_bound_key(
        &self,
        key: &ClientApiKeyId,
        revocation: CancellationToken,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        if revocation.is_cancelled() {
            return Err(ClientAuthenticationError::InvalidKey);
        }
        let snapshot = self
            .snapshots
            .acquire()
            .map_err(|_| ClientAuthenticationError::SnapshotUnavailable)?;
        let policy = snapshot
            .client_policies()
            .find(|p| p.key_id() == key && p.authorize().is_ok())
            .cloned()
            .ok_or(ClientAuthenticationError::InvalidKey)?;
        let bound = self
            .snapshots
            .client_access(&policy)
            .map_err(|_| ClientAuthenticationError::SnapshotUnavailable)?
            .with_revocation(revocation);
        if bound.cancellation().is_cancelled() {
            return Err(ClientAuthenticationError::InvalidKey);
        }
        self.client_api_key_usage.record_used(policy.key_id());
        Ok(AuthenticatedClient {
            snapshot,
            policy,
            bound_authorization: Some(bound),
        })
    }

    fn public_models(&self, client: &AuthenticatedClient) -> Vec<PublicModelId> {
        client
            .snapshot
            .public_models_for_scope(client.policy.account_scope())
    }

    fn client_model_catalog<'a>(
        &'a self,
        client: &'a AuthenticatedClient,
        protocol: &'a str,
        client_version: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PublicModelDescriptor>, ProviderCatalogUnavailable>> {
        Box::pin(async move {
            let scope = client.policy.account_scope();
            let mut result = Vec::new();
            let mut seen = BTreeSet::new();
            for kind in scope.provider_kinds() {
                let provider = self.providers.get(kind).ok_or(ProviderCatalogUnavailable)?;
                let Some(models) = provider
                    .query_client_model_catalog(scope, protocol, client_version)
                    .await?
                else {
                    for profile in client.snapshot.public_model_profiles_for_provider(kind) {
                        if client.snapshot.catalog_model_allowed_for_scope(
                            kind,
                            profile.model(),
                            scope,
                        ) && seen.insert(profile.model().clone())
                        {
                            result.push(PublicModelDescriptor::Adapted(profile));
                        }
                    }
                    continue;
                };
                // 保留上游顺序；映射只选择完整的目标对象，不能跨账号/模型混拼字段。
                let by_id = models
                    .iter()
                    .map(|entry| (entry.model.as_str(), entry))
                    .collect::<std::collections::BTreeMap<_, _>>();
                for entry in &models {
                    let target = client.snapshot.mapped_model(entry.model.as_str());
                    let Some(source) = by_id.get(target.as_str()) else {
                        continue;
                    };
                    let model = PublicModelId::new(entry.model.as_str().to_owned())
                        .map_err(|_| ProviderCatalogUnavailable)?;
                    // 原生目录可能比路由快照更新，不能向客户端公布当前已知不可路由的模型。
                    if !client
                        .snapshot
                        .contains_public_model_for_provider(&model, kind)
                    {
                        continue;
                    }
                    if scope.allows_provider_model(kind, target.as_str())
                        && seen.insert(model.clone())
                    {
                        result.push(PublicModelDescriptor::Native {
                            model,
                            payload: source.payload.clone(),
                        });
                    }
                }
                for model in client.snapshot.public_models_for_provider(kind) {
                    let target = client.snapshot.mapped_model(model.as_str());
                    if target == model.as_str()
                        || seen.contains(&model)
                        || !scope.allows_provider_model(kind, target.as_str())
                    {
                        continue;
                    }
                    if !client
                        .snapshot
                        .contains_public_model_for_provider(&model, kind)
                    {
                        continue;
                    }
                    if let Some(source) = by_id.get(target.as_str()) {
                        seen.insert(model.clone());
                        result.push(PublicModelDescriptor::Native {
                            model,
                            payload: source.payload.clone(),
                        });
                    }
                }
            }
            Ok(result)
        })
    }

    fn contains_public_model(&self, client: &AuthenticatedClient, model: &PublicModelId) -> bool {
        client
            .snapshot
            .contains_public_model_for_scope(model, client.policy.account_scope())
    }

    fn prepare_execution(
        &self,
        client: AuthenticatedClient,
    ) -> Result<PreparedRootExecution, GatewayError> {
        let previous = client
            .bound_authorization
            .as_ref()
            .map_or_else(CancellationToken::new, |access| access.cancellation());
        if previous.is_cancelled() {
            return Err(GatewayError::new(
                GatewayErrorKind::Unauthorized,
                "client authorization was revoked",
            ));
        }
        let client = self
            .authenticate_revocable(client.policy.plaintext_key().expose_for_auth(), previous)
            .map_err(|error| match error {
                ClientAuthenticationError::InvalidKey => {
                    GatewayError::new(GatewayErrorKind::Unauthorized, "client API key is invalid")
                }
                ClientAuthenticationError::SnapshotUnavailable => GatewayError::new(
                    GatewayErrorKind::Internal,
                    "runtime snapshot is unavailable",
                ),
            })?;
        let started_at = SystemTime::now();
        let deadline_at = crate::lifecycle::Deadline::default();
        let authorization = client.bound_authorization.as_ref().ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::Internal,
                "prepared authorization is unavailable",
            )
        })?;
        let lifetime = super::nested::ExecutionLifetime::new();
        let cancellation = CancellationToken::linked([authorization.cancellation()]);
        Ok(PreparedRootExecution {
            model_target: Default::default(),
            lifetime,
            extension_scope: Default::default(),
            parent_deadline: Default::default(),
            settings: crate::routing::RequestSettings::new(client.snapshot.clone())
                .with_execution(&client.policy, None),
            attempt_middleware: None,
            client,
            request_id: new_request_id()?,
            started_at,
            deadline_at,
            cancellation,
        })
    }
    fn start_prepared(
        &self,
        prepared: PreparedRootExecution,
        request: PreparedExecutionRequest,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            self.start_prepared_with_target(PendingStartExecution {
                prepared,
                target: ExecutionTarget::Model(request.public_model),
                operation: request.operation,
                metadata: request.metadata,
            })
            .await
        })
    }
    fn start_prepared_provider_endpoint(
        &self,
        prepared: PreparedRootExecution,
        provider: ProviderKind,
        operation: Operation,
        metadata: ExecutionRequestMetadata,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            self.start_prepared_with_target(PendingStartExecution {
                prepared,
                target: ExecutionTarget::ProviderEndpoint(provider),
                operation,
                metadata,
            })
            .await
        })
    }

    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move { self.start_inner(request).await })
    }

    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move { self.start_provider_endpoint_inner(request).await })
    }
}

impl AccountProbe for DefaultExecutionService {
    fn probe(
        &self,
        request: AccountProbeRequest,
    ) -> BoxFuture<'_, Result<AccountProbeResult, AccountProbeError>> {
        Box::pin(async move { self.probe_inner(request).await })
    }
}

struct AdmissionLease {
    renewals: Vec<Box<dyn super::admission::ClientAdmissionLeaseGuard>>,
    port: Arc<dyn ClientAdmissionPort>,
    scopes: Vec<ClientApiKeyId>,
    model_request_id: ModelRequestId,
}

async fn settle_budget(port: &dyn ClientBudgetPort, charge: ClientBudgetCharge) {
    if let Err(error) = port.settle(charge).await {
        tracing::error!(%error, "Client budget settlement failed; storage will retry on the next request");
    }
}

impl AdmissionLease {
    async fn release(mut self) {
        self.renewals.clear();
        for scope in &self.scopes {
            if let Err(error) = self.port.release(scope, &self.model_request_id).await {
                tracing::warn!(%error, "Client admission 释放失败，依赖租约 TTL 收敛");
            }
        }
    }
}

struct DefaultExecutionSession {
    lifetime: Option<Arc<super::nested::ExecutionLifetime>>,
    // Retain snapshot revocation registration after the request's AuthenticatedClient is consumed.
    _authorization: Option<crate::runtime::ClientAccessGuard>,
    core: ResponseExecutionSession<dyn ExecutionStore>,
    admission: Option<AdmissionLease>,
    cleanup: Option<BoxFuture<'static, ()>>,
    continuation: Arc<dyn NativeContinuationPort>,
    continuation_recorded: bool,
    budget: Option<Arc<dyn ClientBudgetPort>>,
}

impl DefaultExecutionSession {
    fn new(
        core: ResponseExecutionSession<dyn ExecutionStore>,
        admission: AdmissionLease,
        continuation: Arc<dyn NativeContinuationPort>,
        budget: Option<Arc<dyn ClientBudgetPort>>,
        authorization: Option<crate::runtime::ClientAccessGuard>,
        lifetime: Arc<super::nested::ExecutionLifetime>,
    ) -> Self {
        Self {
            _authorization: authorization,
            lifetime: Some(lifetime),
            core,
            admission: Some(admission),
            cleanup: None,
            continuation,
            continuation_recorded: false,
            budget,
        }
    }

    async fn settle_if_finalized(&mut self) {
        if self.core.is_finalized()
            && let Some(admission) = self.admission.take()
        {
            let budget = self.budget.take();
            let charge = self.core.budget_charge();
            // 在首次 await 前把完整清理责任留在会话内。事件等待被取消后，后续 poll
            // 或 detach 继续同一个 future，既不丢失费用，也不重启已完成的结算。
            self.cleanup = Some(Box::pin(async move {
                if let Some(budget) = budget {
                    settle_budget(budget.as_ref(), charge).await;
                }
                admission.release().await;
            }));
        }
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.await;
            self.cleanup = None;
        }
        if self.core.is_finalized() && self.admission.is_none() && self.cleanup.is_none() {
            self.core.release_nested_permit();
            self.lifetime.take();
        }
    }

    async fn record_continuation(&mut self, state: Option<&ProviderSessionState>) {
        if self.continuation_recorded {
            return;
        }
        let Some(state) = state else {
            return;
        };
        let Some(pin) = self.core.native_continuation_pin(state) else {
            return;
        };
        self.continuation_recorded = true;
        record_native_continuation(self.continuation.as_ref(), pin).await;
    }

    async fn finalize_detached(&mut self) {
        if let Err(error) = self.core.cancel_and_finalize().await {
            tracing::warn!(%error, "Detached execution 终态收敛失败");
        }
        self.settle_if_finalized().await;
    }
}

impl Drop for DefaultExecutionSession {
    fn drop(&mut self) {
        self.core.cancel();
    }
}

impl ExecutionSession for DefaultExecutionSession {
    fn send_state(&self) -> UpstreamSendState {
        self.core.current_send_state()
    }
    fn response_control(&self) -> Option<super::response_control::ResponseControl> {
        Some(self.core.response_control())
    }
    fn trace(&self) -> crate::diagnostics::TraceContext {
        self.core.trace()
    }
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            let result = self.core.next_event().await;
            if let Ok(Some(event)) = result.as_ref() {
                self.record_continuation(event.session_update()).await;
            }
            self.settle_if_finalized().await;
            result
        })
    }

    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        Box::pin(async move {
            let result = self.core.collect_uncommitted().await;
            if let Ok(events) = result.as_ref() {
                let state = events.iter().find_map(ProviderEvent::session_update);
                self.record_continuation(state).await;
            }
            self.settle_if_finalized().await;
            result
        })
    }

    fn response_headers(&self) -> &[ProviderResponseHeader] {
        self.core.response_headers()
    }
    fn middleware_response_head(&self) -> Option<&super::attempt_middleware::AttemptResponseHead> {
        self.core.middleware_response_head()
    }

    fn response_status_code(&self) -> Option<u16> {
        self.core.response_status_code()
    }

    fn commit_downstream(
        &mut self,
        client_status_code: Option<u16>,
    ) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            let result = self.core.commit_downstream(client_status_code).await;
            self.settle_if_finalized().await;
            result
        })
    }

    fn record_client_status(
        &mut self,
        client_status_code: u16,
    ) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            let result = self.core.record_client_status(client_status_code).await;
            self.settle_if_finalized().await;
            result
        })
    }

    fn is_finalized(&self) -> bool {
        self.core.is_finalized() && self.admission.is_none() && self.cleanup.is_none()
    }

    fn cancel(&self) {
        self.core.cancel();
    }

    fn detach_finalize(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { self.finalize_detached().await })
    }
}

async fn record_native_continuation(
    continuation: &dyn NativeContinuationPort,
    pin: NativeContinuationPin,
) {
    let provider = pin.provider().as_str().to_owned();
    let account = pin.account().as_str().to_owned();
    let record = continuation.record(pin).fuse();
    let timeout = Delay::new(COORDINATION_TIMEOUT).fuse();
    pin_mut!(record, timeout);
    select_biased! {
        result = record => {
            if let Err(error) = result {
                tracing::warn!(
                    provider = %provider,
                    account = %account,
                    %error,
                    "Continuation affinity 写入失败，后续请求将退化为外部续接"
                );
            }
        },
        _ = timeout => {
            tracing::warn!(
                provider = %provider,
                account = %account,
                "Continuation affinity 后台写入超时，已丢弃本次亲和记录"
            );
        },
    }
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn new_request_id() -> Result<ModelRequestId, GatewayError> {
    ModelRequestId::new(format!("req_{}", Uuid::now_v7().simple()))
        .map_err(|_| GatewayError::new(GatewayErrorKind::Internal, "failed to allocate request ID"))
}

fn map_routing_error(error: crate::validation::RoutingError) -> GatewayError {
    match error {
        crate::validation::RoutingError::ModelNotFound {
            model,
            mapped_model,
        } => GatewayError::new(
            GatewayErrorKind::ModelNotFound,
            if model == mapped_model {
                "the requested model was not found in the provider catalogs available to this API key; check the model name"
            } else {
                "the requested model maps to an upstream model that was not found in the provider catalogs available to this API key; check the configured model mapping"
            },
        ),
        crate::validation::RoutingError::NoCapableProvider { .. }
        | crate::validation::RoutingError::NoCapableProviderEndpoint { .. }
        | crate::validation::RoutingError::EmptyAccountScope => GatewayError::new(
            GatewayErrorKind::NoAvailableProvider,
            "no provider can execute this request",
        ),
        _ => GatewayError::new(
            GatewayErrorKind::Internal,
            "runtime routing configuration is invalid",
        ),
    }
}

fn attach_continuation_session_state(operation: &mut Operation, pin: &NativeContinuationPin) {
    let Some(state) = pin.session_state() else {
        return;
    };
    if state.provider() != pin.provider().as_str()
        || operation.provider_session_state(state.provider()).is_some()
    {
        return;
    }
    operation.set_provider_session_state(state.clone());
}

pub fn gateway_error_from_engine(error: &EngineError) -> GatewayError {
    match error {
        EngineError::Cancelled => {
            GatewayError::new(GatewayErrorKind::Cancelled, "request was cancelled")
        }
        EngineError::Deadline => {
            GatewayError::new(GatewayErrorKind::Timeout, "request deadline elapsed")
        }
        EngineError::Provider(provider) => GatewayError::from_provider(provider),
        EngineError::EmptyRoutingPlan | EngineError::ProviderNotRegistered { .. } => {
            GatewayError::new(
                GatewayErrorKind::NoAvailableProvider,
                "no provider is available",
            )
        }
        _ => GatewayError::new(GatewayErrorKind::Internal, "request execution failed"),
    }
}
