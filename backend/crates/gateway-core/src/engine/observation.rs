//! Native execution facts and bounded, best-effort observation ports.

use super::{
    ExecutionOutcome, ModelRequestFinalization, ModelRequestId, ModelRequestTimings,
    nested::ExtensionCallScope,
};
use crate::{
    account::ProviderAccountId,
    error::{GatewayError, GatewayErrorKind},
    event::{GatewayEvent, ProtocolWireEvent, ProviderResponseObservation},
    identity::ProviderKind,
    metering::{CostEstimate, CostSource, Usage},
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, ConfigRevision, PublicModelId},
    upstream::UpstreamSendState,
};
use std::{
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime},
};

/// 一条 WebSocket 响应事件所属的实际上游 attempt 身份
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketResponseAttempt {
    provider: ProviderKind,
    account_id: ProviderAccountId,
    attempt_index: NonZeroU32,
}

impl WebSocketResponseAttempt {
    /// 固定一条响应事件所属的实际上游 attempt
    #[must_use]
    pub const fn new(
        provider: ProviderKind,
        account_id: ProviderAccountId,
        attempt_index: NonZeroU32,
    ) -> Self {
        Self {
            provider,
            account_id,
            attempt_index,
        }
    }

    #[must_use]
    pub const fn provider(&self) -> &ProviderKind {
        &self.provider
    }

    #[must_use]
    pub const fn account_id(&self) -> &ProviderAccountId {
        &self.account_id
    }

    #[must_use]
    pub const fn attempt_index(&self) -> NonZeroU32 {
        self.attempt_index
    }
}

/// 请求观察使用的冻结 Client Key 与账号组范围
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestObservationScope {
    client_key_id: ClientApiKeyId,
    account_group_ids: Arc<[AccountGroupId]>,
}

impl RequestObservationScope {
    #[must_use]
    pub fn new(client_key_id: ClientApiKeyId, mut account_group_ids: Vec<AccountGroupId>) -> Self {
        account_group_ids.sort();
        account_group_ids.dedup();
        Self {
            client_key_id,
            account_group_ids: account_group_ids.into(),
        }
    }

    #[must_use]
    pub const fn client_key_id(&self) -> &ClientApiKeyId {
        &self.client_key_id
    }

    #[must_use]
    pub fn account_group_ids(&self) -> &[AccountGroupId] {
        &self.account_group_ids
    }
}

/// 实际上游 WebSocket 响应事件的只读观察
///
/// Key 与账号组只用于 Runtime 匹配冻结绑定，不得进入插件 wire；`wire` 保留策略加工前
/// 的 Provider 原始事实，由 Runtime 再按正文读取授权投影
#[derive(Clone)]
pub struct WebSocketResponseObservation {
    event_id: String,
    request_id: ModelRequestId,
    config_revision: ConfigRevision,
    client_scope: RequestObservationScope,
    extension_scope: ExtensionCallScope,
    operation: OperationKind,
    requested_model: Option<PublicModelId>,
    attempt: WebSocketResponseAttempt,
    sequence: u64,
    wire: ProtocolWireEvent,
}

impl WebSocketResponseObservation {
    /// 创建一个已附加冻结 Key/组范围、尚未附加公开模型范围的实际上游事件
    #[must_use]
    pub fn new(
        request_id: ModelRequestId,
        config_revision: ConfigRevision,
        client_scope: RequestObservationScope,
        operation: OperationKind,
        attempt: WebSocketResponseAttempt,
        sequence: u64,
        wire: ProtocolWireEvent,
    ) -> Self {
        Self {
            event_id: format!(
                "{}:websocket:{}:{sequence}",
                request_id.as_str(),
                attempt.attempt_index().get()
            ),
            request_id,
            config_revision,
            client_scope,
            extension_scope: ExtensionCallScope::default(),
            operation,
            requested_model: None,
            attempt,
            sequence,
            wire,
        }
    }

    #[must_use]
    pub fn with_requested_model(mut self, model: PublicModelId) -> Self {
        self.requested_model = Some(model);
        self
    }

    #[must_use]
    pub fn with_extension_scope(mut self, extension_scope: ExtensionCallScope) -> Self {
        self.extension_scope = extension_scope;
        self
    }

    #[must_use]
    pub fn suppresses_plugin(&self, instance_id: &str) -> bool {
        self.extension_scope
            .excluded_instances()
            .contains(instance_id)
    }

    #[must_use]
    pub const fn extension_scope(&self) -> &ExtensionCallScope {
        &self.extension_scope
    }

    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    #[must_use]
    pub const fn request_id(&self) -> &ModelRequestId {
        &self.request_id
    }

    #[must_use]
    pub const fn config_revision(&self) -> ConfigRevision {
        self.config_revision
    }

    #[must_use]
    pub const fn client_key_id(&self) -> &ClientApiKeyId {
        self.client_scope.client_key_id()
    }

    #[must_use]
    pub fn account_group_ids(&self) -> &[AccountGroupId] {
        self.client_scope.account_group_ids()
    }

    #[must_use]
    pub const fn operation(&self) -> OperationKind {
        self.operation
    }

    #[must_use]
    pub const fn requested_model(&self) -> Option<&PublicModelId> {
        self.requested_model.as_ref()
    }

    #[must_use]
    pub const fn provider(&self) -> &ProviderKind {
        self.attempt.provider()
    }

    #[must_use]
    pub const fn account_id(&self) -> &ProviderAccountId {
        self.attempt.account_id()
    }

    #[must_use]
    pub const fn attempt_index(&self) -> NonZeroU32 {
        self.attempt.attempt_index()
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn wire(&self) -> &ProtocolWireEvent {
        &self.wire
    }
}

/// 插件可观察的请求终态；拒绝表示请求没有进入 Provider 执行
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestObservationOutcome {
    Succeeded,
    Failed,
    Rejected,
    Cancelled,
    Incomplete,
}

/// Core 在业务结果确定后产生的一次最终观察，不包含原始正文或凭据
#[derive(Debug, Clone)]
pub struct RequestObservation {
    event_id: String,
    request_id: ModelRequestId,
    config_revision: ConfigRevision,
    client_scope: RequestObservationScope,
    extension_scope: ExtensionCallScope,
    operation: OperationKind,
    requested_model: Option<PublicModelId>,
    provider: Option<ProviderKind>,
    account_id: Option<ProviderAccountId>,
    upstream_model: Option<crate::routing::UpstreamModelId>,
    response_model: Option<String>,
    service_tier: Option<String>,
    outcome: RequestObservationOutcome,
    send_state: UpstreamSendState,
    attempt_count: u32,
    client_status_code: Option<u16>,
    upstream_status_code: Option<u16>,
    error_kind: Option<GatewayErrorKind>,
    retry_after_ms: Option<u64>,
    usage: Usage,
    cost: CostEstimate,
    timings: ModelRequestTimings,
    completed_at: SystemTime,
}

impl RequestObservation {
    /// 创建一个带冻结 Key/组范围、不带 Provider、模型、用量或错误详情的最终事实
    #[must_use]
    pub fn new(
        request_id: ModelRequestId,
        config_revision: ConfigRevision,
        client_scope: RequestObservationScope,
        operation: OperationKind,
        outcome: RequestObservationOutcome,
        send_state: UpstreamSendState,
        completed_at: SystemTime,
    ) -> Self {
        Self {
            event_id: format!("{}:terminal", request_id.as_str()),
            request_id,
            config_revision,
            client_scope,
            extension_scope: ExtensionCallScope::default(),
            operation,
            requested_model: None,
            provider: None,
            account_id: None,
            upstream_model: None,
            response_model: None,
            service_tier: None,
            outcome,
            send_state,
            attempt_count: 0,
            client_status_code: None,
            upstream_status_code: None,
            error_kind: None,
            retry_after_ms: None,
            usage: Usage::new(),
            cost: CostEstimate::unavailable(),
            timings: ModelRequestTimings::default(),
            completed_at,
        }
    }

    #[must_use]
    pub fn with_requested_model(mut self, model: PublicModelId) -> Self {
        self.requested_model = Some(model);
        self
    }

    #[must_use]
    pub fn with_extension_scope(mut self, extension_scope: ExtensionCallScope) -> Self {
        self.extension_scope = extension_scope;
        self
    }

    #[must_use]
    pub fn suppresses_plugin(&self, instance_id: &str) -> bool {
        self.extension_scope
            .excluded_instances()
            .contains(instance_id)
    }

    #[must_use]
    pub const fn extension_scope(&self) -> &ExtensionCallScope {
        &self.extension_scope
    }

    #[must_use]
    pub fn with_provider(mut self, provider: ProviderKind) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub fn account_id(&self) -> Option<&ProviderAccountId> {
        self.account_id.as_ref()
    }

    #[must_use]
    pub fn upstream_model(&self) -> Option<&crate::routing::UpstreamModelId> {
        self.upstream_model.as_ref()
    }

    #[must_use]
    pub fn response_model(&self) -> Option<&str> {
        self.response_model.as_deref()
    }

    #[must_use]
    pub fn service_tier(&self) -> Option<&str> {
        self.service_tier.as_deref()
    }

    #[must_use]
    pub const fn with_attempt_count(mut self, attempt_count: u32) -> Self {
        self.attempt_count = attempt_count;
        self
    }

    #[must_use]
    pub const fn with_client_status_code(mut self, client_status_code: u16) -> Self {
        self.client_status_code = Some(client_status_code);
        self
    }

    #[must_use]
    pub const fn with_upstream_status_code(mut self, upstream_status_code: u16) -> Self {
        self.upstream_status_code = Some(upstream_status_code);
        self
    }

    #[must_use]
    pub const fn with_error_kind(mut self, error_kind: GatewayErrorKind) -> Self {
        self.error_kind = Some(error_kind);
        self
    }

    #[must_use]
    pub const fn with_retry_after_ms(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    #[must_use]
    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage = usage;
        self
    }

    #[must_use]
    pub fn with_cost(mut self, cost: CostEstimate) -> Self {
        self.cost = cost;
        self
    }

    #[must_use]
    pub fn with_timings(mut self, timings: ModelRequestTimings) -> Self {
        self.timings = timings;
        self
    }

    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    #[must_use]
    pub const fn request_id(&self) -> &ModelRequestId {
        &self.request_id
    }

    #[must_use]
    pub const fn config_revision(&self) -> ConfigRevision {
        self.config_revision
    }

    #[must_use]
    pub const fn client_key_id(&self) -> &ClientApiKeyId {
        self.client_scope.client_key_id()
    }

    #[must_use]
    pub fn account_group_ids(&self) -> &[AccountGroupId] {
        self.client_scope.account_group_ids()
    }

    #[must_use]
    pub const fn operation(&self) -> OperationKind {
        self.operation
    }

    #[must_use]
    pub const fn requested_model(&self) -> Option<&PublicModelId> {
        self.requested_model.as_ref()
    }

    #[must_use]
    pub const fn provider(&self) -> Option<&ProviderKind> {
        self.provider.as_ref()
    }

    #[must_use]
    pub const fn outcome(&self) -> RequestObservationOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn send_state(&self) -> UpstreamSendState {
        self.send_state
    }

    #[must_use]
    pub const fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    #[must_use]
    pub const fn client_status_code(&self) -> Option<u16> {
        self.client_status_code
    }

    #[must_use]
    pub const fn upstream_status_code(&self) -> Option<u16> {
        self.upstream_status_code
    }

    #[must_use]
    pub const fn error_kind(&self) -> Option<GatewayErrorKind> {
        self.error_kind
    }

    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }

    #[must_use]
    pub const fn usage(&self) -> &Usage {
        &self.usage
    }

    #[must_use]
    pub const fn cost(&self) -> &CostEstimate {
        &self.cost
    }

    #[must_use]
    pub const fn timings(&self) -> &ModelRequestTimings {
        &self.timings
    }

    #[must_use]
    pub const fn completed_at(&self) -> SystemTime {
        self.completed_at
    }
}

/// Frozen request plan from the native plugin publication. Implementations must
/// enqueue bounded work synchronously; observation cannot wait on plugin RPCs.
/// The plan itself retains its published processes until queued work finishes.
pub trait RequestObserverPlan: Send + Sync {
    fn dispatch_websocket_response(&self, _observation: WebSocketResponseObservation) {}
    fn dispatch(&self, observation: RequestObservation);
}

pub(super) struct FrozenRequestObservationContext {
    pub request_id: ModelRequestId,
    pub config_revision: ConfigRevision,
    pub client_scope: RequestObservationScope,
    pub operation: OperationKind,
    pub requested_model: Option<PublicModelId>,
    pub extension_scope: ExtensionCallScope,
}

/// A projection of the last attempt actually established by the Provider.
/// Never retain a credential, raw upstream ID, diagnostic or candidate fallback.
pub(super) struct ObservedProvider {
    provider: ProviderKind,
    account_id: ProviderAccountId,
    upstream_model: Option<crate::routing::UpstreamModelId>,
}
impl From<&super::provider::ProviderCallMetadata> for ObservedProvider {
    fn from(metadata: &super::provider::ProviderCallMetadata) -> Self {
        Self {
            provider: metadata.provider().clone(),
            account_id: metadata.provider_account_id().clone(),
            upstream_model: metadata.upstream_model().cloned(),
        }
    }
}

/// Shared only by the original start/coordinator/session cleanup paths. Pending
/// facts are copied from the native finalization; dispatch follows native cleanup.
#[derive(Clone)]
pub(super) struct RequestObservationDispatch(Arc<RequestObservationDispatchState>);
struct RequestObservationDispatchState {
    plan: Arc<dyn RequestObserverPlan>,
    context: FrozenRequestObservationContext,
    pending: Mutex<Option<RequestObservation>>,
    dispatched: AtomicBool,
}
impl RequestObservationDispatch {
    pub(super) fn new(
        plan: Arc<dyn RequestObserverPlan>,
        context: FrozenRequestObservationContext,
    ) -> Self {
        Self(Arc::new(RequestObservationDispatchState {
            plan,
            context,
            pending: Mutex::new(None),
            dispatched: AtomicBool::new(false),
        }))
    }

    fn project(
        &self,
        outcome: RequestObservationOutcome,
        send_state: UpstreamSendState,
        completed_at: SystemTime,
    ) -> RequestObservation {
        let context = &self.0.context;
        let mut observation = RequestObservation::new(
            context.request_id.clone(),
            context.config_revision,
            context.client_scope.clone(),
            context.operation,
            outcome,
            send_state,
            completed_at,
        )
        .with_extension_scope(context.extension_scope.clone());
        observation.requested_model = context.requested_model.clone();
        observation
    }

    pub(super) fn record_finalization(
        &self,
        finalization: &ModelRequestFinalization,
        provider: Option<&ObservedProvider>,
        requested_model: Option<&PublicModelId>,
    ) {
        let outcome = match finalization.outcome {
            ExecutionOutcome::Succeeded => RequestObservationOutcome::Succeeded,
            ExecutionOutcome::Failed => RequestObservationOutcome::Failed,
            ExecutionOutcome::Cancelled => RequestObservationOutcome::Cancelled,
            ExecutionOutcome::Incomplete => RequestObservationOutcome::Incomplete,
            ExecutionOutcome::Running => return,
        };
        let mut observation =
            self.project(outcome, finalization.send_state, finalization.completed_at);
        if observation.requested_model.is_none() {
            observation.requested_model = requested_model.cloned();
        }
        if let Some(provider) = provider {
            observation.provider = Some(provider.provider.clone());
            observation.account_id = Some(provider.account_id.clone());
            observation.upstream_model = provider.upstream_model.clone();
        }
        observation.response_model = finalization.upstream_response_model.clone();
        observation.service_tier = finalization.service_tier.clone();
        observation.attempt_count = finalization.attempt_count;
        observation.client_status_code = finalization.client_status_code;
        observation.upstream_status_code = finalization.upstream_status_code;
        observation.error_kind = finalization.error.as_ref().map(GatewayError::kind);
        observation.retry_after_ms = finalization.retry_after_ms;
        observation.usage = finalization.usage.clone();
        observation.cost = finalization.cost.clone();
        observation.timings = finalization.timings.clone();
        self.0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(observation);
    }

    pub(super) fn record_client_status(&self, status: u16) {
        if let Some(observation) = self
            .0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            observation.client_status_code = Some(status);
        }
    }

    pub(super) fn finish_rejection(&self, error: &GatewayError, latency: std::time::Duration) {
        let mut observation = self.project(
            RequestObservationOutcome::Rejected,
            UpstreamSendState::NotSent,
            SystemTime::now(),
        );
        observation.error_kind = Some(error.kind());
        observation.retry_after_ms = error
            .retry_after()
            .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX));
        observation.timings.latency_ms =
            Some(u64::try_from(latency.as_millis()).unwrap_or(u64::MAX));
        self.0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(observation);
        self.finish();
    }

    pub(super) fn finish(&self) {
        let observation = self
            .0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(observation) = observation
            && !self.0.dispatched.swap(true, Ordering::AcqRel)
        {
            isolate_dispatch(|| self.0.plan.dispatch(observation));
        }
    }

    pub(super) fn websocket_response(
        &self,
        attempt: WebSocketResponseAttempt,
        sequence: u64,
        wire: ProtocolWireEvent,
        requested_model: Option<&PublicModelId>,
    ) {
        if self.0.dispatched.load(Ordering::Acquire) {
            return;
        }
        let context = &self.0.context;
        let mut observation = WebSocketResponseObservation::new(
            context.request_id.clone(),
            context.config_revision,
            context.client_scope.clone(),
            context.operation,
            attempt,
            sequence,
            wire,
        )
        .with_extension_scope(context.extension_scope.clone());
        observation.requested_model = context
            .requested_model
            .clone()
            .or_else(|| requested_model.cloned());
        isolate_dispatch(|| self.0.plan.dispatch_websocket_response(observation));
    }
}

fn isolate_dispatch(dispatch: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(dispatch)).is_err() {
        tracing::warn!("request observation dispatch panicked");
    }
}

pub(super) struct ResponseObservation {
    pub(super) timing_started_at: Instant,
    pub(super) usage: Usage,
    pub(super) cost: CostEstimate,
    pub(super) timings: ModelRequestTimings,
    pub(super) client_response_id: Option<String>,
    pub(super) upstream_response_id: Option<String>,
}

impl ResponseObservation {
    pub(super) fn new(timing_started_at: Instant) -> Self {
        Self {
            timing_started_at,
            usage: Usage::new(),
            cost: CostEstimate::unavailable(),
            timings: ModelRequestTimings::default(),
            client_response_id: None,
            upstream_response_id: None,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.timing_started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub(super) fn finish(&mut self) {
        self.timings.latency_ms = Some(self.elapsed_ms());
    }

    pub(super) fn observe_event(&mut self, event: &GatewayEvent) {
        let elapsed = self.elapsed_ms();
        observe_event_timing(&mut self.timings, event, elapsed);
        if let GatewayEvent::Usage(observed) = event {
            self.usage.merge(observed);
        }
        if let GatewayEvent::CalculatedCost(observed) = event
            && self.cost.source() != CostSource::ProviderReported
        {
            self.cost = observed.clone().into_estimate();
        }
        if let GatewayEvent::ProviderCost(observed) = event {
            self.cost = observed.into_estimate();
        }
    }

    pub(super) fn observe_identity(&mut self, event: &GatewayEvent) {
        let metadata = match event {
            GatewayEvent::Started(metadata) | GatewayEvent::Completed(metadata) => metadata,
            _ => return,
        };
        let response_id = metadata.response_id().to_owned();
        self.client_response_id = Some(response_id.clone());
        self.upstream_response_id = Some(response_id);
    }

    pub(super) fn reset_for_attempt(&mut self) {
        self.usage = Usage::new();
        self.cost = CostEstimate::unavailable();
        self.client_response_id = None;
        self.upstream_response_id = None;
        self.timings.transport_decision_wait_ms = None;
        self.timings.connect_ms = None;
        self.timings.headers_ms = None;
        self.timings.first_event_ms = None;
        self.timings.first_reasoning_ms = None;
        self.timings.first_text_ms = None;
        self.timings.first_token_ms = None;
        self.timings.provider_processing_ms = None;
    }

    pub(super) fn observe_response(&mut self, observation: &ProviderResponseObservation) {
        let observed = observation.timings();
        if let Some(value) = observed.transport_decision_wait_ms {
            self.timings.transport_decision_wait_ms = Some(value);
        }
        if let Some(value) = observed.connect_ms {
            self.timings.connect_ms = Some(value);
        }
        if let Some(value) = observed.headers_ms {
            self.timings.headers_ms = Some(value);
        }
        if let Some(value) = observed.first_event_ms {
            self.timings.first_event_ms = Some(value);
        }
        if let Some(value) = observed.first_reasoning_ms {
            self.timings.first_reasoning_ms = Some(value);
        }
        if let Some(value) = observed.first_text_ms {
            self.timings.first_text_ms = Some(value);
        }
        if let Some(value) = observed.first_token_ms {
            self.timings.first_token_ms = Some(value);
        }
        if let Some(value) = observed.provider_processing_ms {
            self.timings.provider_processing_ms = Some(value);
        }
    }
}

fn observe_event_timing(timings: &mut ModelRequestTimings, event: &GatewayEvent, elapsed_ms: u64) {
    timings.first_event_ms.get_or_insert(elapsed_ms);
    match event {
        GatewayEvent::ReasoningDelta(_) => {
            timings.first_reasoning_ms.get_or_insert(elapsed_ms);
            timings.first_token_ms.get_or_insert(elapsed_ms);
        }
        GatewayEvent::TextDelta(_) => {
            timings.first_text_ms.get_or_insert(elapsed_ms);
            timings.first_token_ms.get_or_insert(elapsed_ms);
        }
        // `response.output_item.added` 会先投影一个空参数的 tool delta；它只是结构帧，
        // 不能抢在真实工具参数之前成为首个可消费 token。
        GatewayEvent::ToolCallDelta(delta) if !delta.arguments_delta.is_empty() => {
            timings.first_token_ms.get_or_insert(elapsed_ms);
        }
        GatewayEvent::CalculatedCost(_) | GatewayEvent::ProviderCost(_) => {}
        _ => {}
    }
}
