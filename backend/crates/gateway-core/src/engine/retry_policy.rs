//! Policies may veto recovery; Core alone chooses accounts, delays and budgets.
use super::{EngineError, ModelRequestId};
use crate::{
    error::ProviderErrorKind,
    identity::ProviderKind,
    lifecycle::{CancellationToken, Deadline},
    policy::ClientApiKeyId,
    routing::AccountGroupId,
    upstream::UpstreamSendState,
};
use futures::{FutureExt, future::BoxFuture, pin_mut, select_biased};
use std::{collections::BTreeSet, num::NonZeroU32, sync::Arc, time::Duration};

pub const MAXIMUM_POLICY_WAIT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct RetryInput {
    pub(crate) service_factory: Option<Arc<dyn super::middleware::service::Factory>>,
    pub(crate) request_id: ModelRequestId,
    pub(crate) call_scope: super::nested::ExtensionCallScope,
    pub(crate) client_key_id: ClientApiKeyId,
    pub(crate) account_group_ids: Vec<AccountGroupId>,
    pub(crate) excluded_instances: BTreeSet<String>,
    pub(crate) attempt_index: NonZeroU32,
    pub(crate) provider: ProviderKind,
    pub(crate) model: Option<String>,
    pub(crate) error_kind: ProviderErrorKind,
    pub(crate) upstream_status: Option<u16>,
    pub(crate) send_state: UpstreamSendState,
    pub(crate) remaining_routing_attempts: u32,
    pub(crate) retry_allowed: bool,
    pub(crate) deadline: Deadline,
    pub(crate) cancellation: CancellationToken,
}
impl RetryInput {
    pub fn service_factory(&self) -> Option<Arc<dyn super::middleware::service::Factory>> {
        self.service_factory.clone()
    }
    pub fn call_scope(&self) -> &super::nested::ExtensionCallScope {
        &self.call_scope
    }
    pub fn request_id(&self) -> &ModelRequestId {
        &self.request_id
    }
    pub fn client_key_id(&self) -> &ClientApiKeyId {
        &self.client_key_id
    }
    pub fn account_group_ids(&self) -> &[AccountGroupId] {
        &self.account_group_ids
    }
    pub fn excluded_instances(&self) -> &BTreeSet<String> {
        &self.excluded_instances
    }
    pub fn attempt_index(&self) -> NonZeroU32 {
        self.attempt_index
    }
    pub fn provider(&self) -> &ProviderKind {
        &self.provider
    }
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }
    pub fn error_kind(&self) -> ProviderErrorKind {
        self.error_kind
    }
    pub fn upstream_status(&self) -> Option<u16> {
        self.upstream_status
    }
    pub fn send_state(&self) -> UpstreamSendState {
        self.send_state
    }
    pub fn remaining_routing_attempts(&self) -> u32 {
        self.remaining_routing_attempts
    }
    pub fn retry_allowed(&self) -> bool {
        self.retry_allowed
    }
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    Delegate,
    Stop,
    Retry,
}
pub struct RetryResult {
    pub decision: RetryDecision,
    pub revocation: CancellationToken,
}
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicyFault;
pub trait RetryPolicy: Send + Sync {
    fn cancellation(&self) -> CancellationToken;
    fn decide_retry(
        &self,
        input: RetryInput,
    ) -> BoxFuture<'static, Result<RetryResult, RetryPolicyFault>>;
}

/// Owned by the existing execution session so dropping a read does not reissue RPC.
pub(crate) fn invoke(
    policy: Option<Arc<dyn RetryPolicy>>,
    input: RetryInput,
) -> BoxFuture<'static, Result<RetryDecision, EngineError>> {
    let timeout = input.deadline.bounded(MAXIMUM_POLICY_WAIT);
    Box::pin(async move {
        if input.cancellation.is_cancelled() {
            return Err(EngineError::Cancelled);
        }
        if input.deadline.is_elapsed() {
            return Err(EngineError::Deadline);
        }
        let Some(policy) = policy else {
            return Ok(RetryDecision::Delegate);
        };
        let cancellation = input.cancellation.clone();
        let deadline = input.deadline;
        let decision = policy.decide_retry(input).fuse();
        let cancelled = cancellation.cancelled().fuse();
        let expired = deadline.wait().fuse();
        let timeout = futures_timer::Delay::new(timeout).fuse();
        pin_mut!(decision, cancelled, expired, timeout);
        select_biased! {
            () = cancelled => Err(EngineError::Cancelled),
            () = expired => Err(EngineError::Deadline),
            () = timeout => Ok(RetryDecision::Delegate),
            result = decision => {
                match result {
                    Ok(result) if cancellation.link_parent(result.revocation.clone()) => {
                        if cancellation.is_cancelled() { Err(EngineError::Cancelled) }
                        else if deadline.is_elapsed() { Err(EngineError::Deadline) }
                        else { Ok(result.decision) }
                    }
                    _ => {
                        if cancellation.is_cancelled() { Err(EngineError::Cancelled) }
                        else if deadline.is_elapsed() { Err(EngineError::Deadline) }
                        else { Ok(RetryDecision::Delegate) }
                    },
                }
            },
        }
    })
}
