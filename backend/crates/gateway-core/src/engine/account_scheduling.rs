//! Plugin choices consume Provider-qualified facts; Core retains scope and eligibility.
use super::{AttemptContext, nested::NestedExecutionContext};
use crate::{
    account::{
        AccountCandidate, AccountSelection, AccountSelectionContext, AccountSelector,
        ProviderAccountId,
    },
    identity::ProviderKind,
    lifecycle::{CancellationToken, Deadline},
};
use futures::{FutureExt, future::BoxFuture, pin_mut, select_biased};
use std::{sync::Arc, time::SystemTime};

#[derive(Debug, Clone)]
pub struct AccountScheduleCandidate {
    pub account_id: ProviderAccountId,
    pub weight: u16,
    pub in_flight: u32,
    pub maximum_concurrency: u32,
    pub last_started_at: Option<SystemTime>,
    pub quota_reset_at: Option<SystemTime>,
    pub quota_remaining_rank: Option<u64>,
    pub failure_rate_basis_points: Option<u16>,
    pub first_output_latency_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AccountScheduleInput {
    parent: NestedExecutionContext,
    attempt_index: std::num::NonZeroU32,
    provider: ProviderKind,
    model: Option<String>,
    candidates: Vec<AccountScheduleCandidate>,
    deadline: Deadline,
}
impl AccountScheduleInput {
    pub fn request_id(&self) -> &super::ModelRequestId {
        self.parent.request_id()
    }
    pub fn attempt_index(&self) -> std::num::NonZeroU32 {
        self.attempt_index
    }
    pub fn client_key_id(&self) -> &crate::policy::ClientApiKeyId {
        self.parent.client().policy().key_id()
    }
    pub fn account_group_ids(&self) -> Vec<crate::routing::AccountGroupId> {
        self.parent
            .client()
            .policy()
            .account_scope()
            .routing_snapshot()
            .groups_snapshot()
            .iter()
            .map(|group| group.id().clone())
            .collect()
    }
    pub fn excluded_instances(&self) -> &std::collections::BTreeSet<String> {
        self.parent.scope().excluded_instances()
    }
    pub fn nested_context(&self) -> NestedExecutionContext {
        self.parent.clone()
    }
    pub fn provider(&self) -> &ProviderKind {
        &self.provider
    }
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }
    pub fn candidates(&self) -> &[AccountScheduleCandidate] {
        &self.candidates
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.parent.cancellation()
    }
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountScheduleDecision {
    Delegate,
    Reject,
    Pick(ProviderAccountId),
}
pub struct AccountScheduleResult {
    pub decision: AccountScheduleDecision,
    /// Successful consultations stay attached to the original root cancellation.
    pub revocation: CancellationToken,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccountPolicyError {
    #[error("account scheduling policy rejected the request")]
    Rejected,
    #[error("account scheduling policy failed")]
    Fault,
    #[error("account scheduling candidate became stale")]
    StaleCandidate,
    #[error("account scheduling was cancelled")]
    Cancelled,
    #[error("account scheduling deadline elapsed")]
    Deadline,
}
impl AccountPolicyError {
    pub fn into_provider_error(self) -> crate::error::ProviderError {
        use crate::{
            error::{ProviderError, ProviderErrorKind},
            upstream::UpstreamSendState,
        };
        ProviderError::new(
            match self {
                Self::Rejected => ProviderErrorKind::ExecutionPolicyRejected,
                Self::Fault => ProviderErrorKind::ExecutionPolicyUnavailable,
                Self::StaleCandidate => ProviderErrorKind::NoEligibleAccount,
                Self::Cancelled => ProviderErrorKind::Cancelled,
                Self::Deadline => ProviderErrorKind::Timeout,
            },
            UpstreamSendState::NotSent,
        )
    }
}
#[derive(Debug, Clone, Copy)]
pub struct AccountSchedulingFault;
pub trait AccountScheduler: Send + Sync {
    fn cancellation(&self) -> CancellationToken;
    fn schedule_account(
        &self,
        input: AccountScheduleInput,
    ) -> BoxFuture<'static, Result<AccountScheduleResult, AccountSchedulingFault>>;
}

#[derive(Clone)]
pub struct AccountScheduling(pub(crate) Arc<dyn AccountScheduler>);
impl std::fmt::Debug for AccountScheduling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountScheduling").finish_non_exhaustive()
    }
}

impl AttemptContext {
    /// Invoke before acquiring a lease. A hard pin is never a soft preference.
    pub async fn select_account<'a>(
        &self,
        provider: &ProviderKind,
        model: Option<&str>,
        candidates: &'a [AccountCandidate],
        context: &AccountSelectionContext,
        required: Option<&ProviderAccountId>,
    ) -> Result<Option<AccountSelection<'a>>, AccountPolicyError> {
        let Some(scheduler) = self
            .request
            .account_scheduler
            .as_ref()
            .filter(|_| !self.is_diagnostic_required_account())
        else {
            return Ok(AccountSelector.select(candidates, context));
        };
        let parent = self.nested_context().ok_or(AccountPolicyError::Fault)?;
        parent
            .validate()
            .map_err(|_| AccountPolicyError::Cancelled)?;
        let mut constrained = context.clone();
        for candidate in candidates {
            if candidate.account.provider() != provider
                || required.is_some_and(|required| required != candidate.account.id())
                || model.is_some_and(|model| {
                    constrained
                        .account_scope
                        .as_ref()
                        .is_some_and(|scope| !scope.allows_model(candidate.account.id(), model))
                })
            {
                constrained
                    .excluded_accounts
                    .insert(candidate.account.id().clone());
            }
        }
        let eligible = AccountSelector.policy_candidates(candidates, &constrained);
        if eligible.is_empty() {
            return Ok(None);
        }
        let input = AccountScheduleInput {
            parent: parent.clone().with_revocation(self.cancellation().clone()),
            attempt_index: self.attempt_index(),
            provider: provider.clone(),
            model: model.map(str::to_owned),
            deadline: self.deadline(),
            candidates: eligible
                .iter()
                .map(|candidate| AccountScheduleCandidate {
                    account_id: candidate.account.id().clone(),
                    weight: candidate.account.weight().get(),
                    in_flight: candidate.signals.in_flight,
                    maximum_concurrency: candidate
                        .account
                        .effective_concurrency(context.policy.max_concurrent_per_account())
                        .get(),
                    last_started_at: candidate.signals.last_started_at,
                    quota_reset_at: candidate.signals.quota_reset_at,
                    quota_remaining_rank: candidate.signals.quota_remaining_rank,
                    failure_rate_basis_points: candidate.signals.failure_rate_basis_points,
                    first_output_latency_ms: candidate.signals.first_output_latency_ms,
                })
                .collect(),
        };
        let call = scheduler.0.schedule_account(input).fuse();
        let cancellation = self.cancellation().cancelled().fuse();
        let deadline = self.deadline().wait().fuse();
        pin_mut!(call, cancellation, deadline);
        let result = select_biased! {
            _ = cancellation => return Err(AccountPolicyError::Cancelled),
            _ = deadline => return Err(AccountPolicyError::Deadline),
            result = call => result.map_err(|_| AccountPolicyError::Fault)?,
        };
        if !self.cancellation().link_parent(result.revocation) {
            return Err(AccountPolicyError::Fault);
        }
        if self.cancellation().is_cancelled() || parent.validate().is_err() {
            return Err(AccountPolicyError::Cancelled);
        }
        constrained.now = SystemTime::now();
        match result.decision {
            AccountScheduleDecision::Reject => Err(AccountPolicyError::Rejected),
            AccountScheduleDecision::Delegate => AccountSelector
                .select(candidates, &constrained)
                .map(Some)
                .ok_or(AccountPolicyError::StaleCandidate),
            AccountScheduleDecision::Pick(account) => {
                if !eligible
                    .iter()
                    .any(|candidate| candidate.account.id() == &account)
                {
                    return Err(AccountPolicyError::Fault);
                }
                AccountSelector
                    .select_policy_candidate(candidates, &constrained, &account)
                    .map(Some)
                    .ok_or(AccountPolicyError::StaleCandidate)
            }
        }
    }
}
