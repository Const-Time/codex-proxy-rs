//! SDK scheduling sees only Core-qualified candidates; Provider acquires the lease.
use crate::{RpcSession, models::Models};
use gateway_core::{account::ProviderAccountId, engine::account_scheduling::*};
use gateway_plugin_sdk::{
    Capability, Stage,
    call::policy::{
        AccountScheduleCandidate as WireCandidate, AccountScheduleDecision as WireDecision,
        AccountScheduleRequest,
    },
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

struct ModelScope {
    models: Option<Arc<Models>>,
    context: gateway_plugin_sdk::CallContext,
}
impl Drop for ModelScope {
    fn drop(&mut self) {
        if let Some(models) = &self.models {
            models.finish(&self.context);
        }
    }
}
fn milliseconds(time: Option<SystemTime>) -> Option<u64> {
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}
pub(crate) async fn invoke(
    session: &Arc<RpcSession>,
    models: Option<Arc<Models>>,
    outbound: Arc<crate::outbound::Registry>,
    services: Arc<crate::services::Registry>,
    input: AccountScheduleInput,
) -> Result<AccountScheduleDecision, AccountSchedulingFault> {
    if session.capability_version(Capability::Scheduler) != Some(4)
        || !session.capability_supports_stage(Capability::Scheduler, Stage::Scheduling)
    {
        return Err(AccountSchedulingFault);
    }
    let timeout = input
        .deadline()
        .bounded(session.budget(gateway_plugin_sdk::Stage::Scheduling));
    if timeout.is_zero() || input.cancellation().is_cancelled() {
        return Err(AccountSchedulingFault);
    }
    let mut context = session.context(Stage::Scheduling, timeout);
    context.request_id = Some(input.request_id().as_str().into());
    context.request = crate::request_context::wire(input.nested_context().scope());
    context.attempt_id = Some(format!(
        "{}:{}",
        input.request_id().as_str(),
        input.attempt_index()
    ));
    outbound
        .bind(
            &context,
            input.cancellation(),
            Some(input.nested_context().scope().clone()),
        )
        .map_err(|_| AccountSchedulingFault)?;
    let _network = outbound.guard(&context);
    let _services = services
        .bind_parent(&context, &input.nested_context(), input.cancellation())
        .map_err(|_| AccountSchedulingFault)?;
    if let Some(models) = &models {
        models
            .bind(
                &context,
                Some(input.nested_context()),
                None,
                input.cancellation(),
                None,
                None,
            )
            .map_err(|_| AccountSchedulingFault)?;
    }
    let _scope = ModelScope {
        models,
        context: context.clone(),
    };
    let request = AccountScheduleRequest {
        request_id: input.request_id().as_str().into(),
        attempt_index: input.attempt_index().get(),
        provider: input.provider().as_str().into(),
        model: input.model().map(str::to_owned),
        candidates: input
            .candidates()
            .iter()
            .map(|c| WireCandidate {
                account_id: c.account_id.as_str().into(),
                available: c.available,
                session_bound: c.session_bound,
                role: match c.role {
                    AccountScheduleRole::Ordinary => {
                        gateway_plugin_sdk::call::policy::AccountScheduleRole::Ordinary
                    }
                    AccountScheduleRole::Preferred => {
                        gateway_plugin_sdk::call::policy::AccountScheduleRole::Preferred
                    }
                    AccountScheduleRole::FollowOnly => {
                        gateway_plugin_sdk::call::policy::AccountScheduleRole::FollowOnly
                    }
                    AccountScheduleRole::Pinned => {
                        gateway_plugin_sdk::call::policy::AccountScheduleRole::Pinned
                    }
                },
                weight: c.weight,
                in_flight: c.in_flight,
                maximum_concurrency: c.maximum_concurrency,
                last_started_at_ms: milliseconds(c.last_started_at),
                quota_reset_at_ms: milliseconds(c.quota_reset_at),
                quota_remaining_rank: c.quota_remaining_rank,
                failure_rate_basis_points: c.failure_rate_basis_points,
                first_output_latency_ms: c.first_output_latency_ms,
            })
            .collect(),
    };
    let reply = session
        .call(
            "policy.schedule_account",
            context,
            serde_json::to_value(request).map_err(|_| AccountSchedulingFault)?,
            vec![],
        )
        .await
        .map_err(|_| AccountSchedulingFault)?;
    if !reply.payload.is_empty() {
        session.invalid_response(Stage::Scheduling);
        return Err(AccountSchedulingFault);
    }
    // Serde internally tagged unit variants accept extra fields despite
    // deny_unknown_fields. Enforce the wire shape before decoding the decision.
    let valid_shape = reply.result.as_object().is_some_and(|fields| {
        let member = match fields.get("decision").and_then(serde_json::Value::as_str) {
            Some("pick") => Some("account_id"),
            Some("delegate_filtered") => Some("exclude_account_ids"),
            Some("delegate" | "reject") => None,
            _ => return false,
        };
        fields.len() == if member.is_some() { 2 } else { 1 }
            && fields
                .keys()
                .all(|key| key == "decision" || member == Some(key.as_str()))
    });
    if !valid_shape {
        session.invalid_response(Stage::Scheduling);
        return Err(AccountSchedulingFault);
    }
    let decision: WireDecision = session
        .decode_response(Stage::Scheduling, reply.result)
        .map_err(|_| AccountSchedulingFault)?;
    match decision {
        WireDecision::Delegate => Ok(AccountScheduleDecision::Delegate),
        WireDecision::Reject => Ok(AccountScheduleDecision::Reject),
        WireDecision::DelegateFiltered {
            exclude_account_ids,
        } => {
            let unique: std::collections::BTreeSet<_> = exclude_account_ids.iter().collect();
            if unique.len() != exclude_account_ids.len()
                || exclude_account_ids.iter().any(|id| {
                    !input
                        .candidates()
                        .iter()
                        .any(|c| c.account_id.as_str() == id)
                })
            {
                session.invalid_response(Stage::Scheduling);
                return Err(AccountSchedulingFault);
            }
            let excluded = exclude_account_ids
                .into_iter()
                .map(ProviderAccountId::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| AccountSchedulingFault)?;
            Ok(AccountScheduleDecision::DelegateFiltered(excluded))
        }
        WireDecision::Pick { account_id } => {
            if !input
                .candidates()
                .iter()
                .any(|c| c.account_id.as_str() == account_id && c.available)
            {
                session.invalid_response(Stage::Scheduling);
                return Err(AccountSchedulingFault);
            }
            let account = ProviderAccountId::new(account_id).map_err(|_| AccountSchedulingFault)?;
            Ok(AccountScheduleDecision::Pick(account))
        }
    }
}
