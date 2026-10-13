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
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    input: AccountScheduleInput,
) -> Result<AccountScheduleDecision, AccountSchedulingFault> {
    if session.capability_version(Capability::Scheduler) != Some(1)
        || !session.capability_supports_stage(Capability::Scheduler, Stage::Scheduling)
    {
        return Err(AccountSchedulingFault);
    }
    let timeout = input
        .deadline()
        .bounded(Duration::from_secs(2).min(session.maximum_call_timeout()));
    if timeout.is_zero() || input.cancellation().is_cancelled() {
        return Err(AccountSchedulingFault);
    }
    let mut context = session.context(Stage::Scheduling, timeout);
    context.request_id = Some(input.request_id().as_str().into());
    context.attempt_id = Some(format!(
        "{}:{}",
        input.request_id().as_str(),
        input.attempt_index()
    ));
    if let Some(models) = &models {
        models
            .bind(
                &context,
                Some(input.nested_context()),
                None,
                input.cancellation(),
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
        let pick = fields.get("decision").and_then(serde_json::Value::as_str) == Some("pick");
        fields.len() == (if pick { 2 } else { 1 })
            && fields
                .keys()
                .all(|key| key == "decision" || (pick && key == "account_id"))
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
        WireDecision::Pick { account_id } => {
            if !input
                .candidates()
                .iter()
                .any(|c| c.account_id.as_str() == account_id)
            {
                session.invalid_response(Stage::Scheduling);
                return Err(AccountSchedulingFault);
            }
            let account = ProviderAccountId::new(account_id).map_err(|_| AccountSchedulingFault)?;
            Ok(AccountScheduleDecision::Pick(account))
        }
    }
}
