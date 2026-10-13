//! Retry decisions contain safe facts only and cannot expand Core recovery.
use crate::RpcSession;
use gateway_core::engine::retry_policy::{RetryDecision, RetryInput, RetryPolicyFault};
use gateway_plugin_sdk::{
    Capability, SendState, Stage,
    call::policy::{RetryAction, RetryDecision as WireDecision, RetryDecisionRequest},
};
use std::{sync::Arc, time::Duration};

pub(crate) async fn invoke(
    session: &Arc<RpcSession>,
    outbound: Arc<crate::outbound::Registry>,
    input: RetryInput,
    timeout: Duration,
) -> Result<RetryDecision, RetryPolicyFault> {
    if session.capability_version(Capability::RetryPolicy) != Some(1)
        || !session.capability_supports_stage(Capability::RetryPolicy, Stage::Retry)
    {
        return Err(RetryPolicyFault);
    }
    let timeout = input
        .deadline()
        .bounded(timeout.min(session.maximum_call_timeout()));
    if timeout.is_zero() || input.cancellation().is_cancelled() {
        return Err(RetryPolicyFault);
    }
    let mut allowed_actions = vec![RetryAction::Stop];
    if input.retry_allowed() {
        allowed_actions.push(RetryAction::Retry);
    }
    let request = RetryDecisionRequest {
        request_id: input.request_id().as_str().into(),
        attempt_index: input.attempt_index().get(),
        provider: input.provider().as_str().into(),
        model: input.model().map(str::to_owned),
        error_kind: input.error_kind().as_str().into(),
        upstream_status: input.upstream_status(),
        send_state: match input.send_state() {
            gateway_core::upstream::UpstreamSendState::NotSent => SendState::NotSent,
            gateway_core::upstream::UpstreamSendState::Sent => SendState::Sent,
            gateway_core::upstream::UpstreamSendState::Ambiguous => SendState::Ambiguous,
        },
        remaining_routing_attempts: input.remaining_routing_attempts(),
        remaining_deadline_ms: input.deadline().remaining().map_or(u64::MAX, |left| {
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        }),
        allowed_actions,
    };
    let mut context = session.context(Stage::Retry, timeout);
    context.request_id = Some(request.request_id.clone());
    context.attempt_id = Some(format!("{}:{}", request.request_id, request.attempt_index));
    outbound
        .bind(
            &context,
            input.cancellation(),
            Some(input.call_scope().clone()),
        )
        .map_err(|_| RetryPolicyFault)?;
    let _network = outbound.guard(&context);
    let reply = session
        .call(
            "policy.retry_decision",
            context,
            serde_json::to_value(request).map_err(|_| RetryPolicyFault)?,
            vec![],
        )
        .await
        .map_err(|_| RetryPolicyFault)?;
    if !reply.payload.is_empty()
        || reply
            .result
            .as_object()
            .is_none_or(|fields| fields.len() != 1 || !fields.contains_key("decision"))
    {
        session.invalid_response(Stage::Retry);
        return Err(RetryPolicyFault);
    }
    match session
        .decode_response::<WireDecision>(Stage::Retry, reply.result)
        .map_err(|_| RetryPolicyFault)?
    {
        WireDecision::Delegate => Ok(RetryDecision::Delegate),
        WireDecision::Stop => Ok(RetryDecision::Stop),
        WireDecision::Retry if input.retry_allowed() => Ok(RetryDecision::Retry),
        WireDecision::Retry => {
            session.invalid_response(Stage::Retry);
            Err(RetryPolicyFault)
        }
    }
}
