//! Wire adapter for explicit shared-budget operations; caller identity remains host-owned.
use crate::RpcReply;
use gateway_admin::{
    model::{AdminError, AdminErrorKind, plugins::*, users::SubscriptionTarget},
    ports::plugins::PluginQuotaService,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::shared_budgets as wire};
use serde_json::{Value, json};
fn fault(error: AdminError) -> PluginFault {
    PluginFault::new(
        match error.kind() {
            AdminErrorKind::Invalid => ErrorCode::InvalidInput,
            AdminErrorKind::Conflict => ErrorCode::Conflict,
            AdminErrorKind::NotFound => ErrorCode::Rejected,
            _ => ErrorCode::Uncertain,
        },
        "shared quota operation failed; read current limits or retry a reset with the same event ID",
    )
}
pub(crate) async fn call(
    service: &dyn PluginQuotaService,
    scope: PluginMutationScope,
    method: &str,
    params: Value,
    payload: &[u8],
) -> Result<RpcReply, PluginFault> {
    let invalid = || PluginFault::new(ErrorCode::InvalidInput, "invalid shared quota request");
    if params != json!({}) || payload.len() > 128 * 1024 {
        return Err(invalid());
    }
    let target = |v: wire::Target| SubscriptionTarget {
        user_id: v.user_id,
        group_id: v.group_id,
    };
    let payload = match method {
        wire::GET => {
            let v: wire::Target = serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service.get(scope, target(v)).await.map_err(fault)?;
            serde_json::to_vec(&wire::SharedBudget {
                revision: r.revision.get(),
                target: wire::Target {
                    user_id: r.target.user_id,
                    group_id: r.target.group_id,
                },
                enabled: r.enabled,
                group_daily_limit_usd: r.group_daily_limit_usd,
                group_weekly_limit_usd: r.group_weekly_limit_usd,
                quota_multiplier: r.quota_multiplier,
                daily_limit_usd: r.daily_limit_usd,
                weekly_limit_usd: r.weekly_limit_usd,
                daily_used_usd: r.daily_used_usd,
                weekly_used_usd: r.weekly_used_usd,
                daily_resets_at_ms: r.daily_resets_at.map(|t| t.timestamp_millis()),
                weekly_resets_at_ms: r.weekly_resets_at.map(|t| t.timestamp_millis()),
                last_reset_at_ms: r.last_reset_at.map(|t| t.timestamp_millis()),
            })
        }
        wire::UPDATE_GROUP_LIMITS => {
            let v: wire::UpdateGroupLimits =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service
                .update_group_limits(
                    scope,
                    PluginGroupBudgetUpdate {
                        group_id: v.group_id,
                        expected_revision: v.expected_revision,
                        daily_limit_usd: v.daily_limit_usd,
                        weekly_limit_usd: v.weekly_limit_usd,
                    },
                )
                .await
                .map_err(fault)?;
            serde_json::to_vec(&wire::GroupLimits {
                revision: r.revision.get(),
                group_id: r.value.group_id,
                daily_limit_usd: r.value.daily_limit_usd,
                weekly_limit_usd: r.value.weekly_limit_usd,
            })
        }
        wire::RESET => {
            let v: wire::Reset = serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service
                .reset(
                    scope,
                    PluginBudgetReset {
                        event_id: v.event_id,
                        targets: v.targets.into_iter().map(target).collect(),
                    },
                )
                .await
                .map_err(fault)?;
            serde_json::to_vec(&wire::ResetResult {
                revision: r.revision.get(),
                affected: r.value,
            })
        }
        _ => {
            return Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unsupported shared quota operation",
            ));
        }
    }
    .map_err(|_| invalid())?;
    Ok(RpcReply {
        result: json!({}),
        payload,
    })
}
