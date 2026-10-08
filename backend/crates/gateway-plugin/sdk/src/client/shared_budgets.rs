//! Typed client for the CPR shared-budget extension.
use super::{HostClient, payload_call};
use crate::{PluginFault, call::shared_budgets as wire};
impl HostClient {
    /// Read shared limits and retained usage without changing a window.
    /// # Errors
    /// Fails if unsupported, unauthorized, stale or unavailable.
    pub async fn shared_budget(
        &self,
        target: wire::Target,
    ) -> Result<wire::SharedBudget, PluginFault> {
        payload_call(self, wire::GET, target).await
    }
    /// Update base group limits with the config revision returned by shared_budget.
    /// On conflict or an uncertain result, read current limits before deciding to retry.
    /// # Errors
    /// Fails if unsupported, unauthorized, invalid, stale or unavailable.
    pub async fn update_shared_group_limits(
        &self,
        command: wire::UpdateGroupLimits,
    ) -> Result<wire::GroupLimits, PluginFault> {
        payload_call(self, wire::UPDATE_GROUP_LIMITS, command).await
    }
    /// Reset both windows for explicit user/group pairs. An uncertain result must be
    /// retried with the same event ID and targets, including after a plugin upgrade.
    /// # Errors
    /// Fails if unsupported, unauthorized, invalid, stale or unavailable.
    pub async fn reset_shared_budgets(
        &self,
        command: wire::Reset,
    ) -> Result<wire::ResetResult, PluginFault> {
        payload_call(self, wire::RESET, command).await
    }
}
