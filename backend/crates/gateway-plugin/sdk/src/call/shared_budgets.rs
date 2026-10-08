//! CPR extension v1: explicit user/group budgets. Never aliases the upstream Key budget API.
use serde::{Deserialize, Serialize};
pub const GET: &str = "host.cpr.shared_budgets.v1.get";
pub const UPDATE_GROUP_LIMITS: &str = "host.cpr.shared_budgets.v1.update_group_limits";
pub const RESET: &str = "host.cpr.shared_budgets.v1.reset";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub user_id: String,
    pub group_id: String,
}
/// Stored window observations; reading does not roll a window or refresh upstream quota.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedBudget {
    pub revision: u64,
    pub target: Target,
    pub enabled: bool,
    pub group_daily_limit_usd: String,
    pub group_weekly_limit_usd: String,
    pub quota_multiplier: String,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
    pub daily_used_usd: String,
    pub weekly_used_usd: String,
    pub daily_resets_at_ms: Option<i64>,
    pub weekly_resets_at_ms: Option<i64>,
    pub last_reset_at_ms: Option<i64>,
}
/// Changes base limits for every user of this group, preserving their multipliers and usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGroupLimits {
    pub group_id: String,
    pub expected_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daily_limit_usd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weekly_limit_usd: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLimits {
    pub revision: u64,
    pub group_id: String,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
}
/// Resets daily and weekly usage for all Keys sharing exactly these user/group pairs.
/// Reuse the UUIDv4 event_id after any uncertain result. Never retry with a new ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reset {
    pub event_id: String,
    pub targets: Vec<Target>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetResult {
    pub revision: u64,
    pub affected: u64,
}
