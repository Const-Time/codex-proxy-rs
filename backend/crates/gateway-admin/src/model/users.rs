//! User identities and administrator-managed group grants.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserRole {
    Admin,
    User,
}

impl UserRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    pub id: String,
    pub username: String,
    pub role: UserRole,
    pub enabled: bool,
    pub auth_version: i64,
    pub limits: gateway_core::policy::RateLimits,
    pub group_ids: Vec<String>,
    pub quota_multipliers: std::collections::BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Deliberately no Debug: password material must never enter logs.
pub struct CreateUser {
    pub limits: gateway_core::policy::RateLimits,
    pub username: String,
    pub password: String,
    pub group_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateUser {
    pub quota_multipliers: Option<std::collections::BTreeMap<String, String>>,
    pub limits: gateway_core::policy::RateLimits,
    pub id: String,
    pub enabled: bool,
    pub group_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UserGroup {
    pub id: String,
    pub name: String,
    pub color: String,
    pub enabled: bool,
    pub budget: gateway_core::engine::budget::ClientBudgetStatus,
}

/// One currently authorized user/group subscription, including its effective quota.
#[derive(Debug, Clone)]
pub struct UserSubscription {
    pub user_id: String,
    pub username: String,
    pub enabled: bool,
    pub group_id: String,
    pub group_name: String,
    pub quota_multiplier: String,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
    pub daily_used_usd: String,
    pub weekly_used_usd: String,
    pub daily_resets_at: Option<DateTime<Utc>>,
    pub weekly_resets_at: Option<DateTime<Utc>>,
    pub last_reset_at: Option<DateTime<Utc>>,
    pub last_reset_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscriptionTarget {
    pub user_id: String,
    pub group_id: String,
}
