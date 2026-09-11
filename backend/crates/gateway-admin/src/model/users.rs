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
    pub group_ids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Deliberately no Debug: password material must never enter logs.
pub struct CreateUser {
    pub username: String,
    pub password: String,
    pub group_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateUser {
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
