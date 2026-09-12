//! Allowlisted security audit metadata. Never include headers, bodies or secrets.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationLog {
    pub id: String,
    pub occurred_at: DateTime<Utc>,
    pub actor_user_id: Option<String>,
    pub email: Option<String>,
    pub username: Option<String>,
    pub auth_method: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub duration_ms: u64,
    pub client_ip: Option<String>,
    /// A reported forwarding address, not a verified identity.
    pub forwarded_ip: Option<String>,
    pub request_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationLogQuery {
    pub email: Option<String>,
    pub action: Option<String>,
    pub ip: Option<String>,
    pub method: Option<String>,
    pub auth_method: Option<String>,
    pub result: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub page: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationLogPage {
    pub items: Vec<serde_json::Value>,
    pub total: i64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageKeyOption {
    pub id: String,
    pub name: String,
    pub user_id: Option<String>,
}
