//! A typed service terminal keeps authorization, validation, transactions and side effects.
use crate::lifecycle::{CancellationToken, Deadline};
pub type Value = serde_json::Value;
pub type Next = crate::middleware::Next<Value, Value, Error>;
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
#[error("{message}")]
pub struct Error {
    pub kind: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}
impl Error {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: "invalid".into(),
            message: message.into(),
            details: None,
        }
    }
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: "unavailable".into(),
            message: message.into(),
            details: None,
        }
    }
}
/// Publication and the authenticated caller are captured by the terminal. This
/// context cannot mint an administrative principal or model execution authority.
#[derive(Clone, Debug)]
pub struct Context {
    pub operation: &'static str,
    pub request_id: String,
    pub call_id: String,
    pub parent_call_id: Option<String>,
    pub cancellation: CancellationToken,
    pub deadline_at: Deadline,
    pub call_scope: crate::engine::nested::ExtensionCallScope,
}
