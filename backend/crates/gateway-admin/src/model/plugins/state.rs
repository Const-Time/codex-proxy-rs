//! The schema belongs to stored plugin data; disabled artifact changes do not migrate it.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginStateSchema {
    pub namespace: String,
    pub schema_version: u32,
    pub schema: Value,
    pub maximum_records: u32,
    pub maximum_bytes: u64,
    pub maximum_value_bytes: u32,
    #[serde(default)]
    pub migrates_from: Vec<u32>,
}
#[derive(Clone, PartialEq)]
pub struct PluginStateEntry {
    pub namespace: String,
    pub key: String,
    pub value: Value,
    pub version: u64,
    pub schema_version: u32,
}
#[derive(Clone, Default, PartialEq)]
pub struct PluginStateSnapshot {
    pub namespaces: Vec<PluginStateSchema>,
    pub records: Vec<PluginStateEntry>,
}
/// Both facts are private host data, never a plugin-selected commit boundary.
pub struct PluginStateCommit {
    pub expected: PluginStateSnapshot,
    pub target: PluginStateSnapshot,
}
pub struct PluginStateMigrationBatch {
    pub namespace: String,
    pub from_schema_version: u32,
    pub to_schema_version: u32,
    pub records: Vec<PluginStateEntry>,
}
pub enum PluginStateMigrationAction {
    Keep,
    Replace(Value),
    Delete,
}
pub struct PluginStateMigrationChange {
    pub key: String,
    pub action: PluginStateMigrationAction,
}
