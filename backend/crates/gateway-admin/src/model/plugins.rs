//! 插件持久化事实和管理视图；配置与私有状态不实现内容型 Debug。
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginArtifactInfo {
    pub sha256: String,
    pub plugin_id: String,
    pub version: String,
    pub display_name: String,
    pub manifest: Value,
    pub unsupported_capabilities: Vec<String>,
    pub version_warning: Option<String>,
}
#[derive(Clone)]
pub struct PluginArtifact {
    pub info: PluginArtifactInfo,
    pub archive: Arc<[u8]>,
}
#[derive(Clone, PartialEq)]
pub struct PluginRecord {
    pub id: String,
    pub revision: u64,
    pub name: String,
    pub artifact_sha256: String,
    pub enabled: bool,
    pub configuration: Value,
    pub secrets: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginUpdate {
    pub revision: u64,
    pub name: String,
    pub artifact_sha256: String,
    pub enabled: bool,
    #[serde(default)]
    pub trusted_process: bool,
    pub configuration: Value,
    /// Missing entries retain their value; null explicitly removes an entry.
    #[serde(default)]
    pub secrets: BTreeMap<String, Option<String>>,
}
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginPage {
    pub id: String,
    pub title: String,
    pub entry: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginView {
    pub id: String,
    pub revision: u64,
    pub name: String,
    pub artifact: PluginArtifactInfo,
    pub enabled: bool,
    pub status: String,
    pub configuration: Value,
    pub configured_secrets: Vec<String>,
    pub pages: Vec<PluginPage>,
    pub resources: Vec<PluginResource>,
}
pub struct PluginHttpRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}
pub struct PluginHttpResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}
pub struct PluginStateScope {
    pub instance_id: String,
    pub revision: u64,
    pub namespace: String,
    pub schema_version: u32,
    pub maximum_records: u32,
    pub maximum_bytes: u64,
}
pub struct PluginStateRecord {
    pub value: Value,
    pub version: u64,
    pub schema_version: u32,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginResource {
    pub path: String,
    pub content_type: String,
}

/// Shared generation revocation; the API keeps this lease for the complete model response lifetime.
#[derive(Clone, Default)]
pub struct PluginAccess(gateway_core::lifecycle::CancellationToken);
impl PluginAccess {
    pub fn revoke(&self) {
        self.0.cancel();
    }
    pub fn is_revoked(&self) -> bool {
        self.0.is_cancelled()
    }
    pub async fn revoked(&self) {
        self.0.cancelled().await;
    }
}
