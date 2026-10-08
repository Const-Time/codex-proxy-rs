//! 插件用例、进程与持久化端口；具体实现只在组合根装配。
use crate::model::{AdminError, MutationContext, plugins::*};
use async_trait::async_trait;
use std::sync::Arc;
type Result<T> = std::result::Result<T, AdminError>;

#[async_trait]
pub trait PluginStore: Send + Sync {
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>>;
    async fn delete_artifact(&self, digest: &str, actor: &MutationContext) -> Result<()>;
    async fn artifact_info(&self, digest: &str) -> Result<PluginArtifactInfo>;
    async fn artifact(&self, digest: &str) -> Result<PluginArtifact>;
    async fn put_artifact(&self, artifact: PluginArtifact, actor: &MutationContext) -> Result<()>;
    async fn list(&self) -> Result<Vec<PluginRecord>>;
    async fn save(
        &self,
        record: &PluginRecord,
        expected: u64,
        actor: &MutationContext,
    ) -> Result<()>;
    async fn delete(&self, id: &str, expected: u64, actor: &MutationContext) -> Result<()>;
    async fn state_get(
        &self,
        scope: &PluginStateScope,
        key: &str,
    ) -> Result<Option<PluginStateRecord>>;
    async fn state_put(
        &self,
        scope: &PluginStateScope,
        key: &str,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
    ) -> Result<u64>;
}
#[async_trait]
pub trait PluginProcess: Send + Sync {
    fn access(&self) -> PluginAccess;
    fn status(&self) -> &'static str;
    fn pages(&self) -> Vec<PluginPage>;
    fn resources(&self) -> Vec<PluginResource>;
    fn activate(&self);
    fn revoke(&self);
    async fn stop(&self);
    async fn resource(&self, path: &str) -> Result<PluginHttpResponse>;
    async fn handle(&self, request: PluginHttpRequest) -> Result<PluginHttpResponse>;
}
#[async_trait]
pub trait PluginRuntime: Send + Sync {
    fn describe(&self, info: PluginArtifactInfo) -> Result<PluginArtifactInfo>;
    async fn inspect(&self, archive: Arc<[u8]>) -> Result<PluginArtifact>;
    async fn validate(
        &self,
        record: &PluginRecord,
        artifact: &PluginArtifact,
        previous: Option<&PluginArtifact>,
    ) -> Result<()>;
    async fn prepare(
        &self,
        record: &PluginRecord,
        artifact: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>>;
}
#[async_trait]
pub trait PluginService: Send + Sync {
    fn access(&self, id: &str, revision: u64) -> Result<PluginAccess>;
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>>;
    async fn delete_artifact(&self, digest: &str, actor: &MutationContext) -> Result<()>;
    async fn upload(
        &self,
        archive: Arc<[u8]>,
        actor: &MutationContext,
    ) -> Result<PluginArtifactInfo>;
    async fn list(&self) -> Result<Vec<PluginView>>;
    async fn update(
        &self,
        id: Option<&str>,
        update: PluginUpdate,
        actor: &MutationContext,
    ) -> Result<PluginView>;
    async fn delete(&self, id: &str, revision: u64, actor: &MutationContext) -> Result<()>;
    async fn resource(&self, id: &str, revision: u64, path: &str) -> Result<PluginHttpResponse>;
    async fn handle(
        &self,
        id: &str,
        revision: u64,
        request: PluginHttpRequest,
    ) -> Result<PluginHttpResponse>;
}

/// Only management callbacks can currently obtain this capability. It never refreshes upstream quota.
#[async_trait]
pub trait PluginDataService: Send + Sync {
    async fn key(&self, id: &str) -> Result<PluginKeyFacts>;
    async fn accounts(&self, query: PluginAccountQuery) -> Result<PluginAccountPage>;
    async fn quota(&self, id: &str) -> Result<crate::model::provider_credentials::ProviderQuota>;
}

#[async_trait]
pub trait PluginResourceService: Send + Sync {
    async fn ensure_key(
        &self,
        scope: PluginMutationScope,
        command: PluginKeyEnsure,
    ) -> Result<PluginManagedResource>;
    async fn ensure_group(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupEnsure,
    ) -> Result<PluginManagedResource>;
    async fn change_members(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupMembers,
    ) -> Result<PluginMembersChanged>;
}
