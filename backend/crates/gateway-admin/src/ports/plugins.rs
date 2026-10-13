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
    fn upstream_adapters(&self) -> Vec<PluginUpstreamAdapterDeclaration> {
        Vec::new()
    }

    /// Registration facts only; the original Core snapshot must validate the complete set.
    fn model_aliases(&self) -> Vec<gateway_core::routing::ContributedModelAlias> {
        Vec::new()
    }

    async fn service_middleware(
        &self,
        _context: gateway_core::engine::middleware::service::Context,
        _input: serde_json::Value,
        _next: gateway_core::engine::middleware::service::Next,
    ) -> std::result::Result<serde_json::Value, gateway_core::engine::middleware::service::Error>
    {
        Err(gateway_core::engine::middleware::service::Error::unavailable("服务中间件不可用"))
    }
    async fn websocket_middleware(
        &self,
        _context: gateway_core::engine::middleware::websocket::Context,
        _message: gateway_core::engine::middleware::websocket::Message,
        _next: gateway_core::engine::middleware::websocket::Next,
    ) -> std::result::Result<
        Option<gateway_core::engine::middleware::websocket::Message>,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        Err(gateway_core::engine::middleware::MiddlewareError::Fault)
    }
    async fn http_middleware(
        &self,
        _context: gateway_core::engine::middleware::http::Context,
        _request: gateway_core::engine::middleware::http::Request,
        _next: gateway_core::engine::middleware::http::Next,
    ) -> std::result::Result<
        gateway_core::engine::middleware::http::Response,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        Err(gateway_core::engine::middleware::MiddlewareError::Fault)
    }
    async fn retry_decision(
        &self,
        _input: gateway_core::engine::retry_policy::RetryInput,
        _timeout: std::time::Duration,
    ) -> std::result::Result<
        gateway_core::engine::retry_policy::RetryDecision,
        gateway_core::engine::retry_policy::RetryPolicyFault,
    > {
        Err(gateway_core::engine::retry_policy::RetryPolicyFault)
    }
    async fn schedule_account(
        &self,
        _input: gateway_core::engine::account_scheduling::AccountScheduleInput,
    ) -> std::result::Result<
        gateway_core::engine::account_scheduling::AccountScheduleDecision,
        gateway_core::engine::account_scheduling::AccountSchedulingFault,
    > {
        Err(gateway_core::engine::account_scheduling::AccountSchedulingFault)
    }
    async fn route_model(
        &self,
        _input: gateway_core::engine::model_routing::ModelRouteInput,
    ) -> std::result::Result<
        gateway_core::engine::model_routing::ModelRouteDecision,
        gateway_core::engine::model_routing::ModelRoutingFault,
    > {
        Err(gateway_core::engine::model_routing::ModelRoutingFault)
    }
    async fn middleware(
        &self,
        _context: gateway_core::engine::middleware::MiddlewareContext,
        _request: gateway_core::engine::middleware::MiddlewareRequest,
        _next: gateway_core::engine::middleware::MiddlewareNext,
    ) -> std::result::Result<
        gateway_core::engine::middleware::MiddlewareResponse,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        Err(gateway_core::engine::middleware::MiddlewareError::Fault)
    }
    async fn authenticate(&self, _authorization: String) -> Result<PluginAuthenticationResult> {
        Err(AdminError::unavailable("认证器不可用"))
    }

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
    /// Manifest interpretation stays with the package/protocol owner.
    fn contributes_model_catalog(&self, _artifact: &PluginArtifact) -> Result<bool> {
        Ok(false)
    }
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
    /// Hosts without execution middleware do not install this data-plane port.
    fn middleware_registry(&self) -> Option<crate::plugins::middleware::PluginMiddlewareRegistry> {
        None
    }
    fn freeze_middleware(
        &self,
        _context: gateway_core::engine::middleware::MiddlewareContext,
    ) -> Result<crate::plugins::middleware::PluginMiddlewarePlan> {
        Err(AdminError::unavailable("中间件执行入口未接入"))
    }
    async fn authenticate(&self, _authorization: String) -> Result<PluginAuthenticationDecision> {
        Err(AdminError::unavailable("认证器不可用"))
    }

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

#[async_trait]
pub trait PluginQuotaService: Send + Sync {
    async fn get(
        &self,
        scope: PluginMutationScope,
        target: crate::model::users::SubscriptionTarget,
    ) -> Result<PluginSharedBudget>;
    async fn update_group_limits(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupBudgetUpdate,
    ) -> Result<PluginResourceMutation<PluginGroupBudget>>;
    async fn reset(
        &self,
        scope: PluginMutationScope,
        command: PluginBudgetReset,
    ) -> Result<PluginResourceMutation<u64>>;
}
