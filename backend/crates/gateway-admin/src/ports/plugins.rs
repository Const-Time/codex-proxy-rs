//! 插件用例、进程与持久化端口；具体实现只在组合根装配。
use crate::model::{AdminError, MutationContext, plugins::*};
use async_trait::async_trait;
use std::sync::Arc;
type Result<T> = std::result::Result<T, AdminError>;

/// Host-selected subscribers; scopes belong to the frozen Admin publication.
pub struct PluginObserverTarget {
    pub process: Arc<dyn PluginProcess>,
    pub order: i32,
    pub request_completed: Option<PluginExecutionScope>,
    pub websocket_response: Option<PluginExecutionScope>,
}
pub struct PluginObserverIdentity {
    pub plugin_id: String,
    pub instance_id: String,
}
/// Runtime projects native facts into the SDK envelope before bounded queuing.
/// Neither this envelope nor its request ID grants a model or Admin identity.
pub struct PluginObservation {
    pub request_id: String,
    pub metadata: serde_json::Value,
    pub payload: Arc<[u8]>,
    pub call_scope: gateway_core::engine::nested::ExtensionCallScope,
}

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
    fn command_help(&self, _name: Option<&str>) -> Result<Option<String>> {
        Ok(None)
    }
    async fn command(
        &self,
        _request: PluginCommandRequest,
        _services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
    ) -> std::result::Result<PluginCommandOutput, PluginCommandError> {
        Err(AdminError::unavailable("插件命令执行入口未接入").into())
    }
    fn observer_identity(&self) -> Option<PluginObserverIdentity> {
        None
    }
    async fn observe(
        &self,
        _observation: PluginObservation,
        _services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
        _cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<()> {
        Err(AdminError::unavailable("插件观察能力不可用"))
    }
    fn supports_maintenance(&self) -> bool {
        false
    }
    async fn reconcile(
        &self,
        _cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<()> {
        Err(AdminError::unavailable("插件维护能力不可用"))
    }

    fn upstream_adapter(
        &self,
        _id: &str,
    ) -> Option<Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapter>> {
        None
    }

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
    async fn authenticate(
        &self,
        _authorization: String,
        _origin: Option<gateway_core::engine::middleware::service::Origin>,
    ) -> Result<PluginAuthenticationResult> {
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
    fn observer_plan(
        &self,
        _targets: Vec<PluginObserverTarget>,
        _services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
        _cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<Option<Arc<dyn gateway_core::engine::observation::RequestObserverPlan>>> {
        Err(AdminError::unavailable("插件观察执行入口未接入"))
    }
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
    async fn authenticate(
        &self,
        _authorization: String,
        _origin: Option<gateway_core::engine::middleware::service::Origin>,
    ) -> Result<PluginAuthenticationDecision> {
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

/// Non-secret projections. The runtime authorizes each callback's actual host scope;
/// global Key listing is reserved for system commands. Reads never refresh quota.
#[async_trait]
pub trait PluginDataService: Send + Sync {
    async fn keys(&self, query: PluginKeyQuery) -> Result<PluginKeyPage>;
    async fn key(&self, id: &str) -> Result<PluginKeyFacts>;
    async fn accounts(&self, query: PluginAccountQuery) -> Result<PluginAccountPage>;
    async fn quota(&self, id: &str) -> Result<crate::model::provider_credentials::ProviderQuota>;
}

/// Preparation and native account commit continue after the caller drops its wait.
/// The original graph permit keeps Host drain attached to that work.
#[async_trait]
pub trait PluginCredentialService: Send + Sync {
    async fn accounts(
        &self,
        scope: PluginMutationScope,
        initiator: MutationContext,
        query: PluginCredentialQuery,
    ) -> Result<PluginCredentialPage>;
    async fn credential(
        &self,
        scope: PluginMutationScope,
        initiator: MutationContext,
        account: gateway_core::account::ProviderAccountId,
    ) -> Result<crate::model::provider_credentials::ProviderExportCredentialInput>;
    async fn save(
        &self,
        scope: PluginMutationScope,
        initiator: MutationContext,
        command: PluginCredentialSave,
        cleanup: gateway_core::engine::nested::ExtensionCallScope,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<PluginCredentialSaved>;
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
