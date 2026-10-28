//! 插件持久化事实和管理视图；配置与私有状态不实现内容型 Debug。
mod state;
pub use state::*;
pub mod management;
pub mod middleware;
pub mod policy;
pub use management::*;
pub use middleware::*;
pub use policy::*;
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
    pub observer: Option<PluginObserverBinding>,
    pub upstream_adapter: Option<PluginUpstreamAdapterBinding>,
    pub retry_policy: Option<PluginRetryPolicyBinding>,
    pub account_scheduler: Option<PluginAccountSchedulerBinding>,
    pub model_router: Option<PluginModelRouterBinding>,
    pub middleware: Option<PluginMiddlewareBinding>,
    pub frontend_authentication: Option<PluginAuthenticationBinding>,
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
    pub observer: Option<PluginObserverBinding>,
    pub upstream_adapter: Option<PluginUpstreamAdapterBinding>,
    pub retry_policy: Option<PluginRetryPolicyBinding>,
    pub account_scheduler: Option<PluginAccountSchedulerBinding>,
    pub model_router: Option<PluginModelRouterBinding>,
    pub middleware: Option<PluginMiddlewareBinding>,
    pub frontend_authentication: Option<PluginAuthenticationBinding>,
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
    pub runtime_load: Option<PluginRuntimeLoad>,
    pub observer: Option<PluginObserverBinding>,
    pub upstream_adapter: Option<PluginUpstreamAdapterBinding>,
    pub retry_policy: Option<PluginRetryPolicyBinding>,
    pub account_scheduler: Option<PluginAccountSchedulerBinding>,
    pub model_router: Option<PluginModelRouterBinding>,
    pub middleware: Option<PluginMiddlewareBinding>,
    pub frontend_authentication: Option<PluginAuthenticationBinding>,
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
/// Counts overlap; absent means no live runtime was available to sample.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRuntimeLoad {
    pub active_calls: usize,
    pub executing_calls: usize,
    pub queued_calls: usize,
    pub resuming_calls: usize,
    pub pending_callbacks: usize,
    pub active_streams: usize,
}
pub struct PluginHttpRequest {
    /// Host-only parent graph and ports; absent for callers without service authority.
    pub invocation: Option<PluginInvocationContext>,
    /// Supplied by API authentication; never decoded from plugin input.
    pub actor: super::MutationContext,
    pub method: String,
    pub path: String,
    pub query: String,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

/// Host-issued authority and cancellation; command arguments may contain credentials.
pub enum PluginCommandAction {
    Help(Option<String>),
    Execute {
        name: String,
        arguments: Vec<String>,
    },
}
pub struct PluginCommandRequest {
    pub name: String,
    pub arguments: Vec<String>,
    pub actor: super::MutationContext,
    pub cancellation: gateway_core::lifecycle::CancellationToken,
    pub call_scope: gateway_core::engine::nested::ExtensionCallScope,
}
impl PluginCommandRequest {
    pub fn system(
        name: String,
        arguments: Vec<String>,
        cancellation: gateway_core::lifecycle::CancellationToken,
        call_scope: gateway_core::engine::nested::ExtensionCallScope,
    ) -> Self {
        Self {
            name,
            arguments,
            actor: super::MutationContext {
                actor: super::MutationActor::System,
                request_id: uuid::Uuid::now_v7().to_string(),
            },
            cancellation,
            call_scope,
        }
    }
}
pub struct PluginCommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: u8,
    pub saved_accounts: usize,
}

/// SDK input converted by Runtime; Provider must prepare it before any Store write.
pub enum PluginCredentialSave {
    Create {
        provider: gateway_core::routing::ProviderKind,
        facts: super::provider_credentials::UnverifiedCredentialFacts,
    },
    Replace {
        account_id: gateway_core::account::ProviderAccountId,
        credential_revision: super::Revision,
        facts: super::provider_credentials::UnverifiedCredentialFacts,
    },
}
/// A returned receipt confirms one native account commit, even if a later read fails.
pub struct PluginCredentialSaved {
    pub account_id: gateway_core::account::ProviderAccountId,
    pub credential_revision: Option<super::Revision>,
}
/// Native account query; SDK cursors are stable account IDs, not page offsets.
pub struct PluginCredentialQuery {
    pub provider: Option<gateway_core::routing::ProviderKind>,
    pub cursor: Option<gateway_core::account::ProviderAccountId>,
    pub account_id: Option<gateway_core::account::ProviderAccountId>,
    pub limit: super::PageSize,
}
pub struct PluginCredentialPage {
    pub accounts: Vec<super::accounts::AccountRecord>,
    pub next_cursor: Option<gateway_core::account::ProviderAccountId>,
}
#[derive(Debug, thiserror::Error)]
pub enum PluginCommandError {
    #[error(transparent)]
    Invalid(#[from] super::AdminError),
    #[error(
        "插件命令未完整完成（{reason}），已确认保存 {saved_accounts} 个账号，其余副作用结果未知；请核对后再决定是否重试"
    )]
    Incomplete {
        saved_accounts: usize,
        reason: &'static str,
    },
}
#[derive(Clone)]
pub struct PluginInvocationContext {
    pub context: gateway_core::engine::middleware::http::Context,
    pub http: Option<Arc<dyn gateway_core::engine::middleware::http::Dispatcher>>,
    pub services: Option<Arc<dyn gateway_core::engine::middleware::service::Dispatcher>>,
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
    pub public: bool,
}

/// Shared generation revocation; the API keeps this lease for the complete model response lifetime.
#[derive(Clone, Default)]
pub struct PluginAccess(gateway_core::lifecycle::CancellationToken);
impl PluginAccess {
    pub fn cancellation(&self) -> gateway_core::lifecycle::CancellationToken {
        self.0.clone()
    }
    pub fn linked(access: impl IntoIterator<Item = Self>) -> Self {
        Self(gateway_core::lifecycle::CancellationToken::linked(
            access.into_iter().map(|a| a.0),
        ))
    }
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

/// Non-secret projections for trusted management plugins; no credentials or shared ledger mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginKeyFacts {
    pub id: String,
    pub enabled: bool,
    pub group_ids: Vec<String>,
}
/// Global non-secret Key projection, available only to a host CLI invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginKeyQuery {
    pub cursor: Option<String>,
    pub limit: u16,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginListedKey {
    pub id: String,
    pub name: String,
    pub enabled: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginKeyPage {
    pub keys: Vec<PluginListedKey>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAccountQuery {
    pub provider: Option<String>,
    pub cursor: Option<String>,
    pub limit: u16,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAccountFacts {
    pub id: String,
    pub provider: String,
    pub name: String,
    pub email: Option<String>,
    pub group_ids: Vec<String>,
    pub enabled: bool,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAccountPage {
    pub accounts: Vec<PluginAccountFacts>,
    pub next_cursor: Option<String>,
}

#[derive(Clone)]
pub struct PluginMutationScope {
    pub instance_id: String,
    pub revision: u64,
    pub actor: super::MutationContext,
}
#[derive(Clone)]
pub struct PluginGroupEnsure {
    pub resource_key: String,
    pub name: String,
    pub color: String,
    pub description: Option<String>,
}
#[derive(Clone)]
pub struct PluginGroupMembers {
    pub resource_key: String,
    pub add: Vec<String>,
    pub remove: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginManagedResource {
    pub id: String,
    pub name: String,
    pub enabled: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginResourceMutation<T> {
    pub revision: super::Revision,
    pub value: T,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginMembersChanged {
    pub added: u64,
    pub removed: u64,
}

#[derive(Clone)]
pub struct PluginKeyEnsure {
    pub resource_key: String,
    pub name: String,
    pub group_resource_keys: Vec<String>,
    pub max_concurrency: u64,
    pub requests_per_minute: u64,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
}
pub struct PluginKeyCreate {
    pub resource_key: String,
    pub group_resource_key: String,
    pub key: super::client_keys::NewClientKey,
    /// Expected effective shared limits; never implicitly rewrites a shared budget.
    pub budget: gateway_core::engine::budget::ClientBudgetLimits,
}

/// Explicit shared-ledger operations. A Key is never treated as a budget owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSharedBudget {
    pub revision: super::Revision,
    pub target: super::users::SubscriptionTarget,
    pub enabled: bool,
    pub group_daily_limit_usd: String,
    pub group_weekly_limit_usd: String,
    pub quota_multiplier: String,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
    pub daily_used_usd: String,
    pub weekly_used_usd: String,
    pub daily_resets_at: Option<chrono::DateTime<chrono::Utc>>,
    pub weekly_resets_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_reset_at: Option<chrono::DateTime<chrono::Utc>>,
}
#[derive(Debug, Clone)]
pub struct PluginGroupBudgetUpdate {
    pub group_id: String,
    pub expected_revision: u64,
    pub daily_limit_usd: Option<String>,
    pub weekly_limit_usd: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginGroupBudget {
    pub group_id: String,
    pub daily_limit_usd: String,
    pub weekly_limit_usd: String,
}
#[derive(Debug, Clone)]
pub struct PluginBudgetReset {
    pub event_id: String,
    pub targets: Vec<super::users::SubscriptionTarget>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginAuthenticationBinding {
    pub failure_policy: PluginAuthenticationPolicy,
    pub identities: BTreeMap<String, String>,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginAuthenticationPolicy {
    Reject,
    Delegate,
}
impl PluginAuthenticationBinding {
    pub fn validate(&self) -> Result<(), super::AdminError> {
        if self.identities.is_empty()
            || self.identities.len() > 256
            || self.identities.iter().any(|(principal, key)| {
                !valid_plugin_principal(principal)
                    || gateway_core::policy::ClientApiKeyId::new(key).is_err()
            })
        {
            return Err(super::AdminError::invalid(
                "外部身份映射需包含 1～256 个有效 principal 和 Key ID",
            ));
        }
        Ok(())
    }
}
pub fn valid_plugin_principal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
pub enum PluginAuthenticationResult {
    Authenticated(String),
    NotMatched,
    Rejected,
}
/// A host decision, not a plugin-supplied Key ID. The epoch also guards builtin delegation.
pub enum PluginAuthenticationDecision {
    Builtin {
        access: PluginAccess,
    },
    Authenticated {
        key: gateway_core::policy::ClientApiKeyId,
        access: PluginAccess,
    },
    Rejected,
}

/// Registration facts for overlap checks and binding; no credentials or executable config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginUpstreamAdapterDeclaration {
    pub id: String,
    pub provider: String,
    pub protocol: String,
    pub transport: String,
    pub models: Vec<String>,
    pub authentication_kinds: Vec<String>,
}
