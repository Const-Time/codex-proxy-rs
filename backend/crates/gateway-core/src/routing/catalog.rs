//! Provider-owned model descriptors; opaque protocol bodies remain with the provider.
use super::ProviderKind;
use super::{ModelCapabilities, ModelPresentation};
use super::{PublicModelId, PublicModelProfile, UpstreamModelId};
use crate::operation::RawJsonPayload;
use futures::future::BoxFuture;

use crate::account::ProviderAccountId;
use std::collections::{BTreeMap, BTreeSet};

/// Static registration facts captured from the same extension publication as
/// execution middleware. No process calls or separate routing configuration.
#[derive(Clone)]
pub struct ContributedModelCatalog {
    aliases: std::sync::Arc<[ContributedModelAlias]>,
    cancellation: crate::lifecycle::CancellationToken,
}
impl ContributedModelCatalog {
    #[must_use]
    pub fn new(
        aliases: Vec<ContributedModelAlias>,
        cancellation: crate::lifecycle::CancellationToken,
    ) -> Self {
        Self {
            aliases: aliases.into(),
            cancellation,
        }
    }
    pub fn cancellation(&self) -> crate::lifecycle::CancellationToken {
        self.cancellation.clone()
    }
    pub(crate) fn apply(
        &self,
        snapshot: &super::RuntimeSnapshot,
    ) -> Result<super::RuntimeSnapshot, super::snapshot::RuntimeSnapshotCompileError> {
        if self.cancellation.is_cancelled() {
            return Err(super::snapshot::RuntimeSnapshotCompileError::InvalidExtensionModels);
        }
        snapshot.clone().with_model_aliases(self.aliases.to_vec())
    }
}

/// Core validates a proposed complete registration set before Admin commits an
/// instance revision; each request validates it again against its own snapshot.
pub trait ModelCatalogValidator: Send + Sync {
    fn validate_model_aliases(
        &self,
        aliases: Vec<ContributedModelAlias>,
    ) -> Result<(), super::snapshot::RuntimeSnapshotCompileError>;
}

pub trait ProviderCatalogPort: Send + Sync {
    fn catalog_generations(&self) -> BTreeMap<ProviderKind, ProviderCatalogGeneration>;
    fn query_model_capabilities(
        &self,
        provider: &ProviderKind,
    ) -> BoxFuture<'_, Result<Vec<ProviderModelCapabilities>, ProviderCatalogUnavailable>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderModelDescriptor {
    pub model: UpstreamModelId,
    pub payload: RawJsonPayload,
}

#[derive(Debug, Clone)]
pub enum PublicModelDescriptor {
    Native {
        model: PublicModelId,
        payload: RawJsonPayload,
    },
    Adapted(PublicModelProfile),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("provider model catalog is unavailable")]
pub struct ProviderCatalogUnavailable;

/// Provider 实时目录编译后的单模型能力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderModelCapabilities {
    upstream_model: UpstreamModelId,
    capabilities: ModelCapabilities,
    presentation: Option<ModelPresentation>,
    catalog_accounts: Option<BTreeSet<ProviderAccountId>>,
}

/// Provider 实时目录成功发布后的进程内单调代次。
///
/// 代次只表达“目录内容已经变化”，不承载模型、ETag 或 Provider 私有数据。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderCatalogGeneration(u64);

impl ProviderCatalogGeneration {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl ProviderModelCapabilities {
    #[must_use]
    pub const fn new(upstream_model: UpstreamModelId, capabilities: ModelCapabilities) -> Self {
        Self {
            upstream_model,
            capabilities,
            presentation: None,
            catalog_accounts: None,
        }
    }

    #[must_use]
    pub fn with_presentation(mut self, presentation: ModelPresentation) -> Self {
        self.presentation = Some(presentation);
        self
    }

    /// 声明发现此模型的账号；仅用于目录可见性，不作为推理能力白名单。
    #[must_use]
    pub fn with_catalog_accounts(mut self, accounts: BTreeSet<ProviderAccountId>) -> Self {
        self.catalog_accounts = Some(accounts);
        self
    }

    /// 未提供账号来源的 Provider 沿用 Provider 级目录；空集合表示无可展示来源。
    #[must_use]
    pub const fn catalog_accounts(&self) -> Option<&BTreeSet<ProviderAccountId>> {
        self.catalog_accounts.as_ref()
    }

    #[must_use]
    pub const fn upstream_model(&self) -> &UpstreamModelId {
        &self.upstream_model
    }

    #[must_use]
    pub const fn capabilities(&self) -> &ModelCapabilities {
        &self.capabilities
    }

    #[must_use]
    pub const fn presentation(&self) -> Option<&ModelPresentation> {
        self.presentation.as_ref()
    }
}

/// An immutable plugin contribution. It points directly to a provider-owned
/// model and never grants access to an account or adds provider capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributedModelAlias {
    pub owner: String,
    pub id: PublicModelId,
    pub provider: ProviderKind,
    pub target: UpstreamModelId,
}
