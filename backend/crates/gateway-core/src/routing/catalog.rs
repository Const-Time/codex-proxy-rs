//! Provider-owned model descriptors; opaque protocol bodies remain with the provider.
use super::ProviderKind;
use super::{ModelCapabilities, ModelPresentation};
use super::{PublicModelId, PublicModelProfile, UpstreamModelId};
use crate::operation::RawJsonPayload;
use futures::future::BoxFuture;
use std::collections::BTreeMap;

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
        }
    }

    #[must_use]
    pub fn with_presentation(mut self, presentation: ModelPresentation) -> Self {
        self.presentation = Some(presentation);
        self
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
