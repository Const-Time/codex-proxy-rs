use std::{collections::BTreeMap, sync::Arc};

/// Runtime settings shared by persistent snapshot compilation and request-local derivation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsValues {
    pub(crate) pricing: Arc<crate::metering::PricingOverrides>,
    pub(crate) max_concurrent_per_account: u32,
    pub(crate) request_interval_ms: u64,
    pub(crate) rotation_strategy: String,
    pub(crate) model_mappings: BTreeMap<String, String>,
    pub(crate) min_codex_desktop_version: Option<String>,
    pub(crate) min_codex_cli_version: Option<String>,
}

impl SettingsValues {
    #[must_use]
    pub fn with_pricing(mut self, pricing: crate::metering::PricingOverrides) -> Self {
        self.pricing = Arc::new(pricing);
        self
    }

    #[must_use]
    pub fn new(
        max_concurrent_per_account: u32,
        request_interval_ms: u64,
        rotation_strategy: impl Into<String>,
        model_mappings: BTreeMap<String, String>,
        min_codex_desktop_version: Option<String>,
        min_codex_cli_version: Option<String>,
    ) -> Self {
        Self {
            pricing: Arc::default(),
            max_concurrent_per_account,
            request_interval_ms,
            rotation_strategy: rotation_strategy.into(),
            model_mappings,
            min_codex_desktop_version,
            min_codex_cli_version,
        }
    }
}

impl SettingsValues {
    pub(crate) fn compile(&self) -> Result<super::CompiledSettings, super::InvalidSettings> {
        use super::{CompiledSettings, InvalidSettings};
        use crate::account::{AccountSelectionPolicy, RotationStrategy};
        use crate::policy::{CodexClientMinVersions, CodexClientVersion};
        let strategy = RotationStrategy::parse(&self.rotation_strategy).ok_or(InvalidSettings)?;
        let concurrency =
            std::num::NonZeroU32::new(self.max_concurrent_per_account).ok_or(InvalidSettings)?;
        let version = |value: &Option<String>| {
            value
                .as_deref()
                .map(CodexClientVersion::parse)
                .transpose()
                .map_err(|_| InvalidSettings)
        };
        for models in self.pricing.values() {
            for price in models.values() {
                price.validate().map_err(|_| InvalidSettings)?;
            }
        }
        Ok(CompiledSettings {
            selection: AccountSelectionPolicy::new(
                strategy,
                concurrency,
                std::time::Duration::from_millis(self.request_interval_ms),
            ),
            min_versions: CodexClientMinVersions::new(
                version(&self.min_codex_desktop_version)?,
                version(&self.min_codex_cli_version)?,
            ),
        })
    }
    #[must_use]
    pub fn with_model_mappings(mut self, mappings: BTreeMap<String, String>) -> Self {
        self.model_mappings = mappings;
        self
    }
}
