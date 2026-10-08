//! Validated runtime values used by global and request-local snapshots.
mod values;
pub use values::SettingsValues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("request settings are invalid")]
pub struct InvalidSettings;

pub(crate) struct CompiledSettings {
    pub selection: crate::account::AccountSelectionPolicy,
    pub min_versions: crate::policy::CodexClientMinVersions,
}
