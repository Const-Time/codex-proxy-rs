//! Local settings v1: subscription policy and pricing source verification.
//! These operations deliberately do not impersonate upstream settings contracts.
use super::{
    Operation,
    settings::{ModelPriceOverride, MultiplierScope, MutationContext, PricingOverrides},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettings {
    pub subscription_auto_reset_enabled: bool,
    pub model_mappings: BTreeMap<String, String>,
    pub refresh_margin_seconds: u64,
    pub refresh_concurrency: u32,
    pub max_concurrent_per_account: u32,
    pub request_interval_ms: u64,
    pub rotation_strategy: String,
    pub min_codex_desktop_version: Option<String>,
    pub min_codex_cli_version: Option<String>,
    pub usage_retention_days: u32,
    pub ops_event_retention_days: u32,
    pub audit_retention_days: u32,
    pub config_revision: std::num::NonZeroU64,
    pub updated_at: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceRuntimeSettings {
    pub subscription_auto_reset_enabled: Option<bool>,
    pub model_mappings: BTreeMap<String, String>,
    pub refresh_margin_seconds: u64,
    pub refresh_concurrency: u32,
    pub max_concurrent_per_account: u32,
    pub request_interval_ms: u64,
    pub rotation_strategy: String,
    pub min_codex_desktop_version: Option<String>,
    pub min_codex_cli_version: Option<String>,
    pub usage_retention_days: u32,
    pub ops_event_retention_days: u32,
    pub audit_retention_days: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingCatalog {
    pub defaults: PricingOverrides,
    pub overrides: PricingOverrides,
    pub synced: PricingOverrides,
    pub synced_at: Option<String>,
    pub sync_policy: PricingSyncPolicy,
    pub source_hash: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PricingSyncPolicy {
    pub enabled: bool,
    pub update_existing: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PricingSyncPreview {
    pub prices: PricingOverrides,
    pub skipped: Vec<String>,
    pub source_hash: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PricingChange {
    Replace(ModelPriceOverride),
    Multiplier(u32, MultiplierScope),
    Reset,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdatePricing {
    pub provider: String,
    pub models: Vec<String>,
    pub change: PricingChange,
}
pub struct Load;
impl Operation for Load {
    const NAME: &'static str = "local.settings.v1.load";
    type Input = ();
    type Output = RuntimeSettings;
}
pub struct Replace;
impl Operation for Replace {
    const NAME: &'static str = "local.settings.v1.replace";
    type Input = (MutationContext, ReplaceRuntimeSettings);
    type Output = RuntimeSettings;
}
pub struct Pricing;
impl Operation for Pricing {
    const NAME: &'static str = "local.settings.v1.pricing";
    type Input = ();
    type Output = PricingCatalog;
}
pub struct PreviewPricingSync;
impl Operation for PreviewPricingSync {
    const NAME: &'static str = "local.settings.v1.preview_pricing_sync";
    type Input = ();
    type Output = PricingSyncPreview;
}
pub struct Sync;
impl Operation for Sync {
    const NAME: &'static str = "local.settings.v1.sync_pricing";
    type Input = (MutationContext, PricingSyncPreview);
    type Output = ();
}
pub struct Update;
impl Operation for Update {
    const NAME: &'static str = "local.settings.v1.update_pricing";
    type Input = (MutationContext, UpdatePricing);
    type Output = ();
}
pub struct UpdateSyncPolicy;
impl Operation for UpdateSyncPolicy {
    const NAME: &'static str = "local.settings.v1.update_pricing_sync_policy";
    type Input = (MutationContext, PricingSyncPolicy);
    type Output = ();
}
