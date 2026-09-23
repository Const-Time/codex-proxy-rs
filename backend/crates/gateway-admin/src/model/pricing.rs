//! 全局模型定价的管理合同。

use gateway_core::metering::{ModelPriceOverride, PricingOverrides};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingCatalog {
    pub defaults: PricingOverrides,
    pub overrides: PricingOverrides,
    pub synced: PricingOverrides,
    pub synced_at: Option<chrono::DateTime<chrono::Utc>>,
    pub sync_policy: PricingSyncPolicy,
    pub source_hash: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredPricing {
    pub overrides: PricingOverrides,
    pub synced: PricingOverrides,
    pub synced_at: Option<chrono::DateTime<chrono::Utc>>,
    pub sync_policy: PricingSyncPolicy,
    pub source_hash: Option<String>,
}

/// 默认只自动加入未知模型；已有价格的变更需要管理员确认或显式开启自动更新。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PricingSyncPolicy {
    pub enabled: bool,
    pub update_existing: bool,
}

impl Default for PricingSyncPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            update_existing: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PricingSyncPreview {
    pub prices: PricingOverrides,
    pub skipped: Vec<String>,
    pub source_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPricing {
    pub preview: PricingSyncPreview,
    /// 自动任务携带开始读取时的策略；提交前策略变化则放弃本轮，避免越权改价。
    pub automatic: Option<PricingSyncPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PricingChange {
    Replace(ModelPriceOverride),
    Multiplier(u32),
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePricing {
    pub provider: String,
    pub models: Vec<String>,
    pub change: PricingChange,
}

pub type ProviderPricingCatalog = BTreeMap<String, ModelPriceOverride>;
