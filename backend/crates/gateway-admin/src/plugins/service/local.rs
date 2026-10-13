//! Explicit local v1 settings wire; the domain service remains the authority.
use crate::model::{AdminError, settings as domain};
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

impl From<domain::RuntimeSettings> for RuntimeSettings {
    fn from(value: domain::RuntimeSettings) -> Self {
        Self {
            subscription_auto_reset_enabled: value.subscription_auto_reset_enabled,
            model_mappings: value
                .model_mappings
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            refresh_margin_seconds: value.refresh_margin_seconds,
            refresh_concurrency: value.refresh_concurrency,
            max_concurrent_per_account: value.max_concurrent_per_account,
            request_interval_ms: value.request_interval_ms,
            rotation_strategy: value.rotation_strategy.as_str().into(),
            min_codex_desktop_version: value.min_codex_desktop_version,
            min_codex_cli_version: value.min_codex_cli_version,
            usage_retention_days: value.usage_retention_days,
            ops_event_retention_days: value.ops_event_retention_days,
            audit_retention_days: value.audit_retention_days,
            config_revision: std::num::NonZeroU64::new(value.config_revision.get())
                .expect("positive revision"),
            updated_at: value.updated_at.to_rfc3339(),
        }
    }
}
impl TryFrom<RuntimeSettings> for domain::RuntimeSettings {
    type Error = AdminError;
    fn try_from(value: RuntimeSettings) -> Result<Self, Self::Error> {
        Ok(Self {
            subscription_auto_reset_enabled: value.subscription_auto_reset_enabled,
            model_mappings: mappings(value.model_mappings)?,
            refresh_margin_seconds: value.refresh_margin_seconds,
            refresh_concurrency: value.refresh_concurrency,
            max_concurrent_per_account: value.max_concurrent_per_account,
            request_interval_ms: value.request_interval_ms,
            rotation_strategy: domain::RotationStrategy::parse(&value.rotation_strategy)
                .ok_or_else(|| AdminError::invalid("无效调度策略"))?,
            min_codex_desktop_version: value.min_codex_desktop_version,
            min_codex_cli_version: value.min_codex_cli_version,
            usage_retention_days: value.usage_retention_days,
            ops_event_retention_days: value.ops_event_retention_days,
            audit_retention_days: value.audit_retention_days,
            config_revision: crate::model::Revision::new(value.config_revision.get())
                .expect("positive revision"),
            updated_at: chrono::DateTime::parse_from_rfc3339(&value.updated_at)
                .map_err(|_| AdminError::invalid("无效设置时间"))?
                .with_timezone(&chrono::Utc),
        })
    }
}
impl From<domain::ReplaceRuntimeSettings> for ReplaceRuntimeSettings {
    fn from(value: domain::ReplaceRuntimeSettings) -> Self {
        Self {
            subscription_auto_reset_enabled: value.subscription_auto_reset_enabled,
            model_mappings: value
                .model_mappings
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            refresh_margin_seconds: value.refresh_margin_seconds,
            refresh_concurrency: value.refresh_concurrency,
            max_concurrent_per_account: value.max_concurrent_per_account,
            request_interval_ms: value.request_interval_ms,
            rotation_strategy: value.rotation_strategy.as_str().into(),
            min_codex_desktop_version: value.min_codex_desktop_version,
            min_codex_cli_version: value.min_codex_cli_version,
            usage_retention_days: value.usage_retention_days,
            ops_event_retention_days: value.ops_event_retention_days,
            audit_retention_days: value.audit_retention_days,
        }
    }
}
impl TryFrom<ReplaceRuntimeSettings> for domain::ReplaceRuntimeSettings {
    type Error = AdminError;
    fn try_from(value: ReplaceRuntimeSettings) -> Result<Self, Self::Error> {
        Ok(Self {
            subscription_auto_reset_enabled: value.subscription_auto_reset_enabled,
            model_mappings: mappings(value.model_mappings)?,
            refresh_margin_seconds: value.refresh_margin_seconds,
            refresh_concurrency: value.refresh_concurrency,
            max_concurrent_per_account: value.max_concurrent_per_account,
            request_interval_ms: value.request_interval_ms,
            rotation_strategy: domain::RotationStrategy::parse(&value.rotation_strategy)
                .ok_or_else(|| AdminError::invalid("无效调度策略"))?,
            min_codex_desktop_version: value.min_codex_desktop_version,
            min_codex_cli_version: value.min_codex_cli_version,
            usage_retention_days: value.usage_retention_days,
            ops_event_retention_days: value.ops_event_retention_days,
            audit_retention_days: value.audit_retention_days,
        })
    }
}
fn mappings(value: BTreeMap<String, String>) -> Result<domain::ModelMappings, AdminError> {
    if value.len() > 512 {
        return Err(AdminError::invalid("模型映射数量超过限制"));
    }
    value
        .into_iter()
        .map(|(a, b)| {
            Ok((
                gateway_core::routing::PublicModelId::new(a)
                    .map_err(|_| AdminError::invalid("无效模型映射"))?,
                gateway_core::routing::UpstreamModelId::new(b)
                    .map_err(|_| AdminError::invalid("无效模型映射"))?,
            ))
        })
        .collect()
}
