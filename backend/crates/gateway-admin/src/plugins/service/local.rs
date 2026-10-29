//! Local settings v2 adds reset-credit policy without changing the published v1 wire.
use super::local_v1 as v1;
use crate::model::{AdminError, settings as domain};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettings {
    pub reset_credit_lead_minutes: u32,
    pub reset_credit_max_weekly_remaining_percent: Option<u32>,
    #[serde(flatten)]
    pub base: v1::RuntimeSettings,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceRuntimeSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_credit_lead_minutes: Option<u32>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_threshold",
        skip_serializing_if = "Option::is_none"
    )]
    pub reset_credit_max_weekly_remaining_percent: Option<Option<u32>>,
    #[serde(flatten)]
    pub base: v1::ReplaceRuntimeSettings,
}
fn deserialize_optional_threshold<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<u32>>, D::Error> {
    <Option<u32> as serde::Deserialize>::deserialize(deserializer).map(Some)
}
impl From<domain::RuntimeSettings> for RuntimeSettings {
    fn from(value: domain::RuntimeSettings) -> Self {
        Self {
            reset_credit_lead_minutes: value.reset_credit_lead_minutes,
            reset_credit_max_weekly_remaining_percent: value
                .reset_credit_max_weekly_remaining_percent,
            base: value.into(),
        }
    }
}
impl TryFrom<RuntimeSettings> for domain::RuntimeSettings {
    type Error = AdminError;
    fn try_from(value: RuntimeSettings) -> Result<Self, Self::Error> {
        let mut result: Self = value.base.try_into()?;
        result.reset_credit_lead_minutes = value.reset_credit_lead_minutes;
        result.reset_credit_max_weekly_remaining_percent =
            value.reset_credit_max_weekly_remaining_percent;
        Ok(result)
    }
}
impl From<domain::ReplaceRuntimeSettings> for ReplaceRuntimeSettings {
    fn from(value: domain::ReplaceRuntimeSettings) -> Self {
        Self {
            reset_credit_lead_minutes: value.reset_credit_lead_minutes,
            reset_credit_max_weekly_remaining_percent: value
                .reset_credit_max_weekly_remaining_percent,
            base: value.into(),
        }
    }
}
impl TryFrom<ReplaceRuntimeSettings> for domain::ReplaceRuntimeSettings {
    type Error = AdminError;
    fn try_from(value: ReplaceRuntimeSettings) -> Result<Self, Self::Error> {
        let mut result: Self = value.base.try_into()?;
        result.reset_credit_lead_minutes = value.reset_credit_lead_minutes;
        result.reset_credit_max_weekly_remaining_percent =
            value.reset_credit_max_weekly_remaining_percent;
        Ok(result)
    }
}
