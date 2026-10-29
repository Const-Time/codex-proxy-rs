//! Local settings v2 adds reset-card policy; v1 operations retain their original contract.
use super::{Operation, local_settings_v1 as v1};
use serde::{Deserialize, Serialize};
pub use v1::{MutationActor, MutationContext};
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
pub struct Load;
impl Operation for Load {
    const NAME: &'static str = "local.settings.v2.load";
    type Input = ();
    type Output = RuntimeSettings;
}
pub struct Replace;
impl Operation for Replace {
    const NAME: &'static str = "local.settings.v2.replace";
    type Input = (MutationContext, ReplaceRuntimeSettings);
    type Output = RuntimeSettings;
}
