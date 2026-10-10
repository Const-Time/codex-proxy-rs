//! Shared fee-component selection for model pricing and group billing.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CostComponent {
    Input,
    Output,
    CacheRead,
    CacheWrite,
}

/// `all` also covers non-token fees; a component selection never changes them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum MultiplierScope {
    #[default]
    All,
    Components(BTreeSet<CostComponent>),
}

impl MultiplierScope {
    pub fn includes(&self, component: CostComponent) -> bool {
        match self {
            Self::All => true,
            Self::Components(items) => items.contains(&component),
        }
    }
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if matches!(self, Self::Components(items) if items.is_empty()) {
            return Err("倍率至少选择一个费用项");
        }
        Ok(())
    }
    pub fn label(&self) -> String {
        match self {
            Self::All => "全部费用".to_owned(),
            Self::Components(items) => items
                .iter()
                .map(|item| match item {
                    CostComponent::Input => "输入",
                    CostComponent::Output => "输出",
                    CostComponent::CacheRead => "缓存读",
                    CostComponent::CacheWrite => "缓存写",
                })
                .collect::<Vec<_>>()
                .join("、"),
        }
    }
}
