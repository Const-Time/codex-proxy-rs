//! Routing placement shares the existing execution scope and instance revision.
use super::PluginExecutionScope;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRoutingFailurePolicy {
    Reject,
    Delegate,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginModelRouterBinding {
    pub order: i32,
    pub scope: PluginExecutionScope,
    pub failure_policy: PluginRoutingFailurePolicy,
}
impl PluginModelRouterBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !(-10_000..=10_000).contains(&self.order) || !self.scope.is_valid() {
            return Err(super::super::AdminError::invalid(
                "模型路由顺序或作用范围无效",
            ));
        }
        Ok(())
    }
}
