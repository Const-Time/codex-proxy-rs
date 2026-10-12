//! Routing and scheduling share the existing execution scope and instance revision.
use super::PluginExecutionScope;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginPolicyFailurePolicy {
    Reject,
    Delegate,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginModelRouterBinding {
    pub order: i32,
    pub scope: PluginExecutionScope,
    pub failure_policy: PluginPolicyFailurePolicy,
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginAccountSchedulerBinding {
    pub order: i32,
    pub scope: PluginExecutionScope,
    pub failure_policy: PluginPolicyFailurePolicy,
}
impl PluginAccountSchedulerBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !(-10_000..=10_000).contains(&self.order) || !self.scope.is_valid() {
            return Err(super::super::AdminError::invalid(
                "账号调度顺序或作用范围无效",
            ));
        }
        Ok(())
    }
}
