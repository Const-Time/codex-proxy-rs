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

/// Retry faults always delegate; bindings cannot introduce a separate fail-closed policy.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginRetryPolicyBinding {
    pub order: i32,
    pub scope: PluginExecutionScope,
}
impl PluginRetryPolicyBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !(-10_000..=10_000).contains(&self.order) || !self.scope.is_valid() {
            return Err(super::super::AdminError::invalid(
                "重试策略顺序或作用范围无效",
            ));
        }
        Ok(())
    }
}

/// All registered adapters in this contribution share a host-owned request scope.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginUpstreamAdapterBinding {
    pub scope: PluginExecutionScope,
}
impl PluginUpstreamAdapterBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !self.scope.is_valid() {
            return Err(super::super::AdminError::invalid("上游适配器作用范围无效"));
        }
        Ok(())
    }
}

/// Event scopes share one instance order; observation failures never control execution.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginObserverBinding {
    pub order: i32,
    pub request_completed: Option<PluginExecutionScope>,
    pub websocket_response: Option<PluginExecutionScope>,
}
impl PluginObserverBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !(-10_000..=10_000).contains(&self.order)
            || (self.request_completed.is_none() && self.websocket_response.is_none())
            || self
                .request_completed
                .iter()
                .chain(self.websocket_response.iter())
                .any(|scope| !scope.is_valid())
        {
            return Err(super::super::AdminError::invalid(
                "观察订阅顺序、事件或作用范围无效",
            ));
        }
        Ok(())
    }
}
