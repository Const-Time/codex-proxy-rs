//! Host-owned placement and scope; these facts never enter plugin configuration.
use gateway_core::{
    engine::middleware::{MiddlewareContext, MiddlewareMount},
    policy::ClientApiKeyId,
    routing::AccountGroupId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginMiddlewareBinding {
    pub order: i32,
    pub mounts: Vec<PluginMiddlewareMount>,
    pub scope: PluginExecutionScope,
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginMiddlewareMount {
    Http,
    #[serde(rename = "websocket")]
    WebSocket,
    Service,
    Request,
    Attempt,
}
impl PluginMiddlewareMount {
    pub fn mount(self) -> MiddlewareMount {
        match self {
            Self::Http => MiddlewareMount::Http,
            Self::WebSocket => MiddlewareMount::WebSocket,
            Self::Service => MiddlewareMount::Service,
            Self::Request => MiddlewareMount::Request,
            Self::Attempt => MiddlewareMount::Attempt,
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginExecutionScope {
    All {},
    Keys { ids: Vec<String> },
    Groups { ids: Vec<String> },
}
impl PluginExecutionScope {
    pub fn is_valid(&self) -> bool {
        let valid_ids = |ids: &[String]| {
            !ids.is_empty()
                && ids.len() <= 256
                && ids.iter().collect::<std::collections::BTreeSet<_>>().len() == ids.len()
        };
        match self {
            PluginExecutionScope::All {} => true,
            PluginExecutionScope::Keys { ids } => {
                valid_ids(ids) && ids.iter().all(|id| ClientApiKeyId::new(id).is_ok())
            }
            PluginExecutionScope::Groups { ids } => {
                valid_ids(ids) && ids.iter().all(|id| AccountGroupId::new(id).is_ok())
            }
        }
    }
    pub fn matches(&self, key: &ClientApiKeyId, groups: &[AccountGroupId]) -> bool {
        match self {
            Self::All {} => true,
            Self::Keys { ids } => ids.iter().any(|id| id == key.as_str()),
            Self::Groups { ids } => groups.iter().any(|g| ids.iter().any(|id| id == g.as_str())),
        }
    }
}
impl PluginMiddlewareBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        if !(-10_000..=10_000).contains(&self.order)
            || self.mounts.is_empty()
            || self.mounts.len() > 5
            || self
                .mounts
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.mounts.len()
            || (self.mounts.iter().any(|m| {
                matches!(
                    m,
                    PluginMiddlewareMount::Http
                        | PluginMiddlewareMount::WebSocket
                        | PluginMiddlewareMount::Service
                )
            }) && !matches!(self.scope, PluginExecutionScope::All {}))
            || !self.scope.is_valid()
        {
            return Err(super::super::AdminError::invalid(
                "中间件顺序、挂载位置或作用范围无效",
            ));
        }
        Ok(())
    }
    pub fn matches(&self, context: &MiddlewareContext) -> bool {
        self.mounts
            .iter()
            .any(|mount| mount.mount() == context.mount)
            && self
                .scope
                .matches(&context.client_key_id, &context.account_group_ids)
    }
}
