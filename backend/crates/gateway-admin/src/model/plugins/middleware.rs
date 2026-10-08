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
    pub scope: PluginMiddlewareScope,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginMiddlewareMount {
    Request,
    Attempt,
}
impl PluginMiddlewareMount {
    pub fn mount(self) -> MiddlewareMount {
        match self {
            Self::Request => MiddlewareMount::Request,
            Self::Attempt => MiddlewareMount::Attempt,
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginMiddlewareScope {
    All {},
    Keys { ids: Vec<String> },
    Groups { ids: Vec<String> },
}
impl PluginMiddlewareBinding {
    pub fn validate(&self) -> Result<(), super::super::AdminError> {
        let valid_ids = |ids: &[String]| {
            !ids.is_empty()
                && ids.len() <= 256
                && ids.iter().collect::<std::collections::BTreeSet<_>>().len() == ids.len()
        };
        let valid_scope = match &self.scope {
            PluginMiddlewareScope::All {} => true,
            PluginMiddlewareScope::Keys { ids } => {
                valid_ids(ids) && ids.iter().all(|id| ClientApiKeyId::new(id).is_ok())
            }
            PluginMiddlewareScope::Groups { ids } => {
                valid_ids(ids) && ids.iter().all(|id| AccountGroupId::new(id).is_ok())
            }
        };
        if !(-10_000..=10_000).contains(&self.order)
            || self.mounts.is_empty()
            || self.mounts.len() > 2
            || (self.mounts.len() == 2 && self.mounts[0] == self.mounts[1])
            || !valid_scope
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
            && match &self.scope {
                PluginMiddlewareScope::All {} => true,
                PluginMiddlewareScope::Keys { ids } => {
                    ids.iter().any(|id| id == context.client_key_id.as_str())
                }
                PluginMiddlewareScope::Groups { ids } => context
                    .account_group_ids
                    .iter()
                    .any(|group| ids.iter().any(|id| id == group.as_str())),
            }
    }
}
