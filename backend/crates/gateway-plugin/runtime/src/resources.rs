//! Management caller identity stays in the host, indexed by an authenticated RPC resource scope.
use crate::RpcReply;
use gateway_admin::{
    model::{AdminErrorKind, MutationContext, plugins::*},
    ports::plugins::PluginResourceService,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::resources};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(crate) struct ManagementCalls(Mutex<BTreeMap<String, MutationContext>>);
impl ManagementCalls {
    pub(crate) fn enter(self: &Arc<Self>, scope: String, actor: MutationContext) -> ManagementCall {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope.clone(), actor);
        ManagementCall {
            calls: self.clone(),
            scope,
        }
    }
    pub(crate) fn actor(&self, scope: &str) -> Option<MutationContext> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(scope)
            .cloned()
    }
}
pub(crate) struct ManagementCall {
    calls: Arc<ManagementCalls>,
    scope: String,
}
impl Drop for ManagementCall {
    fn drop(&mut self) {
        self.calls
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.scope);
    }
}
pub(crate) async fn call(
    service: &dyn PluginResourceService,
    scope: PluginMutationScope,
    method: &str,
    params: Value,
    payload: &[u8],
) -> Result<RpcReply, PluginFault> {
    let invalid = || PluginFault::new(ErrorCode::InvalidInput, "invalid plugin resource request");
    if params != json!({}) || payload.len() > 128 * 1024 {
        return Err(invalid());
    }
    let error = |e: gateway_admin::model::AdminError| {
        PluginFault::new(
            match e.kind() {
                AdminErrorKind::Invalid => ErrorCode::InvalidInput,
                AdminErrorKind::NotFound => ErrorCode::Rejected,
                AdminErrorKind::Conflict => ErrorCode::Conflict,
                _ => ErrorCode::Uncertain,
            },
            "plugin resource operation failed; query or repeat the same resource key to recover",
        )
    };
    let payload = match method {
        resources::KEY_ENSURE => {
            let v: resources::KeyEnsureRequest =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service
                .ensure_key(
                    scope,
                    PluginKeyEnsure {
                        resource_key: v.resource_key,
                        name: v.name,
                        group_resource_keys: v.group_resource_keys,
                        max_concurrency: v.max_concurrency,
                        requests_per_minute: v.requests_per_minute,
                        daily_limit_usd: v.daily_limit_usd,
                        weekly_limit_usd: v.weekly_limit_usd,
                    },
                )
                .await
                .map_err(error)?;
            serde_json::to_vec(&resources::ManagedResource {
                id: r.id,
                name: r.name,
                enabled: r.enabled,
            })
        }
        resources::GROUP_ENSURE => {
            let v: resources::GroupEnsureRequest =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service
                .ensure_group(
                    scope,
                    PluginGroupEnsure {
                        resource_key: v.resource_key,
                        name: v.name,
                        color: v.color,
                        description: v.description,
                    },
                )
                .await
                .map_err(error)?;
            serde_json::to_vec(&resources::ManagedResource {
                id: r.id,
                name: r.name,
                enabled: r.enabled,
            })
        }
        resources::GROUP_MEMBERS => {
            let v: resources::GroupMembersChange =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let r = service
                .change_members(
                    scope,
                    PluginGroupMembers {
                        resource_key: v.resource_key,
                        add: v.add,
                        remove: v.remove,
                    },
                )
                .await
                .map_err(error)?;
            serde_json::to_vec(&resources::GroupMembersChanged {
                added: r.added,
                removed: r.removed,
            })
        }
        _ => {
            return Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unsupported resource operation",
            ));
        }
    }
    .map_err(|_| invalid())?;
    Ok(RpcReply {
        result: json!({}),
        payload,
    })
}
