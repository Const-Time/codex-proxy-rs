//! Native invocation identities stay in the host, indexed by their authenticated RPC scope.
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
pub(crate) struct InvocationCalls(Mutex<BTreeMap<String, InvocationEntry>>);
struct InvocationEntry {
    caller: Option<Arc<InvocationCaller>>,
    pending: Vec<gateway_core::engine::nested::ExtensionCallCompletion>,
}
pub(crate) struct InvocationCaller {
    calls: std::sync::Weak<InvocationCalls>,
    actor: MutationContext,
    context: gateway_plugin_sdk::CallContext,
    pub(crate) graph: gateway_core::engine::nested::ExtensionCallScope,
    pub(crate) deadline: gateway_core::lifecycle::Deadline,
    pub(crate) invocation: Option<PluginInvocationContext>,
    pub(crate) cancellation: gateway_core::lifecycle::CancellationToken,
}
impl InvocationCalls {
    pub(crate) fn enter(
        self: &Arc<Self>,
        context: gateway_plugin_sdk::CallContext,
        actor: MutationContext,
        mut invocation: Option<PluginInvocationContext>,
        revocation: gateway_core::lifecycle::CancellationToken,
        graph: Option<gateway_core::engine::nested::ExtensionCallScope>,
    ) -> Result<InvocationCall, PluginFault> {
        let scope = context.resource_scope_id.clone();
        let graph = invocation
            .as_ref()
            .map(|origin| origin.context.call_scope.clone())
            .or(graph)
            .unwrap_or_default();
        let deadline = invocation
            .as_ref()
            .map(|origin| origin.context.deadline_at)
            .unwrap_or_default()
            .cap(
                std::time::SystemTime::now() + std::time::Duration::from_millis(context.timeout_ms),
            );
        let cancellation = gateway_core::lifecycle::CancellationToken::linked(
            std::iter::once(revocation)
                .chain(invocation.iter().map(|i| i.context.cancellation.clone())),
        );
        if let Some(invocation) = &mut invocation {
            invocation.context.parent_call_id = Some(std::mem::replace(
                &mut invocation.context.call_id,
                scope.clone(),
            ));
            invocation.context.request_id = actor.request_id.clone();
            invocation.context.cancellation = cancellation.clone();
            invocation.services = invocation.services.as_ref().map(|dispatcher| {
                dispatcher.scoped(&gateway_core::engine::middleware::service::Context {
                    operation: "management.handle",
                    request_id: actor.request_id.clone(),
                    call_id: scope.clone(),
                    parent_call_id: invocation.context.parent_call_id.clone(),
                    cancellation: cancellation.clone(),
                    deadline_at: invocation.context.deadline_at,
                    call_scope: invocation.context.call_scope.clone(),
                    dispatcher: None,
                })
            });
        }
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, entry| {
            entry.caller.is_some() || entry.pending.iter().any(|pending| !pending.is_complete())
        });
        if entries.len() >= 32 || entries.contains_key(&scope) {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "native invocation capacity exhausted",
            ));
        }
        entries.insert(
            scope.clone(),
            InvocationEntry {
                caller: Some(Arc::new(InvocationCaller {
                    calls: Arc::downgrade(self),
                    actor,
                    context,
                    graph,
                    deadline,
                    invocation,
                    cancellation,
                })),
                pending: vec![],
            },
        );
        Ok(InvocationCall {
            calls: self.clone(),
            scope,
        })
    }
    pub(crate) fn actor(&self, scope: &str) -> Option<MutationContext> {
        self.caller(scope).map(|caller| caller.actor.clone())
    }
    pub(crate) fn caller(&self, scope: &str) -> Option<Arc<InvocationCaller>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(scope)
            .and_then(|entry| entry.caller.clone())
    }
    pub(crate) fn revoke(&self) {
        for caller in self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter_map(|entry| entry.caller.as_ref())
        {
            caller.cancellation.cancel();
        }
    }
    pub(crate) async fn drain(&self) {
        // RPC shutdown must first prevent callbacks from starting new native work.
        let pending: Vec<_> = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values_mut()
            .flat_map(|entry| {
                if let Some(caller) = entry.caller.take() {
                    caller.cancellation.cancel();
                }
                entry.pending.clone()
            })
            .collect();
        futures::future::join_all(pending.iter().map(|pending| pending.wait())).await;
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}
impl InvocationCaller {
    pub(crate) fn track(
        self: &Arc<Self>,
        cleanup: &gateway_core::engine::nested::ExtensionCallScope,
    ) -> Result<(), PluginFault> {
        let pending = cleanup.completion().ok_or_else(|| {
            PluginFault::new(
                ErrorCode::PermissionDenied,
                "native cleanup must own a child permit",
            )
        })?;
        let calls = self
            .calls
            .upgrade()
            .ok_or_else(|| PluginFault::new(ErrorCode::Cancelled, "native invocation ended"))?;
        let mut entries = calls
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries
            .get_mut(&self.context.resource_scope_id)
            .filter(|entry| {
                entry
                    .caller
                    .as_ref()
                    .is_some_and(|caller| Arc::ptr_eq(caller, self))
                    && !self.cancellation.is_cancelled()
            })
            .ok_or_else(|| PluginFault::new(ErrorCode::Cancelled, "native invocation ended"))?;
        entry.pending.retain(|pending| !pending.is_complete());
        entry.pending.push(pending);
        Ok(())
    }
    pub(crate) fn actor(&self) -> &MutationContext {
        &self.actor
    }
    pub(crate) fn authorizes(&self, context: &gateway_plugin_sdk::CallContext) -> bool {
        let mut expected = self.context.clone();
        expected.call_id = context.call_id;
        expected.resource_stream = context.resource_stream;
        expected == *context && !self.cancellation.is_cancelled() && !self.deadline.is_elapsed()
    }
}
pub(crate) struct InvocationCall {
    calls: Arc<InvocationCalls>,
    scope: String,
}
impl Drop for InvocationCall {
    fn drop(&mut self) {
        let pending = {
            let mut entries = self
                .calls
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(entry) = entries.get_mut(&self.scope) else {
                return;
            };
            if let Some(caller) = entry.caller.take() {
                caller.cancellation.cancel();
            }
            entry.pending.retain(|pending| !pending.is_complete());
            if entry.pending.is_empty() {
                entries.remove(&self.scope);
                return;
            }
            entry.pending.clone()
        };
        let calls = Arc::downgrade(&self.calls);
        let scope = self.scope.clone();
        tokio::spawn(async move {
            futures::future::join_all(pending.iter().map(|pending| pending.wait())).await;
            if let Some(calls) = calls.upgrade() {
                let mut entries = calls
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if entries.get(&scope).is_some_and(|entry| {
                    entry.caller.is_none()
                        && entry.pending.iter().all(|pending| pending.is_complete())
                }) {
                    entries.remove(&scope);
                }
            }
        });
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
