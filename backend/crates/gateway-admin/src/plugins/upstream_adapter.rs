//! Compile registered adapter facts into the existing publication; no credential or RPC ownership.
use super::{
    middleware::{PluginMiddlewareRegistry, Publication},
    *,
};
use gateway_core::{
    engine::{
        AttemptContext,
        upstream_adapter::{UpstreamAdapter, UpstreamAdapterPlan},
    },
    error::{ProviderError, ProviderErrorKind},
    identity::ProviderKind,
    lifecycle::CancellationToken,
    routing::UpstreamModelId,
    upstream::UpstreamSendState,
};

fn scopes_overlap(left: &PluginExecutionScope, right: &PluginExecutionScope) -> bool {
    match (left, right) {
        (PluginExecutionScope::Keys { ids: left }, PluginExecutionScope::Keys { ids: right })
        | (
            PluginExecutionScope::Groups { ids: left },
            PluginExecutionScope::Groups { ids: right },
        ) => left.iter().any(|id| right.contains(id)),
        // A Key can belong to a group; configuration cannot prove these scopes disjoint.
        _ => true,
    }
}
fn declarations_overlap(
    left: &PluginUpstreamAdapterDeclaration,
    right: &PluginUpstreamAdapterDeclaration,
) -> bool {
    left.provider == right.provider
        && (left.models.is_empty()
            || right.models.is_empty()
            || left.models.iter().any(|model| right.models.contains(model)))
}
impl Publication {
    pub(super) fn validate_upstream_candidate(
        &self,
        record: &PluginRecord,
        process: Option<&Arc<dyn PluginProcess>>,
    ) -> Result<(), AdminError> {
        let Some(binding) = record.upstream_adapter.as_ref().filter(|_| record.enabled) else {
            return Ok(());
        };
        binding.validate()?;
        let process = process.ok_or_else(|| AdminError::unavailable("上游适配器尚未完成注册"))?;
        let declarations = process.upstream_adapters();
        if declarations.is_empty()
            || declarations
                .iter()
                .any(|d| process.upstream_adapter(&d.id).is_none())
        {
            return Err(AdminError::invalid("插件没有可执行的上游适配器"));
        }
        for (index, left) in declarations.iter().enumerate() {
            if declarations[index + 1..]
                .iter()
                .any(|right| declarations_overlap(left, right))
            {
                return Err(AdminError::invalid(
                    "上游适配器的 Provider 与公开模型范围重叠",
                ));
            }
        }
        for (id, entry) in &self.entries {
            let Some(other) = entry.upstream.as_ref().filter(|_| *id != record.id) else {
                continue;
            };
            if !scopes_overlap(&binding.scope, &other.scope) {
                continue;
            }
            let Some(existing) = entry.process.as_ref() else {
                return Err(AdminError::unavailable(
                    "重叠作用范围内的已有适配器尚未恢复",
                ));
            };
            if existing.upstream_adapters().iter().any(|left| {
                declarations
                    .iter()
                    .any(|right| declarations_overlap(left, right))
            }) {
                return Err(AdminError::invalid(
                    "上游适配器的 Provider、公开模型和作用范围重叠",
                ));
            }
        }
        Ok(())
    }
}
impl std::fmt::Debug for PluginMiddlewareRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginMiddlewareRegistry")
            .field("instances", &self.0.entries.len())
            .finish_non_exhaustive()
    }
}
impl UpstreamAdapterPlan for PluginMiddlewareRegistry {
    fn cancellation(&self) -> CancellationToken {
        self.1.clone()
    }
    fn select(
        &self,
        context: &AttemptContext,
        provider: &ProviderKind,
        model: &UpstreamModelId,
    ) -> Result<Option<Arc<dyn UpstreamAdapter>>, ProviderError> {
        let unavailable =
            || ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent);
        if !self.0.blocked.is_empty() || self.0.epoch.is_revoked() || self.1.is_cancelled() {
            return Err(unavailable());
        }
        let groups = context
            .account_scope()
            .map(|scope| {
                scope
                    .routing_snapshot()
                    .groups_snapshot()
                    .iter()
                    .map(|g| g.id().clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let model = context
            .requested_model()
            .map_or(model.as_str(), |m| m.as_str());
        let mut selected = None;
        for (id, entry) in &self.0.entries {
            if context
                .nested_context()
                .is_some_and(|parent| parent.scope().excluded_instances().contains(id))
            {
                continue;
            }
            let Some(binding) = entry
                .upstream
                .as_ref()
                .filter(|b| b.scope.matches(context.client_api_key_ref(), &groups))
            else {
                continue;
            };
            let _ = binding;
            let process = entry.process.as_ref().ok_or_else(unavailable)?;
            for declaration in process.upstream_adapters() {
                if declaration.provider != provider.as_str()
                    || (!declaration.models.is_empty()
                        && !declaration.models.iter().any(|m| m == model))
                {
                    continue;
                }
                let access = process.access();
                if process.status() != "ready" || access.is_revoked() || selected.is_some() {
                    return Err(unavailable());
                }
                let adapter = process
                    .upstream_adapter(&declaration.id)
                    .ok_or_else(unavailable)?;
                if !self.1.link_parent(access.cancellation()) {
                    return Err(unavailable());
                }
                selected = Some(adapter);
            }
        }
        Ok(selected)
    }
}
