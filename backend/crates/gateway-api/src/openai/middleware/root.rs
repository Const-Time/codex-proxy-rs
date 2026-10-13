//! Freeze one publication and link both middleware mounts to the Core root.
use gateway_admin::plugins::middleware::{PluginMiddlewarePlan, PluginMiddlewareRegistry};
use gateway_core::{
    engine::{
        execution::{ClientTransport, PreparedRootExecution},
        middleware::*,
    },
    operation::{Operation, OperationKind},
};
use std::sync::Arc;

pub(super) struct RootPlan {
    pub prepared: PreparedRootExecution,
    pub plan: PluginMiddlewarePlan,
    pub has_attempts: bool,
    pub revocation: gateway_core::lifecycle::CancellationToken,
}
pub(super) fn context(
    prepared: &PreparedRootExecution,
    operation: &Operation,
    endpoint: &str,
    transport: ClientTransport,
    model: Option<String>,
) -> MiddlewareContext {
    MiddlewareContext {
        excluded_plugin_instances: prepared
            .nested_context()
            .scope()
            .excluded_instances()
            .clone(),
        request_id: prepared.request_id().clone(),
        client_key_id: prepared.client().policy().key_id().clone(),
        account_group_ids: prepared
            .client()
            .policy()
            .account_scope()
            .routing_snapshot()
            .groups_snapshot()
            .iter()
            .map(|g| g.id().clone())
            .collect(),
        mount: MiddlewareMount::Request,
        attempt_index: None,
        operation: operation.kind(),
        endpoint: endpoint.into(),
        transport,
        provider: None,
        model,
        account_id: None,
        cancellation: prepared.cancellation(),
        deadline_at: prepared.deadline_at(),
    }
}
pub(super) fn bind(
    prepared: PreparedRootExecution,
    registry: PluginMiddlewareRegistry,
    context: MiddlewareContext,
) -> Result<RootPlan, MiddlewareError> {
    let prepared = prepared.with_model_catalog(
        registry
            .model_catalog()
            .map_err(|_| MiddlewareError::Fault)?,
    )?;
    // Native children retain the service publication issued to their parent.
    let prepared = if prepared.nested_context().service_factory().is_none() {
        match registry.service_factory() {
            Some(factory) => prepared.with_service_factory(factory),
            None => prepared,
        }
    } else {
        prepared
    };
    let prepared = if prepared.nested_context().observer_plan().is_none() {
        match registry.observer_plan() {
            Some(plan) => prepared.with_observer_plan(plan),
            None => prepared,
        }
    } else {
        prepared
    };
    let registry = Arc::new(registry);
    let attempt_context = MiddlewareContext {
        mount: MiddlewareMount::Attempt,
        ..context.clone()
    };
    let attempts = registry
        .freeze(attempt_context.clone())
        .map_err(|_| MiddlewareError::Fault)?;
    let has_attempts = !attempts.is_empty();
    let prepared = prepared
        .with_revocation(attempts.cancellation())
        .with_model_router(registry.clone())
        .with_account_scheduler(registry.clone())
        .with_retry_policy(registry.clone())
        .with_upstream_adapters(registry.clone());
    let prepared = if has_attempts {
        let adapter = if context.operation == OperationKind::Generate {
            super::responses_attempt_middleware(registry.clone(), attempt_context)
        } else {
            super::raw_json_attempt_middleware(registry.clone(), attempt_context)
        };
        prepared.with_attempt_middleware(adapter)
    } else {
        prepared
    };
    let plan = registry
        .freeze(MiddlewareContext {
            cancellation: prepared.cancellation(),
            ..context
        })
        .map_err(|_| MiddlewareError::Fault)?;
    let cancellation = gateway_core::lifecycle::CancellationToken::linked([
        plan.cancellation(),
        attempts.cancellation(),
    ]);
    Ok(RootPlan {
        prepared: prepared.with_revocation(cancellation.clone()),
        plan,
        has_attempts,
        revocation: cancellation,
    })
}
