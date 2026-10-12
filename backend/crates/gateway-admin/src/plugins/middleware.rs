//! Atomic publication and frozen invocation plans. Unavailable required layers
//! remain gates; a failed replacement must never silently remove middleware.
use super::*;
use futures::future::BoxFuture;
use gateway_core::{
    engine::middleware::*,
    lifecycle::CancellationToken,
    middleware::{Middleware, compose},
};
use std::collections::BTreeSet;

#[derive(Clone)]
struct Entry {
    binding: Option<PluginMiddlewareBinding>,
    router: Option<PluginModelRouterBinding>,
    retry: Option<PluginRetryPolicyBinding>,
    scheduler: Option<PluginAccountSchedulerBinding>,
    process: Option<Arc<dyn PluginProcess>>,
}
struct SelectedMiddleware {
    processes: Vec<Arc<dyn PluginProcess>>,
    tokens: Vec<CancellationToken>,
}
#[derive(Clone, Default)]
pub(super) struct Publication {
    entries: BTreeMap<String, Entry>,
    blocked: BTreeSet<String>,
    epoch: PluginAccess,
}
impl Publication {
    pub(super) fn snapshot(&self) -> PluginMiddlewareRegistry {
        PluginMiddlewareRegistry(
            self.clone(),
            CancellationToken::linked([self.epoch.cancellation()]),
        )
    }
    pub(super) fn publish(
        &mut self,
        record: &PluginRecord,
        process: Option<Arc<dyn PluginProcess>>,
    ) {
        let binding = record.middleware.as_ref().filter(|_| record.enabled);
        let router = record.model_router.as_ref().filter(|_| record.enabled);
        let retry = record.retry_policy.as_ref().filter(|_| record.enabled);
        let scheduler = record.account_scheduler.as_ref().filter(|_| record.enabled);
        if binding.is_some_and(|binding| binding.validate().is_err())
            || router.is_some_and(|r| r.validate().is_err())
            || retry.is_some_and(|r| r.validate().is_err())
            || scheduler.is_some_and(|s| s.validate().is_err())
        {
            self.block(&record.id);
            return;
        }
        if self.entries.contains_key(&record.id)
            || self.blocked.contains(&record.id)
            || binding.is_some()
            || router.is_some()
            || retry.is_some()
            || scheduler.is_some()
        {
            self.epoch.revoke();
            self.epoch = PluginAccess::default();
            self.entries.remove(&record.id);
            self.blocked.remove(&record.id);
            if binding.is_some() || router.is_some() || scheduler.is_some() || retry.is_some() {
                self.entries.insert(
                    record.id.clone(),
                    Entry {
                        binding: binding.cloned(),
                        router: router.cloned(),
                        retry: retry.cloned(),
                        scheduler: scheduler.cloned(),
                        process,
                    },
                );
            }
        }
    }
    pub(super) fn block(&mut self, id: &str) {
        self.epoch.revoke();
        self.blocked.insert(id.to_owned());
    }
    pub(super) fn remove(&mut self, id: &str) {
        if self.entries.remove(id).is_some() | self.blocked.remove(id) {
            self.epoch.revoke();
            self.epoch = PluginAccess::default();
        }
    }
    pub(super) fn shutdown(&self) {
        self.epoch.revoke();
    }
    fn select(
        &self,
        excluded: &BTreeSet<String>,
        matches: impl Fn(&PluginMiddlewareBinding) -> bool,
    ) -> Result<SelectedMiddleware, AdminError> {
        if !self.blocked.is_empty() || self.epoch.is_revoked() {
            return Err(AdminError::unavailable("中间件配置状态未确认"));
        }
        let mut selected = self
            .entries
            .iter()
            .filter(|(id, entry)| {
                !excluded.contains(*id) && entry.binding.as_ref().is_some_and(&matches)
            })
            .collect::<Vec<_>>();
        selected.sort_by_key(|(id, entry)| (entry.binding.as_ref().map_or(0, |b| b.order), *id));
        let mut processes = Vec::with_capacity(selected.len());
        let mut tokens = vec![self.epoch.cancellation()];
        for (_, entry) in selected {
            let process = entry
                .process
                .as_ref()
                .filter(|p| p.status() == "ready")
                .ok_or_else(|| AdminError::unavailable("必需的中间件暂不可用"))?;
            let access = process.access();
            if access.is_revoked() {
                return Err(AdminError::unavailable("中间件版本已失效"));
            }
            tokens.push(access.cancellation());
            processes.push(process.clone());
        }
        Ok(SelectedMiddleware { processes, tokens })
    }
    fn freeze_http(
        &self,
        mut context: gateway_core::engine::middleware::http::Context,
    ) -> Result<super::http::PluginHttpMiddlewarePlan, AdminError> {
        let SelectedMiddleware { processes, tokens } =
            self.select(&context.excluded_plugin_instances, |binding| {
                binding.mounts.contains(&PluginMiddlewareMount::Http)
                    && matches!(binding.scope, PluginExecutionScope::All {})
            })?;
        context.cancellation =
            CancellationToken::linked(std::iter::once(context.cancellation.clone()).chain(tokens));
        Ok(super::http::PluginHttpMiddlewarePlan::new(
            context, processes,
        ))
    }
    pub(super) fn freeze(
        &self,
        mut context: MiddlewareContext,
    ) -> Result<PluginMiddlewarePlan, AdminError> {
        if !self.blocked.is_empty() || self.epoch.is_revoked() {
            return Err(AdminError::unavailable("中间件配置状态未确认"));
        }
        if !matches!(
            context.mount,
            MiddlewareMount::Request | MiddlewareMount::Attempt
        ) {
            return Err(AdminError::invalid("此中间件挂载位置尚未接入"));
        }
        let SelectedMiddleware { processes, tokens } = self
            .select(&context.excluded_plugin_instances, |binding| {
                binding.matches(&context)
            })?;
        let tokens = std::iter::once(context.cancellation.clone()).chain(tokens);
        context.cancellation = CancellationToken::linked(tokens);
        Ok(PluginMiddlewarePlan { context, processes })
    }
}

/// A single publication snapshot, captured before request preparation. A version
/// switch revokes it even if preparation has not yet entered the first layer.
pub struct PluginMiddlewareRegistry(Publication, CancellationToken);
impl PluginMiddlewareRegistry {
    pub fn freeze_http(
        &self,
        mut context: gateway_core::engine::middleware::http::Context,
    ) -> Result<super::http::PluginHttpMiddlewarePlan, AdminError> {
        context.cancellation = CancellationToken::linked([context.cancellation, self.1.clone()]);
        self.0.freeze_http(context)
    }
    pub fn freeze(
        &self,
        mut context: MiddlewareContext,
    ) -> Result<PluginMiddlewarePlan, AdminError> {
        context.cancellation = CancellationToken::linked([context.cancellation, self.1.clone()]);
        self.0.freeze(context)
    }
}

impl gateway_core::engine::model_routing::ModelRouter for PluginMiddlewareRegistry {
    fn cancellation(&self) -> CancellationToken {
        self.1.clone()
    }
    fn route_model(
        &self,
        input: gateway_core::engine::model_routing::ModelRouteInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::model_routing::ModelRouteResult,
            gateway_core::engine::model_routing::ModelRoutingFault,
        >,
    > {
        use gateway_core::engine::model_routing::*;
        let publication = self.0.clone();
        let revocation = self.1.clone();
        Box::pin(async move {
            if !publication.blocked.is_empty() || publication.epoch.is_revoked() {
                return Err(ModelRoutingFault);
            }
            let mut selected = publication
                .entries
                .iter()
                .filter(|(id, entry)| {
                    !input.excluded_instances().contains(*id)
                        && entry.router.as_ref().is_some_and(|binding| {
                            binding
                                .scope
                                .matches(input.client_key_id(), &input.account_group_ids())
                        })
                })
                .collect::<Vec<_>>();
            selected.sort_by_key(|(id, entry)| (entry.router.as_ref().map_or(0, |r| r.order), *id));
            let mut tokens = vec![publication.epoch.cancellation()];
            let mut decision = ModelRouteDecision::Unhandled;
            for (_, entry) in selected {
                let binding = entry.router.as_ref().ok_or(ModelRoutingFault)?;
                let result = if let Some(process) = entry
                    .process
                    .as_ref()
                    .filter(|p| p.status() == "ready" && !p.access().is_revoked())
                {
                    let access = process.access();
                    let result = tokio::select! {
                        biased;
                        () = access.revoked() => Err(ModelRoutingFault),
                        result = process.route_model(input.clone()) => result,
                    };
                    if result.is_ok() && !access.is_revoked() {
                        tokens.push(access.cancellation());
                    } else if access.is_revoked() {
                        if binding.failure_policy == PluginPolicyFailurePolicy::Delegate {
                            continue;
                        } else {
                            return Err(ModelRoutingFault);
                        }
                    }
                    result
                } else {
                    Err(ModelRoutingFault)
                };
                match result {
                    Ok(ModelRouteDecision::Unhandled) => {}
                    Ok(result) => {
                        decision = result;
                        break;
                    }
                    Err(_) if binding.failure_policy == PluginPolicyFailurePolicy::Delegate => {}
                    Err(_) => return Err(ModelRoutingFault),
                }
            }
            for token in tokens {
                if !revocation.link_parent(token) {
                    return Err(ModelRoutingFault);
                }
            }
            Ok(ModelRouteResult {
                decision,
                revocation,
            })
        })
    }
}

impl gateway_core::engine::account_scheduling::AccountScheduler for PluginMiddlewareRegistry {
    fn cancellation(&self) -> CancellationToken {
        self.1.clone()
    }
    fn schedule_account(
        &self,
        input: gateway_core::engine::account_scheduling::AccountScheduleInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::account_scheduling::AccountScheduleResult,
            gateway_core::engine::account_scheduling::AccountSchedulingFault,
        >,
    > {
        use gateway_core::engine::account_scheduling::*;
        let publication = self.0.clone();
        let revocation = self.1.clone();
        Box::pin(async move {
            if !publication.blocked.is_empty() || publication.epoch.is_revoked() {
                return Err(AccountSchedulingFault);
            }
            let selected = publication
                .entries
                .iter()
                .filter(|(id, entry)| {
                    !input.excluded_instances().contains(*id)
                        && entry.scheduler.as_ref().is_some_and(|binding| {
                            binding
                                .scope
                                .matches(input.client_key_id(), &input.account_group_ids())
                        })
                })
                .min_by_key(|(id, entry)| (entry.scheduler.as_ref().map_or(0, |s| s.order), *id));
            let mut tokens = vec![publication.epoch.cancellation()];
            let decision = if let Some((_, entry)) = selected {
                let binding = entry.scheduler.as_ref().ok_or(AccountSchedulingFault)?;
                let result = if let Some(process) = entry
                    .process
                    .as_ref()
                    .filter(|p| p.status() == "ready" && !p.access().is_revoked())
                {
                    let access = process.access();
                    let result = tokio::select! {
                        biased;
                        ()=access.revoked()=>Err(AccountSchedulingFault),
                        result=process.schedule_account(input)=>result,
                    };
                    if result.is_ok() && !access.is_revoked() {
                        tokens.push(access.cancellation());
                        result
                    } else {
                        Err(AccountSchedulingFault)
                    }
                } else {
                    Err(AccountSchedulingFault)
                };
                match result {
                    Ok(decision) => decision,
                    Err(_) if binding.failure_policy == PluginPolicyFailurePolicy::Delegate => {
                        AccountScheduleDecision::Delegate
                    }
                    Err(_) => return Err(AccountSchedulingFault),
                }
            } else {
                AccountScheduleDecision::Delegate
            };
            for token in tokens {
                if !revocation.link_parent(token) {
                    return Err(AccountSchedulingFault);
                }
            }
            Ok(AccountScheduleResult {
                decision,
                revocation,
            })
        })
    }
}

/// Only the manager can construct a plan. The API/Provider terminal retains
/// identity, admission and settlement; it must link this cancellation before start.
pub struct PluginMiddlewarePlan {
    context: MiddlewareContext,
    processes: Vec<Arc<dyn PluginProcess>>,
}
impl PluginMiddlewarePlan {
    pub fn cancellation(&self) -> CancellationToken {
        self.context.cancellation.clone()
    }
    pub fn is_empty(&self) -> bool {
        self.processes.is_empty()
    }
    pub async fn run(
        self,
        request: MiddlewareRequest,
        terminal: MiddlewareNext,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        let cancellation = self.context.cancellation.clone();
        let layers: Vec<Middleware<MiddlewareRequest, MiddlewareResponse, MiddlewareError>> = self
            .processes
            .into_iter()
            .map(|process| {
                let context = self.context.clone();
                Box::new(move |request, next| {
                    Box::pin(async move { process.middleware(context, request, next).await })
                        as BoxFuture<'static, _>
                }) as Middleware<_, _, _>
            })
            .collect();
        let next = compose(layers, move |request| terminal.run(request));
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(MiddlewareError::Fault),
            result = next.run(request) => result?,
        };
        let (protocol, status, headers, body, envelope) = response.into_parts();
        let mut response = MiddlewareResponse::new(
            protocol,
            status,
            headers,
            Box::new(RevocableBody { body, cancellation }),
        );
        if let Some(envelope) = envelope {
            response = response.with_envelope(envelope);
        }
        Ok(response)
    }
}

/// Holds revocation beyond RPC End and checks the final commit, including
/// pass-through and short-circuit responses that never create a plugin stream.
struct RevocableBody {
    body: Box<dyn MiddlewareBody>,
    cancellation: CancellationToken,
}
impl MiddlewareBody for RevocableBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => Err(MiddlewareError::Fault),
                result = self.body.next_frame() => result,
            }
        })
    }
    fn commit_downstream(
        &mut self,
        status: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            if self.cancellation.is_cancelled() {
                return Err(MiddlewareError::Fault);
            }
            // Never select-drop a commit with possible ledger side effects.
            self.body.commit_downstream(status).await
        })
    }
    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        self.body.record_client_status(status)
    }
    fn is_finalized(&self) -> bool {
        self.body.is_finalized()
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        self.body.close()
    }
}

impl gateway_core::engine::retry_policy::RetryPolicy for PluginMiddlewareRegistry {
    fn cancellation(&self) -> CancellationToken {
        self.1.clone()
    }
    fn decide_retry(
        &self,
        input: gateway_core::engine::retry_policy::RetryInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::retry_policy::RetryResult,
            gateway_core::engine::retry_policy::RetryPolicyFault,
        >,
    > {
        use gateway_core::engine::retry_policy::*;
        let publication = self.0.clone();
        let revocation = self.1.clone();
        Box::pin(async move {
            if !publication.blocked.is_empty() || publication.epoch.is_revoked() {
                return Err(RetryPolicyFault);
            }
            let began = std::time::Instant::now();
            let budget = input.deadline().bounded(MAXIMUM_POLICY_WAIT);
            let mut selected = publication
                .entries
                .iter()
                .filter(|(id, entry)| {
                    !input.excluded_instances().contains(*id)
                        && entry.retry.as_ref().is_some_and(|binding| {
                            binding
                                .scope
                                .matches(input.client_key_id(), input.account_group_ids())
                        })
                })
                .collect::<Vec<_>>();
            selected.sort_by_key(|(id, entry)| (entry.retry.as_ref().map_or(0, |r| r.order), *id));
            let mut decision = RetryDecision::Delegate;
            for (_, entry) in selected {
                let timeout = input
                    .deadline()
                    .bounded(budget.saturating_sub(began.elapsed()));
                if timeout.is_zero() {
                    break;
                }
                if input.cancellation().is_cancelled() || revocation.is_cancelled() {
                    return Err(RetryPolicyFault);
                }
                let Some(process) = entry
                    .process
                    .as_ref()
                    .filter(|p| p.status() == "ready" && !p.access().is_revoked())
                else {
                    continue;
                };
                let access = process.access();
                let result = tokio::select! {
                    biased;
                    ()=access.revoked()=>Err(RetryPolicyFault),
                    result=process.retry_decision(input.clone(),timeout)=>result,
                };
                if access.is_revoked() {
                    continue;
                }
                let Ok(result) = result else {
                    continue;
                };
                // Link each successful consultation before entering the next RPC,
                // so a crashed preceding delegate can still revoke the whole chain.
                if !revocation.link_parent(access.cancellation()) {
                    return Err(RetryPolicyFault);
                }
                if revocation.is_cancelled() {
                    return Err(RetryPolicyFault);
                }
                if result != RetryDecision::Delegate {
                    decision = result;
                    break;
                }
            }
            Ok(RetryResult {
                decision,
                revocation,
            })
        })
    }
}
