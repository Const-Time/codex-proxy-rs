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
    binding: PluginMiddlewareBinding,
    process: Option<Arc<dyn PluginProcess>>,
}
#[derive(Clone, Default)]
pub(super) struct Publication {
    entries: BTreeMap<String, Entry>,
    blocked: BTreeSet<String>,
    epoch: PluginAccess,
}
impl Publication {
    pub(super) fn snapshot(&self) -> PluginMiddlewareRegistry {
        PluginMiddlewareRegistry(self.clone())
    }
    pub(super) fn publish(
        &mut self,
        record: &PluginRecord,
        process: Option<Arc<dyn PluginProcess>>,
    ) {
        let binding = record.middleware.as_ref().filter(|_| record.enabled);
        if binding.is_some_and(|binding| binding.validate().is_err()) {
            self.block(&record.id);
            return;
        }
        if self.entries.contains_key(&record.id)
            || self.blocked.contains(&record.id)
            || binding.is_some()
        {
            self.epoch.revoke();
            self.epoch = PluginAccess::default();
            self.entries.remove(&record.id);
            self.blocked.remove(&record.id);
            if let Some(binding) = binding {
                self.entries.insert(
                    record.id.clone(),
                    Entry {
                        binding: binding.clone(),
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
        let mut selected = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.binding.matches(&context))
            .collect::<Vec<_>>();
        selected.sort_by_key(|(id, entry)| (entry.binding.order, *id));
        let mut processes = Vec::with_capacity(selected.len());
        let mut tokens = vec![context.cancellation.clone(), self.epoch.cancellation()];
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
        context.cancellation = CancellationToken::linked(tokens);
        Ok(PluginMiddlewarePlan { context, processes })
    }
}

/// A single publication snapshot, captured before request preparation. A version
/// switch revokes it even if preparation has not yet entered the first layer.
pub struct PluginMiddlewareRegistry(Publication);
impl PluginMiddlewareRegistry {
    pub fn freeze(&self, context: MiddlewareContext) -> Result<PluginMiddlewarePlan, AdminError> {
        self.0.freeze(context)
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
