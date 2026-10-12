//! Frozen HTTP placement uses the same publication, generation and cancellation owners.
use crate::ports::plugins::PluginProcess;
use futures::future::BoxFuture;
use gateway_core::{
    engine::middleware::{MiddlewareError, http},
    lifecycle::CancellationToken,
    middleware::{Middleware, compose},
};
use std::sync::Arc;

pub struct PluginHttpMiddlewarePlan {
    context: http::Context,
    processes: Vec<Arc<dyn PluginProcess>>,
}
impl PluginHttpMiddlewarePlan {
    pub(super) fn new(context: http::Context, processes: Vec<Arc<dyn PluginProcess>>) -> Self {
        Self { context, processes }
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.context.cancellation.clone()
    }
    pub fn is_empty(&self) -> bool {
        self.processes.is_empty()
    }
    pub async fn run(
        self,
        request: http::Request,
        terminal: http::Next,
    ) -> Result<http::Response, MiddlewareError> {
        let cancellation = self.context.cancellation.clone();
        let deadline = self.context.deadline_at;
        let layers: Vec<Middleware<http::Request, http::Response, MiddlewareError>> = self
            .processes
            .into_iter()
            .map(|process| {
                let context = self.context.clone();
                Box::new(move |request, next| {
                    Box::pin(async move { process.http_middleware(context, request, next).await })
                        as BoxFuture<'static, _>
                }) as Middleware<_, _, _>
            })
            .collect();
        let next = compose(layers, move |request| terminal.run(request));
        let response = tokio::select! {
            biased;
            ()=cancellation.cancelled()=>return Err(MiddlewareError::Fault),
            ()=deadline.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
            result=next.run(request)=>result?,
        };
        if cancellation.is_cancelled() {
            return Err(MiddlewareError::Fault);
        }
        if deadline.is_elapsed() {
            return Err(MiddlewareError::Engine(
                gateway_core::engine::EngineError::Deadline,
            ));
        }
        Ok(response.map(|body| http::guard_body(body, cancellation, deadline)))
    }
}
