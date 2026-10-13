//! Frozen raw WebSocket layers share the publication and generation cancellation owners.
use crate::ports::plugins::PluginProcess;
use gateway_core::{
    engine::middleware::{MiddlewareError, websocket as core},
    lifecycle::CancellationToken,
    middleware::{Middleware, compose},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct PluginWebSocketMiddlewarePlan {
    processes: Arc<[Arc<dyn PluginProcess>]>,
    cancellation: CancellationToken,
}
impl PluginWebSocketMiddlewarePlan {
    pub(super) fn new(
        processes: Vec<Arc<dyn PluginProcess>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            processes: processes.into(),
            cancellation,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.processes.is_empty()
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub async fn run(
        &self,
        mut context: core::Context,
        message: core::Message,
        terminal: core::Next,
    ) -> Result<Option<core::Message>, MiddlewareError> {
        context.cancellation =
            CancellationToken::linked([context.cancellation, self.cancellation.clone()]);
        let cancellation = context.cancellation.clone();
        let deadline = context.deadline_at;
        let layers: Vec<Middleware<core::Message, Option<core::Message>, MiddlewareError>> = self
            .processes
            .iter()
            .map(|process| {
                let process = process.clone();
                let context = context.clone();
                Box::new(move |message, next| {
                    Box::pin(
                        async move { process.websocket_middleware(context, message, next).await },
                    ) as futures::future::BoxFuture<'static, _>
                }) as Middleware<_, _, _>
            })
            .collect();
        let next = compose(layers, move |message| terminal.run(message));
        tokio::select! {biased;
            ()=cancellation.cancelled()=>Err(MiddlewareError::Fault),
            ()=deadline.wait()=>Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
            result=next.run(message)=>{
                if cancellation.is_cancelled() { return Err(MiddlewareError::Fault); }
                if deadline.is_elapsed() { return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)); }
                result
            },
        }
    }
}
