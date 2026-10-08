//! Request middleware keeps Core's admission, atomic delivery and cleanup owners.
mod attempt;
mod body;
mod delivery;
pub(super) mod request;

pub use delivery::deliver_middleware_response;
use gateway_core::engine::{
    execution::ExecutionSession,
    middleware::{MiddlewareError, MiddlewareResponse},
};

/// Build an uncommitted response for a middleware terminal. Dropping the response
/// delegates cancellation and settlement to the existing PendingExecution owner.
pub async fn execution_middleware_response(
    session: Box<dyn ExecutionSession>,
    streaming: bool,
) -> Result<MiddlewareResponse, MiddlewareError> {
    body::ExecutionBody::response(session, streaming).await
}

/// Bind the Responses wire adapter to a frozen, root-scoped publication snapshot.
/// Core remains the sole owner of provider execution and original receipts.
pub fn responses_attempt_middleware(
    registry: std::sync::Arc<gateway_admin::plugins::middleware::PluginMiddlewareRegistry>,
    mut context: gateway_core::engine::middleware::MiddlewareContext,
) -> std::sync::Arc<dyn gateway_core::engine::attempt_middleware::AttemptMiddleware> {
    context.mount = gateway_core::engine::middleware::MiddlewareMount::Attempt;
    let streaming = context.transport == gateway_core::engine::execution::ClientTransport::HttpSse;
    std::sync::Arc::new(attempt::ResponsesAttempt {
        registry,
        context,
        streaming,
    })
}
