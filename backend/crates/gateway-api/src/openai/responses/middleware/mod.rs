//! Request middleware keeps Core's admission, atomic delivery and cleanup owners.
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
