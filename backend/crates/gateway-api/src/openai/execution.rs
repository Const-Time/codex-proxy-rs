//! The API owns starts until the receiving request takes over the session.
//!
//! Admission and budget ports can have committed side effects before their await
//! returns. Dropping an HTTP handler or an RPC next callback must not drop that
//! in-progress start. Drive it to completion and hand off a guarded result.
use std::future::Future;

use gateway_core::engine::execution::StartedExecution;
use gateway_core::error::{GatewayError, GatewayErrorKind};
use tokio::sync::oneshot;

use super::responses::PendingExecution;

pub(super) async fn start(
    future: impl Future<Output = Result<StartedExecution, GatewayError>> + Send + 'static,
) -> Result<StartedExecution, GatewayError> {
    let (sender, receiver) = oneshot::channel();
    // There is deliberately no select/timeout around `future`: Core owns the
    // deadline and cleanup of effects that may already have been admitted.
    drop(tokio::spawn(async move {
        if sender.is_closed() {
            return;
        }
        let result = future.await.map(|started| PendingStart(Some(started)));
        // A closed receiver, including one dropped with an unread queued result,
        // releases PendingStart and therefore retains the finalization owner.
        let _ = sender.send(result);
    }));
    let mut pending = receiver.await.map_err(|_| {
        GatewayError::new(GatewayErrorKind::Internal, "execution start task stopped")
    })??;
    pending.0.take().ok_or_else(|| {
        GatewayError::new(
            GatewayErrorKind::Internal,
            "execution session is unavailable",
        )
    })
}

struct PendingStart(Option<StartedExecution>);

impl Drop for PendingStart {
    fn drop(&mut self) {
        if let Some(started) = self.0.take() {
            drop(PendingExecution::new(started.session));
        }
    }
}
