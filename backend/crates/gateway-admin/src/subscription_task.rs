//! Reconcile subscriptions and sample quota estimates without an open administrator page.
use crate::use_case::accounts::DefaultAccountsService;
use futures::future::BoxFuture;
use gateway_core::task::{DaemonTask, WorkerTaskError};
use std::{sync::Arc, time::Duration};

pub(crate) struct SubscriptionTask(pub Arc<DefaultAccountsService>);
impl DaemonTask for SubscriptionTask {
    fn run(
        &self,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        self.run_with_context(gateway_core::task::WorkerDaemonContext::new(cancellation))
    }

    fn run_with_context(
        &self,
        context: gateway_core::task::WorkerDaemonContext,
    ) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        let cancellation = context.cancellation().clone();
        Box::pin(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    result = self.0.sync_quota_observations() => {
                        if result.is_err() { tracing::warn!("background quota synchronization failed; will retry"); } else { context.succeeded(); }
                    }
                }
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        })
    }
}
