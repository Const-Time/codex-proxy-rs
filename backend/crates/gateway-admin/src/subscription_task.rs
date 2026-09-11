//! Reconcile persisted provider quota observations even when no administrator page is open.
use crate::use_case::accounts::DefaultAccountsService;
use futures::future::BoxFuture;
use gateway_core::{
    lifecycle::CancellationToken,
    task::{DaemonTask, WorkerTaskError},
};
use std::{sync::Arc, time::Duration};

pub(crate) struct SubscriptionTask(pub Arc<DefaultAccountsService>);
impl DaemonTask for SubscriptionTask {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    result = self.0.sync_subscription_quotas() => {
                        if result.is_err() { tracing::warn!("subscription quota synchronization failed; will retry"); }
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
