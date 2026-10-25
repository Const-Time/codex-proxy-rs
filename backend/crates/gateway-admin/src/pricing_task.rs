//! 周期同步只在控制面运行；失败保留 PostgreSQL 中的最后成功价目。
use crate::SettingsService;
use futures::future::BoxFuture;
use gateway_core::task::{DaemonTask, WorkerTaskError};
use std::{sync::Arc, time::Duration};

pub(crate) struct PricingTask(pub Arc<dyn SettingsService>);

impl DaemonTask for PricingTask {
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
                    biased;
                    () = cancellation.cancelled() => return Ok(()),
                    result = self.0.refresh_pricing() => {
                        if let Err(error) = result {
                            tracing::warn!(error = %error, "pricing synchronization failed; retaining last validated prices");
                        } else { context.succeeded(); }
                    }
                }
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(600)) => {}
                }
            }
        })
    }
}
