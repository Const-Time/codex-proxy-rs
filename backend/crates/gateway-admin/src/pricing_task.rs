//! 周期同步只在控制面运行；失败保留 PostgreSQL 中的最后成功价目。
use crate::SettingsService;
use futures::future::BoxFuture;
use gateway_core::{
    lifecycle::CancellationToken,
    task::{DaemonTask, WorkerTaskError},
};
use std::{sync::Arc, time::Duration};

pub(crate) struct PricingTask(pub Arc<dyn SettingsService>);

impl DaemonTask for PricingTask {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Ok(()),
                    result = self.0.refresh_pricing() => {
                        if let Err(error) = result {
                            tracing::warn!(error = %error, "pricing synchronization failed; retaining last validated prices");
                        }
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
