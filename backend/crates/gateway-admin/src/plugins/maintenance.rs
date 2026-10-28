//! Published instances reconcile under Host supervision and native Core availability.
use super::PluginManager;
use futures::{FutureExt as _, StreamExt, future::BoxFuture};
use gateway_core::{
    lifecycle::CancellationToken,
    runtime::SnapshotPublications,
    task::{
        DaemonRestartPolicy, DaemonTask, WorkerContribution, WorkerDefinitionError, WorkerId,
        WorkerKind, WorkerRegistration, WorkerRunnable, WorkerTaskError,
    },
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::Notify, task::JoinHandle};

const INTERVAL: Duration = Duration::from_secs(30);
const RETRY: Duration = Duration::from_secs(5);

impl PluginManager {
    pub fn maintenance_worker(
        self: &Arc<Self>,
        publications: SnapshotPublications,
    ) -> Result<WorkerContribution, WorkerDefinitionError> {
        Ok(WorkerContribution::Registration(
            WorkerRegistration::try_new(
                WorkerId::try_new(
                    WorkerKind::RuntimeSnapshotReconciliation,
                    "plugin-maintenance",
                )?,
                WorkerRunnable::Daemon {
                    restart: DaemonRestartPolicy::try_new(Duration::from_secs(1), INTERVAL)?,
                    task: Box::new(Maintenance {
                        manager: self.clone(),
                        publications,
                    }),
                },
            )?,
        ))
    }
}

struct Maintenance {
    manager: Arc<PluginManager>,
    publications: SnapshotPublications,
}
impl DaemonTask for Maintenance {
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
            let mut publications = self.publications.subscribe();
            let mut publication = publications.next().await.expect("initial Core publication");
            context.succeeded();
            let mut active = BTreeMap::<String, MaintenanceJob>::new();
            let mut dirty = true;
            loop {
                if cancellation.is_cancelled() || self.manager.stopping.is_cancelled() {
                    drain(&mut active).await;
                    return Ok(());
                }
                dirty |= self
                    .manager
                    .maintenance_changed
                    .notified()
                    .now_or_never()
                    .is_some();
                let current: BTreeMap<_, _> = if publication.revision().is_some()
                    && !publication.cancellation().is_cancelled()
                {
                    self.manager
                        .active
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .iter()
                        .filter(|(_, (_, p))| {
                            p.supports_maintenance()
                                && p.status() == "ready"
                                && !p.access().is_revoked()
                        })
                        .map(|(id, entry)| (id.clone(), entry.clone()))
                        .collect()
                } else {
                    BTreeMap::new()
                };
                let retired: Vec<_> = active
                    .iter()
                    .filter(|(id, job)| {
                        current.get(*id).is_none_or(|(revision, process)| {
                            *revision != job.revision || !Arc::ptr_eq(process, &job.process)
                        }) || job.stop.is_cancelled()
                            || job.task.is_finished()
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in retired {
                    if let Some(job) = active.remove(&id) {
                        job.stop().await;
                    }
                }
                for (id, (revision, process)) in current {
                    if let Some(job) = active.get(&id) {
                        if dirty {
                            job.notify.notify_one();
                        }
                        continue;
                    }
                    let stop = CancellationToken::linked([
                        cancellation.clone(),
                        self.manager.stopping.clone(),
                        publication.cancellation(),
                        process.access().cancellation(),
                    ]);
                    let notify = Arc::new(Notify::new());
                    notify.notify_one();
                    let task = tokio::spawn(reconcile(
                        id.clone(),
                        process.clone(),
                        stop.clone(),
                        notify.clone(),
                    ));
                    active.insert(
                        id,
                        MaintenanceJob {
                            revision,
                            process,
                            stop,
                            notify,
                            task,
                        },
                    );
                }
                dirty = false;
                tokio::select! { biased;
                    () = cancellation.cancelled() => {},
                    () = self.manager.stopping.cancelled() => {},
                    next = publications.next() => {
                        let Some(next) = next else {
                            drain(&mut active).await;
                            return Err(WorkerTaskError::safe("Core publication stream ended"));
                        };
                        dirty = next.revision() != publication.revision();
                        publication = next;
                    },
                    () = self.manager.maintenance_changed.notified() => { dirty = true; },
                    () = tokio::time::sleep(INTERVAL) => {},
                }
            }
        })
    }
}

struct MaintenanceJob {
    revision: u64,
    process: Arc<dyn crate::ports::plugins::PluginProcess>,
    stop: CancellationToken,
    notify: Arc<Notify>,
    task: JoinHandle<()>,
}
impl MaintenanceJob {
    async fn stop(self) {
        self.stop.cancel();
        let _ = self.task.await;
        self.process.settle_maintenance().await;
    }
}
async fn drain(jobs: &mut BTreeMap<String, MaintenanceJob>) {
    for job in jobs.values() {
        job.stop.cancel();
    }
    for (_, job) in std::mem::take(jobs) {
        job.stop().await;
    }
}

async fn reconcile(
    id: String,
    process: Arc<dyn crate::ports::plugins::PluginProcess>,
    stop: CancellationToken,
    dirty: Arc<Notify>,
) {
    loop {
        tokio::select! { biased;
            () = stop.cancelled() => return,
            () = dirty.notified() => {},
            () = tokio::time::sleep(INTERVAL) => {},
        }
        let result = tokio::select! { biased;
            () = stop.cancelled() => return,
            result = process.reconcile(stop.clone()) => result,
        };
        if result.is_err() {
            tracing::warn!(instance_id = id, "插件维护失败，等待重试");
            tokio::select! { biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(RETRY) => {},
            }
            dirty.notify_one();
        }
    }
}
