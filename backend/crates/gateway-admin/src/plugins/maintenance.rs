//! Published instances reconcile under Host supervision and native Core availability.
use super::{ActivePlugins, PluginManager};
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
use tokio::{sync::Notify, task::JoinSet};

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
            let mut active = ActivePlugins::new();
            let mut jobs = JoinSet::new();
            let mut notifications = Vec::<Arc<Notify>>::new();
            let mut batch = CancellationToken::new();
            let mut dirty = true;
            loop {
                if cancellation.is_cancelled() || self.manager.stopping.is_cancelled() {
                    drain(&batch, &mut jobs).await;
                    return Ok(());
                }
                dirty |= self
                    .manager
                    .maintenance_changed
                    .notified()
                    .now_or_never()
                    .is_some();
                let current = if publication.revision().is_some()
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
                if batch.is_cancelled() || !same_instances(&active, &current) {
                    drain(&batch, &mut jobs).await;
                    active = current;
                    notifications.clear();
                    batch = CancellationToken::linked([
                        cancellation.clone(),
                        self.manager.stopping.clone(),
                        publication.cancellation(),
                    ]);
                    for (id, (_, process)) in &active {
                        let notify = Arc::new(Notify::new());
                        notify.notify_one();
                        notifications.push(notify.clone());
                        let process = process.clone();
                        let id = id.clone();
                        let stop = CancellationToken::linked([
                            batch.clone(),
                            process.access().cancellation(),
                        ]);
                        jobs.spawn(async move {
                            reconcile(id, process, stop, notify).await;
                        });
                    }
                } else if dirty {
                    for notify in &notifications {
                        notify.notify_one();
                    }
                }
                dirty = false;
                tokio::select! { biased;
                    () = cancellation.cancelled() => {},
                    () = self.manager.stopping.cancelled() => {},
                    next = publications.next() => {
                        let Some(next) = next else {
                            drain(&batch, &mut jobs).await;
                            return Err(WorkerTaskError::safe("Core publication stream ended"));
                        };
                        dirty = next.revision() != publication.revision();
                        publication = next;
                    },
                    () = self.manager.maintenance_changed.notified() => { dirty = true; },
                    () = batch.cancelled(), if !active.is_empty() => {},
                    ended = jobs.join_next(), if !jobs.is_empty() => {
                        if ended.is_some_and(|result| result.is_err()) {
                            drain(&batch, &mut jobs).await;
                            return Err(WorkerTaskError::safe("plugin maintenance task failed"));
                        }
                        dirty = true;
                    },
                }
            }
        })
    }
}

fn same_instances(a: &ActivePlugins, b: &ActivePlugins) -> bool {
    a.len() == b.len()
        && a.iter().all(|(id, (revision, process))| {
            b.get(id)
                .is_some_and(|(other, p)| revision == other && Arc::ptr_eq(process, p))
        })
}
async fn drain(stop: &CancellationToken, jobs: &mut JoinSet<()>) {
    stop.cancel();
    while jobs.join_next().await.is_some() {}
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
