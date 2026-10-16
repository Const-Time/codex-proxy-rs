use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationContext, plugins::*},
    plugins::PluginManager,
    ports::plugins::{PluginProcess, PluginRuntime, PluginStore},
};
use gateway_core::{
    lifecycle::CancellationToken,
    runtime::RuntimeSnapshotHandle,
    task::{WorkerContribution, WorkerKind, WorkerRunnable},
};
use std::{sync::Arc, time::Duration};

struct Unused;
#[async_trait]
impl PluginStore for Unused {
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        unreachable!()
    }
    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn artifact_info(&self, _: &str) -> Result<PluginArtifactInfo, AdminError> {
        unreachable!()
    }
    async fn artifact(&self, _: &str) -> Result<PluginArtifact, AdminError> {
        unreachable!()
    }
    async fn put_artifact(&self, _: PluginArtifact, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn list(&self) -> Result<Vec<PluginRecord>, AdminError> {
        unreachable!()
    }
    async fn save(
        &self,
        _: &PluginRecord,
        _: u64,
        _: &MutationContext,
        _: Option<&PluginStateCommit>,
    ) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: u64, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn state_get(
        &self,
        _: &PluginStateScope,
        _: &str,
    ) -> Result<Option<PluginStateRecord>, AdminError> {
        unreachable!()
    }
    async fn state_put(
        &self,
        _: &PluginStateScope,
        _: &str,
        _: Option<u64>,
        _: Option<serde_json::Value>,
    ) -> Result<u64, AdminError> {
        unreachable!()
    }
}
#[async_trait]
impl PluginRuntime for Unused {
    fn describe(&self, _: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        unreachable!()
    }
    async fn inspect(&self, _: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        unreachable!()
    }
    async fn validate(
        &self,
        _: &PluginRecord,
        _: &PluginArtifactInfo,
        _: Option<&PluginArtifactInfo>,
    ) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn prepare(
        &self,
        _: &PluginRecord,
        _: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        unreachable!()
    }
}

#[tokio::test(start_paused = true)]
async fn empty_maintenance_is_a_host_daemon_and_stops_without_reading_store_or_preparing_candidates()
 {
    let manager = Arc::new(PluginManager::new(Arc::new(Unused), Arc::new(Unused)));
    let snapshots = RuntimeSnapshotHandle::default();
    let WorkerContribution::Registration(registration) = manager
        .maintenance_worker(snapshots.publications())
        .unwrap()
    else {
        panic!("registration");
    };
    assert_eq!(
        registration.id.kind(),
        WorkerKind::RuntimeSnapshotReconciliation
    );
    let WorkerRunnable::Daemon { restart, task } = registration.runnable else {
        panic!("daemon");
    };
    assert_eq!(restart.initial_backoff(), Duration::from_secs(1));
    assert_eq!(restart.maximum_backoff(), Duration::from_secs(30));
    let stop = CancellationToken::new();
    let cancellation = stop.clone();
    let worker = tokio::spawn(async move { task.run(cancellation).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
    assert!(!worker.is_finished());
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
