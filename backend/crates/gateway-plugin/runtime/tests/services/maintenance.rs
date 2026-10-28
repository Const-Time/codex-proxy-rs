use super::*;
use gateway_admin::{
    plugins::PluginManager,
    ports::plugins::{PluginProcess, PluginRuntime, PluginStore},
};
use gateway_core::{
    account::{AccountSelectionPolicy, RotationStrategy},
    lifecycle::CancellationToken,
    routing::{ConfigRevision, RuntimeSnapshot},
    runtime::RuntimeSnapshotHandle,
    task::{WorkerContribution, WorkerRunnable, WorkerTaskError},
};
use gateway_plugin_runtime::PluginEngine;
use std::num::NonZeroU32;

fn snapshot(revision: u64) -> RuntimeSnapshot {
    RuntimeSnapshot::new(
        ConfigRevision::new(revision).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(1).unwrap(),
            Duration::ZERO,
        ),
        vec![],
        vec![],
        vec![],
    )
    .unwrap()
}

#[tokio::test]
async fn maintenance_account_probe_is_bound_only_while_active_and_never_charges_a_user_key() {
    let fixture = MaintenanceFixture::new("maintenance-account-probe").await;
    let process = fixture.process().await;
    assert!(process.reconcile(Default::default()).await.is_err());
    process.activate();
    tokio::time::timeout(
        Duration::from_secs(5),
        process.reconcile(Default::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let model = fixture.model_probe.as_ref().unwrap();
    assert_eq!(
        model.core.starts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(model.core.charges.lock().unwrap().is_empty());
    assert_eq!(
        model.core.bodies.lock().unwrap()[0]["max_output_tokens"],
        128
    );
    process.revoke();
    assert!(process.reconcile(Default::default()).await.is_err());
    assert_eq!(
        model.core.starts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    process.stop().await;
    fixture.manager.shutdown().await;
    model.manager.shutdown().await;
}
#[tokio::test]
async fn maintenance_model_detection_uses_full_budget_and_never_charges_a_user_key() {
    let fixture = MaintenanceFixture::new("maintenance-model-detection").await;
    let process = fixture.process().await;
    assert!(process.reconcile(Default::default()).await.is_err());
    process.activate();
    tokio::time::timeout(
        Duration::from_secs(5),
        process.reconcile(Default::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let model = fixture.model_probe.as_ref().unwrap();
    assert_eq!(
        model.core.starts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(model.core.charges.lock().unwrap().is_empty());
    assert_eq!(
        model.core.bodies.lock().unwrap()[0]["max_output_tokens"],
        4096
    );
    process.revoke();
    assert!(process.reconcile(Default::default()).await.is_err());
    assert_eq!(
        model.core.starts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    process.stop().await;
    fixture.manager.shutdown().await;
    model.manager.shutdown().await;
}
struct MaintenanceFixture {
    manager: Arc<PluginManager>,
    engine: Arc<PluginEngine>,
    store: Arc<MemoryStore>,
    probe: Arc<Probe>,
    digest: String,
    id: String,
    model_probe: Option<crate::models::fixture::Fixture>,
    _cache: tempfile::TempDir,
}
impl MaintenanceFixture {
    async fn new(mode: &str) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let probe = Arc::new(Probe::default());
        let port = factory(&probe);
        let model_probe = if matches!(
            mode,
            "maintenance-account-probe" | "maintenance-model-detection"
        ) {
            Some(crate::models::fixture::Fixture::new().await)
        } else {
            None
        };
        let mut runtime = PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_services(Arc::new(move || Ok(Some(port.clone()))));
        if let Some(model) = &model_probe {
            runtime = runtime
                .with_models(
                    model.execution.clone(),
                    Arc::new(crate::models::fixture::Keys),
                )
                .with_account_probe(model.execution.clone());
        }
        let engine = Arc::new(runtime);
        let manager = Arc::new(PluginManager::new(store.clone(), engine.clone()));
        let artifact = manager
            .upload(maintenance_archive("1.0.0"), &actor())
            .await
            .unwrap();
        assert!(artifact.unsupported_capabilities.is_empty());
        let mut command = update(&artifact.sha256, 0, true);
        command.configuration = json!({"adapter_mode":mode});
        let view = manager.update(None, command, &actor()).await.unwrap();
        Self {
            manager,
            engine,
            store,
            probe,
            digest: artifact.sha256,
            id: view.id,
            model_probe,
            _cache: cache,
        }
    }
    async fn count(&self) -> u64 {
        self.store
            .state_get(
                &PluginStateScope {
                    instance_id: self.id.clone(),
                    revision: self.revision(),
                    namespace: "counter".into(),
                    schema_version: 1,
                    maximum_records: 10,
                    maximum_bytes: 1024,
                },
                "reconcile",
            )
            .await
            .unwrap()
            .map_or(0, |r| r.value.as_u64().unwrap())
    }
    fn revision(&self) -> u64 {
        self.store.records.lock().unwrap()[&self.id].revision
    }
    async fn wait_count(&self, expected: u64) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while self.count().await < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn process(&self) -> Arc<dyn PluginProcess> {
        let record = self.store.records.lock().unwrap()[&self.id].clone();
        self.engine
            .prepare(&record, self.store.artifact(&self.digest).await.unwrap())
            .await
            .unwrap()
    }
    fn worker(
        &self,
        snapshots: &RuntimeSnapshotHandle,
    ) -> (
        CancellationToken,
        tokio::task::JoinHandle<Result<(), WorkerTaskError>>,
    ) {
        let WorkerContribution::Registration(registration) = self
            .manager
            .maintenance_worker(snapshots.publications())
            .unwrap()
        else {
            panic!("maintenance registration");
        };
        let WorkerRunnable::Daemon { task, .. } = registration.runnable else {
            panic!("supervised daemon");
        };
        let stop = CancellationToken::new();
        let cancellation = stop.clone();
        (
            stop,
            tokio::spawn(async move { task.run(cancellation).await }),
        )
    }
    async fn change(&self, enabled: bool, mode: &str) {
        let mut command = update(&self.digest, self.revision(), enabled);
        command.configuration = json!({"adapter_mode":mode});
        self.manager
            .update(Some(&self.id), command, &actor())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn maintenance_uses_native_service_scope_and_state_only_after_activation() {
    let f = MaintenanceFixture::new("maintenance-service").await;
    assert_eq!(f.count().await, 0); // Preparing and publishing does not run reconciliation.
    let process = f.process().await;
    assert!(process.supports_maintenance());
    assert!(process.reconcile(Default::default()).await.is_err());
    assert!(f.probe.facts.lock().unwrap().is_empty());
    process.activate();
    for _ in 0..35 {
        process.reconcile(Default::default()).await.unwrap();
    }
    assert_eq!(f.count().await, 35);
    let scopes = {
        let facts = f.probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 35);
        facts
            .iter()
            .map(|fact| {
                assert_eq!(fact.instance, f.id);
                assert_eq!(fact.revision, 1);
                assert!(fact.context.parent_call_id.is_none());
                assert!(!fact.context.request_id.is_empty());
                assert!(fact.context.cancellation.is_cancelled());
                fact.context.request_id.clone()
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(scopes.len(), 35);
    process.revoke();
    assert!(process.reconcile(Default::default()).await.is_err());
    assert_eq!(f.count().await, 35);
    process.stop().await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn maintenance_requires_core_publication_and_coalesces_idempotent_publications() {
    let f = MaintenanceFixture::new("maintenance-service").await;
    let snapshots = RuntimeSnapshotHandle::default();
    let (stop, worker) = f.worker(&snapshots);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.count().await, 0);
    snapshots.publish(snapshot(1));
    f.wait_count(1).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.count().await, 1);
    snapshots.publish(snapshot(1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.count().await, 1);
    snapshots.publish(snapshot(2));
    f.wait_count(2).await;
    snapshots.suspend();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.count().await, 2);
    snapshots.publish(snapshot(2));
    f.wait_count(3).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    snapshots.publish(snapshot(3));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.count().await, 3);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn maintenance_cancels_pending_service_before_replacement_and_disable() {
    let f = MaintenanceFixture::new("maintenance-wait").await;
    f.probe.wait.store(true, Ordering::SeqCst);
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (_, worker) = f.worker(&snapshots);
    tokio::time::timeout(Duration::from_secs(5), f.probe.entered.notified())
        .await
        .unwrap();
    let old = f.probe.facts.lock().unwrap()[0]
        .context
        .cancellation
        .clone();
    assert!(!old.is_cancelled());
    f.probe.wait.store(false, Ordering::SeqCst);
    f.change(true, "maintenance-service").await;
    f.wait_count(1).await;
    assert!(old.is_cancelled());
    assert!(f.probe.dropped.load(Ordering::SeqCst));
    assert_eq!(f.probe.facts.lock().unwrap().last().unwrap().revision, 2);
    f.change(false, "maintenance-service").await;
    let calls = f.probe.facts.lock().unwrap().len();
    snapshots.publish(snapshot(2));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.probe.facts.lock().unwrap().len(), calls);
    f.manager.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn core_suspension_cancels_old_work_even_when_resume_notification_coalesces() {
    let f = MaintenanceFixture::new("maintenance-wait").await;
    f.probe.wait.store(true, Ordering::SeqCst);
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (stop, worker) = f.worker(&snapshots);
    tokio::time::timeout(Duration::from_secs(5), f.probe.entered.notified())
        .await
        .unwrap();
    let old = f.probe.facts.lock().unwrap()[0]
        .context
        .cancellation
        .clone();
    f.probe.wait.store(false, Ordering::SeqCst);
    snapshots.suspend();
    snapshots.publish(snapshot(1));
    f.wait_count(1).await;
    assert!(old.is_cancelled());
    assert!(f.probe.dropped.load(Ordering::SeqCst));
    assert_eq!(f.probe.facts.lock().unwrap().len(), 2);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.manager.shutdown().await;
}

#[tokio::test]
async fn rejected_maintenance_retries_without_blocking_other_instances() {
    let f = MaintenanceFixture::new("maintenance-fault").await;
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (stop, worker) = f.worker(&snapshots);
    f.wait_count(1).await;
    let other = f
        .manager
        .update(None, update(&f.digest, 0, true), &actor())
        .await
        .unwrap();
    let other_scope = PluginStateScope {
        instance_id: other.id.clone(),
        revision: 1,
        namespace: "counter".into(),
        schema_version: 1,
        maximum_records: 10,
        maximum_bytes: 1024,
    };
    let read_other = || f.store.state_get(&other_scope, "reconcile");
    tokio::time::timeout(Duration::from_secs(5), async {
        while read_other().await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = f.count().await;
    f.wait_count(before + 1).await;
    assert!(!worker.is_finished());
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.manager.shutdown().await;
}

#[tokio::test]
async fn malformed_maintenance_responses_reject_only_the_call() {
    for mode in ["maintenance-envelope", "maintenance-payload"] {
        let f = MaintenanceFixture::new(mode).await;
        let process = f.process().await;
        process.activate();
        assert!(process.reconcile(Default::default()).await.is_err());
        assert!(!process.access().is_revoked());
        assert_eq!(process.status(), "ready");
        process.reconcile(Default::default()).await.unwrap();
        assert_eq!(f.count().await, 2);
        process.stop().await;
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn maintenance_preserves_old_generation_on_failed_cas_and_stops_on_unknown_commit() {
    let f = MaintenanceFixture::new("maintenance-service").await;
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (stop, worker) = f.worker(&snapshots);
    f.wait_count(1).await;
    f.store.save_fault.store(1, Ordering::SeqCst);
    let mut command = update(&f.digest, 1, true);
    command.configuration = json!({"adapter_mode":"maintenance-service"});
    assert!(
        f.manager
            .update(Some(&f.id), command, &actor())
            .await
            .is_err()
    );
    assert_eq!(f.revision(), 1);
    snapshots.publish(snapshot(2));
    f.wait_count(2).await;
    assert!(
        f.probe
            .facts
            .lock()
            .unwrap()
            .iter()
            .all(|fact| fact.revision == 1)
    );
    f.store.save_fault.store(2, Ordering::SeqCst);
    f.change(true, "maintenance-service").await;
    f.wait_count(3).await;
    assert_eq!(f.probe.facts.lock().unwrap().last().unwrap().revision, 2);
    f.store.save_fault.store(3, Ordering::SeqCst);
    let mut command = update(&f.digest, 2, true);
    command.configuration = json!({"adapter_mode":"maintenance-service"});
    assert!(
        f.manager
            .update(Some(&f.id), command, &actor())
            .await
            .is_err()
    );
    let calls = f.probe.facts.lock().unwrap().len();
    snapshots.publish(snapshot(3));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.probe.facts.lock().unwrap().len(), calls);
    assert!(!worker.is_finished());
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.manager.shutdown().await;
}

#[tokio::test]
async fn maintenance_runs_periodically_and_host_shutdown_cancels_pending_rpc() {
    let f = MaintenanceFixture::new("maintenance-service").await;
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (stop, worker) = f.worker(&snapshots);
    f.wait_count(1).await;
    tokio::time::timeout(Duration::from_secs(33), async {
        while f.count().await < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    f.probe.wait.store(true, Ordering::SeqCst);
    snapshots.publish(snapshot(2));
    tokio::time::timeout(Duration::from_secs(5), f.probe.entered.notified())
        .await
        .unwrap();
    let scope = f
        .probe
        .facts
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .context
        .cancellation
        .clone();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(scope.is_cancelled());
    assert!(f.probe.dropped.load(Ordering::SeqCst));
    f.manager.shutdown().await;
}

#[tokio::test]
async fn unrelated_instance_publication_does_not_cancel_running_maintenance() {
    let f = MaintenanceFixture::new("maintenance-wait").await;
    f.probe.wait.store(true, Ordering::SeqCst);
    let snapshots = RuntimeSnapshotHandle::new(snapshot(1));
    let (stop, worker) = f.worker(&snapshots);
    tokio::time::timeout(Duration::from_secs(5), f.probe.entered.notified())
        .await
        .unwrap();
    let old = f.probe.facts.lock().unwrap()[0]
        .context
        .cancellation
        .clone();
    let mut command = update(&f.digest, 0, true);
    command.configuration = json!({"adapter_mode":"maintenance-service"});
    f.manager.update(None, command, &actor()).await.unwrap();
    snapshots.publish(snapshot(2));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!old.is_cancelled());
    assert!(!f.probe.dropped.load(Ordering::SeqCst));
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(old.is_cancelled());
    f.manager.shutdown().await;
}
