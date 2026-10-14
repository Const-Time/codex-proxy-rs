//! Exercise publication with real plugin processes. The test composition enables
//! the kernel explicitly; production still rejects middleware until API mounts exist.
mod attempt;
use super::*;
use crate::support::{MemoryStore, actor, update};
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, AdminErrorKind, plugins::*},
    plugins::PluginManager,
    ports::plugins::{PluginProcess, PluginRuntime, PluginService, PluginStore},
};
use gateway_plugin_runtime::PluginEngine;

pub(crate) struct KernelRuntime(pub(crate) PluginEngine);
#[async_trait]
impl PluginRuntime for KernelRuntime {
    async fn shutdown(&self) {
        self.0.shutdown().await;
    }
    fn observer_plan(
        &self,
        targets: Vec<gateway_admin::ports::plugins::PluginObserverTarget>,
        services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<Option<Arc<dyn gateway_core::engine::observation::RequestObserverPlan>>, AdminError>
    {
        self.0.observer_plan(targets, services, cancellation)
    }
    fn contributes_model_catalog(&self, artifact: &PluginArtifact) -> Result<bool, AdminError> {
        self.0.contributes_model_catalog(artifact)
    }
    fn describe(&self, info: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        self.0.describe(info)
    }
    async fn inspect(&self, bytes: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        self.0.inspect(bytes).await
    }
    async fn validate(
        &self,
        record: &PluginRecord,
        artifact: &PluginArtifact,
        old: Option<&PluginArtifact>,
    ) -> Result<(), AdminError> {
        let mut candidate = record.clone();
        candidate.enabled = false;
        self.0.validate(&candidate, artifact, old).await
    }
    async fn prepare(
        &self,
        record: &PluginRecord,
        artifact: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        self.0.prepare(record, artifact).await
    }
}
struct Installation {
    store: Arc<MemoryStore>,
    manager: PluginManager,
    digest: String,
    _cache: tempfile::TempDir,
}
impl Installation {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(KernelRuntime(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )));
        let manager = PluginManager::new(store.clone(), runtime.clone());
        let digest = manager
            .upload(crate::support::archive("1.0.0", true), &actor())
            .await
            .unwrap()
            .sha256;
        Self {
            store,
            manager,
            digest,
            _cache: cache,
        }
    }
    fn restart_runtime(&self) -> Arc<KernelRuntime> {
        Arc::new(KernelRuntime(PluginEngine::new(
            self._cache.path().into(),
            "3.11.2".parse().unwrap(),
            self.store.clone(),
        )))
    }
    fn command(&self, revision: u64, order: i32) -> PluginUpdate {
        let mut command = update(&self.digest, revision, true);
        command.middleware = Some(PluginMiddlewareBinding {
            order,
            mounts: vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
            scope: PluginExecutionScope::All {},
        });
        command
    }
}
async fn run_chain(manager: &PluginManager) -> (Vec<String>, Vec<String>) {
    let trace = Arc::new(Trace::default());
    let response = manager
        .freeze_middleware(context(false))
        .unwrap()
        .run(request("chain"), terminal(trace.clone(), false))
        .await
        .unwrap();
    let (_, _, headers, body, _) = response.into_parts();
    let entered = trace
        .request
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .headers()
        .iter()
        .map(|h| String::from_utf8(h.value().to_vec()).unwrap())
        .collect();
    let returned = headers
        .iter()
        .map(|h| String::from_utf8(h.value().to_vec()).unwrap())
        .collect();
    assert_eq!(trace.next.load(Ordering::SeqCst), 1);
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    body.close().await;
    trace.closed_once().await;
    (entered, returned)
}

#[tokio::test]
async fn nested_ancestry_excludes_initiating_instances_from_both_mounts() {
    let f = Installation::new().await;
    let a = f
        .manager
        .update(None, f.command(0, -1), &actor())
        .await
        .unwrap();
    let b = f
        .manager
        .update(None, f.command(0, 1), &actor())
        .await
        .unwrap();
    for mount in [MiddlewareMount::Request, MiddlewareMount::Attempt] {
        let mut scoped = context(false);
        scoped.mount = mount;
        scoped.excluded_plugin_instances.insert(a.id.clone());
        let plan = f.manager.freeze_middleware(scoped).unwrap();
        assert!(!plan.is_empty());
        if mount == MiddlewareMount::Request {
            let trace = Arc::new(Trace::default());
            let response = plan
                .run(request("chain"), terminal(trace.clone(), false))
                .await
                .unwrap();
            let values: Vec<String> = trace
                .request
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .headers()
                .iter()
                .map(|header| String::from_utf8(header.value().to_vec()).unwrap())
                .collect();
            assert_eq!(values, vec![format!("{}:1", b.id)]);
            let (_, _, _, body, _) = response.into_parts();
            body.close().await;
        }
        let mut all = context(false);
        all.mount = mount;
        all.excluded_plugin_instances
            .extend([a.id.clone(), b.id.clone()]);
        assert!(f.manager.freeze_middleware(all).unwrap().is_empty());
    }
    // An initiating process that is unavailable still must not be re-entered.
    assert!(
        f.manager
            .handle(&a.id, 1, crate::support::request("crash"))
            .await
            .is_err()
    );
    let mut descendant = context(false);
    descendant.excluded_plugin_instances.insert(a.id);
    assert!(f.manager.freeze_middleware(descendant).is_ok());
    assert!(f.manager.freeze_middleware(context(false)).is_err());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn frozen_binding_orders_nested_rpc_by_order_then_id_and_matches_host_scope() {
    let f = Installation::new().await;
    let empty = f.manager.freeze_middleware(context(false)).unwrap();
    assert!(empty.is_empty());
    let a = f
        .manager
        .update(None, f.command(0, 10), &actor())
        .await
        .unwrap();
    assert!(empty.cancellation().is_cancelled());
    let b = f
        .manager
        .update(None, f.command(0, -1), &actor())
        .await
        .unwrap();
    let mut excluded = f.command(0, -10);
    excluded.middleware.as_mut().unwrap().scope = PluginExecutionScope::Keys {
        ids: vec!["key_other".into()],
    };
    let excluded = f.manager.update(None, excluded, &actor()).await.unwrap();
    let expected = vec![format!("{}:1", b.id), format!("{}:1", a.id)];
    assert_eq!(
        run_chain(&f.manager).await,
        (expected.clone(), expected.into_iter().rev().collect())
    );
    let mut equal = f.command(1, 10);
    equal.middleware.as_mut().unwrap().scope = PluginExecutionScope::Groups {
        ids: vec!["grp_00000000000000000000000000000077".into()],
    };
    f.manager
        .update(Some(&b.id), equal, &actor())
        .await
        .unwrap();
    assert_eq!(run_chain(&f.manager).await.0, vec![format!("{}:1", a.id)]);
    let mut scoped = context(false);
    scoped.account_group_ids.push(
        gateway_core::routing::AccountGroupId::new("grp_00000000000000000000000000000077").unwrap(),
    );
    let trace = Arc::new(Trace::default());
    let result = f
        .manager
        .freeze_middleware(scoped)
        .unwrap()
        .run(request("chain"), terminal(trace.clone(), false))
        .await
        .unwrap();
    let values: Vec<String> = trace
        .request
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .headers()
        .iter()
        .map(|h| String::from_utf8(h.value().to_vec()).unwrap())
        .collect();
    let mut expected = vec![(a.id, 1), (b.id, 2)];
    expected.sort();
    assert_eq!(
        values,
        expected
            .into_iter()
            .map(|(id, revision)| format!("{id}:{revision}"))
            .collect::<Vec<_>>()
    );
    let (_, _, _, body, _) = result.into_parts();
    body.close().await;
    // An unavailable unmatched scope does not reject an unrelated Key.
    assert!(
        f.manager
            .handle(&excluded.id, 1, crate::support::request("crash"))
            .await
            .is_err()
    );
    assert!(f.manager.freeze_middleware(context(false)).is_ok());
    let mut other = context(false);
    other.client_key_id = gateway_core::policy::ClientApiKeyId::new("key_other").unwrap();
    assert!(f.manager.freeze_middleware(other).is_err());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn failed_upgrade_keeps_old_plan_and_successful_disable_revokes_unread_body_after_rpc_end() {
    let f = Installation::new().await;
    let installed = f
        .manager
        .update(None, f.command(0, 1), &actor())
        .await
        .unwrap();
    let plan = f.manager.freeze_middleware(context(false)).unwrap();
    let access = plan.cancellation();
    let mut invalid = f.command(1, 2);
    invalid.configuration = json!({"fail":true});
    assert!(
        f.manager
            .update(Some(&installed.id), invalid, &actor())
            .await
            .is_err()
    );
    assert!(!access.is_cancelled());
    f.store.save_fault.store(1, Ordering::SeqCst);
    assert!(
        f.manager
            .update(Some(&installed.id), f.command(1, 2), &actor())
            .await
            .is_err()
    );
    assert!(!access.is_cancelled());
    let trace = Arc::new(Trace::default());
    let (_, _, _, mut body, _) = plan
        .run(request("pass"), terminal(trace.clone(), false))
        .await
        .unwrap()
        .into_parts();
    assert_eq!(trace.reads.load(Ordering::SeqCst), 0);
    let mut disable = f.command(1, 1);
    disable.enabled = false;
    f.manager
        .update(Some(&installed.id), disable, &actor())
        .await
        .unwrap();
    assert!(access.is_cancelled());
    assert!(body.next_frame().await.is_err());
    assert!(body.commit_downstream(Some(200)).await.is_err());
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    body.close().await;
    trace.closed_once().await;
    assert!(
        f.manager
            .freeze_middleware(context(false))
            .unwrap()
            .is_empty()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn unknown_commit_blocks_plans_until_repair_and_failed_restore_retains_gate() {
    let f = Installation::new().await;
    let installed = f
        .manager
        .update(None, f.command(0, 1), &actor())
        .await
        .unwrap();
    let old = f
        .manager
        .freeze_middleware(context(false))
        .unwrap()
        .cancellation();
    f.store.save_fault.store(2, Ordering::SeqCst);
    f.manager
        .update(Some(&installed.id), f.command(1, 2), &actor())
        .await
        .unwrap();
    assert!(old.is_cancelled());
    assert_eq!(
        run_chain(&f.manager).await.0,
        vec![format!("{}:2", installed.id)]
    );
    let old = f
        .manager
        .freeze_middleware(context(false))
        .unwrap()
        .cancellation();
    f.store.save_fault.store(3, Ordering::SeqCst);
    assert_eq!(
        f.manager
            .update(Some(&installed.id), f.command(2, 3), &actor())
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::UpstreamResultUnknown
    );
    assert!(old.is_cancelled());
    assert!(f.manager.freeze_middleware(context(false)).is_err());
    f.manager
        .update(Some(&installed.id), f.command(3, 4), &actor())
        .await
        .unwrap();
    assert_eq!(
        run_chain(&f.manager).await.0,
        vec![format!("{}:4", installed.id)]
    );
    f.manager.shutdown().await;
    f.store
        .records
        .lock()
        .unwrap()
        .get_mut(&installed.id)
        .unwrap()
        .configuration = json!({"fail":true});
    let restored = PluginManager::new(f.store.clone(), f.restart_runtime());
    restored.restore().await.unwrap();
    assert!(restored.freeze_middleware(context(false)).is_err());
    restored.shutdown().await;
    f.store
        .records
        .lock()
        .unwrap()
        .get_mut(&installed.id)
        .unwrap()
        .middleware
        .as_mut()
        .unwrap()
        .mounts
        .clear();
    let restored = PluginManager::new(f.store.clone(), f.restart_runtime());
    restored.restore().await.unwrap();
    assert!(
        restored.freeze_middleware(context(false)).is_err(),
        "invalid restored placement must not silently match nothing"
    );
    let mut disable = f.command(4, 4);
    disable.enabled = false;
    restored
        .update(Some(&installed.id), disable, &actor())
        .await
        .unwrap();
    assert!(
        restored
            .freeze_middleware(context(false))
            .unwrap()
            .is_empty()
    );
    restored.delete(&installed.id, 5, &actor()).await.unwrap();
    restored.shutdown().await;
}

#[tokio::test]
async fn disable_cancels_inflight_next_and_releases_terminal_without_committing() {
    let f = Installation::new().await;
    let installed = f
        .manager
        .update(None, f.command(0, 1), &actor())
        .await
        .unwrap();
    let plan = f.manager.freeze_middleware(context(false)).unwrap();
    let trace = Arc::new(Trace::default());
    let pending = tokio::spawn(plan.run(request("wait"), terminal(trace.clone(), false)));
    tokio::time::timeout(Duration::from_secs(3), trace.entered.notified())
        .await
        .unwrap();
    let mut disable = f.command(1, 1);
    disable.enabled = false;
    f.manager
        .update(Some(&installed.id), disable, &actor())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    trace.closed_once().await;
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn binding_validation_rejects_empty_ambiguous_and_undeclared_scopes_without_publishing() {
    let f = Installation::new().await;
    for binding in [
        json!({"order":0,"mounts":[],"scope":{"kind":"all"}}),
        json!({"order":0,"mounts":["request","request"],"scope":{"kind":"all"}}),
        json!({"order":10001,"mounts":["request"],"scope":{"kind":"all"}}),
        json!({"order":0,"mounts":["request"],"scope":{"kind":"keys","ids":[]}}),
        json!({"order":0,"mounts":["request"],"scope":{"kind":"keys","ids":["key_a","key_a"]}}),
        json!({"order":0,"mounts":["request"],"scope":{"kind":"groups","ids":[""]}}),
        json!({"order":0,"mounts":["http"],"scope":{"kind":"keys","ids":["key_a"]}}),
        json!({"order":0,"mounts":["websocket"],"scope":{"kind":"groups","ids":["group_a"]}}),
        json!({"order":0,"mounts":["service","request"],"scope":{"kind":"keys","ids":["key_a"]}}),
    ] {
        let mut command = f.command(0, 0);
        command.middleware = Some(serde_json::from_value(binding).unwrap());
        assert!(f.manager.update(None, command, &actor()).await.is_err());
    }
    for binding in [
        json!({"order":0,"mounts":["unknown_mount"],"scope":{"kind":"all"}}),
        json!({"order":0,"mounts":["request"],"scope":{"kind":"all","ids":["key_a"]}}),
    ] {
        assert!(serde_json::from_value::<PluginMiddlewareBinding>(binding).is_err());
    }
    for mount in [
        PluginMiddlewareMount::Http,
        PluginMiddlewareMount::WebSocket,
        PluginMiddlewareMount::Service,
    ] {
        let mut command = f.command(0, 0);
        command.middleware.as_mut().unwrap().mounts = vec![mount];
        // The wire recognizes these mounts; the fixture only declares request/attempt.
        assert!(f.manager.update(None, command, &actor()).await.is_err());
    }
    let artifact = f
        .manager
        .upload(crate::support::archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let mut command = f.command(0, 0);
    command.artifact_sha256 = artifact.sha256;
    assert!(f.manager.update(None, command, &actor()).await.is_err());
    assert!(
        f.manager
            .freeze_middleware(context(false))
            .unwrap()
            .is_empty()
    );
    // Production still rejects enabling a manifest before data-plane integration.
    let production = Arc::new(PluginEngine::new(
        f._cache.path().into(),
        "3.11.2".parse().unwrap(),
        f.store.clone(),
    ));
    let manager = PluginManager::new(f.store.clone(), production.clone());
    assert!(
        manager
            .update(None, f.command(0, 0), &actor())
            .await
            .is_err()
    );
    let mut draft = f.command(0, 0);
    draft.enabled = false;
    let saved = manager.update(None, draft, &actor()).await.unwrap();
    assert!(saved.middleware.is_some());
    let mut record = f
        .store
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == saved.id)
        .unwrap();
    let mut declared = f.store.artifact(&record.artifact_sha256).await.unwrap();
    declared.info.manifest["contributes"]["middleware"]["stages"] =
        json!(["http", "websocket", "service", "request", "attempt"]);
    for mount in [
        PluginMiddlewareMount::Http,
        PluginMiddlewareMount::WebSocket,
        PluginMiddlewareMount::Service,
    ] {
        record.middleware.as_mut().unwrap().mounts = vec![mount];
        production.validate(&record, &declared, None).await.unwrap();
        record.enabled = true;
        assert!(production.validate(&record, &declared, None).await.is_err());
        record.enabled = false;
    }
    assert!(
        manager
            .freeze_middleware(context(false))
            .unwrap()
            .is_empty()
    );
    manager.shutdown().await;
    f.manager.shutdown().await;
}
