use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationContext, plugins::*},
    plugins::PluginManager,
    ports::plugins::*,
};
use gateway_core::{
    engine::middleware::{MiddlewareError, http},
    lifecycle::{CancellationToken, Deadline},
    middleware::compose,
};
use http_body_util::BodyExt;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

fn artifact() -> PluginArtifact {
    PluginArtifact {
        info: PluginArtifactInfo {
            sha256: "a".repeat(64),
            plugin_id: "fixture".into(),
            version: "1".into(),
            display_name: "fixture".into(),
            manifest: json!({}),
            unsupported_capabilities: vec![],
            version_warning: None,
        },
        archive: Arc::from([]),
    }
}
struct Store(Mutex<Vec<PluginRecord>>);
#[async_trait]
impl PluginStore for Store {
    async fn list(&self) -> Result<Vec<PluginRecord>, AdminError> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn artifact(&self, _: &str) -> Result<PluginArtifact, AdminError> {
        Ok(artifact())
    }
    async fn artifact_info(&self, _: &str) -> Result<PluginArtifactInfo, AdminError> {
        Ok(artifact().info)
    }
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        unreachable!()
    }
    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn put_artifact(&self, _: PluginArtifact, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: u64, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn save(
        &self,
        row: &PluginRecord,
        expected: u64,
        _: &MutationContext,
    ) -> Result<(), AdminError> {
        let mut rows = self.0.lock().unwrap();
        let current = rows.iter_mut().find(|r| r.id == row.id).unwrap();
        if current.revision != expected {
            return Err(AdminError::conflict("stale"));
        }
        *current = row.clone();
        Ok(())
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
struct Process {
    id: String,
    access: PluginAccess,
    ready: AtomicBool,
    wait: AtomicBool,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
}
#[async_trait]
impl PluginProcess for Process {
    async fn http_middleware(
        &self,
        context: http::Context,
        mut request: http::Request,
        next: http::Next,
    ) -> Result<http::Response, MiddlewareError> {
        assert_eq!(context.request_id, "http_request");
        assert!(context.parent_call_id.is_none());
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.wait.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        request
            .headers_mut()
            .append("x-chain", self.id.parse().unwrap());
        let mut response = next.run(request).await?;
        response
            .headers_mut()
            .append("x-chain", self.id.parse().unwrap());
        Ok(response)
    }
    fn access(&self) -> PluginAccess {
        self.access.clone()
    }
    fn status(&self) -> &'static str {
        if self.ready.load(Ordering::SeqCst) {
            "ready"
        } else {
            "faulted"
        }
    }
    fn pages(&self) -> Vec<PluginPage> {
        vec![]
    }
    fn resources(&self) -> Vec<PluginResource> {
        vec![]
    }
    fn activate(&self) {}
    fn revoke(&self) {
        self.access.revoke();
    }
    async fn stop(&self) {
        self.access.revoke();
    }
    async fn resource(&self, _: &str) -> Result<PluginHttpResponse, AdminError> {
        unreachable!()
    }
    async fn handle(&self, _: PluginHttpRequest) -> Result<PluginHttpResponse, AdminError> {
        unreachable!()
    }
}
struct Runtime(BTreeMap<String, Arc<Process>>);
#[async_trait]
impl PluginRuntime for Runtime {
    fn describe(&self, info: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        Ok(info)
    }
    async fn inspect(&self, _: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        unreachable!()
    }
    async fn validate(
        &self,
        _: &PluginRecord,
        _: &PluginArtifact,
        _: Option<&PluginArtifact>,
    ) -> Result<(), AdminError> {
        Ok(())
    }
    async fn prepare(
        &self,
        row: &PluginRecord,
        _: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        Ok(self.0[&row.id].clone())
    }
}
async fn fixture() -> (PluginManager, Vec<Arc<Process>>) {
    let mut records = vec![];
    let mut processes = vec![];
    for (id, order, mount) in [
        ("b", 0, PluginMiddlewareMount::Http),
        ("a", 0, PluginMiddlewareMount::Http),
        ("first", -1, PluginMiddlewareMount::Http),
        ("request_only", -2, PluginMiddlewareMount::Request),
    ] {
        records.push(PluginRecord {
            id: id.into(),
            revision: 1,
            name: id.into(),
            artifact_sha256: "a".repeat(64),
            enabled: true,
            configuration: json!({}),
            secrets: BTreeMap::new(),
            frontend_authentication: None,
            middleware: Some(PluginMiddlewareBinding {
                order,
                mounts: vec![mount],
                scope: PluginExecutionScope::All {},
            }),
            model_router: None,
            account_scheduler: None,
            upstream_adapter: None,
            retry_policy: None,
        });
        processes.push(Arc::new(Process {
            id: id.into(),
            access: PluginAccess::default(),
            ready: AtomicBool::new(true),
            wait: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
        }));
    }
    let runtime = Runtime(
        processes
            .iter()
            .map(|p| (p.id.clone(), p.clone()))
            .collect(),
    );
    let manager = PluginManager::new(Arc::new(Store(Mutex::new(records))), Arc::new(runtime));
    manager.restore().await.unwrap();
    (manager, processes)
}
fn context() -> http::Context {
    http::Context {
        request_id: "http_request".into(),
        call_id: "root".into(),
        parent_call_id: None,
        call_scope: Default::default(),
        model_parent: None,
        cancellation: CancellationToken::new(),
        deadline_at: Deadline::default(),
    }
}
fn terminal(calls: Arc<AtomicUsize>) -> http::Next {
    compose(vec![], move |request: http::Request| {
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let mut response = http::Response::new(http::empty_body());
            *response.headers_mut() = request.headers().clone();
            Ok(response)
        })
    })
}

#[tokio::test]
async fn pre_auth_http_plan_uses_publication_order_and_all_ancestor_exclusions_without_key_scope() {
    let (manager, processes) = fixture().await;
    let registry = manager.middleware_registry().unwrap();
    let mut context = context();
    context.call_scope = context.call_scope.fork_http("a".into()).unwrap();
    let plan = registry.freeze_http(context).unwrap();
    assert!(!plan.is_empty());
    let calls = Arc::new(AtomicUsize::new(0));
    let response = plan
        .run(
            http::Request::new(http::empty_body()),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    let chain = response
        .headers()
        .get_all("x-chain")
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(chain, ["first", "b", "b", "first"]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(processes[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(processes[3].calls.load(Ordering::SeqCst), 0);
    manager.shutdown().await;
}

#[tokio::test]
async fn required_http_process_fault_is_a_gate_and_never_silently_delegates() {
    let (manager, processes) = fixture().await;
    processes[0].ready.store(false, Ordering::SeqCst);
    assert!(
        manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_err()
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn frozen_http_plan_is_revoked_before_entry_and_never_runs_terminal() {
    let (manager, processes) = fixture().await;
    let plan = manager
        .middleware_registry()
        .unwrap()
        .freeze_http(context())
        .unwrap();
    let cancellation = plan.cancellation();
    processes[0].access.revoke();
    assert!(cancellation.is_cancelled());
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        plan.run(
            http::Request::new(http::empty_body()),
            terminal(calls.clone())
        )
        .await
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    manager.shutdown().await;
}

#[tokio::test]
async fn http_plugin_revoke_interrupts_wait_without_invoking_following_layers_or_terminal() {
    let (manager, processes) = fixture().await;
    let waiting = processes[2].clone();
    waiting.wait.store(true, Ordering::SeqCst);
    let plan = manager
        .middleware_registry()
        .unwrap()
        .freeze_http(context())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut run = Box::pin(plan.run(
        http::Request::new(http::empty_body()),
        terminal(calls.clone()),
    ));
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=waiting.entered.notified()=>{}}
    waiting.access.revoke();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(processes[0].calls.load(Ordering::SeqCst), 0);
    manager.shutdown().await;
}

#[tokio::test]
async fn http_plan_deadline_is_checked_before_any_side_effect() {
    let (manager, processes) = fixture().await;
    let mut context = context();
    context.deadline_at = Deadline::from_timeout(SystemTime::now(), Some(Duration::ZERO)).unwrap();
    let plan = manager
        .middleware_registry()
        .unwrap()
        .freeze_http(context)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        plan.run(
            http::Request::new(http::empty_body()),
            terminal(calls.clone())
        )
        .await,
        Err(MiddlewareError::Engine(
            gateway_core::engine::EngineError::Deadline
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        processes
            .iter()
            .all(|p| p.calls.load(Ordering::SeqCst) == 0)
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn http_response_guard_rejects_buffered_bytes_after_rpc_returns() {
    let (manager, processes) = fixture().await;
    let plan = manager
        .middleware_registry()
        .unwrap()
        .freeze_http(context())
        .unwrap();
    let next = compose(vec![], |_| {
        Box::pin(async {
            Ok(http::Response::new(
                http_body_util::Full::new(bytes::Bytes::from_static(b"buffered"))
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            ))
        })
    });
    let response = plan
        .run(http::Request::new(http::empty_body()), next)
        .await
        .unwrap();
    processes[0].access.revoke();
    assert!(response.into_body().collect().await.is_err());
    manager.shutdown().await;
}

#[test]
fn frontend_mounts_require_global_scope_and_reject_duplicate_mounts() {
    for mount in [
        PluginMiddlewareMount::Http,
        PluginMiddlewareMount::WebSocket,
        PluginMiddlewareMount::Service,
    ] {
        let binding = PluginMiddlewareBinding {
            order: 0,
            mounts: vec![mount],
            scope: PluginExecutionScope::All {},
        };
        binding.validate().unwrap();
        for scope in [
            PluginExecutionScope::Keys {
                ids: vec!["key_valid".into()],
            },
            PluginExecutionScope::Groups {
                ids: vec!["grp_valid".into()],
            },
        ] {
            assert!(
                PluginMiddlewareBinding {
                    scope,
                    ..binding.clone()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            PluginMiddlewareBinding {
                mounts: vec![mount, mount],
                ..binding
            }
            .validate()
            .is_err()
        );
    }
    let mounts: Vec<PluginMiddlewareMount> = serde_json::from_value(json!([
        "http",
        "websocket",
        "service",
        "request",
        "attempt"
    ]))
    .unwrap();
    assert_eq!(mounts.len(), 5);
    PluginMiddlewareBinding {
        order: -1,
        mounts,
        scope: PluginExecutionScope::All {},
    }
    .validate()
    .unwrap();
}
