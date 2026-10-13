use super::*;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_core::engine::middleware::websocket as core;

struct Sender {
    messages: Mutex<Vec<core::Message>>,
    fail: bool,
}
impl core::Sender for Sender {
    fn send(&self, message: core::Message) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            if self.fail {
                return Err(MiddlewareError::Rejected);
            }
            self.messages.lock().unwrap().push(message);
            Ok(())
        })
    }
}
struct Fixture {
    manager: PluginManager,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let store = Arc::new(crate::support::MemoryStore::default());
        let cache = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(
            store.clone(),
            Arc::new(super::plan::KernelRuntime(
                gateway_plugin_runtime::PluginEngine::new(
                    cache.path().into(),
                    "3.11.2".parse().unwrap(),
                    store,
                ),
            )),
        );
        let artifact = manager
            .upload(
                crate::support::http_archive("1.0.0"),
                &crate::support::actor(),
            )
            .await
            .unwrap();
        let mut command = crate::support::update(&artifact.sha256, 0, true);
        command.middleware = Some(PluginMiddlewareBinding {
            order: 0,
            mounts: vec![PluginMiddlewareMount::WebSocket],
            scope: PluginExecutionScope::All {},
        });
        manager
            .update(None, command, &crate::support::actor())
            .await
            .unwrap();
        Self {
            manager,
            _cache: cache,
        }
    }
    fn plan(
        &self,
        cancellation: CancellationToken,
    ) -> gateway_admin::plugins::websocket::PluginWebSocketMiddlewarePlan {
        self.manager
            .middleware_registry()
            .unwrap()
            .freeze_websocket(&Default::default(), cancellation)
            .unwrap()
    }
    async fn change(&self, enabled: bool) {
        let record = self.manager.list().await.unwrap().remove(0);
        let mut command = crate::support::update(&record.artifact.sha256, record.revision, enabled);
        command.middleware = record.middleware;
        self.manager
            .update(Some(&record.id), command, &crate::support::actor())
            .await
            .unwrap();
    }
}
fn context(mode: &str, sender: Arc<Sender>) -> core::Context {
    core::Context {
        origin: None,
        connection_id: "ws-fixture".into(),
        direction: core::Direction::Incoming,
        headers: vec![MiddlewareHeader::new(
            "x-fixture-mode",
            Bytes::copy_from_slice(mode.as_bytes()),
        )]
        .into(),
        excluded_plugin_instances: Default::default(),
        cancellation: Default::default(),
        deadline_at: Default::default(),
        sender,
    }
}
fn sender(fail: bool) -> Arc<Sender> {
    Arc::new(Sender {
        messages: Mutex::new(vec![]),
        fail,
    })
}
fn message(kind: core::Kind, payload: impl Into<Bytes>) -> core::Message {
    core::Message {
        kind,
        payload: payload.into(),
    }
}
fn identity() -> core::Next {
    gateway_core::middleware::compose(vec![], |m| Box::pin(async { Ok(Some(m)) }))
}

#[tokio::test]
async fn sdk_raw_websocket_preserves_all_kinds_bytes_and_large_lazy_payloads() {
    let f = Fixture::new().await;
    for kind in [
        core::Kind::Text,
        core::Kind::Binary,
        core::Kind::Ping,
        core::Kind::Pong,
        core::Kind::Close { code: Some(1000) },
    ] {
        let result = f
            .plan(Default::default())
            .run(
                context("pass", sender(false)),
                message(kind, vec![0, 255, 0]),
                identity(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.kind, kind);
        assert_eq!(result.payload.as_ref(), &[0, 255, 0]);
    }
    let result = f
        .plan(Default::default())
        .run(
            context("pass", sender(false)),
            message(core::Kind::Binary, vec![7; 9 * 1024 * 1024]),
            identity(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.payload.len(), 9 * 1024 * 1024);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_websocket_can_rewrite_drop_send_and_only_consume_next_once() {
    let f = Fixture::new().await;
    for mode in ["rewrite", "drop", "send", "double"] {
        let output = sender(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let next = gateway_core::middleware::compose(vec![], move |m| {
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(Some(m))
            })
        });
        let result = f
            .plan(Default::default())
            .run(
                context(mode, output.clone()),
                message(core::Kind::Binary, vec![0, 255]),
                next,
            )
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(mode != "drop"));
        match mode {
            "rewrite" => assert_eq!(result.unwrap().payload.as_ref(), &[0, 255, 42]),
            "send" => assert_eq!(
                output.messages.lock().unwrap()[0].payload.as_ref(),
                &[0, 255]
            ),
            _ => assert!(result.is_none()),
        }
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_websocket_partial_read_transfers_only_unread_payload_and_errors_stay_original() {
    let f = Fixture::new().await;
    let result = f
        .plan(Default::default())
        .run(
            context("partial", sender(false)),
            message(core::Kind::Binary, vec![7; 9 * 1024 * 1024]),
            identity(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(result.payload.len() < 9 * 1024 * 1024 && !result.payload.is_empty());
    for mode in ["next-fail", "send-fail", "both-fail"] {
        let next = gateway_core::middleware::compose(vec![], |_| {
            Box::pin(async {
                Err(MiddlewareError::Engine(
                    gateway_core::engine::EngineError::Deadline,
                ))
            })
        });
        let error = f
            .plan(Default::default())
            .run(
                context(mode, sender(true)),
                message(core::Kind::Binary, vec![0]),
                next,
            )
            .await
            .err()
            .unwrap();
        if mode == "send-fail" {
            assert!(matches!(error, MiddlewareError::Rejected));
        } else {
            assert!(matches!(
                error,
                MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)
            ));
        }
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn forged_websocket_handles_fault_the_generation_and_explicit_restart_recovers() {
    let f = Fixture::new().await;
    assert!(
        f.plan(Default::default())
            .run(
                context("forged", sender(false)),
                message(core::Kind::Binary, vec![0]),
                identity()
            )
            .await
            .is_err()
    );
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_websocket(&Default::default(), Default::default())
            .is_err()
    );
    f.change(true).await;
    assert!(
        f.plan(Default::default())
            .run(
                context("pass", sender(false)),
                message(core::Kind::Binary, vec![0]),
                identity()
            )
            .await
            .unwrap()
            .is_some()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn websocket_waits_are_cancelled_by_disable_and_frozen_ancestor_exclusion_is_respected() {
    let f = Fixture::new().await;
    let record = f.manager.list().await.unwrap().remove(0);
    let excluded = [record.id.clone()].into();
    let plan = f
        .manager
        .middleware_registry()
        .unwrap()
        .freeze_websocket(&excluded, Default::default())
        .unwrap();
    assert!(plan.is_empty());
    assert!(
        plan.run(
            context("wait", sender(false)),
            message(core::Kind::Binary, vec![0]),
            identity()
        )
        .await
        .unwrap()
        .is_some()
    );
    let plan = f.plan(Default::default());
    let run = tokio::spawn(async move {
        plan.run(
            context("wait", sender(false)),
            message(core::Kind::Binary, vec![0]),
            identity(),
        )
        .await
    });
    tokio::task::yield_now().await;
    f.change(false).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), run)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    f.manager.shutdown().await;
}
