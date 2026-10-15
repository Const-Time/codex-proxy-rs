//! Real SDK callbacks verify host diagnostics, saturation and accepted-write ownership.
use crate::support::{MemoryStore, actor, archive, request, update};
use gateway_admin::{
    plugins::PluginManager,
    ports::plugins::{PluginRuntime, PluginService, PluginStore},
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::{Duration, SystemTime},
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone)]
struct Captured {
    level: String,
    fields: BTreeMap<String, String>,
}
#[derive(Default)]
struct State {
    records: Vec<Captured>,
    blocked: BTreeSet<String>,
    entering: BTreeMap<String, usize>,
}
#[derive(Default)]
struct Capture {
    state: Mutex<State>,
    released: Condvar,
    changed: tokio::sync::Notify,
}
struct Visitor(BTreeMap<String, String>);
impl Visit for Visitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "gateway_plugin"
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() != "gateway_plugin" {
            return;
        }
        let mut visitor = Visitor(BTreeMap::new());
        event.record(&mut visitor);
        let id = visitor.0["instance_id"].clone();
        let mut state = self.state.lock().unwrap();
        *state.entering.entry(id.clone()).or_default() += 1;
        self.changed.notify_waiters();
        while state.blocked.contains(&id) {
            state = self.released.wait(state).unwrap();
        }
        state.records.push(Captured {
            level: event.metadata().level().to_string(),
            fields: visitor.0,
        });
        self.changed.notify_waiters();
    }
}
pub(crate) fn initialize_capture() {
    capture();
}
fn capture() -> Arc<Capture> {
    static CAPTURE: OnceLock<Arc<Capture>> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Arc::new(Capture::default());
            tracing::subscriber::set_global_default(capture.clone()).unwrap();
            capture
        })
        .clone()
}
impl Capture {
    async fn wait_for(&self, id: &str, count: usize, entering: bool) {
        let deadline: gateway_core::lifecycle::Deadline =
            (SystemTime::now() + Duration::from_secs(5)).into();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            // Each instance waits on its own predicate. Register before reading
            // it, and wake every waiter so another instance cannot steal a log.
            changed.as_mut().enable();
            let current = {
                let state = self.state.lock().unwrap();
                if entering {
                    state.entering.get(id).copied().unwrap_or_default()
                } else {
                    state
                        .records
                        .iter()
                        .filter(|r| r.fields["instance_id"] == id)
                        .count()
                }
            };
            if current >= count {
                return;
            }
            tokio::select! { biased;
                () = deadline.wait() => {
                    let (entered, recorded) = {
                        let state = self.state.lock().unwrap();
                        (state.entering.get(id).copied().unwrap_or_default(),
                            state.records.iter().filter(|r| r.fields["instance_id"] == id).count())
                    };
                    panic!("bounded host log wait expired: instance={id}, entering={entering}, expected={count}, entered={entered}, recorded={recorded}");
                },
                () = changed => {},
            }
        }
    }
    fn records(&self, id: &str) -> Vec<Captured> {
        self.state
            .lock()
            .unwrap()
            .records
            .iter()
            .filter(|r| r.fields["instance_id"] == id)
            .cloned()
            .collect()
    }
}
#[tokio::test]
async fn log_waiters_for_distinct_instances_observe_their_own_event() {
    let capture = Arc::new(Capture::default());
    let unrelated = capture.wait_for("unrelated", 1, true);
    tokio::pin!(unrelated);
    assert!(futures::poll!(unrelated.as_mut()).is_pending());
    let matching = capture.wait_for("matching", 1, true);
    tokio::pin!(matching);
    assert!(futures::poll!(matching.as_mut()).is_pending());
    let block = Block::new(capture.clone(), "matching".into());
    let writer_capture = capture.clone();
    let write = tokio::task::spawn_blocking(move || {
        tracing::subscriber::with_default(writer_capture, || {
            tracing::info!(target: "gateway_plugin", instance_id = "matching", "fixture log");
        });
    });
    tokio::time::timeout(Duration::from_secs(1), matching)
        .await
        .expect("an unrelated instance must not consume this instance's wakeup");
    assert!(futures::poll!(unrelated.as_mut()).is_pending());
    drop(block);
    write.await.unwrap();
    assert_eq!(capture.records("matching").len(), 1);
}
struct Block {
    capture: Arc<Capture>,
    id: String,
}
impl Block {
    fn new(capture: Arc<Capture>, id: String) -> Self {
        capture.state.lock().unwrap().blocked.insert(id.clone());
        Self { capture, id }
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        self.capture.state.lock().unwrap().blocked.remove(&self.id);
        self.capture.released.notify_all();
    }
}
struct Fixture {
    manager: Arc<PluginManager>,
    runtime: Arc<PluginEngine>,
    store: Arc<MemoryStore>,
    digest: String,
    id: String,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = Arc::new(PluginManager::new(store.clone(), runtime.clone()));
        let digest = manager
            .upload(archive("1.0.0", false), &actor())
            .await
            .unwrap()
            .sha256;
        let installed = manager
            .update(None, update(&digest, 0, true), &actor())
            .await
            .unwrap();
        Self {
            manager,
            runtime,
            store,
            digest,
            id: installed.id,
            _cache: cache,
        }
    }
    async fn call(&self, id: &str, revision: u64, params: Value, binary: bool) -> Value {
        let mut input = request("host-log");
        input.body =
            serde_json::to_vec(&json!({"operation":"host-log","params":params,"binary":binary}))
                .unwrap();
        let response = self.manager.handle(id, revision, input).await.unwrap();
        serde_json::from_slice(&response.body).unwrap()
    }
    async fn log(&self, level: &str) -> Value {
        self.call(
            &self.id,
            1,
            json!({"level":level,"event":"fixture.safe","fields":{}}),
            false,
        )
        .await
    }
}

#[tokio::test]
async fn native_logging_keeps_host_identity_levels_and_original_redaction() {
    let capture = capture();
    let f = Fixture::new().await;
    for level in ["debug", "info", "warn", "error"] {
        assert_eq!(f.log(level).await, json!({"recorded":true}));
    }
    assert_eq!(f.call(&f.id,1,json!({"event":"rt_fixture_secret_event","fields":{"authorization":"Bearer fixture_log_secret","unknown_key":"fixture_untrusted_text","nested":{"refresh_token":"fixture_rt_secret"},"count":3}}),false).await,json!({"recorded":true}));
    capture.wait_for(&f.id, 5, false).await;
    let records = capture.records(&f.id);
    assert_eq!(
        records
            .iter()
            .map(|r| r.level.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["DEBUG", "INFO", "WARN", "ERROR"])
    );
    for r in &records {
        assert_eq!(r.fields["generation"], "1");
        assert_eq!(r.fields["plugin_id"], "local.fixture");
        assert!(r.fields["event"].contains("sha256"));
        let encoded = format!("{:?}", r.fields);
        for secret in [
            "rt_fixture_secret_event",
            "fixture_log_secret",
            "fixture_untrusted_text",
            "fixture_rt_secret",
        ] {
            assert!(!encoded.contains(secret));
        }
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn malformed_log_requests_do_not_write_or_break_the_plugin() {
    let capture = capture();
    let f = Fixture::new().await;
    let fields: BTreeMap<_, _> = (0..33).map(|n| (format!("f{n}"), json!(n))).collect();
    for (params, binary) in [
        (json!({"event":"bad/event"}), false),
        (json!({"event":"safe","level":"fatal"}), false),
        (json!({"event":"safe","fields":fields}), false),
        (
            json!({"event":"safe","fields":{"data":"x".repeat(8192)}}),
            false,
        ),
        (json!({"event":"safe","actor":"system"}), false),
        (json!({"event":"safe"}), true),
    ] {
        assert_eq!(
            f.call(&f.id, 1, params, binary).await,
            json!({"error":{"code":"invalid_input"}})
        );
    }
    assert!(capture.records(&f.id).is_empty());
    assert_eq!(f.log("info").await, json!({"recorded":true}));
    f.manager.shutdown().await;
    assert_eq!(capture.records(&f.id).len(), 1);
}

#[tokio::test]
async fn one_incarnation_has_a_fixed_window_and_exhaustion_does_not_reserve_capacity() {
    let capture = capture();
    let f = Fixture::new().await;
    tokio::time::pause();
    // Keep the host clock fixed while the real SDK child exchanges frames.
    // A runnable task prevents virtual time from jumping to the RPC watchdog.
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    struct Ticker(tokio::task::JoinHandle<()>);
    impl Drop for Ticker {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let ticker = Ticker(ticker);
    for count in 1..=64 {
        assert_eq!(f.log("info").await, json!({"recorded":true}));
        capture.wait_for(&f.id, count, false).await;
    }
    assert_eq!(f.log("info").await, json!({"recorded":false}));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(f.log("info").await, json!({"recorded":true}));
    capture.wait_for(&f.id, 65, false).await;
    drop(ticker);
    tokio::time::resume();
    f.manager.shutdown().await;
    let records = capture.records(&f.id);
    assert_eq!(records.len(), 65);
    assert_eq!(records.last().unwrap().fields["suppressed_logs"], "1");
}

#[tokio::test]
async fn blocked_writes_share_capacity_across_instances_and_shutdown_keeps_their_ownership() {
    let capture = capture();
    let f = Fixture::new().await;
    let second = f
        .manager
        .update(None, update(&f.digest, 0, true), &actor())
        .await
        .unwrap();
    let block = Block::new(capture.clone(), f.id.clone());
    for _ in 0..8 {
        assert_eq!(f.log("info").await, json!({"recorded":true}));
    }
    capture.wait_for(&f.id, 8, true).await;
    assert_eq!(
        f.call(&second.id, 1, json!({"event":"fixture.other"}), false)
            .await,
        json!({"recorded":false})
    );
    let manager = f.manager.clone();
    let shutdown = tokio::spawn(async move {
        manager.shutdown().await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!shutdown.is_finished());
    drop(block);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(capture.records(&f.id).len(), 8);
    assert!(capture.records(&second.id).is_empty());
}

#[tokio::test]
async fn repeated_process_stop_waits_for_the_same_write_after_a_waiter_is_cancelled() {
    let capture = capture();
    let f = Fixture::new().await;
    let record = f.store.records.lock().unwrap()[&f.id].clone();
    let process = f
        .runtime
        .prepare(&record, f.store.artifact(&f.digest).await.unwrap())
        .await
        .unwrap();
    process.activate();
    let block = Block::new(capture.clone(), f.id.clone());
    let mut input = request("host-log");
    input.body = serde_json::to_vec(
        &json!({"operation":"host-log","params":{"event":"fixture.stop"},"binary":false}),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&process.handle(input).await.unwrap().body).unwrap(),
        json!({"recorded":true})
    );
    capture.wait_for(&f.id, 1, true).await;
    let first_process = process.clone();
    let first = tokio::spawn(async move {
        first_process.stop().await;
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!first.is_finished());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second_process = process.clone();
    let second = tokio::spawn(async move {
        second_process.stop().await;
    });
    let third_process = process.clone();
    let third = tokio::spawn(async move {
        third_process.stop().await;
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!second.is_finished());
    assert!(!third.is_finished());
    drop(block);
    tokio::time::timeout(Duration::from_secs(5), async {
        second.await.unwrap();
        third.await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(capture.records(&f.id).len(), 1);
    process.stop().await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn cancelled_manager_shutdown_keeps_active_processes_and_resumes_the_same_drain() {
    let capture = capture();
    let f = Fixture::new().await;
    let block = Block::new(capture.clone(), f.id.clone());
    assert_eq!(f.log("info").await, json!({"recorded":true}));
    capture.wait_for(&f.id, 1, true).await;
    let manager = f.manager.clone();
    let first = tokio::spawn(async move {
        manager.shutdown().await;
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!first.is_finished());
    assert!(f.manager.access(&f.id, 1).is_err());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let manager = f.manager.clone();
    let second = tokio::spawn(async move {
        manager.shutdown().await;
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!second.is_finished());
    drop(block);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(capture.records(&f.id).len(), 1);
    f.manager.shutdown().await;
    // Runtime registration stays closed after successful or repeated Host exit.
    let record = f.store.records.lock().unwrap()[&f.id].clone();
    assert!(
        f.runtime
            .prepare(&record, f.store.artifact(&f.digest).await.unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn host_shutdown_waits_for_failed_and_cancelled_candidate_registration_logs() {
    for cancelled in [false, true] {
        let capture = capture();
        let f = Fixture::new().await;
        let candidate = f
            .manager
            .update(None, update(&f.digest, 0, false), &actor())
            .await
            .unwrap();
        let block = Block::new(capture.clone(), candidate.id.clone());
        let mut settings = update(&f.digest, candidate.revision, true);
        settings.configuration = json!({"adapter_mode": if cancelled { "registration-log-pending" } else { "registration-log-fault" }});
        let manager = f.manager.clone();
        let id = candidate.id.clone();
        let preparing =
            tokio::spawn(async move { manager.update(Some(&id), settings, &actor()).await });
        capture.wait_for(&candidate.id, 1, true).await;
        if cancelled {
            assert!(!preparing.is_finished());
            preparing.abort();
            assert!(preparing.await.err().unwrap().is_cancelled());
        } else {
            assert!(
                tokio::time::timeout(Duration::from_secs(5), preparing)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
        }
        // The failed candidate was never published into active instances.
        assert!(f.manager.access(&candidate.id, candidate.revision).is_err());
        if cancelled {
            let runtime = f.runtime.clone();
            let first = tokio::spawn(async move {
                runtime.shutdown().await;
            });
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(!first.is_finished());
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
        }
        let manager = f.manager.clone();
        let shutdown = tokio::spawn(async move {
            manager.shutdown().await;
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!shutdown.is_finished());
        drop(block);
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capture.records(&candidate.id).len(), 1);
        assert!(capture.records(&f.id).is_empty());
    }
}
