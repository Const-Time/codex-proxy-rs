use crate::{
    services::{Probe, factory},
    support::*,
};
use gateway_admin::{
    model::{AdminErrorKind, plugins::*},
    ports::plugins::{
        PluginObservation, PluginObserverTarget, PluginProcess, PluginRuntime, PluginStore,
    },
};
use gateway_core::{
    account::ProviderAccountId,
    engine::{middleware::service as core, nested::ExtensionCallScope, observation::*},
    event::ProtocolWireEvent,
    lifecycle::CancellationToken,
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, ConfigRevision, ProviderKind, PublicModelId},
    upstream::UpstreamSendState,
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    num::NonZeroU32,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

struct Fixture {
    engine: PluginEngine,
    process: Arc<dyn PluginProcess>,
    store: Arc<MemoryStore>,
    global: Arc<Probe>,
    id: String,
    record: PluginRecord,
    artifact: PluginArtifact,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new(mode: &str) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let global = Arc::new(Probe::default());
        let global_port = factory(&global);
        let engine = PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_services(Arc::new(move || Ok(Some(global_port.clone()))));
        let artifact = engine.inspect(observer_archive("1.0.0")).await.unwrap();
        assert!(
            artifact
                .info
                .unsupported_capabilities
                .iter()
                .any(|c| c == "observer")
        );
        let id = format!("observer_{}", uuid::Uuid::new_v4().simple());
        let record = PluginRecord {
            upstream_adapter: None,
            retry_policy: None,
            account_scheduler: None,
            model_router: None,
            middleware: None,
            frontend_authentication: None,
            id: id.clone(),
            revision: 7,
            name: "Observer fixture".into(),
            artifact_sha256: artifact.info.sha256.clone(),
            enabled: true,
            configuration: json!({"adapter_mode":mode}),
            secrets: Default::default(),
        };
        store
            .records
            .lock()
            .unwrap()
            .insert(id.clone(), record.clone());
        // This inner bridge test uses a real prepared process. The public Admin
        // capability gate remains closed until publication and API wiring exist.
        let process = engine.prepare(&record, artifact.clone()).await.unwrap();
        Self {
            engine,
            process,
            store,
            global,
            id,
            record,
            artifact,
            _cache: cache,
        }
    }
    fn target(
        &self,
        order: i32,
        completed: Option<PluginExecutionScope>,
        websocket: Option<PluginExecutionScope>,
    ) -> PluginObserverTarget {
        PluginObserverTarget {
            process: self.process.clone(),
            order,
            request_completed: completed,
            websocket_response: websocket,
        }
    }
    fn plan(
        &self,
        targets: Vec<PluginObserverTarget>,
        services: Option<Arc<dyn core::Factory>>,
        cancellation: CancellationToken,
    ) -> Arc<dyn RequestObserverPlan> {
        self.engine
            .observer_plan(targets, services, cancellation)
            .unwrap()
            .unwrap()
    }
    async fn record(&self, event: &str) -> Option<Value> {
        self.store
            .state_get(
                &PluginStateScope {
                    instance_id: self.id.clone(),
                    revision: 7,
                    namespace: "observations".into(),
                    schema_version: 1,
                    maximum_records: 256,
                    maximum_bytes: 4 * 1024 * 1024,
                },
                event,
            )
            .await
            .unwrap()
            .map(|r| r.value)
    }
    async fn wait_record(&self, event: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(record) = self.record(event).await {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn stop(&self) {
        self.process.revoke();
        self.process.stop().await;
    }
}
fn all() -> Option<PluginExecutionScope> {
    Some(PluginExecutionScope::All {})
}
fn scope() -> RequestObservationScope {
    RequestObservationScope::new(
        ClientApiKeyId::new("key_observed").unwrap(),
        vec![AccountGroupId::new("grp_00000000000000000000000000000001").unwrap()],
    )
}
fn terminal(id: &str) -> RequestObservation {
    RequestObservation::new(
        gateway_core::engine::ModelRequestId::new(id).unwrap(),
        ConfigRevision::new(31).unwrap(),
        scope(),
        OperationKind::Generate,
        RequestObservationOutcome::Rejected,
        UpstreamSendState::NotSent,
        SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    )
    .with_requested_model(PublicModelId::new("gpt-observed").unwrap())
    .with_client_status_code(429)
    .with_retry_after_ms(123)
}
fn frame(
    id: &str,
    sequence: u64,
    attempt: u32,
    wire: ProtocolWireEvent,
) -> WebSocketResponseObservation {
    WebSocketResponseObservation::new(
        gateway_core::engine::ModelRequestId::new(id).unwrap(),
        ConfigRevision::new(31).unwrap(),
        scope(),
        OperationKind::Generate,
        WebSocketResponseAttempt::new(
            ProviderKind::new("openai").unwrap(),
            ProviderAccountId::new("acct_observed").unwrap(),
            NonZeroU32::new(attempt).unwrap(),
        ),
        sequence,
        wire,
    )
    .with_requested_model(PublicModelId::new("gpt-observed").unwrap())
}
fn json_frame(id: &str, sequence: u64) -> WebSocketResponseObservation {
    frame(
        id,
        sequence,
        1,
        ProtocolWireEvent::json(
            "openai",
            Some("response.output_text.delta".into()),
            json!({"type":"response.output_text.delta","delta":sequence}),
        )
        .unwrap(),
    )
}
fn wire(id: &str, call_scope: ExtensionCallScope) -> PluginObservation {
    PluginObservation {
        request_id: id.into(),
        metadata: json!({"event":"request_completed","data":{
            "event_id":format!("{id}:terminal"),"request_id":id,"config_revision":31,"operation":"generate",
            "completed_at_ms":10000,"terminal":{"outcome":"rejected","send_state":"not_sent","attempt_count":0},"usage":{}
        }}),
        payload: Arc::from([]),
        call_scope,
    }
}
async fn wait_facts(probe: &Probe, count: usize) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while probe.facts.lock().unwrap().len() < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_cancel(probe: &Probe) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if probe
                .facts
                .lock()
                .unwrap()
                .iter()
                .all(|f| f.context.cancellation.is_cancelled())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn prepared_observer_is_inactive_and_public_capability_remains_gated() {
    let f = Fixture::new("observer").await;
    assert!(f.process.observer_identity().is_none());
    assert!(
        f.engine
            .observer_plan(vec![f.target(0, all(), None)], None, Default::default())
            .is_err()
    );
    assert!(
        f.engine
            .validate(&f.record, &f.artifact, None)
            .await
            .is_err()
    );
    f.process.activate();
    let identity = f.process.observer_identity().unwrap();
    assert_eq!(identity.instance_id, f.id);
    assert_eq!(identity.plugin_id, "local.fixture");
    f.process.revoke();
    assert!(f.process.observer_identity().is_none());
    assert!(
        f.process
            .observe(
                wire("req_revoked", Default::default()),
                None,
                Default::default()
            )
            .await
            .is_err()
    );
    f.stop().await;
}

#[tokio::test]
async fn observer_rpc_uses_frozen_factory_native_identity_and_releases_every_scope() {
    let f = Fixture::new("observer-service").await;
    f.process.activate();
    let frozen = Arc::new(Probe::default());
    let graph = ExtensionCallScope::default()
        .fork_http("plugin_ancestor".into())
        .unwrap();
    for n in 0..35 {
        f.process
            .observe(
                wire(&format!("req_observer_{n}"), graph.clone()),
                Some(factory(&frozen)),
                Default::default(),
            )
            .await
            .unwrap();
    }
    assert!(f.global.facts.lock().unwrap().is_empty());
    let facts = frozen.facts.lock().unwrap().clone();
    assert_eq!(facts.len(), 35);
    let mut call_ids = std::collections::BTreeSet::new();
    for (n, fact) in facts.iter().enumerate() {
        assert_eq!(fact.instance, f.id);
        assert_eq!(fact.revision, 7);
        assert_eq!(fact.context.operation, "observer.observe");
        assert_eq!(fact.context.request_id, format!("req_observer_{n}"));
        assert!(fact.context.parent_call_id.is_none());
        assert!(fact.context.cancellation.is_cancelled());
        assert!(
            fact.context
                .call_scope
                .excluded_instances()
                .contains("plugin_ancestor")
        );
        assert!(
            fact.context.deadline_at.bounded(Duration::from_secs(120)) <= Duration::from_secs(2)
        );
        call_ids.insert(fact.context.call_id.clone());
    }
    assert_eq!(call_ids.len(), 35);
    f.stop().await;
}

#[tokio::test]
async fn explicit_missing_frozen_service_never_reads_the_new_global_publication() {
    let f = Fixture::new("observer-denied").await;
    f.process.activate();
    f.process
        .observe(
            wire("req_no_service", Default::default()),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(f.global.facts.lock().unwrap().is_empty());
    f.stop().await;
}

#[tokio::test]
async fn rpc_accepts_empty_or_null_and_faults_invalid_envelope_or_payload() {
    for mode in [
        "observer",
        "observer-null",
        "observer-envelope",
        "observer-payload",
    ] {
        let f = Fixture::new(mode).await;
        f.process.activate();
        let result = f
            .process
            .observe(
                wire("req_reply", Default::default()),
                None,
                Default::default(),
            )
            .await;
        if matches!(mode, "observer" | "observer-null") {
            result.unwrap();
            assert_eq!(f.process.status(), "ready");
        } else {
            assert_eq!(result.unwrap_err().kind(), AdminErrorKind::BadGateway);
            assert!(f.process.access().is_revoked());
            assert!(f.process.observer_identity().is_none());
        }
        f.stop().await;
    }
}

#[tokio::test]
async fn native_rejection_projects_unknown_cost_and_frozen_facts_without_provider() {
    let f = Fixture::new("observer-service").await;
    f.process.activate();
    let frozen = Arc::new(Probe::default());
    let plan = f.plan(
        vec![f.target(0, all(), None)],
        Some(factory(&frozen)),
        Default::default(),
    );
    plan.dispatch(terminal("req_projection"));
    let record = f.wait_record("req_projection:terminal").await;
    wait_facts(&frozen, 1).await;
    wait_cancel(&frozen).await;
    let data = &record["event"]["data"];
    assert_eq!(record["event"]["event"], "request_completed");
    assert_eq!(data["config_revision"], 31);
    assert_eq!(data["client_key_id"], "key_observed");
    assert_eq!(data["requested_model"], "gpt-observed");
    assert_eq!(data["completed_at_ms"], 10000);
    assert_eq!(data["terminal"]["attempt_count"], 0);
    assert_eq!(data["terminal"]["client_status_code"], 429);
    assert_eq!(
        data["usage"]["cost"],
        json!({"status":"unknown","source":"unavailable"})
    );
    assert_eq!(data["usage"]["failure"]["retry_after_ms"], 123);
    for field in ["provider", "account_id", "upstream_model", "error_message"] {
        assert!(data.get(field).is_none());
    }
    assert!(f.global.facts.lock().unwrap().is_empty());
    f.stop().await;
}

#[tokio::test]
async fn plan_filters_frozen_key_groups_and_all_ancestors() {
    let a = Fixture::new("observer").await;
    let b = Fixture::new("observer").await;
    a.process.activate();
    b.process.activate();
    let graph = ExtensionCallScope::default()
        .fork_http(a.id.clone())
        .unwrap();
    let plan = a.plan(
        vec![
            a.target(-10, all(), all()),
            b.target(
                10,
                Some(PluginExecutionScope::Groups {
                    ids: vec!["grp_00000000000000000000000000000001".into()],
                }),
                Some(PluginExecutionScope::Keys {
                    ids: vec!["key_observed".into()],
                }),
            ),
        ],
        None,
        Default::default(),
    );
    plan.dispatch_websocket_response(
        json_frame("req_scope", 1).with_extension_scope(graph.clone()),
    );
    plan.dispatch(terminal("req_scope").with_extension_scope(graph));
    b.wait_record("req_scope:websocket:1:1").await;
    b.wait_record("req_scope:terminal").await;
    assert!(a.record("req_scope:terminal").await.is_none());
    assert!(a.record("req_scope:websocket:1:1").await.is_none());
    let wrong = a.plan(
        vec![b.target(
            0,
            Some(PluginExecutionScope::Keys {
                ids: vec!["key_other".into()],
            }),
            Some(PluginExecutionScope::Groups {
                ids: vec!["grp_00000000000000000000000000000002".into()],
            }),
        )],
        None,
        Default::default(),
    );
    wrong.dispatch_websocket_response(json_frame("req_other", 1));
    wrong.dispatch(terminal("req_other"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(b.record("req_other:terminal").await.is_none());
    assert!(b.record("req_other:websocket:1:1").await.is_none());
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn websocket_queue_preserves_original_bytes_attempts_and_serial_sequence() {
    let f = Fixture::new("observer-service").await;
    f.process.activate();
    let probe = Arc::new(Probe::default());
    let plan = f.plan(
        vec![f.target(0, None, all())],
        Some(factory(&probe)),
        Default::default(),
    );
    let raw = b" { \"type\" : \"response.created\", \"original\":true } ";
    plan.dispatch_websocket_response(frame(
        "req_ws",
        1,
        1,
        ProtocolWireEvent::raw_json("openai", bytes::Bytes::copy_from_slice(raw)).unwrap(),
    ));
    plan.dispatch_websocket_response(json_frame("req_ws", 2));
    plan.dispatch_websocket_response(frame(
        "req_ws",
        3,
        2,
        ProtocolWireEvent::raw_sse("openai", bytes::Bytes::from_static(b"data: fixture\n\n"))
            .unwrap(),
    ));
    let first = f.wait_record("req_ws:websocket:1:1").await;
    let second = f.wait_record("req_ws:websocket:1:2").await;
    let third = f.wait_record("req_ws:websocket:2:3").await;
    wait_facts(&probe, 3).await;
    wait_cancel(&probe).await;
    assert_eq!(first["payload"], String::from_utf8_lossy(raw).as_ref());
    assert_eq!(
        second["event"]["data"]["event_type"],
        "response.output_text.delta"
    );
    assert_eq!(third["event"]["data"]["attempt_index"], 2);
    assert_eq!(third["event"]["data"]["sequence"], 3);
    assert_eq!(third["event"]["data"]["payload_included"], false);
    assert!(third["event"]["data"].get("event_type").is_none());
    assert_eq!(third["payload"], "");
    let ids: Vec<_> = probe
        .facts
        .lock()
        .unwrap()
        .iter()
        .map(|f| f.context.call_id.clone())
        .collect();
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3
    );
    plan.dispatch(terminal("req_ws")); // Close queues even with no terminal subscriber.
    f.stop().await;
}

#[tokio::test]
async fn oversized_raw_and_serialized_frames_drop_without_breaking_later_delivery() {
    let f = Fixture::new("observer").await;
    f.process.activate();
    let plan = f.plan(vec![f.target(0, None, all())], None, Default::default());
    plan.dispatch_websocket_response(frame(
        "req_large",
        1,
        1,
        ProtocolWireEvent::raw_json("openai", bytes::Bytes::from(vec![b'x'; 1024 * 1024])).unwrap(),
    ));
    plan.dispatch_websocket_response(frame(
        "req_large",
        2,
        1,
        ProtocolWireEvent::json("openai", None, json!({"large":"x".repeat(1024*1024)})).unwrap(),
    ));
    plan.dispatch_websocket_response(json_frame("req_large", 3));
    f.wait_record("req_large:websocket:1:3").await;
    assert!(f.record("req_large:websocket:1:1").await.is_none());
    assert!(f.record("req_large:websocket:1:2").await.is_none());
    assert_eq!(f.process.status(), "ready");
    plan.dispatch(terminal("req_large"));
    f.stop().await;
}

#[tokio::test]
async fn publication_cancel_and_rpc_deadline_drop_original_callbacks() {
    for cancel in [true, false] {
        let f = Fixture::new("observer-wait").await;
        f.process.activate();
        let probe = Arc::new(Probe::default());
        probe.wait.store(true, Ordering::SeqCst);
        let token = CancellationToken::new();
        let request_token = token.clone();
        let process = f.process.clone();
        let port = factory(&probe);
        let rpc = tokio::spawn(async move {
            process
                .observe(
                    wire("req_wait", Default::default()),
                    Some(port),
                    request_token,
                )
                .await
        });
        wait_facts(&probe, 1).await;
        if cancel {
            token.cancel();
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(4), rpc)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        wait_cancel(&probe).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !probe.dropped.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        f.stop().await;
    }
}

#[tokio::test]
async fn terminal_capacity_is_bounded_and_epoch_cancel_releases_inflight_callbacks() {
    let f = Fixture::new("observer-wait").await;
    f.process.activate();
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let token = CancellationToken::new();
    let plan = f.plan(
        vec![f.target(0, all(), None)],
        Some(factory(&probe)),
        token.clone(),
    );
    for n in 0..32 {
        plan.dispatch(terminal(&format!("req_capacity_{n}")));
    }
    wait_facts(&probe, 16).await;
    assert_eq!(probe.facts.lock().unwrap().len(), 16);
    token.cancel();
    wait_cancel(&probe).await;
    plan.dispatch(terminal("req_after_epoch"));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(f.record("req_after_epoch:terminal").await.is_none());
    f.stop().await;
}

#[tokio::test]
async fn stopping_one_subscriber_does_not_cancel_the_other_and_plan_drop_drains() {
    let a = Fixture::new("observer-fault").await;
    let b = Fixture::new("observer").await;
    a.process.activate();
    b.process.activate();
    let plan = a.plan(
        vec![a.target(0, all(), all()), b.target(1, all(), all())],
        None,
        Default::default(),
    );
    plan.dispatch_websocket_response(json_frame("req_fault", 1));
    b.wait_record("req_fault:websocket:1:1").await;
    a.stop().await;
    plan.dispatch_websocket_response(json_frame("req_fault", 2));
    drop(plan); // The worker holds a weak queue map and cannot keep its sender alive.
    b.wait_record("req_fault:websocket:1:2").await;
    b.stop().await;
}

#[tokio::test]
async fn invalid_subscription_order_duplicate_instance_and_scope_are_rejected() {
    let f = Fixture::new("observer").await;
    f.process.activate();
    for targets in [
        vec![f.target(10001, all(), None)],
        vec![f.target(0, None, None)],
        vec![f.target(0, Some(PluginExecutionScope::Keys { ids: vec![] }), None)],
        vec![f.target(0, all(), None), f.target(0, None, all())],
    ] {
        assert!(
            f.engine
                .observer_plan(targets, None, Default::default())
                .is_err()
        );
    }
    assert!(
        f.engine
            .observer_plan(vec![], None, Default::default())
            .unwrap()
            .is_none()
    );
    f.stop().await;
}

#[tokio::test]
async fn subscribers_follow_frozen_order_and_instance_tie_breaker() {
    let a = Fixture::new("observer-service").await;
    let b = Fixture::new("observer-service").await;
    a.process.activate();
    b.process.activate();
    let probe = Arc::new(Probe::default());
    for (name, ao, bo) in [("req_order", 10, -10), ("req_tie", 0, 0)] {
        let plan = a.plan(
            vec![a.target(ao, all(), None), b.target(bo, all(), None)],
            Some(factory(&probe)),
            Default::default(),
        );
        let before = probe.facts.lock().unwrap().len();
        plan.dispatch(terminal(name));
        wait_facts(&probe, before + 2).await;
        wait_cancel(&probe).await;
        let ids: Vec<_> = probe.facts.lock().unwrap()[before..]
            .iter()
            .map(|f| f.instance.clone())
            .collect();
        let expected = if ao != bo {
            vec![b.id.clone(), a.id.clone()]
        } else {
            let mut ids = vec![a.id.clone(), b.id.clone()];
            ids.sort();
            ids
        };
        assert_eq!(ids, expected);
    }
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn websocket_payload_budget_is_global_and_released_after_timeout() {
    let f = Fixture::new("observer-wait").await;
    f.process.activate();
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let plan = f.plan(
        vec![f.target(0, None, all())],
        Some(factory(&probe)),
        Default::default(),
    );
    for n in 0..6 {
        plan.dispatch_websocket_response(frame(
            &format!("req_bytes_{n}"),
            1,
            1,
            ProtocolWireEvent::json("openai", None, json!({"large":"x".repeat(200_000)})).unwrap(),
        ));
    }
    wait_facts(&probe, 5).await;
    assert_eq!(probe.facts.lock().unwrap().len(), 5);
    assert!(f.record("req_bytes_5:websocket:1:1").await.is_none());
    wait_cancel(&probe).await;
    probe.wait.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(30)).await;
    plan.dispatch_websocket_response(json_frame("req_bytes_5", 2));
    f.wait_record("req_bytes_5:websocket:1:2").await;
    wait_facts(&probe, 6).await;
    for n in 0..6 {
        plan.dispatch(terminal(&format!("req_bytes_{n}")));
    }
    f.stop().await;
}

#[tokio::test]
async fn websocket_worker_limit_and_revocation_release_only_relevant_subscribers() {
    let a = Fixture::new("observer").await;
    let b = Fixture::new("observer").await;
    a.process.activate();
    b.process.activate();
    let plan = a.plan(
        vec![
            a.target(
                0,
                None,
                Some(PluginExecutionScope::Keys {
                    ids: vec!["key_observed".into()],
                }),
            ),
            b.target(
                1,
                None,
                Some(PluginExecutionScope::Keys {
                    ids: vec!["key_other".into()],
                }),
            ),
        ],
        None,
        Default::default(),
    );
    for n in 0..16 {
        plan.dispatch_websocket_response(json_frame(&format!("req_workers_{n}"), 1));
    }
    for n in 0..16 {
        a.wait_record(&format!("req_workers_{n}:websocket:1:1"))
            .await;
    }
    plan.dispatch_websocket_response(json_frame("req_workers_overflow", 1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        a.record("req_workers_overflow:websocket:1:1")
            .await
            .is_none()
    );
    a.stop().await;
    // b stays active but is not a subscriber for those requests. Idle workers
    // must stop when their own subscriber leases end, without a queue-map cycle.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let other = WebSocketResponseObservation::new(
        gateway_core::engine::ModelRequestId::new("req_workers_reused").unwrap(),
        ConfigRevision::new(31).unwrap(),
        RequestObservationScope::new(ClientApiKeyId::new("key_other").unwrap(), vec![]),
        OperationKind::Generate,
        WebSocketResponseAttempt::new(
            ProviderKind::new("openai").unwrap(),
            ProviderAccountId::new("acct_observed").unwrap(),
            NonZeroU32::new(1).unwrap(),
        ),
        1,
        ProtocolWireEvent::json("openai", None, json!({"event":"available"})).unwrap(),
    );
    plan.dispatch_websocket_response(other);
    b.wait_record("req_workers_reused:websocket:1:1").await;
    plan.dispatch(terminal("req_workers_reused"));
    b.stop().await;
}

#[tokio::test]
async fn websocket_queue_overflow_keeps_order_and_does_not_block_later_frames() {
    let f = Fixture::new("observer-wait").await;
    f.process.activate();
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let plan = f.plan(
        vec![f.target(0, None, all())],
        Some(factory(&probe)),
        Default::default(),
    );
    // No await in this current-thread runtime: the worker cannot receive before
    // these synchronous dispatches fill the original 16-slot channel.
    for sequence in 1..=200 {
        plan.dispatch_websocket_response(json_frame("req_overflow", sequence));
    }
    wait_facts(&probe, 1).await;
    probe.wait.store(false, Ordering::SeqCst);
    f.wait_record("req_overflow:websocket:1:16").await;
    wait_facts(&probe, 16).await;
    wait_cancel(&probe).await;
    assert_eq!(probe.facts.lock().unwrap().len(), 16);
    for sequence in [17, 100, 200] {
        assert!(
            f.record(&format!("req_overflow:websocket:1:{sequence}"))
                .await
                .is_none()
        );
    }
    plan.dispatch_websocket_response(json_frame("req_overflow", 201));
    f.wait_record("req_overflow:websocket:1:201").await;
    assert_eq!(f.process.status(), "ready");
    plan.dispatch(terminal("req_overflow"));
    f.stop().await;
}
