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
            !artifact
                .info
                .unsupported_capabilities
                .iter()
                .any(|c| c == "observer")
        );
        let id = format!("observer_{}", uuid::Uuid::new_v4().simple());
        let record = PluginRecord {
            observer: None,
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
        // The native prepared process remains inactive until its original owner publishes it.
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
async fn prepared_observer_is_inactive_and_declared_capability_is_validated() {
    let f = Fixture::new("observer").await;
    assert!(f.process.observer_identity().is_none());
    assert!(
        f.engine
            .observer_plan(vec![f.target(0, all(), None)], None, Default::default())
            .is_err()
    );
    f.engine
        .validate(&f.record, &f.artifact, None)
        .await
        .unwrap();
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

struct Published {
    manager: gateway_admin::plugins::PluginManager,
    store: Arc<MemoryStore>,
    id: String,
    digest: String,
    _cache: tempfile::TempDir,
}
impl Published {
    async fn new(mode: &str) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let engine = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = gateway_admin::plugins::PluginManager::new(store.clone(), engine.clone());
        use gateway_admin::ports::plugins::PluginService;
        let artifact = manager
            .upload(observer_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        command.configuration = json!({"adapter_mode":mode});
        command.observer = Some(PluginObserverBinding {
            order: -3,
            request_completed: all(),
            websocket_response: all(),
        });
        let installed = manager.update(None, command, &actor()).await.unwrap();
        Self {
            manager,
            store,
            id: installed.id,
            digest: artifact.sha256,
            _cache: cache,
        }
    }
    fn restart_runtime(&self) -> Arc<PluginEngine> {
        Arc::new(PluginEngine::new(
            self._cache.path().into(),
            "3.11.2".parse().unwrap(),
            self.store.clone(),
        ))
    }
    fn registry(&self) -> gateway_admin::plugins::middleware::PluginMiddlewareRegistry {
        use gateway_admin::ports::plugins::PluginService;
        self.manager.middleware_registry().unwrap()
    }
    async fn wait(&self, event: &str, revision: u64) -> Value {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(record) = self
                    .store
                    .state_get(
                        &PluginStateScope {
                            instance_id: self.id.clone(),
                            revision,
                            namespace: "observations".into(),
                            schema_version: 1,
                            maximum_records: 256,
                            maximum_bytes: 4 * 1024 * 1024,
                        },
                        event,
                    )
                    .await
                    .unwrap()
                {
                    return record.value;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn change(&self, enabled: bool, mode: &str) {
        use gateway_admin::ports::plugins::PluginService;
        let record = self.store.records.lock().unwrap()[&self.id].clone();
        let mut command = update(&self.digest, record.revision, enabled);
        command.configuration = json!({"adapter_mode":mode});
        command.observer = record.observer;
        self.manager
            .update(Some(&self.id), command, &actor())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn original_publication_shares_observer_budget_without_revoking_business() {
    let f = Published::new("observer").await;
    let first = f.registry();
    let second = f.registry();
    let plan = first.observer_plan().unwrap();
    assert!(Arc::ptr_eq(&plan, &second.observer_plan().unwrap()));
    let business = first.model_catalog().unwrap().cancellation();
    plan.dispatch(terminal("req_published"));
    f.wait("req_published:terminal", 1).await;
    f.change(false, "observer").await;
    assert!(!business.is_cancelled());
    assert!(f.registry().observer_plan().is_none());
    plan.dispatch(terminal("req_old_publication"));
    tokio::time::sleep(Duration::from_millis(30)).await;
    f.change(true, "observer").await;
    assert!(
        f.store
            .state_get(
                &PluginStateScope {
                    instance_id: f.id.clone(),
                    revision: 3,
                    namespace: "observations".into(),
                    schema_version: 1,
                    maximum_records: 256,
                    maximum_bytes: 4 * 1024 * 1024
                },
                "req_old_publication:terminal"
            )
            .await
            .unwrap()
            .is_none()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn frozen_observation_scope_survives_failed_upgrade_and_restores_actual_revision() {
    use gateway_admin::ports::plugins::PluginService;
    let f = Published::new("observer").await;
    let plan = f.registry().observer_plan().unwrap();
    let mut bad = update(&f.digest, 1, true);
    bad.configuration = json!({"fail":true});
    bad.observer = f.store.records.lock().unwrap()[&f.id].observer.clone();
    assert!(f.manager.update(Some(&f.id), bad, &actor()).await.is_err());
    assert!(Arc::ptr_eq(&plan, &f.registry().observer_plan().unwrap()));
    plan.dispatch_websocket_response(json_frame("req_upgrade_old", 1));
    f.wait("req_upgrade_old:websocket:1:1", 1).await;
    f.manager.shutdown().await;
    let restored = gateway_admin::plugins::PluginManager::new(f.store.clone(), f.restart_runtime());
    restored.restore().await.unwrap();
    let restored_plan = restored
        .middleware_registry()
        .unwrap()
        .observer_plan()
        .unwrap();
    assert!(!Arc::ptr_eq(&plan, &restored_plan));
    restored_plan.dispatch(terminal("req_restored"));
    f.wait("req_restored:terminal", 1).await;
    restored.shutdown().await;
}

#[tokio::test]
async fn failed_observer_restore_does_not_gate_model_catalog_or_other_instances() {
    use gateway_admin::ports::plugins::PluginService;
    let f = Published::new("observer").await;
    f.manager.shutdown().await;
    f.store
        .records
        .lock()
        .unwrap()
        .get_mut(&f.id)
        .unwrap()
        .configuration = json!({"fail":true});
    let manager = gateway_admin::plugins::PluginManager::new(f.store.clone(), f.restart_runtime());
    manager.restore().await.unwrap();
    let registry = manager.middleware_registry().unwrap();
    assert!(registry.model_catalog().is_ok());
    assert!(registry.observer_plan().is_none());
    assert_eq!(manager.list().await.unwrap()[0].status, "failed");
    manager.shutdown().await;
}

#[tokio::test]
async fn observer_only_unknown_commit_revokes_observation_without_gating_business() {
    let f = Published::new("observer").await;
    let first = f.registry();
    let plan = first.observer_plan().unwrap();
    let business = first.model_catalog().unwrap().cancellation();
    f.store.save_fault.store(3, Ordering::SeqCst);
    use gateway_admin::ports::plugins::PluginService;
    let mut command = update(&f.digest, 1, false);
    command.observer = f.store.records.lock().unwrap()[&f.id].observer.clone();
    assert_eq!(
        f.manager
            .update(Some(&f.id), command, &actor())
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::UpstreamResultUnknown
    );
    assert!(!business.is_cancelled());
    assert!(f.registry().model_catalog().is_ok());
    assert!(f.registry().observer_plan().is_none());
    plan.dispatch(terminal("req_unknown_commit"));
    f.manager.shutdown().await;
}

struct ApiLifecycle;
impl gateway_core::health::WorkerHealthSource for ApiLifecycle {
    fn snapshot(&self) -> Vec<gateway_core::health::WorkerHealthSnapshot> {
        vec![]
    }
}
struct ApiConnection;
impl gateway_core::lifecycle::ConnectionGuard for ApiConnection {}
impl gateway_core::lifecycle::ConnectionLifecycle for ApiLifecycle {
    fn try_register(
        &self,
    ) -> Result<
        Box<dyn gateway_core::lifecycle::ConnectionGuard>,
        gateway_core::lifecycle::ConnectionDraining,
    > {
        Ok(Box::new(ApiConnection))
    }
    fn cancellation(&self) -> CancellationToken {
        CancellationToken::new()
    }
    fn is_draining(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn original_api_core_sdk_observer_persists_one_native_terminal_in_postgres() {
    use gateway_admin::ports::plugins::PluginService;
    use gateway_core::engine::middleware::http as http_core;
    use http_body_util::BodyExt as _;
    let Some(environment) = crate::services::upstream_sdk::environment::Environment::new().await
    else {
        return;
    };
    let native = crate::models::fixture::Fixture::new().await;
    let store = environment.store.plugin_store();
    let manager = Arc::new(
        gateway_admin::plugins::PluginManager::new(
            store.clone(),
            Arc::new(PluginEngine::new(
                environment.cache.path().join("observer"),
                "3.11.2".parse().unwrap(),
                store.clone(),
            )),
        )
        .with_services(environment.admin.services().service_settings()),
    );
    let artifact = manager
        .upload(observer_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 0, true);
    command.secrets.clear();
    command.configuration = json!({"adapter_mode":"observer"});
    command.observer = Some(PluginObserverBinding {
        order: 0,
        request_completed: all(),
        websocket_response: all(),
    });
    let installed = manager.update(None, command, &actor()).await.unwrap();
    let lifecycle = Arc::new(ApiLifecycle);
    let api = gateway_api::initialize(
        gateway_api::ApiConfig {
            asset_directory: environment.cache.path().into(),
            cors_allowed_origins: vec![],
            request_timeout_seconds: None,
            request_id_header: "x-request-id".into(),
        },
        native.execution.clone(),
        environment.admin.services().with_plugins(manager.clone()),
        vec![],
        lifecycle.clone(),
        lifecycle,
    )
    .unwrap();
    for (index, stream) in [false, true].into_iter().enumerate() {
        let context = http_core::Context {
            request_id: format!("req_observer_api_{index}"),
            call_id: format!("observer-api-call-{index}"),
            parent_call_id: None,
            call_scope: Default::default(),
            model_parent: None,
            cancellation: CancellationToken::new(),
            deadline_at: Default::default(),
        }
        .child("native-api-caller".into())
        .unwrap();
        let body = http_body_util::Full::new(bytes::Bytes::from(
            serde_json::to_vec(
                &json!({"model":"gpt-native","input":"observer API fixture","stream":stream}),
            )
            .unwrap(),
        ))
        .map_err(|never| match never {})
        .boxed_unsync();
        let request = http::Request::post("/v1/responses")
            .header("authorization", "Bearer sk_native_fixture")
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        let response = api.dispatcher().dispatch(context, request).await.unwrap();
        assert_eq!(response.status(), 200);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).contains("completed"));
        native.clean(index + 1).await;
        let request_id = native.core.requests.lock().unwrap()[index]
            .id
            .as_str()
            .to_owned();
        let event_id = format!("{request_id}:terminal");
        let scope = PluginStateScope {
            instance_id: installed.id.clone(),
            revision: 1,
            namespace: "observations".into(),
            schema_version: 1,
            maximum_records: 256,
            maximum_bytes: 4 * 1024 * 1024,
        };
        let saved = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(record) = store.state_get(&scope, &event_id).await.unwrap() {
                    break record.value;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let data = &saved["event"]["data"];
        assert_eq!(data["request_id"], request_id);
        assert_eq!(data["provider"], "openai");
        assert_eq!(data["account_id"], "acct_allowed");
        assert_eq!(data["requested_model"], "gpt-native");
        assert_eq!(data["terminal"]["outcome"], "succeeded");
        assert_eq!(data["terminal"]["attempt_count"], 1);
        assert_eq!(
            data["usage"]["cost"]["total"]["amount"].as_str().unwrap(),
            native.core.charges.lock().unwrap()[index]
                .amount_usd
                .canonical()
        );
        assert_eq!(saved["count"], 1);
        assert_eq!(saved["payload_len"], 0);
        assert_eq!(native.core.finalizations.lock().unwrap().len(), index + 1);
        assert_eq!(native.core.active.load(Ordering::SeqCst), 0);
    }
    manager.shutdown().await;
    drop(api);
    drop(manager);
    drop(store);
    native.manager.shutdown().await;
    drop(native);
    environment.close().await;
}
