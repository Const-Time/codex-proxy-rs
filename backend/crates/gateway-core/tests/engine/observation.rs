use super::execution::{
    Admissions, Budget, ChargedProvider, RecordingClientApiKeyUsage, TrackingExecutionStore,
    UnusedContinuation, assert_cleanup_completed, assert_failure, consume_charged_prefix,
    known_charge, prepare, request, start_snapshot, start_snapshot_with_clients,
    start_snapshot_with_policy,
};
use async_trait::async_trait;
use futures::{channel::oneshot, executor::block_on, future::BoxFuture};
use gateway_core::engine::execution::{
    PreparedExecutionRequest, PreparedRootExecution, StartedExecution,
};
use gateway_core::engine::observation::{
    RequestObservation, RequestObservationOutcome, RequestObserverPlan,
    WebSocketResponseObservation,
};
use gateway_core::event::{ProtocolWireEvent, ProviderResponseObservation};
use gateway_core::{
    account::ProviderAccountId,
    engine::{
        AttemptContext,
        execution::{ClientTransport, DefaultExecutionService, ExecutionService},
        provider::{
            Provider, ProviderCallMetadata, ProviderRegistry, ProviderRequest, ProviderStream,
        },
    },
    error::{GatewayErrorKind, ProviderError, ProviderErrorKind},
    event::{GatewayEvent, ProviderEvent, ResponseMeta},
    metering::ProviderReportedCost,
    operation::OperationKind,
    policy::{ClientApiKeyId, ClientPolicy, PlaintextClientApiKey, RateLimits},
    routing::{
        ClientRoutingScope, ConfigRevision, FrozenAccountScope, ProviderCatalogGeneration,
        ProviderKind, ProviderModelCapabilities, PublicModelId, RuntimeAccount,
        RuntimeAccountDirectory,
    },
    runtime::RuntimeSnapshotHandle,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Observer {
    admissions: Arc<Admissions>,
    completed: Mutex<Vec<RequestObservation>>,
    websocket: Mutex<Vec<WebSocketResponseObservation>>,
    panic: bool,
}
impl Observer {
    fn new(admissions: Arc<Admissions>) -> Self {
        Self {
            admissions,
            completed: Mutex::default(),
            websocket: Mutex::default(),
            panic: false,
        }
    }
}
impl RequestObserverPlan for Observer {
    fn dispatch(&self, observation: RequestObservation) {
        assert!(!self.admissions.active.load(Ordering::SeqCst));
        self.completed.lock().unwrap().push(observation);
        assert!(!self.panic, "fixture observer fault");
    }
    fn dispatch_websocket_response(&self, observation: WebSocketResponseObservation) {
        self.websocket.lock().unwrap().push(observation);
        assert!(!self.panic, "fixture observer fault");
    }
}

fn fixture(
    admissions: Arc<Admissions>,
    budget: Arc<Budget>,
    store: Arc<TrackingExecutionStore>,
    provider: Arc<dyn Provider>,
) -> DefaultExecutionService {
    DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot()),
        store,
        ProviderRegistry::new([provider]).unwrap(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
    .with_budget(budget)
}

async fn start(
    service: &DefaultExecutionService,
    prepared: PreparedRootExecution,
    transport: ClientTransport,
) -> StartedExecution {
    let input = request(service, transport);
    service
        .start_prepared(
            prepared,
            PreparedExecutionRequest {
                public_model: input.public_model,
                operation: input.operation,
                metadata: input.metadata,
            },
        )
        .await
        .unwrap()
}

#[test]
fn completed_uses_frozen_identity_and_native_terminal_cost_after_cleanup_once() {
    block_on(async {
        for transport in [
            ClientTransport::HttpJson,
            ClientTransport::HttpSse,
            ClientTransport::WebSocket,
        ] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let observer = Arc::new(Observer::new(admissions.clone()));
            let service = fixture(
                admissions.clone(),
                budget.clone(),
                store.clone(),
                Arc::new(ChargedProvider { fail: false }),
            );
            let prepared = prepare(&service).with_observer_plan(observer.clone());
            let id = prepared.request_id().clone();
            let mut started = start(&service, prepared, transport).await;
            if transport == ClientTransport::HttpJson {
                assert_eq!(
                    started.session.collect_uncommitted().await.unwrap().len(),
                    3
                );
                assert!(observer.completed.lock().unwrap().is_empty());
                started.session.commit_downstream(Some(207)).await.unwrap();
            } else {
                consume_charged_prefix(started.session.as_mut()).await;
                started.session.record_client_status(207).await.unwrap();
                assert!(started.session.next_event().await.unwrap().is_some());
            }
            assert!(started.session.next_event().await.unwrap().is_none());
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &id);
            let events = observer.completed.lock().unwrap();
            assert_eq!(events.len(), 1);
            let event = &events[0];
            let records = store.finalizations.lock().unwrap();
            let record = &records[0];
            assert_eq!(event.event_id(), format!("{}:terminal", id.as_str()));
            assert_eq!(event.request_id(), &id);
            assert_eq!(event.config_revision(), ConfigRevision::new(1).unwrap());
            assert_eq!(event.client_key_id().as_str(), "key_start_test");
            assert_eq!(event.operation(), OperationKind::Generate);
            assert_eq!(event.requested_model().unwrap().as_str(), "gpt-start");
            assert_eq!(event.account_id().unwrap().as_str(), "acct_start");
            assert_eq!(event.provider().unwrap().as_str(), "openai");
            assert_eq!(event.upstream_model().unwrap().as_str(), "gpt-start");
            assert_eq!(event.outcome(), RequestObservationOutcome::Succeeded);
            assert_eq!(event.send_state(), UpstreamSendState::Sent);
            assert_eq!(event.attempt_count(), 1);
            assert_eq!(event.client_status_code(), Some(207));
            assert_eq!(event.cost(), &record.cost);
            assert_eq!(event.usage(), &record.usage);
            assert_eq!(event.timings(), &record.timings);
            assert_eq!(event.completed_at(), record.completed_at);
            assert!(event.error_kind().is_none());
            assert!(observer.websocket.lock().unwrap().is_empty());
        }
    });
}

#[test]
fn cancelled_terminal_polls_retain_facts_until_store_settlement_and_release_finish() {
    block_on(async {
        for fail in [false, true] {
            let (release_write, write_gate) = oneshot::channel();
            let (release_settle, settle_gate) = oneshot::channel();
            let (release_slot, slot_gate) = oneshot::channel();
            let admissions = Arc::new(Admissions {
                release_gate: Mutex::new(Some(slot_gate)),
                ..Default::default()
            });
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                settlement_gate: Mutex::new(Some(settle_gate)),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore {
                finalize_gate: Mutex::new(Some(write_gate)),
                ..Default::default()
            });
            let observer = Arc::new(Observer::new(admissions.clone()));
            let service = fixture(
                admissions.clone(),
                budget.clone(),
                store.clone(),
                Arc::new(ChargedProvider { fail }),
            );
            let prepared = prepare(&service).with_observer_plan(observer.clone());
            let mut started = start(&service, prepared, ClientTransport::HttpSse).await;
            consume_charged_prefix(started.session.as_mut()).await;
            if !fail {
                assert!(started.session.next_event().await.unwrap().is_some());
            }
            let mut terminal = started.session.next_event();
            assert!(futures::poll!(terminal.as_mut()).is_pending());
            assert_eq!(store.finalizes.load(Ordering::SeqCst), 1);
            assert_eq!(budget.settlements.load(Ordering::SeqCst), 0);
            assert!(observer.completed.lock().unwrap().is_empty());
            drop(terminal);
            release_write.send(()).unwrap();
            let mut terminal = started.session.next_event();
            assert!(futures::poll!(terminal.as_mut()).is_pending());
            assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 0);
            assert!(observer.completed.lock().unwrap().is_empty());
            drop(terminal);
            release_settle.send(()).unwrap();
            let mut terminal = started.session.next_event();
            assert!(futures::poll!(terminal.as_mut()).is_pending());
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
            assert!(observer.completed.lock().unwrap().is_empty());
            drop(terminal);
            release_slot.send(()).unwrap();
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
            assert_eq!(store.finalizes.load(Ordering::SeqCst), 1);
            let events = observer.completed.lock().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].cost(),
                &store.finalizations.lock().unwrap()[0].cost
            );
            assert_eq!(
                events[0].outcome(),
                if fail {
                    RequestObservationOutcome::Incomplete
                } else {
                    RequestObservationOutcome::Succeeded
                }
            );
        }
    });
}

#[test]
fn pre_provider_rejections_have_no_fictitious_attempt_provider_or_zero_cost() {
    block_on(async {
        for cause in ["model", "budget", "cancel", "key"] {
            let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                reject: cause == "budget",
                active: admissions.active.clone(),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let observer = Arc::new(Observer::new(admissions.clone()));
            let service = DefaultExecutionService::new(
                snapshots.clone(),
                store.clone(),
                ProviderRegistry::default(),
                admissions.clone(),
                Arc::new(UnusedContinuation),
                Arc::new(RecordingClientApiKeyUsage::default()),
            )
            .with_budget(budget.clone());
            let mut input = request(&service, ClientTransport::HttpJson);
            let prepared = service
                .prepare_execution(input.client)
                .unwrap()
                .with_observer_plan(observer.clone());
            let id = prepared.request_id().clone();
            match cause {
                "model" => input.public_model = PublicModelId::new("absent-model").unwrap(),
                "cancel" => prepared.cancellation().cancel(),
                "key" => snapshots.publish(start_snapshot_with_policy(
                    2,
                    false,
                    RateLimits::unlimited(),
                )),
                _ => {}
            }
            assert!(
                service
                    .start_prepared(
                        prepared,
                        PreparedExecutionRequest {
                            public_model: input.public_model,
                            operation: input.operation,
                            metadata: input.metadata
                        }
                    )
                    .await
                    .is_err()
            );
            let events = observer.completed.lock().unwrap();
            assert_eq!(events.len(), 1);
            let event = &events[0];
            assert_eq!(event.request_id(), &id);
            assert_eq!(event.config_revision(), ConfigRevision::new(1).unwrap());
            assert_eq!(event.outcome(), RequestObservationOutcome::Rejected);
            assert_eq!(event.send_state(), UpstreamSendState::NotSent);
            assert_eq!(event.attempt_count(), 0);
            assert!(event.provider().is_none());
            assert!(event.account_id().is_none());
            assert!(event.upstream_model().is_none());
            assert!(event.client_status_code().is_none());
            assert_eq!(
                event.cost().status(),
                gateway_core::metering::CostEstimateStatus::Unknown
            );
            assert!(event.cost().total().is_none());
            assert!(event.error_kind().is_some());
            assert!(store.attempts.lock().unwrap().is_empty());
            assert!(store.finalizations.lock().unwrap().is_empty());
            assert_eq!(budget.settlements.load(Ordering::SeqCst), 0);
        }
    });
}

#[test]
fn nested_calls_inherit_the_same_plan_and_real_ancestry_without_keeping_parent_alive() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let observer = Arc::new(Observer::new(admissions.clone()));
        let service = fixture(
            admissions.clone(),
            budget.clone(),
            store.clone(),
            Arc::new(ChargedProvider { fail: false }),
        );
        let parent = prepare(&service).with_observer_plan(observer.clone());
        let parent_authority = parent.nested_context();
        let child = service
            .prepare_nested(parent_authority.clone(), "observer-a".into(), None)
            .unwrap();
        let child_authority = child.nested_context();
        let grandchild = service
            .prepare_nested(child_authority.clone(), "observer-b".into(), None)
            .unwrap();
        let ids = [
            grandchild.request_id().clone(),
            child.request_id().clone(),
            parent.request_id().clone(),
        ];
        for prepared in [grandchild, child, parent] {
            let mut started = start(&service, prepared, ClientTransport::HttpJson).await;
            started.session.collect_uncommitted().await.unwrap();
            started.session.commit_downstream(Some(200)).await.unwrap();
            assert!(started.session.next_event().await.unwrap().is_none());
            started.session.detach_finalize().await;
        }
        assert_eq!(budget.charges.lock().unwrap().len(), 3);
        let events = observer.completed.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.request_id().clone())
                .collect::<Vec<_>>(),
            ids
        );
        assert!(events[0].suppresses_plugin("observer-a"));
        assert!(events[0].suppresses_plugin("observer-b"));
        assert!(events[1].suppresses_plugin("observer-a"));
        assert!(!events[1].suppresses_plugin("observer-b"));
        assert!(!events[2].suppresses_plugin("observer-a"));
        assert_eq!(
            events[0].extension_scope().parent_request_id(),
            Some(&ids[1])
        );
        assert_eq!(
            events[1].extension_scope().parent_request_id(),
            Some(&ids[2])
        );
        assert!(events[2].extension_scope().parent_request_id().is_none());
        assert_failure(
            service.prepare_nested(parent_authority, "observer-c".into(), None),
            GatewayErrorKind::Cancelled,
        );
        assert_failure(
            service.prepare_nested(child_authority, "observer-c".into(), None),
            GatewayErrorKind::Cancelled,
        );
    });
}

struct WireProvider {
    transport: &'static str,
    fail: bool,
}
#[async_trait]
impl Provider for WireProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(vec![])
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        _: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let candidate = request.candidate();
        let metadata = ProviderCallMetadata::new(
            candidate.provider().clone(),
            candidate.upstream_model().unwrap().clone(),
            ProviderAccountId::new("acct_start").unwrap(),
            UpstreamTransport::new(self.transport).unwrap(),
        );
        let response = ResponseMeta::new("resp_wire", "gpt-start");
        let mut first = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Started(response.clone())],
            wire("response.created"),
        );
        first.attach_observation(
            ProviderResponseObservation::new(UpstreamTransport::new(self.transport).unwrap())
                .with_status_code(200)
                .with_service_tier_if_valid("priority")
                .with_upstream_response_model_if_valid("actual-response-model"),
        );
        let terminal = if self.fail {
            Err(
                ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
                    .with_status(429)
                    .with_atomic_client_events(vec![ProviderEvent::wire(wire("response.failed"))]),
            )
        } else {
            Ok(ProviderEvent::canonical_with_wire(
                vec![GatewayEvent::Completed(response)],
                wire("response.completed"),
            ))
        };
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(vec![
                Ok(first),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(known_charge().scaled()).unwrap(),
                )
                .into()),
                terminal,
            ]),
            (),
        ))
    }
}
fn wire(event: &str) -> ProtocolWireEvent {
    ProtocolWireEvent::json(
        "openai",
        Some(event.into()),
        json!({"type":event,"original":"fixture-body"}),
    )
    .unwrap()
}

#[test]
fn websocket_events_follow_actual_upstream_transport_and_preserve_original_frames() {
    block_on(async {
        for upstream in ["http_sse", "websocket"] {
            for client in [ClientTransport::HttpJson, ClientTransport::WebSocket] {
                let admissions = Arc::new(Admissions::default());
                let budget = Arc::new(Budget {
                    active: admissions.active.clone(),
                    ..Default::default()
                });
                let store = Arc::new(TrackingExecutionStore::default());
                let observer = Arc::new(Observer::new(admissions.clone()));
                let service = fixture(
                    admissions.clone(),
                    budget.clone(),
                    store,
                    Arc::new(WireProvider {
                        transport: upstream,
                        fail: false,
                    }),
                );
                let prepared = prepare(&service).with_observer_plan(observer.clone());
                let mut started = start(&service, prepared, client).await;
                started.session.collect_uncommitted().await.unwrap();
                started.session.commit_downstream(Some(200)).await.unwrap();
                assert!(started.session.next_event().await.unwrap().is_none());
                assert_cleanup_completed(&admissions, &budget, &started.request_id);
                let frames = observer.websocket.lock().unwrap();
                assert_eq!(frames.len(), if upstream == "websocket" { 2 } else { 0 });
                for (index, frame) in frames.iter().enumerate() {
                    assert_eq!(frame.request_id(), &started.request_id);
                    assert_eq!(frame.client_key_id().as_str(), "key_start_test");
                    assert_eq!(frame.attempt_index().get(), 1);
                    assert_eq!(frame.sequence(), index as u64 + 1);
                    assert_eq!(
                        frame.event_id(),
                        format!("{}:websocket:1:{}", started.request_id.as_str(), index + 1)
                    );
                    assert_eq!(frame.wire().data()["original"], "fixture-body");
                    assert_eq!(
                        frame.wire().event_type(),
                        Some(if index == 0 {
                            "response.created"
                        } else {
                            "response.completed"
                        })
                    );
                }
                let completed = observer.completed.lock().unwrap();
                assert_eq!(completed.len(), 1);
                assert_eq!(completed[0].service_tier(), Some("priority"));
                assert_eq!(completed[0].response_model(), Some("actual-response-model"));
            }
        }
    });
}

#[test]
fn atomic_failure_frames_are_observed_once_and_observer_faults_preserve_native_results() {
    block_on(async {
        for panic in [false, true] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                active: admissions.active.clone(),
                ..Default::default()
            });
            let store = Arc::new(TrackingExecutionStore::default());
            let observer = Arc::new(Observer {
                panic,
                ..Observer::new(admissions.clone())
            });
            let service = fixture(
                admissions.clone(),
                budget.clone(),
                store.clone(),
                Arc::new(WireProvider {
                    transport: "websocket",
                    fail: true,
                }),
            );
            let prepared = prepare(&service).with_observer_plan(observer.clone());
            let mut started = start(&service, prepared, ClientTransport::WebSocket).await;
            consume_charged_prefix(started.session.as_mut()).await;
            while let Ok(Some(_)) = started.session.next_event().await {}
            started.session.detach_finalize().await;
            assert_cleanup_completed(&admissions, &budget, &started.request_id);
            let frames = observer.websocket.lock().unwrap();
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0].sequence(), 1);
            assert_eq!(frames[1].sequence(), 2);
            assert_eq!(frames[1].wire().event_type(), Some("response.failed"));
            let completed = observer.completed.lock().unwrap();
            assert_eq!(completed.len(), 1);
            assert_eq!(
                completed[0].outcome(),
                RequestObservationOutcome::Incomplete
            );
            assert_eq!(completed[0].upstream_status_code(), Some(429));
            assert_eq!(
                completed[0].cost(),
                &store.finalizations.lock().unwrap()[0].cost
            );
        }
    });
}

struct RewriteAttempt;
impl gateway_core::engine::attempt_middleware::AttemptMiddleware for RewriteAttempt {
    fn invoke(
        &self,
        request: ProviderRequest,
        _: AttemptContext,
        next: gateway_core::engine::attempt_middleware::AttemptNext,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::attempt_middleware::AttemptOutput,
            gateway_core::engine::middleware::MiddlewareError,
        >,
    > {
        use futures::StreamExt;
        Box::pin(async move {
            let source = next.run(request.operation().clone()).await?;
            let events = source.into_events().map(|result| {
                result.map(|event| {
                    if event.wire_event().is_some() {
                        let changed = ProtocolWireEvent::json(
                            "openai",
                            event.wire_event().unwrap().event_type().map(str::to_owned),
                            json!({"original":"rewritten-output"}),
                        )
                        .unwrap();
                        event.with_wire_projection(Some(changed))
                    } else {
                        GatewayEvent::ProviderCost(
                            ProviderReportedCost::from_usd_ticks(known_charge().scaled() * 3)
                                .unwrap(),
                        )
                        .into()
                    }
                })
            });
            Ok(
                (Box::pin(events) as gateway_core::engine::attempt_middleware::AttemptEvents)
                    .into(),
            )
        })
    }
}

#[test]
fn attempt_rewrites_cannot_change_observed_original_websocket_frames_or_cost() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let observer = Arc::new(Observer::new(admissions.clone()));
        let service = fixture(
            admissions.clone(),
            budget.clone(),
            store.clone(),
            Arc::new(WireProvider {
                transport: "websocket",
                fail: false,
            }),
        );
        let prepared = prepare(&service)
            .with_observer_plan(observer.clone())
            .with_attempt_middleware(Arc::new(RewriteAttempt));
        let mut started = start(&service, prepared, ClientTransport::HttpJson).await;
        let output = started.session.collect_uncommitted().await.unwrap();
        assert_eq!(
            output[0].wire_event().unwrap().data()["original"],
            "rewritten-output"
        );
        assert!(output[1].canonical_facts().iter().any(|fact|matches!(fact,GatewayEvent::ProviderCost(cost) if cost.into_estimate().total().unwrap().amount()!=known_charge())));
        started.session.commit_downstream(Some(200)).await.unwrap();
        assert!(started.session.next_event().await.unwrap().is_none());
        assert_cleanup_completed(&admissions, &budget, &started.request_id);
        let frames = observer.websocket.lock().unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            frames
                .iter()
                .all(|frame| frame.wire().data()["original"] == "fixture-body")
        );
        let completed = observer.completed.lock().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].cost(),
            &store.finalizations.lock().unwrap()[0].cost
        );
        assert_eq!(
            completed[0].cost().total().unwrap().amount(),
            known_charge()
        );
    });
}

#[derive(Default)]
struct RetryProvider {
    calls: AtomicUsize,
    exhausted: bool,
}
#[async_trait]
impl Provider for RetryProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(vec![])
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call > 0 && self.exhausted {
            return Err(ProviderError::new(
                ProviderErrorKind::NoEligibleAccount,
                UpstreamSendState::NotSent,
            ));
        }
        let account = ProviderAccountId::new(if call == 0 {
            "acct_start"
        } else {
            "acct_second"
        })
        .unwrap();
        assert!(!context.excluded_accounts().contains(&account));
        let candidate = request.candidate();
        let metadata = ProviderCallMetadata::new(
            candidate.provider().clone(),
            candidate.upstream_model().unwrap().clone(),
            account,
            UpstreamTransport::new("websocket").unwrap(),
        );
        let events = if call == 0 {
            vec![Err(ProviderError::new(
                ProviderErrorKind::RateLimited,
                UpstreamSendState::Sent,
            )
            .with_status(429)
            .with_replay_safe()
            .with_pre_delivery_retry()
            .with_atomic_client_events(vec![
                ProviderEvent::wire(wire("response.created")),
                ProviderEvent::wire(wire("response.failed")),
            ]))]
        } else {
            let response = ResponseMeta::new("resp_retry", "gpt-start");
            vec![
                Ok(ProviderEvent::canonical_with_wire(
                    vec![GatewayEvent::Started(response.clone())],
                    wire("response.created"),
                )),
                Ok(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(known_charge().scaled()).unwrap(),
                )
                .into()),
                Ok(ProviderEvent::canonical_with_wire(
                    vec![GatewayEvent::Completed(response)],
                    wire("response.completed"),
                )),
            ]
        };
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(events),
            (),
        ))
    }
}

#[test]
fn discarded_retry_failure_frames_keep_a_single_sequence_and_final_actual_account() {
    block_on(async {
        let provider = ProviderKind::new("openai").unwrap();
        let directory = Arc::new(RuntimeAccountDirectory::new(
            ["acct_start", "acct_second"]
                .into_iter()
                .map(|id| {
                    (
                        ProviderAccountId::new(id).unwrap(),
                        RuntimeAccount::new(provider.clone(), BTreeSet::new()),
                    )
                })
                .collect(),
        ));
        let snapshot = start_snapshot_with_clients(
            1,
            vec![ClientPolicy::new(
                ClientApiKeyId::new("key_start_test").unwrap(),
                PlaintextClientApiKey::new("sk_start_test").unwrap(),
                Arc::new(FrozenAccountScope::new(
                    directory,
                    ClientRoutingScope::all_accounts(),
                )),
                true,
                RateLimits::unlimited(),
            )],
        );
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let provider = Arc::new(RetryProvider::default());
        let observer = Arc::new(Observer::new(admissions.clone()));
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(snapshot),
            store.clone(),
            ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        )
        .with_budget(budget.clone());
        let prepared = prepare(&service).with_observer_plan(observer.clone());
        let mut started = start(&service, prepared, ClientTransport::HttpJson).await;
        let output = started.session.collect_uncommitted().await.unwrap();
        assert_eq!(output.len(), 3);
        assert!(output.iter().all(|event| {
            event
                .wire_event()
                .is_none_or(|wire| wire.event_type() != Some("response.failed"))
        }));
        started.session.commit_downstream(Some(200)).await.unwrap();
        assert!(started.session.next_event().await.unwrap().is_none());
        assert_cleanup_completed(&admissions, &budget, &started.request_id);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        let frames = observer.websocket.lock().unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(
            frames
                .iter()
                .map(WebSocketResponseObservation::sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            frames
                .iter()
                .map(|event| event.attempt_index().get())
                .collect::<Vec<_>>(),
            vec![1, 1, 2, 2]
        );
        assert_eq!(frames[1].wire().event_type(), Some("response.failed"));
        assert_eq!(frames[3].account_id().as_str(), "acct_second");
        let completed = observer.completed.lock().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].attempt_count(), 2);
        assert_eq!(completed[0].account_id().unwrap().as_str(), "acct_second");
        assert_eq!(
            completed[0].cost(),
            &store.finalizations.lock().unwrap()[0].cost
        );
    });
}

#[test]
fn selection_failure_after_retry_keeps_the_last_real_account_without_reobserving_saved_frames() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let observer = Arc::new(Observer::new(admissions.clone()));
        let provider = Arc::new(RetryProvider {
            exhausted: true,
            ..Default::default()
        });
        let service = fixture(
            admissions.clone(),
            budget.clone(),
            store.clone(),
            provider.clone(),
        );
        let prepared = prepare(&service).with_observer_plan(observer.clone());
        let mut started = start(&service, prepared, ClientTransport::HttpJson).await;
        assert!(
            matches!(started.session.collect_uncommitted().await,Err(gateway_core::engine::EngineError::Provider(error)) if error.kind()==ProviderErrorKind::RateLimited)
        );
        let _ = started.session.next_event().await;
        started.session.detach_finalize().await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(observer.websocket.lock().unwrap().len(), 2);
        let events = observer.completed.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attempt_count(), 1);
        assert_eq!(events[0].provider().unwrap().as_str(), "openai");
        assert_eq!(events[0].account_id().unwrap().as_str(), "acct_start");
        assert_eq!(events[0].upstream_model().unwrap().as_str(), "gpt-start");
        assert_eq!(events[0].upstream_status_code(), Some(429));
        assert_eq!(
            events[0].cost(),
            &store.finalizations.lock().unwrap()[0].cost
        );
        assert_eq!(
            events[0].cost().status(),
            gateway_core::metering::CostEstimateStatus::Unknown
        );
        assert!(events[0].cost().total().is_none());
        assert!(!admissions.active.load(Ordering::SeqCst));
        assert_eq!(budget.settlements.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn a_later_core_publication_cannot_replace_the_requests_frozen_plan_or_revision() {
    block_on(async {
        let snapshots = RuntimeSnapshotHandle::new(start_snapshot());
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let original = Arc::new(Observer::new(admissions.clone()));
        let later = Arc::new(Observer::new(admissions.clone()));
        let service = DefaultExecutionService::new(
            snapshots.clone(),
            Arc::new(TrackingExecutionStore::default()),
            ProviderRegistry::new([Arc::new(ChargedProvider { fail: false }) as Arc<dyn Provider>])
                .unwrap(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(RecordingClientApiKeyUsage::default()),
        )
        .with_budget(budget.clone());
        let original_request = prepare(&service).with_observer_plan(original.clone());
        snapshots.publish(start_snapshot_with_policy(2, true, RateLimits::unlimited()));
        let later_request = prepare(&service).with_observer_plan(later.clone());
        for prepared in [original_request, later_request] {
            let mut started = start(&service, prepared, ClientTransport::HttpJson).await;
            started.session.collect_uncommitted().await.unwrap();
            started.session.commit_downstream(Some(200)).await.unwrap();
            assert!(started.session.next_event().await.unwrap().is_none());
        }
        assert_eq!(original.completed.lock().unwrap().len(), 1);
        assert_eq!(later.completed.lock().unwrap().len(), 1);
        assert_eq!(
            original.completed.lock().unwrap()[0].config_revision(),
            ConfigRevision::new(1).unwrap()
        );
        assert_eq!(
            later.completed.lock().unwrap()[0].config_revision(),
            ConfigRevision::new(2).unwrap()
        );
        assert_eq!(budget.charges.lock().unwrap().len(), 2);
    });
}

#[test]
fn execution_cancellation_dispatches_original_partial_cost_after_native_cleanup() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let budget = Arc::new(Budget {
            active: admissions.active.clone(),
            ..Default::default()
        });
        let store = Arc::new(TrackingExecutionStore::default());
        let observer = Arc::new(Observer::new(admissions.clone()));
        let service = fixture(
            admissions.clone(),
            budget.clone(),
            store.clone(),
            Arc::new(ChargedProvider { fail: false }),
        );
        let prepared = prepare(&service).with_observer_plan(observer.clone());
        let mut started = start(&service, prepared, ClientTransport::HttpSse).await;
        consume_charged_prefix(started.session.as_mut()).await;
        started.session.cancel();
        assert!(matches!(
            started.session.next_event().await,
            Err(gateway_core::engine::EngineError::Cancelled)
        ));
        started.session.detach_finalize().await;
        assert_cleanup_completed(&admissions, &budget, &started.request_id);
        let events = observer.completed.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome(), RequestObservationOutcome::Cancelled);
        assert_eq!(events[0].attempt_count(), 1);
        assert_eq!(events[0].send_state(), UpstreamSendState::Sent);
        assert_eq!(
            events[0].cost(),
            &store.finalizations.lock().unwrap()[0].cost
        );
        assert_eq!(events[0].cost().total().unwrap().amount(), known_charge());
    });
}
