use super::maintenance::{configure, context, worker};
use super::*;
use futures::future::BoxFuture;
use gateway_core::{
    account::{AccountConcurrencyLimit, AccountWeight},
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeasePort, ProviderLeaseRequest,
        ProviderSchedulingLeaseRequest, ProviderSchedulingState, ProviderStoreError,
        ProviderStoreErrorKind, ProviderStorePorts,
    },
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[derive(Default)]
struct SchedulingLeases {
    requests: Mutex<Vec<ProviderSchedulingLeaseRequest>>,
    business_in_flight: AtomicU32,
    unavailable: AtomicBool,
}

impl ProviderLeasePort for SchedulingLeases {
    fn load_state<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ProviderKind,
        _: &'a [ProviderAccountId],
    ) -> BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>> {
        Box::pin(async { panic!("maintenance must not select a different account") })
    }

    fn try_acquire(
        &self,
        request: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async move {
            let ProviderLeaseRequest::Scheduling(request) = request else {
                panic!("scheduling lease")
            };
            let limit = request.max_concurrent().get();
            self.requests.lock().unwrap().push(request);
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Unavailable,
                    "test scheduling",
                ));
            }
            if self.business_in_flight.load(Ordering::SeqCst) >= limit {
                return Ok(ProviderLeaseAcquisition::Busy {
                    retry_after: Some(Duration::from_millis(25)),
                });
            }
            Ok(ProviderLeaseAcquisition::Acquired(Box::new(())))
        })
    }
}

async fn scheduled_bundle(
    config: &OpenAiConfig,
    accounts: &Arc<MemoryAccountStore>,
    store: &Arc<StateStore>,
    leases: Arc<SchedulingLeases>,
) -> provider_openai::ProviderBundle {
    let ports = provider_ports_with(accounts.clone(), Arc::new(TestOAuthPending::default()));
    let ports = ProviderStorePorts::new(
        ports.accounts(),
        leases,
        ports.session_affinity(),
        ports.session_exclusions(),
        ports.catalog_cache(),
        ports.artifact_profiles(),
        ports.credential_state(),
        ports.cooldowns(),
        ports.runtime_policy(),
        ports.oauth_pending(),
    );
    provider_openai::initialize_with_turn_state(config.clone(), ports, store.clone())
        .await
        .unwrap()
}

#[tokio::test]
async fn maintenance_shares_effective_account_capacity_not_a_hardcoded_one() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 103))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    for (configured, in_flight, allowed) in [
        (Some(10), 1, true),
        (Some(10), 10, false),
        (None, 1, true),
        (Some(1), 1, false),
    ] {
        let accounts = accounts().await;
        accounts.set_scheduling(
            "acct_state",
            configured.map(|limit| AccountConcurrencyLimit::new(limit).unwrap()),
            AccountWeight::default(),
        );
        let store = Arc::new(StateStore::default());
        let leases = Arc::new(SchedulingLeases::default());
        leases.business_in_flight.store(in_flight, Ordering::SeqCst);
        let mut bundle = scheduled_bundle(&config.config, &accounts, &store, leases.clone()).await;
        let service = bundle.turn_state_service().unwrap();
        configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
        worker(&mut bundle)
            .run_cycle(context(CancellationToken::new()))
            .await
            .unwrap();
        let view = service.view().await.unwrap();
        assert_eq!(view.targets[0].candidate_count, usize::from(allowed));
        assert_eq!(view.targets[0].hourly_used, if allowed { 2 } else { 0 });
        for request in leases.requests.lock().unwrap().iter() {
            assert_eq!(request.max_concurrent().get(), configured.unwrap_or(4));
            assert_eq!(request.request_interval(), Duration::ZERO);
        }
        if !allowed {
            assert_eq!(view.targets[0].wait_reason, "account_busy");
            assert!(view.targets[0].last_message.contains("25 毫秒"));
        }
    }
}

#[tokio::test]
async fn scheduling_storage_failure_is_not_reported_as_a_busy_account() {
    let server = MockServer::start().await;
    let config = valid_config();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let leases = Arc::new(SchedulingLeases::default());
    leases.unavailable.store(true, Ordering::SeqCst);
    let mut bundle = scheduled_bundle(&config.config, &accounts, &store, leases).await;
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    worker(&mut bundle)
        .run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let view = service.view().await.unwrap();
    assert_eq!(view.targets[0].wait_reason, "storage_unavailable");
    assert_eq!(view.targets[0].hourly_used, 0);
    assert!(store.probes.lock().unwrap().is_empty());
}

fn token_at(blocks: usize, at: i64) -> String {
    let mut bytes = vec![117; 57 + 16 * blocks];
    bytes[0] = 0x80;
    bytes[1..9].copy_from_slice(&(at as u64).to_be_bytes());
    base64::engine::general_purpose::URL_SAFE.encode(bytes)
}

#[tokio::test]
async fn rejected_candidates_have_precise_diagnostics_and_usage_audits_without_raw_tokens() {
    let now = Utc::now().timestamp();
    for (returned, reason, detail) in [
        (None, "state_missing", "未返回"),
        (Some("not-a-state".to_owned()), "state_structure", "11 字符"),
        (
            Some(token(11, 112)),
            "state_shape_mismatch",
            "312 字符 / 11 块",
        ),
        (
            Some(token_at(10, now - 3000)),
            "state_ttl",
            "至少需要 900 秒",
        ),
        (
            Some(token_at(10, now + 3600)),
            "state_future",
            "签发时间超前",
        ),
    ] {
        let server = MockServer::start().await;
        let mut response =
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream");
        if let Some(token) = &returned {
            response = response.insert_header("x-codex-turn-state", token.as_str());
        }
        Mock::given(method("POST"))
            .respond_with(response)
            .mount(&server)
            .await;
        let mut config = valid_config();
        config.config.api.base_url = server.uri();
        let accounts = accounts().await;
        let store = Arc::new(StateStore::default());
        let mut bundle = bundle(&config.config, &accounts, &store).await;
        let service = bundle.turn_state_service().unwrap();
        configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
        worker(&mut bundle)
            .run_cycle(context(CancellationToken::new()))
            .await
            .unwrap();
        let view = service.view().await.unwrap();
        let target = &view.targets[0];
        assert_eq!(target.wait_reason, reason, "{view:?}");
        assert_eq!(target.candidate_count, 0);
        assert_eq!(target.hourly_used, 1);
        assert!(target.last_message.contains(detail), "{view:?}");
        let entry = target.history.last().unwrap();
        assert_eq!(entry.direction, "rejected");
        assert_eq!(entry.message, target.last_message);
        if let Some(token) = returned {
            assert!(!serde_json::to_string(&view).unwrap().contains(&token));
        }
        {
            let probes = store.probes.lock().unwrap();
            assert_eq!(probes[1]["succeeded"], true);
            assert_eq!(probes[1]["input"], 1);
            assert_eq!(probes[1]["message"], target.last_message);
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn duplicate_state_is_diagnosed_and_each_account_only_has_one_maintenance_job_per_cycle() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 120))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let mut bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4", "gpt-5.5"], 10).await;
    let view = service.view().await.unwrap();
    service
        .configure(
            TurnStateSettingsInput {
                revision: view.revision,
                policy: TurnStatePolicy {
                    concurrency: 4,
                    ..view.policy
                },
                pools: vec![TurnStatePoolInput {
                    id: "test-pool".into(),
                    name: "test".into(),
                    enabled: true,
                    mode: "fixed".into(),
                    endpoint: None,
                    bearer: None,
                    json_pointer: String::new(),
                }],
                targets: view.targets.into_iter().map(|t| t.target).collect(),
            },
            &mutation(),
        )
        .await
        .unwrap();
    let task = worker(&mut bundle);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(
        service
            .view()
            .await
            .unwrap()
            .targets
            .iter()
            .filter(|t| t.candidate_count > 0)
            .count(),
        1
    );
    // Let the second model fill, then manually refresh the first: identical token is not verified again.
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    service
        .action("acct_state", "gpt-5.4", "refresh", &mutation())
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let view = service.view().await.unwrap();
    assert_eq!(view.targets[0].wait_reason, "fresh");
    assert!(view.targets[0].last_message.contains("重复 State"));
    assert_eq!(server.received_requests().await.unwrap().len(), 5);
}

#[tokio::test]
async fn business_activity_survives_a_busy_runtime_lock_and_another_instances_commit() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let mut first = bundle(&config.config, &accounts, &store).await;
    let service = first.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    let second = bundle(&config.config, &accounts, &store).await;
    let other = second.turn_state_service().unwrap();
    store.block_next_load.store(true, Ordering::SeqCst);
    let held_view = tokio::spawn({
        let service = service.clone();
        async move { service.view().await.unwrap() }
    });
    store.load_started.notified().await;
    // On this current-thread runtime a first poll reaches guard_request's blocked
    // runtime lock, after note_traffic, without any upstream I/O.
    {
        let business = execute(&first, operation(None, false), "acct_state");
        tokio::pin!(business);
        assert!(futures::poll!(business.as_mut()).is_pending());
        store.release_load.notify_one();
        held_view.await.unwrap();
        business.await;
    }
    let at = service.view().await.unwrap().targets[0].last_traffic_at;
    assert!(at.is_some(), "runtime contention must not discard activity");
    // The second instance has no local activity; its edit must win for settings
    // without erasing the first instance's pending timestamp.
    other
        .action("acct_state", "gpt-5.4", "pause", &mutation())
        .await
        .unwrap();
    let reloaded = service.view().await.unwrap();
    assert!(!reloaded.targets[0].target.enabled);
    assert_eq!(reloaded.targets[0].last_traffic_at, at);
    worker(&mut first)
        .run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let restarted = bundle(&config.config, &accounts, &store).await;
    let persisted = restarted
        .turn_state_service()
        .unwrap()
        .view()
        .await
        .unwrap();
    assert_eq!(persisted.targets[0].last_traffic_at, at);
    assert!(!persisted.targets[0].target.enabled);
}

#[tokio::test]
async fn repeated_traffic_keeps_uncommitted_discovery_across_a_storage_revision_change() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &[], 10).await;
    service
        .configure_account(
            "acct_state",
            TurnStateAccountUpdate {
                revision: service.view().await.unwrap().revision,
                fingerprint_convergence: true,
                takeover: true,
                maintenance: Some(TurnStateMaintenance {
                    auto_models: true,
                    pool_id: Some("test-pool".into()),
                    ..Default::default()
                }),
            },
            &mutation(),
        )
        .await
        .unwrap();
    let (revision, sealed) = store.load().await.unwrap();
    execute(&bundle, operation(None, false), "acct_state").await;
    execute(&bundle, operation(None, false), "acct_state").await;
    // Equivalent to another instance advancing a budget/worker CAS without a
    // configuration edit. It cannot know about the uncommitted local discovery.
    store.save(revision, sealed.unwrap(), None).await.unwrap();
    let view = service.view().await.unwrap();
    assert_eq!(view.targets.len(), 1);
    assert!(view.targets[0].automatic);
    assert!(view.targets[0].last_traffic_at.is_some());
}

#[tokio::test]
async fn pending_activity_does_not_restore_an_automatic_target_removed_by_another_instance() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let first = bundle(&config.config, &accounts, &store).await;
    let service = first.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &[], 10).await;
    service
        .configure_account(
            "acct_state",
            TurnStateAccountUpdate {
                revision: service.view().await.unwrap().revision,
                fingerprint_convergence: true,
                takeover: true,
                maintenance: Some(TurnStateMaintenance {
                    auto_models: true,
                    pool_id: Some("test-pool".into()),
                    ..Default::default()
                }),
            },
            &mutation(),
        )
        .await
        .unwrap();
    execute(&first, operation(None, false), "acct_state").await;
    // Persist the discovery, then queue another uncommitted activity event.
    set_account(service.as_ref(), true, true).await;
    execute(&first, operation(None, false), "acct_state").await;
    let second = bundle(&config.config, &accounts, &store).await;
    let other = second.turn_state_service().unwrap();
    let view = other.view().await.unwrap();
    assert_eq!(view.targets.len(), 1);
    other
        .configure(
            TurnStateSettingsInput {
                revision: view.revision,
                policy: view.policy,
                targets: vec![],
                pools: vec![TurnStatePoolInput {
                    id: "test-pool".into(),
                    name: "test".into(),
                    enabled: true,
                    mode: "fixed".into(),
                    endpoint: None,
                    bearer: None,
                    json_pointer: String::new(),
                }],
            },
            &mutation(),
        )
        .await
        .unwrap();
    assert!(service.view().await.unwrap().targets.is_empty());
    set_account(service.as_ref(), true, false).await;
    execute(&first, operation(None, false), "acct_state").await;
    assert!(service.view().await.unwrap().targets.is_empty());
}
