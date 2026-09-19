use super::*;
use gateway_admin::ports::turn_state::TurnStateService;
use gateway_core::task::{
    ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind, WorkerRunnable,
};

fn worker(bundle: &mut provider_openai::ProviderBundle) -> Box<dyn ScheduledTask> {
    bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|contribution| {
            if let WorkerContribution::Registration(registration) = contribution
                && registration.id.owner() == "openai-turn-state"
                && let WorkerRunnable::Scheduled { task, .. } = registration.runnable
            {
                Some(task)
            } else {
                None
            }
        })
        .unwrap()
}

fn context(cancellation: CancellationToken) -> WorkerCycleContext {
    WorkerCycleContext::new(
        WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "openai-turn-state").unwrap(),
        None,
        cancellation,
    )
}

async fn configure(service: &dyn TurnStateService, proxy: &str, models: &[&str], limit: u32) {
    service
        .configure(
            TurnStateSettingsInput {
                revision: service.view().await.unwrap().revision,
                policy: TurnStatePolicy {
                    enabled: true,
                    concurrency: 1,
                    ..Default::default()
                },
                pools: vec![TurnStatePoolInput {
                    id: "test-pool".to_owned(),
                    name: "test".to_owned(),
                    enabled: true,
                    mode: "fixed".to_owned(),
                    endpoint: Some(proxy.to_owned()),
                    bearer: None,
                    json_pointer: String::new(),
                }],
                targets: models
                    .iter()
                    .map(|model| TurnStateTargetInput {
                        account_id: "acct_state".to_owned(),
                        model: (*model).to_owned(),
                        pool_id: "test-pool".to_owned(),
                        enabled: true,
                    })
                    .collect(),
            },
            &mutation(),
        )
        .await
        .unwrap();
    service
        .configure_account(
            "acct_state",
            TurnStateAccountUpdate {
                revision: service.view().await.unwrap().revision,
                fingerprint_convergence: true,
                takeover: true,
                maintenance: Some(TurnStateMaintenance {
                    max_probes_per_hour: limit,
                    idle_seconds: 0,
                    ..Default::default()
                }),
            },
            &mutation(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn fixed_probe_counts_both_phases_and_records_real_usage_without_recursive_traffic() {
    let server = MockServer::start().await;
    let candidate = token(10, 101);
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", candidate.as_str())
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
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 2).await;
    let task = worker(&mut bundle);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let view = service.view().await.unwrap();
    assert_eq!(view.targets[0].candidate_count, 1, "{view:?}");
    assert_eq!(view.targets[0].hourly_used, 2);
    assert_eq!(view.targets[0].last_traffic_at, None);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].headers.get("x-codex-turn-state").is_none());
    assert_eq!(requests[1].headers["x-codex-turn-state"], candidate);
    {
        let probes = store.probes.lock().unwrap();
        assert_eq!(probes.len(), 4);
        assert_eq!(probes[0]["phase"], "collect");
        assert_eq!(probes[2]["phase"], "verify");
        assert_eq!(probes[3]["input"], 1);
        assert_eq!(probes[3]["sent"], candidate);
    }
    service
        .action("acct_state", "gpt-5.4", "revoke", &mutation())
        .await
        .unwrap();
    service
        .action("acct_state", "gpt-5.4", "resume", &mutation())
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(
        service.view().await.unwrap().targets[0].wait_reason,
        "budget"
    );
}

#[tokio::test]
async fn fair_cursor_and_hourly_budget_survive_settings_refresh_and_restart() {
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
    configure(
        service.as_ref(),
        &server.uri(),
        &["gpt-5.4", "gpt-5.5", "gpt-5.6"],
        2,
    )
    .await;
    worker(&mut first)
        .run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    service
        .action("acct_state", "gpt-5.4", "refresh", &mutation())
        .await
        .unwrap();
    let mut second = bundle(&config.config, &accounts, &store).await;
    let second_service = second.turn_state_service().unwrap();
    let task = worker(&mut second);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let view = second_service.view().await.unwrap();
    assert!(
        view.targets
            .iter()
            .all(|t| t.hourly_used == 2 && t.wait_reason == "budget")
    );
    {
        let probes = store.probes.lock().unwrap();
        assert_eq!(probes[0]["model"], "gpt-5.4");
        assert_eq!(probes[2]["model"], "gpt-5.5");
    }
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn upstream_429_blocks_all_models_and_manual_refresh_without_rotation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "3600")
                .set_body_json(
                    json!({"error":{"message":"rate limited","code":"rate_limit_exceeded"}}),
                ),
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
    let task = worker(&mut bundle);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4", "gpt-5.5"], 10).await;
    service
        .action("acct_state", "gpt-5.5", "refresh", &mutation())
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    let view = service.view().await.unwrap();
    assert!(
        view.targets
            .iter()
            .all(|t| t.wait_reason == "upstream_cooldown"),
        "{view:?}"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert!(accounts.account("acct_state").unwrap().enabled());
}

#[tokio::test]
async fn cancellation_finishes_audit_and_never_publishes_partial_candidate() {
    let server = MockServer::start().await;
    let sent = Arc::new(tokio::sync::Notify::new());
    let signal = sent.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 102))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream")
                .set_delay(Duration::from_secs(20))
        })
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let mut bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    let task = worker(&mut bundle);
    let cancellation = CancellationToken::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(task.run_cycle(context(cancellation.clone())), async {
            sent.notified().await;
            cancellation.cancel();
        })
        .0
        .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 0);
    let probes = store.probes.lock().unwrap();
    assert_eq!(probes.len(), 2);
    assert_eq!(probes[1]["succeeded"], false);
}

#[tokio::test]
async fn auto_discovery_uses_real_business_without_requiring_a_state_and_rejects_stale_forms() {
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
    configure(service.as_ref(), &server.uri(), &[], 2).await;
    let before = service
        .configure_account(
            "acct_state",
            TurnStateAccountUpdate {
                revision: service.view().await.unwrap().revision,
                fingerprint_convergence: true,
                takeover: true,
                maintenance: Some(TurnStateMaintenance {
                    max_probes_per_hour: 2,
                    auto_models: true,
                    pool_id: Some("test-pool".to_owned()),
                    ..Default::default()
                }),
            },
            &mutation(),
        )
        .await
        .unwrap();
    execute(&bundle, operation(None, false), "acct_state").await;
    let view = service.view().await.unwrap();
    assert_eq!(view.targets.len(), 1);
    assert_eq!(view.targets[0].target.model, "gpt-5.4");
    assert!(view.targets[0].automatic);
    assert!(view.targets[0].last_traffic_at.is_some());
    assert_eq!(view.targets[0].candidate_count, 0);
    assert_eq!(view.targets[0].hourly_used, 0);
    assert!(store.probes.lock().unwrap().is_empty());
    assert!(
        service
            .configure_account(
                "acct_state",
                TurnStateAccountUpdate {
                    revision: before.revision,
                    fingerprint_convergence: false,
                    takeover: true,
                    maintenance: None,
                },
                &mutation()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn verification_requires_completed_sse_not_just_a_200_header_and_candidate() {
    let server = MockServer::start().await;
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST")).respond_with(move |_: &wiremock::Request| {
        let collect = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        ResponseTemplate::new(200).insert_header("x-codex-turn-state", token(10, 119))
            .set_body_raw(if collect { COMPLETED_SESSION_SSE } else {
                "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"usage\":{\"input_tokens\":4}}}\n\n"
            }, "text/event-stream")
    }).mount(&server).await;
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
    assert_eq!(view.targets[0].candidate_count, 0);
    assert!(!view.targets[0].target.enabled);
    assert!(accounts.account("acct_state").unwrap().enabled());
    let probes = store.probes.lock().unwrap();
    assert_eq!(probes[3]["succeeded"], false);
    assert_eq!(probes[3]["input"], 4);
}

#[tokio::test]
async fn stream_rate_limit_uses_account_cooldown_even_with_http_200() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200).set_body_raw(
        "data: {\"type\":\"error\",\"status\":429,\"error\":{\"code\":\"rate_limit_exceeded\",\"retry_after_seconds\":3600}}\n\n",
        "text/event-stream"
    )).mount(&server).await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let mut bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4", "gpt-5.5"], 10).await;
    let task = worker(&mut bundle);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert!(
        service
            .view()
            .await
            .unwrap()
            .targets
            .iter()
            .all(|t| t.wait_reason == "upstream_cooldown")
    );
}
