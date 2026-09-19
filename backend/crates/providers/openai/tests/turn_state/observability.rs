use super::maintenance::{configure, context, worker};
use super::*;
use gateway_admin::{
    model::proxies::{ProxyQualityReport, ProxyTestResult},
    ports::proxy::ProxyProbe,
};
use gateway_core::account::OutboundProxy;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Detector(AtomicUsize);

#[async_trait]
impl ProxyProbe for Detector {
    async fn test(&self, _: &OutboundProxy) -> ProxyTestResult {
        unreachable!()
    }
    async fn quality(&self, _: &OutboundProxy) -> ProxyQualityReport {
        unreachable!()
    }
    async fn test_egress(&self, proxy: Option<&OutboundProxy>) -> Option<ProxyTestResult> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        Some(ProxyTestResult {
            success: true,
            latency_ms: 1,
            exit_ip: Some(
                if proxy.is_some() {
                    format!("203.0.113.{attempt}")
                } else {
                    "198.51.100.1".to_owned()
                }
                .parse()
                .unwrap(),
            ),
            location: None,
            message: "test".to_owned(),
        })
    }
}

async fn set_pool_mode(
    service: &dyn gateway_admin::ports::turn_state::TurnStateService,
    mode: &str,
) {
    let view = service.view().await.unwrap();
    service
        .configure(
            TurnStateSettingsInput {
                revision: view.revision,
                policy: view.policy,
                pools: vec![TurnStatePoolInput {
                    id: "test-pool".to_owned(),
                    name: "test".to_owned(),
                    enabled: true,
                    mode: mode.to_owned(),
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
}

#[tokio::test]
async fn rotating_rejections_open_new_connections_and_detect_each_attempt_within_budget() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    for mode in ["rotating", "gateway"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        // Keep every socket alive: reusing a previous connection instead of
        // opening the next proxy connection would time out this test.
        let proxy_task = tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .unwrap()
                            .trim()
                            .parse()
                            .unwrap();
                        if request.len() >= end + 4 + length {
                            assert!(headers.starts_with("post http://upstream.invalid/"));
                            break;
                        }
                    }
                }
                // Completed SSE but no candidate: must rotate on the next attempt.
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
                    COMPLETED_SESSION_SSE.len(),
                    COMPLETED_SESSION_SSE
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                sockets.push(socket);
            }
        });
        let mut config = valid_config();
        config.config.api.base_url = "http://upstream.invalid".to_owned();
        let store = Arc::new(StateStore::default());
        let detector = Arc::new(Detector::default());
        let mut bundle = provider_openai::initialize_with_state_probe(
            config.config,
            provider_ports_with(accounts().await, Arc::new(TestOAuthPending::default())),
            store.clone(),
            detector.clone(),
        )
        .await
        .unwrap();
        let service = bundle.turn_state_service().unwrap();
        configure(service.as_ref(), &endpoint, &["gpt-5.4"], 3).await;
        set_pool_mode(service.as_ref(), mode).await;
        let task = worker(&mut bundle);
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            task.run_cycle(context(CancellationToken::new())),
        )
        .await;
        proxy_task.abort();
        let _ = proxy_task.await;
        outcome.unwrap().unwrap();
        assert_eq!(connections.load(Ordering::SeqCst), 3, "{mode}");
        assert_eq!(detector.0.load(Ordering::SeqCst), 3, "{mode}");
        {
            let probes = store.probes.lock().unwrap();
            assert_eq!(probes.len(), 6);
            for (index, pair) in probes.chunks_exact(2).enumerate() {
                assert_eq!(pair[0]["cycle"], probes[0]["cycle"]);
                assert_eq!(pair[0]["phase"], "collect");
                assert_eq!(pair[1]["decision"], "rejected");
                assert_eq!(pair[1]["succeeded"], true);
                assert_eq!(pair[1]["egress"]["ip"], format!("203.0.113.{}", index + 1));
                assert_eq!(pair[1]["egress"]["rotating"], true);
            }
        }
        service
            .action("acct_state", "gpt-5.4", "refresh", &mutation())
            .await
            .unwrap();
        task.run_cycle(context(CancellationToken::new()))
            .await
            .unwrap();
        assert_eq!(
            detector.0.load(Ordering::SeqCst),
            3,
            "budget blocks further attempts"
        );
        let view = service.view().await.unwrap();
        assert_eq!(view.targets[0].hourly_used, 3);
        assert_eq!(view.targets[0].wait_reason, "budget");
    }
}

#[tokio::test]
async fn switching_to_rotating_does_not_reuse_or_pollute_fixed_egress_cache() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let store = Arc::new(StateStore::default());
    let detector = Arc::new(Detector::default());
    let mut bundle = provider_openai::initialize_with_state_probe(
        config.config,
        provider_ports_with(accounts().await, Arc::new(TestOAuthPending::default())),
        store.clone(),
        detector.clone(),
    )
    .await
    .unwrap();
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    let task = worker(&mut bundle);
    for mode in ["fixed", "rotating", "fixed"] {
        set_pool_mode(service.as_ref(), mode).await;
        service
            .action("acct_state", "gpt-5.4", "refresh", &mutation())
            .await
            .unwrap();
        task.run_cycle(context(CancellationToken::new()))
            .await
            .unwrap();
    }
    assert_eq!(detector.0.load(Ordering::SeqCst), 4);
    let probes = store.probes.lock().unwrap();
    let ips: Vec<_> = probes
        .chunks_exact(2)
        .map(|p| p[1]["egress"]["ip"].as_str().unwrap())
        .collect();
    assert_eq!(
        ips,
        [
            "203.0.113.1",
            "203.0.113.2",
            "203.0.113.3",
            "203.0.113.4",
            "203.0.113.1"
        ]
    );
    assert_eq!(probes.last().unwrap()["egress"]["rotating"], false);
}

#[tokio::test]
async fn probe_logs_distinguish_acceptance_verification_and_two_egress_routes() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 101))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let detector = Arc::new(Detector::default());
    let mut bundle = provider_openai::initialize_with_state_probe(
        config.config,
        provider_ports_with(accounts, Arc::new(TestOAuthPending::default())),
        store.clone(),
        detector.clone(),
    )
    .await
    .unwrap();
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    let task = worker(&mut bundle);
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    {
        let probes = store.probes.lock().unwrap();
        assert_eq!(probes.len(), 4);
        assert_eq!(probes[0]["cycle"], probes[2]["cycle"]);
        assert_eq!(probes[0]["pool"], "test-pool");
        assert!(probes[2]["pool"].is_null());
        assert_eq!(probes[1]["decision"], "accepted");
        assert_eq!(probes[3]["decision"], "verified");
        assert_eq!(probes[1]["egress"]["ip"], "203.0.113.1");
        assert_eq!(probes[3]["egress"]["ip"], "198.51.100.1");
        assert_eq!(probes[3]["egress"]["source"], "independent_proxy_test");
    }
    assert_eq!(detector.0.load(Ordering::SeqCst), 2);
    service
        .action("acct_state", "gpt-5.4", "refresh", &mutation())
        .await
        .unwrap();
    task.run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(
        detector.0.load(Ordering::SeqCst),
        2,
        "fresh detection snapshot should be reused"
    );
    let probes = store.probes.lock().unwrap();
    assert_eq!(
        probes.last().unwrap()["decision"],
        "rejected",
        "duplicate completed responses are not new candidates"
    );
    assert_eq!(probes.last().unwrap()["succeeded"], true);
}

struct UnavailableDetector;
#[async_trait]
impl ProxyProbe for UnavailableDetector {
    async fn test(&self, _: &OutboundProxy) -> ProxyTestResult {
        unreachable!()
    }
    async fn quality(&self, _: &OutboundProxy) -> ProxyQualityReport {
        unreachable!()
    }
    async fn test_egress(&self, _: Option<&OutboundProxy>) -> Option<ProxyTestResult> {
        None
    }
}

#[tokio::test]
async fn rotating_pool_stops_on_authentication_and_rate_limit_responses() {
    for status in [401, 403, 407, 429] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header("retry-after", "60")
                    .set_body_json(json!({"error":{"message":"denied"}})),
            )
            .mount(&server)
            .await;
        let mut config = valid_config();
        config.config.api.base_url = server.uri();
        let store = Arc::new(StateStore::default());
        let accounts = accounts().await;
        let mut bundle = bundle(&config.config, &accounts, &store).await;
        let service = bundle.turn_state_service().unwrap();
        configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
        set_pool_mode(service.as_ref(), "rotating").await;
        worker(&mut bundle)
            .run_cycle(context(CancellationToken::new()))
            .await
            .unwrap();
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "{status}"
        );
        assert_eq!(service.view().await.unwrap().targets[0].hourly_used, 1);
        assert_eq!(store.probes.lock().unwrap()[1]["decision"], "failed");
        assert!(accounts.account("acct_state").unwrap().enabled());
    }
}

#[tokio::test]
async fn unavailable_egress_detection_does_not_reject_a_valid_candidate() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", token(10, 102))
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let store = Arc::new(StateStore::default());
    let mut bundle = provider_openai::initialize_with_state_probe(
        config.config,
        provider_ports_with(accounts().await, Arc::new(TestOAuthPending::default())),
        store.clone(),
        Arc::new(UnavailableDetector),
    )
    .await
    .unwrap();
    let service = bundle.turn_state_service().unwrap();
    configure(service.as_ref(), &server.uri(), &["gpt-5.4"], 10).await;
    worker(&mut bundle)
        .run_cycle(context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 1);
    let probes = store.probes.lock().unwrap();
    assert_eq!(probes[3]["decision"], "verified");
    assert!(probes[3]["egress"]["ip"].is_null());
    assert!(probes[3]["egress"]["location"].is_null());
}
