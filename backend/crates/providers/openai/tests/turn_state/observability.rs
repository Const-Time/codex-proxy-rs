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
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(ProxyTestResult {
            success: true,
            latency_ms: 1,
            exit_ip: Some(
                if proxy.is_some() {
                    "203.0.113.1"
                } else {
                    "198.51.100.1"
                }
                .parse()
                .unwrap(),
            ),
            location: None,
            message: "test".to_owned(),
        })
    }
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
