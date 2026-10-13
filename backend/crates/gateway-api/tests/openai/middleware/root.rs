use super::{fixture::*, *};
use gateway_admin::ports::plugins::PluginService;
use gateway_core::engine::observation::RequestObservationOutcome;
use std::time::Duration;

async fn observed(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.plugin.observed.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn json_and_sse_bind_original_observer_plan_and_deliver_native_terminal_after_cleanup() {
    for stream in [false, true] {
        let f = Fixture::new(Mode::Observation, false).await;
        let response = f.request(stream).await;
        assert_eq!(response.status(), 200);
        bytes(response).await;
        f.settled().await;
        observed(&f).await;
        let facts = f.plugin.observed.lock().unwrap().clone();
        assert_eq!(facts.len(), 1);
        let fact = &facts[0];
        assert_eq!(fact.outcome(), RequestObservationOutcome::Succeeded);
        assert_eq!(fact.client_key_id().as_str(), "key_api_test");
        assert_eq!(fact.provider().unwrap().as_str(), "openai");
        assert_eq!(fact.account_id().unwrap().as_str(), "acct_api_test");
        assert_eq!(fact.requested_model().unwrap().as_str(), "model-a");
        assert_eq!(fact.client_status_code(), Some(200));
        assert!(fact.cost().total().is_some());
        assert_eq!(fact.attempt_count(), 1);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        assert!(f.plugin.observed_websocket.lock().unwrap().is_empty()); // Actual upstream is HTTP.
        drop(facts);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn image_and_search_ingress_bind_same_native_terminal_port_and_original_fee() {
    for endpoint in [
        "/v1/images/generations",
        "/v1/images/edits",
        "/v1/alpha/search",
    ] {
        let f = Fixture::new(Mode::Observation, false).await;
        let response = f.raw_request(endpoint).await;
        assert_eq!(response.status(), 202);
        bytes(response).await;
        f.settled().await;
        observed(&f).await;
        let facts = f.plugin.observed.lock().unwrap().clone();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].outcome(), RequestObservationOutcome::Succeeded);
        assert_eq!(facts[0].client_status_code(), Some(202));
        assert_eq!(facts[0].attempt_count(), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        drop(facts);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn observer_only_revocation_does_not_cancel_buffered_successful_json() {
    let f = Fixture::new(Mode::Observation, false).await;
    let response = f.request(false).await;
    assert_eq!(response.status(), 200);
    // Unlike a consulted routing or scheduling policy, this process only
    // observes. Its revocation must not become a business cancellation source.
    f.plugin.access.revoke();
    assert!(String::from_utf8_lossy(&bytes(response).await).contains("resp_original"));
    f.settled().await;
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_ok()
    );
    f.manager.shutdown().await;
}
