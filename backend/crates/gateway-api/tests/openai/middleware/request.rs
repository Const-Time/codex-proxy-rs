use super::fixture::*;
use super::*;
use axum::http::StatusCode;
use futures::StreamExt;
use gateway_admin::model::plugins::PluginMiddlewareMount;
use gateway_core::metering::Decimal;
#[tokio::test]
async fn request_rewrite_preserves_core_identity_deadline_and_single_settlement() {
    for streaming in [false, true] {
        let f = Fixture::new(Mode::Rewrite, true).await;
        let response = f.request(streaming).await;
        if response.status() != StatusCode::OK {
            panic!(
                "response: {}",
                String::from_utf8_lossy(&bytes(response).await)
            );
        }
        let id = response.headers()["x-gateway-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let output = bytes(response).await;
        assert!(String::from_utf8_lossy(&output).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
        let contexts = f.plugin.seen.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].request_id.as_str(), id);
        let seen = f.effects.requests.lock().unwrap();
        assert_eq!(seen[0].0, "key_api_test");
        assert_eq!(seen[0].1, "model-b");
        assert_eq!(seen[0].2, "changed input");
        assert_eq!(seen[0].3, contexts[0].deadline_at);
        assert_eq!(f.effects.charges.lock().unwrap()[0].request_id.as_str(), id);
        let headers = f.plugin.headers.lock().unwrap();
        assert!(headers.iter().any(|h| h == "x-custom"));
        for excluded in [
            "authorization",
            "x-client-hop",
            "x-openai-actor-authorization",
            "connection",
        ] {
            assert!(!headers.iter().any(|h| h == excluded));
        }
    }
}
#[tokio::test]
async fn request_rewrite_cannot_bypass_model_validation_or_change_transport() {
    for mode in [Mode::InvalidModel, Mode::Transport] {
        let f = Fixture::new(mode, true).await;
        let response = f.request(false).await;
        assert!(response.status().is_client_error(), "{}", response.status());
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        assert!(f.effects.charges.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn rejected_plugin_output_keeps_real_cost_without_committing_delivery() {
    let f = Fixture::new(Mode::BadResponse, true).await;
    let response = f.request(false).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    f.settled().await;
    assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn revoked_required_plugin_fails_closed_before_provider_start() {
    let f = Fixture::new(Mode::Rewrite, true).await;
    f.plugin.access.revoke();
    assert_eq!(
        f.request(false).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn empty_registry_preserves_original_request_and_response_path() {
    let f = Fixture::new(Mode::Rewrite, false).await;
    let response = f.request(false).await;
    if response.status() != StatusCode::OK {
        panic!(
            "response: {}",
            String::from_utf8_lossy(&bytes(response).await)
        );
    }
    bytes(response).await;
    f.settled().await;
    assert!(f.plugin.seen.lock().unwrap().is_empty());
    let seen = f.effects.requests.lock().unwrap();
    assert_eq!(seen[0].1, "model-a");
    assert_eq!(seen[0].2, "original input");
}

#[tokio::test]
async fn revoking_plugin_interrupts_live_sse_and_releases_core_admission() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![mount]).await;
        *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
        let response = f.request(true).await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!first.is_empty());
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 1);
        f.plugin.access.revoke();
        let mut output = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => output.extend_from_slice(&chunk),
                    Err(_) => break,
                }
            }
        })
        .await
        .expect("revocation must interrupt stalled source");
        assert!(!String::from_utf8_lossy(&output).contains("response.completed"));
        for _ in 0..100 {
            if f.effects.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
        let charges = f.effects.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount_usd, Decimal::ZERO);
    }
}
