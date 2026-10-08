use super::{fixture::*, *};
use axum::http::StatusCode;
use gateway_admin::model::plugins::PluginMiddlewareMount;
use gateway_core::operation::{ImageRequestKind, Operation};
const ENDPOINTS: [&str; 3] = [
    "/v1/images/generations",
    "/v1/images/edits",
    "/v1/alpha/search",
];

#[tokio::test]
async fn raw_endpoints_preserve_kind_context_fixed_route_and_one_settlement() {
    for endpoint in ENDPOINTS {
        for mounts in [
            vec![PluginMiddlewareMount::Request],
            vec![PluginMiddlewareMount::Attempt],
            vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        ] {
            let count = mounts.len();
            let f = Fixture::with_mounts(Mode::Rewrite, mounts).await;
            let response = f.raw_request(endpoint).await;
            let status = response.status();
            let id = response.headers()["x-gateway-request-id"]
                .to_str()
                .unwrap()
                .to_owned();
            let output = bytes(response).await;
            assert_eq!(
                status,
                StatusCode::ACCEPTED,
                "{}",
                String::from_utf8_lossy(&output)
            );
            assert_eq!(&output[..], b"{ \"data\": [1], \"data\": [2] }\n");
            f.settled().await;
            assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
            assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
            let requests = f.effects.raw_requests.lock().unwrap();
            let payload = match &requests[0] {
                Operation::GenerateImage(image) => {
                    assert_eq!(
                        image.kind(),
                        if endpoint.ends_with("edits") {
                            ImageRequestKind::Edit
                        } else {
                            ImageRequestKind::Generation
                        }
                    );
                    assert_eq!(
                        image.payload().context()["image_turn_id"],
                        "image-turn-original"
                    );
                    image.payload()
                }
                Operation::Search(search) => {
                    assert_eq!(endpoint, ENDPOINTS[2]);
                    search.payload()
                }
                _ => panic!("raw operation changed"),
            };
            let body: serde_json::Value = serde_json::from_slice(payload.body()).unwrap();
            assert_eq!(body["input"], "changed input");
            assert_eq!(body["unknown"], json!([1, 2]));
            let context = serde_json::to_string(payload.context()).unwrap();
            assert!(context.contains("session-original"));
            assert!(
                !context.contains("authorization")
                    && !context.contains("forged-account")
                    && !context.contains("x-plugin-hop")
            );
            let seen = f.plugin.seen.lock().unwrap();
            assert_eq!(seen.len(), count);
            for context in seen.iter() {
                assert_eq!(context.request_id.as_str(), id);
                assert_eq!(context.client_key_id.as_str(), "key_api_test");
                assert_eq!(context.endpoint, endpoint);
                assert!(context.model.is_none());
            }
            assert_eq!(f.effects.charges.lock().unwrap()[0].request_id.as_str(), id);
        }
    }
}
#[tokio::test]
async fn raw_empty_plan_preserves_large_bytes_without_plugin_limits() {
    let f = Fixture::new(Mode::Rewrite, false).await;
    let body = Bytes::from(format!("{{\"image\":\"{}\"}}", "a".repeat(8 * 1024 * 1024)));
    *f.effects.raw_response.lock().unwrap() = Some(body.clone());
    let response = f.raw_request(ENDPOINTS[0]).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let output = axum::body::to_bytes(response.into_body(), 10 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(output, body);
    f.settled().await;
    let requests = f.effects.raw_requests.lock().unwrap();
    let Operation::GenerateImage(image) = &requests[0] else {
        panic!()
    };
    assert_eq!(
        &image.payload().body()[..],
        b"{ \"prompt\": \"original\", \"unknown\": [1, 2] } "
    );
}
#[tokio::test]
async fn raw_plugin_heads_and_bodies_do_not_replace_upstream_cost_or_status() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        for mode in [Mode::Head, Mode::ResponseRewrite] {
            let f = Fixture::with_mounts(mode, vec![mount]).await;
            let response = f.raw_request(ENDPOINTS[2]).await;
            assert_eq!(
                response.status(),
                if matches!(mode, Mode::Head) {
                    StatusCode::CREATED
                } else {
                    StatusCode::ACCEPTED
                }
            );
            if matches!(mode, Mode::Head) {
                assert_eq!(response.headers()["x-plugin-result"], "changed");
                assert!(!response.headers().contains_key("set-cookie"));
            }
            let output = bytes(response).await;
            if matches!(mode, Mode::ResponseRewrite) {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&output).unwrap()["id"],
                    "resp_rewritten"
                );
            }
            f.settled().await;
            assert_eq!(
                f.effects.finalizations.lock().unwrap()[0].upstream_status_code,
                Some(202)
            );
        }
    }
}
#[tokio::test]
async fn raw_short_circuit_never_calls_provider() {
    for endpoint in ENDPOINTS {
        for mount in [
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ] {
            let f = Fixture::with_mounts(Mode::ShortCircuitRaw, vec![mount]).await;
            let response = f.raw_request(endpoint).await;
            let status = response.status();
            let output = bytes(response).await;
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "{}",
                String::from_utf8_lossy(&output)
            );
            assert_eq!(&output[..], b"denied");
            assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
            assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
            assert!(
                f.effects
                    .charges
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|charge| charge.amount_usd == gateway_core::metering::Decimal::ZERO)
            );
        }
    }
}
#[tokio::test]
async fn invalid_raw_projection_cancels_before_commit_but_retains_cost() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::BadResponse, vec![mount]).await;
        assert!(f.raw_request(ENDPOINTS[0]).await.status().is_server_error());
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn oversized_raw_document_fails_at_plugin_boundary_without_losing_cost() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![mount]).await;
        *f.effects.raw_response.lock().unwrap() = Some(Bytes::from(format!(
            "{{\"image\":\"{}\"}}",
            "a".repeat(8 * 1024 * 1024)
        )));
        assert!(f.raw_request(ENDPOINTS[0]).await.status().is_server_error());
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn raw_revocation_interrupts_waiting_document_and_releases_admission() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![mount]).await;
        *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
        let request = f.raw_request(ENDPOINTS[1]);
        tokio::pin!(request);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::select! {
                _ = &mut request => panic!("source must wait"),
                () = async { while f.effects.calls.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
            }
            f.plugin.access.revoke();
            assert!(request.await.status().is_server_error());
        }).await.expect("revocation must interrupt document collection");
        // The provider reported cost before waiting; revocation must keep that receipt.
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn dropped_raw_http_request_finalizes_original_execution_once() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![mount]).await;
        *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
        let mut request = Box::pin(f.raw_request(ENDPOINTS[2]));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::select! {
                _ = &mut request => panic!("source must wait"),
                () = async { while f.effects.calls.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
            }
        }).await.unwrap();
        drop(request);
        f.settled().await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn revoked_raw_plugin_fails_closed_before_start() {
    for mount in [
        PluginMiddlewareMount::Request,
        PluginMiddlewareMount::Attempt,
    ] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![mount]).await;
        f.plugin.access.revoke();
        for endpoint in ENDPOINTS {
            assert_eq!(
                f.raw_request(endpoint).await.status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        assert!(f.effects.charges.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn raw_endpoints_propagate_request_settings_to_attempts() {
    for endpoint in ENDPOINTS {
        let f = Fixture::with_mounts(
            Mode::Settings,
            vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        )
        .await;
        let response = f.raw_request(endpoint).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        bytes(response).await;
        f.settled().await;
        assert_eq!(
            serde_json::to_value(f.effects.settings.lock().unwrap()[0].values()).unwrap()["request_interval_ms"],
            137
        );
        f.manager.shutdown().await;
    }
}
