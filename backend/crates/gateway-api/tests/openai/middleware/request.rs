use super::fixture::*;
use super::*;
use axum::http::StatusCode;
use futures::StreamExt;
use gateway_admin::model::plugins::PluginMiddlewareMount;
use gateway_core::metering::Decimal;

#[tokio::test]
async fn catalog_alias_is_listed_and_executes_through_json_and_sse_with_one_charge() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    for stream in [false, true] {
        let f = Fixture::new(Mode::Catalog, false).await;
        for endpoint in [
            "/v1/models",
            "/v1/models/plugin-model",
            "/v1/models?client_version=0.155.0",
        ] {
            let response = f
                .app
                .clone()
                .oneshot(
                    Request::get(endpoint)
                        .header("authorization", "Bearer sk_middleware")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
            let body = bytes(response).await;
            assert!(
                String::from_utf8_lossy(&body).contains("plugin-model"),
                "{endpoint}: {}",
                String::from_utf8_lossy(&body)
            );
            if endpoint.contains('?') {
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let alias = value["models"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|model| model["slug"] == "plugin-model")
                    .unwrap();
                assert_eq!(alias["native_field"], json!({"effort":"high"}));
            }
        }
        let response = f
            .app
            .clone()
            .oneshot(
                Request::post("/v1/responses")
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"model":"plugin-model","input":"original input","stream":stream})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(String::from_utf8_lossy(&bytes(response).await).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.requests.lock().unwrap()[0].1, "model-b");
        assert!(f.plugin.seen.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn catalog_config_conflicts_and_revocation_cannot_deliver_an_old_alias() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let f = Fixture::new(Mode::Catalog, false).await;
    let response = f
        .app
        .clone()
        .oneshot(
            Request::post("/v1/responses")
                .header("authorization", "Bearer sk_middleware")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"model":"plugin-model","input":"original input","stream":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    f.settled().await;
    f.plugin.access.revoke();
    assert!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .is_err()
    );
    let response = f
        .app
        .clone()
        .oneshot(
            Request::get("/v1/models")
                .header("authorization", "Bearer sk_middleware")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    f.manager.shutdown().await;
    let f = Fixture::new(Mode::Catalog, false).await;
    let base = f.snapshots.acquire().unwrap();
    f.snapshots
        .publish(
            base.as_ref()
                .clone()
                .with_model_mappings(std::collections::BTreeMap::from([(
                    "plugin-model".into(),
                    "model-a".into(),
                )])),
        );
    let response = f
        .app
        .clone()
        .oneshot(
            Request::get("/v1/models")
                .header("authorization", "Bearer sk_middleware")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn catalog_revocation_interrupts_a_waiting_native_directory_query() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let f = Fixture::new(Mode::Catalog, false).await;
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    *f.effects.catalog_gate.lock().unwrap() = Some(receiver);
    let call = f.app.clone().oneshot(
        Request::get("/v1/models?client_version=0.155.0")
            .header("authorization", "Bearer sk_middleware")
            .body(Body::empty())
            .unwrap(),
    );
    tokio::pin!(call);
    tokio::select! {_=&mut call=>panic!("catalog should wait"),_=async {while f.effects.catalog_calls.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    f.plugin.access.revoke();
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn catalog_revoked_between_authentication_and_lookup_cannot_lose_its_guard() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let f = Fixture::new(Mode::Catalog, false).await;
    *f.effects.revoke_catalog_before_lookup.lock().unwrap() = Some(f.plugin.access.clone());
    let response = f
        .app
        .clone()
        .oneshot(
            Request::get("/v1/models?client_version=0.155.0")
                .header("authorization", "Bearer sk_middleware")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(f.effects.catalog_calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn consulted_policy_revocation_blocks_buffered_json_after_core_settlement() {
    for f in [
        Fixture::with_router().await,
        Fixture::with_scheduler().await,
        Fixture::with_retry().await,
    ] {
        let response = f.request(false).await;
        assert_eq!(response.status(), StatusCode::OK);
        f.settled().await;
        f.plugin.access.revoke();
        assert!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .is_err(),
            "revocation must guard already buffered delivery"
        );
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn retry_policy_runs_for_json_and_sse_before_visible_delivery_with_one_charge() {
    for streaming in [false, true] {
        let f = Fixture::with_retry().await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::OK);
        let output = String::from_utf8_lossy(&bytes(response).await).into_owned();
        assert!(output.contains("resp_original"), "{output}");
        f.settled().await;
        assert_eq!(f.plugin.retry_calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 2);
        assert!(f.plugin.seen.lock().unwrap().is_empty());
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        assert_eq!(f.effects.charges.lock().unwrap()[0].amount_usd.scaled(), 25);
        assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn model_routing_runs_without_request_middleware_for_json_and_sse() {
    for streaming in [false, true] {
        let f = Fixture::with_router().await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::OK);
        let id = response.headers()["x-gateway-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert!(String::from_utf8_lossy(&bytes(response).await).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.plugin.routing_calls.load(Ordering::SeqCst), 1);
        assert!(f.plugin.seen.lock().unwrap().is_empty());
        let seen = f.effects.requests.lock().unwrap();
        assert_eq!(
            (&seen[0].0[..], &seen[0].1[..], &seen[0].2[..]),
            ("key_api_test", "model-b", "original input")
        );
        assert_eq!(f.effects.charges.lock().unwrap()[0].request_id.as_str(), id);
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn account_scheduling_runs_for_json_and_sse_with_one_original_charge() {
    for streaming in [false, true] {
        let f = Fixture::with_scheduler().await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(String::from_utf8_lossy(&bytes(response).await).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.plugin.scheduling_calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.plugin.routing_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        f.manager.shutdown().await;
    }
}
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

#[tokio::test]
async fn request_capabilities_are_checked_by_core_and_survive_attempt_redecode() {
    use axum::{body::Body, http::Request};
    use gateway_core::operation::Feature;
    use tower::ServiceExt;
    for both in [false, true] {
        for streaming in [false, true] {
            for (mode, extra, supported, success) in [
                (Mode::HandleTools, json!({"tools":[{}]}), false, true),
                (
                    Mode::HandleTools,
                    json!({"tools":[{}],"reasoning":{}}),
                    false,
                    false,
                ),
                (Mode::FalseHandleTools, json!({"tools":[{}]}), false, false),
                (Mode::RequireReasoning, json!({}), false, false),
                (Mode::RequireReasoning, json!({}), true, true),
            ] {
                let mounts = if both {
                    vec![
                        PluginMiddlewareMount::Request,
                        PluginMiddlewareMount::Attempt,
                    ]
                } else {
                    vec![PluginMiddlewareMount::Request]
                };
                let features = if supported {
                    vec![Feature::Reasoning]
                } else {
                    vec![]
                };
                let f = Fixture::with_features(mode, mounts, &features).await;
                let mut body = extra;
                body["model"] = json!("model-a");
                body["input"] = json!("original input");
                body["stream"] = json!(streaming);
                let response = f
                    .app
                    .clone()
                    .oneshot(
                        Request::post("/v1/responses")
                            .header("authorization", "Bearer sk_middleware")
                            .header("content-type", "application/json")
                            .body(Body::from(body.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response.status().is_success(),
                    success,
                    "status={} both={both} streaming={streaming}",
                    response.status()
                );
                let output = bytes(response).await;
                if success {
                    assert!(
                        String::from_utf8_lossy(&output).contains("resp_original"),
                        "{}",
                        String::from_utf8_lossy(&output)
                    );
                    f.settled().await;
                    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
                    assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
                    let operations = f.effects.operations.lock().unwrap();
                    assert_eq!(
                        operations[0]
                            .capability_requirements()
                            .features()
                            .contains(&Feature::Reasoning),
                        supported
                    );
                } else {
                    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
                    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn request_settings_reach_attempt_and_provider_without_changing_frozen_input() {
    for streaming in [false, true] {
        let f = Fixture::with_mounts(
            Mode::Settings,
            vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        )
        .await;
        for _ in 0..2 {
            let response = f.request(streaming).await;
            assert_eq!(response.status(), StatusCode::OK);
            bytes(response).await;
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while f.effects.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(f.effects.finalizations.lock().unwrap().len(), 2);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 2);
        {
            let settings = f.effects.settings.lock().unwrap();
            assert_eq!(settings.len(), 2);
            for settings in settings.iter() {
                assert_eq!(
                    serde_json::to_value(settings.values()).unwrap()["request_interval_ms"],
                    137
                );
                assert_eq!(
                    settings.execution_values().unwrap().timeout_ms,
                    Some(60_000)
                );
                assert_eq!(
                    settings.inspect()["execution"]["input"]["fast_mode"],
                    "default"
                );
                assert_eq!(settings.inspect()["execution"]["fast_mode"]["order"], 1);
            }
        }
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 2);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn zero_timeout_fails_before_admission_and_provider() {
    let f = Fixture::new(Mode::ZeroTimeout, true).await;
    let response = f.request(false).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
