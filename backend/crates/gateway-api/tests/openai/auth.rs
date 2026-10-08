use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

use gateway_api::openai::auth::{
    ClientApiKeyAuthError, bearer_client_api_key, identify_codex_client,
};
use gateway_core::policy::CodexClientKind;

use crate::admin::plugin_models::{Fixture, until};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures::StreamExt;
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use tower::ServiceExt;

fn external_request(path: &str, authorization: &str, stream: bool) -> Request<Body> {
    Request::builder()
        .method(if path.starts_with("/v1/models") {
            "GET"
        } else {
            "POST"
        })
        .uri(path)
        .header(AUTHORIZATION, authorization)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model":"model-a","input":"hello","stream":stream}).to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn external_authentication_protects_all_ingress_routes_and_resolves_host_bound_key() {
    let f = Fixture::new(false).await;
    for path in [
        "/v1/responses",
        "/v1/models",
        "/v1/models/model-a",
        "/v1/images/generations",
        "/v1/images/edits",
        "/v1/alpha/search",
    ] {
        for (header, status) in [
            ("Bearer rejected", StatusCode::UNAUTHORIZED),
            ("Bearer missing-binding", StatusCode::UNAUTHORIZED),
            ("Bearer plugin-error", StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let response = f
                .app
                .clone()
                .oneshot(external_request(path, header, false))
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path}: {header}");
            let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("private plugin error"));
        }
        assert!(f.trace.starts.lock().unwrap().is_empty());
        let response = f
            .app
            .clone()
            .oneshot(external_request(path, "Bearer external", false))
            .await
            .unwrap();
        if path.contains("images") || path.contains("search") {
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        } else {
            assert_eq!(response.status(), StatusCode::OK);
        }
        to_bytes(response.into_body(), 1 << 20).await.unwrap();
        if !path.starts_with("/v1/models") {
            assert_eq!(
                *f.trace.starts.lock().unwrap(),
                vec![("key_api_test".into(), path.into())]
            );
            f.trace.starts.lock().unwrap().clear();
        }
    }
    let builtin = format!("Bearer sk_{}", "a".repeat(43));
    assert_eq!(
        f.app
            .clone()
            .oneshot(external_request("/v1/models", &builtin, false))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    f.snapshots.suspend();
    assert_eq!(
        f.app
            .clone()
            .oneshot(external_request("/v1/models", "Bearer external", false))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn external_authentication_rejects_ambiguous_and_oversized_envelopes() {
    let f = Fixture::new(false).await;
    let mut duplicate = external_request("/v1/models", "Bearer external", false);
    duplicate
        .headers_mut()
        .append(AUTHORIZATION, HeaderValue::from_static("Bearer external"));
    let oversized = external_request("/v1/models", &"x".repeat(8193), false);
    for request in [duplicate, oversized] {
        assert_eq!(
            f.app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn external_authorization_cancels_idle_response_bodies_and_discards_queued_chunks() {
    for cause in ["plugin", "key", "disconnect"] {
        let f = Fixture::new(false).await;
        let response = f
            .app
            .clone()
            .oneshot(external_request("/v1/responses", "Bearer external", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        until(|| f.trace.events.load(Ordering::SeqCst) >= 2).await;
        let mut response = Some(response);
        match cause {
            "plugin" => f.plugin.revoke(),
            "key" => f.snapshots.suspend(),
            _ => drop(response.take()),
        }
        until(|| f.trace.finalized.load(Ordering::SeqCst) == 1).await;
        assert!(f.trace.cancelled.load(Ordering::SeqCst) > 0);
        if let Some(response) = response {
            assert!(
                response
                    .into_body()
                    .into_data_stream()
                    .next()
                    .await
                    .unwrap()
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn external_authorization_closes_idle_websocket_and_rejects_future_handshakes() {
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest},
    };
    for cause in ["plugin", "key"] {
        let f = Fixture::new(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = f.app;
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let request = || {
            let mut r = format!("ws://{address}/v1/responses")
                .into_client_request()
                .unwrap();
            r.headers_mut()
                .insert(AUTHORIZATION, HeaderValue::from_static("Bearer external"));
            r
        };
        let (mut socket, _) = connect_async(request()).await.unwrap();
        if cause == "plugin" {
            f.plugin.revoke();
        } else {
            f.snapshots.suspend();
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match socket.next().await {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    _ => {}
                }
            }
        })
        .await
        .expect("idle WebSocket must observe revoked authority");
        assert!(connect_async(request()).await.is_err());
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn external_authorization_keeps_policy_registration_until_buffered_delivery_finishes() {
    let f = Fixture::new(false).await;
    let response = f
        .app
        .clone()
        .oneshot(external_request("/v1/responses", "Bearer external", false))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Allow the body pump to enqueue the complete JSON and finish.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    f.snapshots.suspend();
    assert!(
        response
            .into_body()
            .into_data_stream()
            .next()
            .await
            .unwrap()
            .is_err()
    );
}

#[test]
fn bearer_client_api_key_should_reject_missing_authorization() {
    assert_eq!(
        bearer_client_api_key(&HeaderMap::new()),
        Err(ClientApiKeyAuthError::MissingAuthorization)
    );
}

#[test]
fn actor_authorization_marker_should_not_replace_client_api_key() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-openai-actor-authorization",
        HeaderValue::from_static("proxy-managed"),
    );

    assert_eq!(
        bearer_client_api_key(&headers),
        Err(ClientApiKeyAuthError::MissingAuthorization)
    );
}

#[test]
fn actor_authorization_marker_should_preserve_existing_bearer_authentication() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-openai-actor-authorization",
        HeaderValue::from_static("proxy-managed"),
    );
    headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk_client"));

    assert_eq!(bearer_client_api_key(&headers), Ok("sk_client"));
}

#[test]
fn bearer_client_api_key_should_reject_non_utf8_authorization() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
    );

    assert_eq!(
        bearer_client_api_key(&headers),
        Err(ClientApiKeyAuthError::MalformedAuthorization)
    );
}

#[test]
fn bearer_client_api_key_should_require_exact_bearer_scheme() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_static("bearer sk_client"));

    assert_eq!(
        bearer_client_api_key(&headers),
        Err(ClientApiKeyAuthError::MalformedAuthorization)
    );
}

#[test]
fn bearer_client_api_key_should_reject_empty_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer    "));

    assert_eq!(
        bearer_client_api_key(&headers),
        Err(ClientApiKeyAuthError::MalformedAuthorization)
    );
}

#[test]
fn bearer_client_api_key_should_reject_non_gateway_key_prefix() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer xai-secret"));

    assert_eq!(
        bearer_client_api_key(&headers),
        Err(ClientApiKeyAuthError::InvalidKeyFormat)
    );
}

#[test]
fn bearer_client_api_key_should_return_trimmed_gateway_key() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer   sk_client_secret   "),
    );

    assert_eq!(bearer_client_api_key(&headers), Ok("sk_client_secret"));
}

#[test]
fn client_auth_failure_reasons_should_be_stable_and_secret_free() {
    assert_eq!(
        [
            ClientApiKeyAuthError::MissingAuthorization,
            ClientApiKeyAuthError::MalformedAuthorization,
            ClientApiKeyAuthError::InvalidKeyFormat,
            ClientApiKeyAuthError::InvalidKey,
            ClientApiKeyAuthError::RuntimeUnavailable,
        ]
        .map(ClientApiKeyAuthError::reason),
        [
            "missing_authorization",
            "malformed_authorization",
            "invalid_key_format",
            "invalid_key",
            "runtime_unavailable",
        ]
    );
}

#[test]
fn desktop_headers_should_take_precedence_over_embedded_cli_marker() {
    let mut headers = HeaderMap::new();
    headers.insert("originator", HeaderValue::from_static("Codex Desktop"));
    headers.insert("version", HeaderValue::from_static("26.825.6671"));
    headers.insert(
        "user-agent",
        HeaderValue::from_static("codex_cli_rs/0.39.0 terminal (Codex Desktop; 26.1.0)"),
    );

    let identified = identify_codex_client(&headers).expect("recognized desktop");

    assert_eq!(identified.kind(), CodexClientKind::Desktop);
    assert_eq!(
        identified.version().map(ToString::to_string).as_deref(),
        Some("26.825.6671")
    );
}

#[test]
fn cli_user_agent_should_expose_semver_or_recognized_missing_version() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "user-agent",
        HeaderValue::from_static("codex_cli_rs/0.39.0 (Linux; x86_64)"),
    );
    let identified = identify_codex_client(&headers).expect("recognized CLI");
    assert_eq!(identified.kind(), CodexClientKind::Cli);
    assert_eq!(
        identified.version().map(ToString::to_string).as_deref(),
        Some("0.39.0")
    );

    headers.insert(
        "user-agent",
        HeaderValue::from_static("codex-cli/not-a-version"),
    );
    assert!(
        identify_codex_client(&headers)
            .expect("recognized invalid CLI")
            .version()
            .is_none()
    );
}
