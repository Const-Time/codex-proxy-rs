use super::fixture::*;
use super::*;
use axum::http::StatusCode;
use gateway_admin::model::plugins::PluginMiddlewareMount;
use gateway_core::metering::Decimal;

pub(super) struct Synthetic(pub Option<MiddlewareFrame>);
impl MiddlewareBody for Synthetic {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move { Ok(self.0.take()) })
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            drop(self);
        })
    }
}
#[tokio::test]
async fn attempt_bridge_runs_json_and_sse_inside_the_original_core_execution() {
    for streaming in [false, true] {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![PluginMiddlewareMount::Attempt]).await;
        let response = f.request(streaming).await;
        let status = response.status();
        let output = bytes(response).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&output)
        );
        assert!(String::from_utf8_lossy(&output).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
        let seen = f.plugin.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].mount, MiddlewareMount::Attempt);
        assert_eq!(seen[0].attempt_index, Some(1));
        assert_eq!(seen[0].provider.as_ref().unwrap().as_str(), "openai");
        assert_eq!(seen[0].model.as_deref(), Some("model-a"));
        let requests = f.effects.requests.lock().unwrap();
        assert_eq!(requests[0].1, "model-a");
        assert_eq!(requests[0].2, "changed input");
        assert_eq!(requests[0].3, seen[0].deadline_at);
    }
}
#[tokio::test]
async fn attempt_bridge_cannot_rewrite_frozen_routing_or_transport() {
    for mode in [Mode::InvalidModel, Mode::Transport] {
        let f = Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Attempt]).await;
        assert!(f.request(false).await.status().is_server_error());
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        for _ in 0..100 {
            if f.effects.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn attempt_bridge_client_head_is_distinct_from_upstream_observation() {
    for streaming in [false, true] {
        let f = Fixture::with_mounts(Mode::Head, vec![PluginMiddlewareMount::Attempt]).await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-plugin-result"], "changed");
        assert!(!response.headers().contains_key("set-cookie"));
        bytes(response).await;
        f.settled().await;
        let rows = f.effects.finalizations.lock().unwrap();
        assert_eq!(rows[0].client_status_code, Some(201));
        assert_ne!(rows[0].upstream_status_code, Some(201));
    }
}
#[tokio::test]
async fn attempt_bridge_short_circuit_supports_json_errors_on_sse_requests() {
    for streaming in [false, true] {
        let f =
            Fixture::with_mounts(Mode::ShortCircuit, vec![PluginMiddlewareMount::Attempt]).await;
        let response = f.request(streaming).await;
        let status = response.status();
        let output = bytes(response).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{}",
            String::from_utf8_lossy(&output)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output).unwrap()["error"]["message"],
            "blocked"
        );
        for _ in 0..100 {
            if f.effects.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        {
            let charges = f.effects.charges.lock().unwrap();
            assert_eq!(charges.len(), 1);
            assert_eq!(charges[0].amount_usd, Decimal::ZERO);
        }
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn request_and_attempt_share_identity_and_keep_one_charge() {
    let f = Fixture::with_mounts(
        Mode::Rewrite,
        vec![
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ],
    )
    .await;
    let response = f.request(false).await;
    assert_eq!(response.status(), StatusCode::OK);
    bytes(response).await;
    f.settled().await;
    let seen = f.plugin.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].mount, MiddlewareMount::Request);
    assert_eq!(seen[1].mount, MiddlewareMount::Attempt);
    assert_eq!(seen[0].request_id, seen[1].request_id);
    assert_eq!(seen[0].deadline_at, seen[1].deadline_at);
    assert_eq!(seen[1].model.as_deref(), Some("model-b"));
}
#[tokio::test]
async fn attempt_output_failure_keeps_original_cost_and_never_retries_provider() {
    let f = Fixture::with_mounts(Mode::BadResponse, vec![PluginMiddlewareMount::Attempt]).await;
    let response = f.request(false).await;
    assert!(response.status().is_server_error());
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn attempt_json_rewrite_changes_bytes_without_changing_charge() {
    let f = Fixture::with_mounts(Mode::ResponseRewrite, vec![PluginMiddlewareMount::Attempt]).await;
    let response = f.request(false).await;
    let status = response.status();
    let output = bytes(response).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&output)
    );
    assert!(String::from_utf8_lossy(&output).contains("rewritten"));
    f.settled().await;
}

#[tokio::test]
async fn attempt_terminal_expansion_is_validated_before_any_part_is_delivered() {
    let f = Fixture::with_mounts(Mode::ExpandInvalid, vec![PluginMiddlewareMount::Attempt]).await;
    let response = f.request(true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let output = bytes(response).await;
    assert!(!String::from_utf8_lossy(&output).contains("must-not-leak"));
    assert!(!String::from_utf8_lossy(&output).contains("response.completed"));
    assert!(String::from_utf8_lossy(&output).contains("response.failed"));
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn attempt_raw_error_body_is_preserved_for_json_and_sse_clients() {
    for streaming in [false, true] {
        let f =
            Fixture::with_mounts(Mode::ShortCircuitRaw, vec![PluginMiddlewareMount::Attempt]).await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(&bytes(response).await[..], b"denied");
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        f.manager.shutdown().await;
    }
}
