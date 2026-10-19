//! Raw HTTP chunks retain the native execution and account lease owners.
use super::fixture::Fixture;
use gateway_core::{
    engine::{ExecutionOutcome, execution::ClientTransport},
    upstream::UpstreamSendState,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn raw_body_tail_failure_preserves_native_cost_and_invalid_streams_never_replay() {
    for mode in ["tail-fault", "limit", "mixed"] {
        eprintln!("raw body failure scenario: {mode}");
        let Some(f) = Fixture::new().await else {
            return;
        };
        let mut started = f
            .start_with_fields(
                "http",
                json!({"raw_body":mode}).as_object().unwrap().clone(),
                None,
            )
            .await;
        let error = started.session.collect_uncommitted().await.unwrap_err();
        eprintln!("raw body {mode}: {error:?}");
        assert!(started.session.is_finalized());
        started.session.detach_finalize().await;
        f.clean(1).await;
        {
            let rows = f.core.finalizations.lock().unwrap();
            assert_eq!(
                rows[0].outcome,
                // Invalid business streams fail this execution, without revoking the generation.
                ExecutionOutcome::Failed,
                "{mode}"
            );
            assert_eq!(rows[0].attempt_count, 1);
            assert_eq!(rows[0].send_state, UpstreamSendState::Sent);
            if mode == "tail-fault" {
                assert_eq!(rows[0].usage.input_tokens, Some(10));
                assert_eq!(rows[0].cost.total().unwrap().amount().scaled(), 570_000);
                assert_eq!(
                    f.core.charges.lock().unwrap()[0].amount_usd.scaled(),
                    570_000
                );
            }
        }
        assert_eq!(f.server.requests.lock().unwrap().len(), 1);
        assert_eq!(f.in_flight().await, 0);
        f.close().await;
    }
}

#[tokio::test]
async fn abandoning_partial_raw_body_after_cancel_revocation_or_disable_releases_the_original_account_once()
 {
    for change in ["cancel", "key", "disable"] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let mut started = f
            .start_with_fields(
                "http",
                json!({"raw_body":"wait"}).as_object().unwrap().clone(),
                None,
            )
            .await;
        let first = tokio::time::timeout(Duration::from_secs(5), started.session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(first.into_provider_events().iter().any(|event| {
            event
                .wire_event()
                .is_some_and(|wire| wire.raw_body_chunk().is_some())
        }));
        assert_eq!(f.in_flight().await, 1);
        match change {
            "key" => f.revoke_key(),
            "disable" => f.disable().await,
            _ => started.session.cancel(),
        }
        // Abandon the uncommitted batch through the existing session owner.
        started.session.detach_finalize().await;
        f.clean(1).await;
        assert_eq!(f.in_flight().await, 0);
        assert_eq!(f.server.requests.lock().unwrap().len(), 1);
        assert_eq!(f.core.commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
        assert_eq!(
            f.core.finalizations.lock().unwrap()[0].outcome,
            ExecutionOutcome::Cancelled
        );
        f.close().await;
    }
}

#[tokio::test]
async fn raw_http_body_cannot_be_delivered_as_sse_or_websocket_frames() {
    for transport in [ClientTransport::HttpSse, ClientTransport::WebSocket] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let mut started = f
            .start_transport("sk_native_fixture", "raw-body", None, transport)
            .await
            .unwrap();
        assert!(started.session.collect_uncommitted().await.is_err());
        started.session.detach_finalize().await;
        f.clean(1).await;
        assert_eq!(f.server.requests.lock().unwrap().len(), 1);
        assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
        assert_eq!(f.core.commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        f.close().await;
    }
}
