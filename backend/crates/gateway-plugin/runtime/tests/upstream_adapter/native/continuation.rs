use super::fixture::Fixture;
use gateway_core::{
    engine::{
        ExecutionOutcome,
        continuation::{NativeContinuationPort, NativeContinuationScope, PreviousResponseId},
    },
    error::GatewayErrorKind,
    policy::ClientApiKeyId,
    upstream::UpstreamSendState,
};
use std::time::Duration;

pub(super) async fn complete(f: &Fixture, previous: Option<&str>, count: usize) {
    let mut started = f
        .start_with("sk_native_fixture", "continuation", previous)
        .await
        .unwrap();
    let events = started.session.collect_uncommitted().await.unwrap();
    assert!(events.iter().any(|event| event.session_update().is_some()));
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
    f.clean(count).await;
}

#[tokio::test]
async fn real_core_native_adapter_continuation_roundtrips_through_original_redis_for_both_providers()
 {
    for provider in ["openai", "xai"] {
        let Some(f) = Fixture::for_provider(provider).await else {
            return;
        };
        complete(&f, None, 1).await;
        let pin = f
            .continuation
            .resolve(
                &ClientApiKeyId::new("key_alice").unwrap(),
                &PreviousResponseId::new("adapter-response"),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pin.account(), &f.account);
        assert_eq!(pin.provider().as_str(), provider);
        assert_eq!(pin.scope(), NativeContinuationScope::Persisted);
        assert!(pin.session_state().is_some());
        assert_eq!(
            f.in_flight().await,
            0,
            "persistent continuation holds no native account lease"
        );
        complete(&f, Some("adapter-response"), 2).await;
        assert_eq!(f.server.requests.lock().unwrap().len(), 2);
        assert_eq!(f.core.attempts.lock().unwrap().len(), 2);
        assert!(
            f.core
                .finalizations
                .lock()
                .unwrap()
                .iter()
                .all(|row| row.outcome == ExecutionOutcome::Succeeded && row.attempt_count == 1)
        );
        assert!(
            f.core
                .charges
                .lock()
                .unwrap()
                .iter()
                .all(|charge| charge.amount_usd.scaled() == 570_000)
        );
        f.close().await;
    }
}

#[tokio::test]
async fn real_core_native_adapter_continuation_rejects_other_key_before_admission_and_network() {
    for provider in ["openai", "xai"] {
        let Some(f) = Fixture::for_provider(provider).await else {
            return;
        };
        complete(&f, None, 1).await;
        let (_block, pending) = tokio::sync::oneshot::channel();
        *f.core.admit_gate.lock().unwrap() = Some(pending);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            f.start_with("sk_native_other", "continuation", Some("adapter-response")),
        )
        .await
        .expect("continuation ownership must reject before waiting for admission");
        let error = match result {
            Ok(_) => panic!("the second valid Key must not reuse the first Key's continuation"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), GatewayErrorKind::PolicyDenied);
        assert!(
            f.core.admit_gate.lock().unwrap().is_some(),
            "the original admission port was never entered"
        );
        assert_eq!(f.core.requests.lock().unwrap().len(), 1);
        assert_eq!(f.core.attempts.lock().unwrap().len(), 1);
        assert_eq!(f.core.charges.lock().unwrap().len(), 1);
        assert_eq!(f.server.requests.lock().unwrap().len(), 1);
        assert_eq!(f.in_flight().await, 0);
        f.close().await;
    }
}

#[tokio::test]
async fn real_core_native_adapter_upgrade_rejects_old_continuation_without_falling_through() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        complete(&f, None, 1).await;
        f.upgrade().await;
        let mut started = f
            .start_with(
                "sk_native_fixture",
                "continuation",
                Some("adapter-response"),
            )
            .await
            .unwrap();
        let error = started.session.collect_uncommitted().await.unwrap_err();
        assert_replay_required(&error);
        started.session.detach_finalize().await;
        f.clean(2).await;
        {
            let rows = f.core.finalizations.lock().unwrap();
            assert_eq!(rows[1].outcome, ExecutionOutcome::Failed);
            assert_eq!(rows[1].send_state, UpstreamSendState::NotSent);
            assert_eq!(rows[1].attempt_count, 1);
        }
        assert_eq!(f.core.charges.lock().unwrap()[1].amount_usd.scaled(), 0);
        assert_eq!(
            f.server.requests.lock().unwrap().len(),
            1,
            "old plugin state never reaches native or new-generation network"
        );
        complete(&f, None, 3).await;
        assert_eq!(f.server.requests.lock().unwrap().len(), 2);
        f.close().await;
    }
}

pub(super) fn assert_replay_required(error: &gateway_core::engine::EngineError) {
    use gateway_core::error::{
        ContinuationFailure, ContinuationRecoveryDisposition, ProviderErrorKind,
    };
    let gateway_core::engine::EngineError::Provider(provider) = error else {
        panic!("expected an original Provider continuation failure: {error:?}")
    };
    assert_eq!(
        provider.kind(),
        ProviderErrorKind::ContinuationRecoveryRequired
    );
    assert_eq!(provider.send_state(), UpstreamSendState::NotSent);
    assert_eq!(
        provider.continuation_failure(),
        Some(ContinuationFailure::HistoryUnavailable)
    );
    assert_eq!(
        provider.continuation_recovery_disposition(),
        Some(ContinuationRecoveryDisposition::ClientReplayRequired)
    );
    let error = gateway_core::engine::execution::gateway_error_from_engine(error);
    assert_eq!(error.kind(), GatewayErrorKind::InvalidRequest);
    assert_eq!(
        error.client_error_code(),
        Some("previous_response_not_found")
    );
}
