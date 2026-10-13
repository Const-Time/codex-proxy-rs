use super::{
    continuation::{assert_replay_required, complete},
    fixture::Fixture,
};
use gateway_core::engine::ExecutionOutcome;

#[tokio::test]
async fn real_core_persisted_adapter_state_requires_replay_after_restart_or_credential_revision_change()
 {
    for provider in ["openai", "xai"] {
        for change in ["restart", "credential"] {
            let Some(mut f) = Fixture::for_provider(provider).await else {
                return;
            };
            complete(&f, None, 1).await;
            if change == "restart" {
                f.restart().await;
            } else {
                f.advance_credential().await;
            }
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
            assert_eq!(
                f.core.finalizations.lock().unwrap()[1].outcome,
                ExecutionOutcome::Failed
            );
            assert_eq!(f.core.finalizations.lock().unwrap()[1].attempt_count, 1);
            assert_eq!(f.core.charges.lock().unwrap()[1].amount_usd.scaled(), 0);
            assert_eq!(f.server.requests.lock().unwrap().len(), 1);
            complete(&f, None, 3).await;
            assert_eq!(f.server.requests.lock().unwrap().len(), 2);
            f.close().await;
        }
    }
}
