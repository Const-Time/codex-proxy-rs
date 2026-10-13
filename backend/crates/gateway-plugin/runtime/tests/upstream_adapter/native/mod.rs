mod continuation;
mod fixture;
mod xai;
use fixture::Fixture;
use gateway_core::{engine::ExecutionOutcome, upstream::UpstreamSendState};
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn real_core_openai_and_sdk_adapter_preserve_scope_observed_tier_and_single_settlement() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for (index, mode) in ["http", "http-stream"].into_iter().enumerate() {
        let mut started = f.start(mode).await;
        assert_eq!(f.server.requests.lock().unwrap().len(), index);
        let events = started.session.collect_uncommitted().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
        assert!(
            !started.session.is_finalized(),
            "buffered JSON has not crossed the commit boundary"
        );
        started.session.commit_downstream(Some(200)).await.unwrap();
        assert!(started.session.is_finalized());
        started.session.detach_finalize().await;
        f.clean(index + 1).await;
        assert_eq!(f.in_flight().await, 0);
        let rows = f.core.finalizations.lock().unwrap();
        let row = &rows[index];
        assert_eq!(row.outcome, ExecutionOutcome::Succeeded);
        assert_eq!(row.attempt_count, 1);
        assert_eq!(row.usage.input_tokens, Some(10));
        assert_eq!(row.usage.output_tokens, Some(2));
        assert_eq!(row.service_tier.as_deref(), Some("priority"));
        assert_eq!(row.cost.total().unwrap().amount().scaled(), 570_000);
        assert_eq!(
            f.core.charges.lock().unwrap()[index].amount_usd.scaled(),
            570_000
        );
        assert_eq!(
            f.core.charges.lock().unwrap()[index].request_id,
            started.request_id
        );
        let requests = f.server.requests.lock().unwrap();
        assert_eq!(requests.len(), index + 1);
        assert!(requests[index].contains("authorization: Bearer fixture-provider-secret"));
        assert!(requests[index].contains(&format!("chatgpt-account-id: chatgpt-{}", f.account)));
        assert!(!requests[index].contains("fixture-forbidden-secret"));
        assert!(!requests[index].contains("cookie:"));
    }
    assert_eq!(f.core.requests.lock().unwrap().len(), 2);
    assert_eq!(f.core.attempts.lock().unwrap().len(), 2);
    assert_eq!(
        f.core.commits.load(Ordering::SeqCst),
        2,
        "each buffered JSON response commits once"
    );
    f.close().await;
}

#[tokio::test]
async fn real_core_adapter_tail_fault_keeps_native_metering_and_never_replays_sent_request() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let mut started = f.start("tail-fault").await;
    assert!(started.session.collect_uncommitted().await.is_err());
    assert!(started.session.is_finalized());
    started.session.detach_finalize().await;
    f.clean(1).await;
    assert_eq!(f.in_flight().await, 0);
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, ExecutionOutcome::Failed);
        assert_eq!(rows[0].send_state, UpstreamSendState::Sent);
        assert_eq!(rows[0].attempt_count, 1);
        assert_eq!(rows[0].cost.total().unwrap().amount().scaled(), 570_000);
        assert_eq!(
            f.core.charges.lock().unwrap()[0].amount_usd.scaled(),
            570_000
        );
        assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    }
    f.close().await;
}

#[tokio::test]
async fn real_core_adapter_key_revocation_cancels_live_native_account_and_original_charge() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let mut started = f.start("wait").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), started.session.next_event())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), f.server.entered.notified())
        .await
        .unwrap();
    assert_eq!(f.in_flight().await, 1);
    f.revoke_key();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            started.session.collect_uncommitted()
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(started.session.is_finalized());
    started.session.detach_finalize().await;
    f.clean(1).await;
    assert_eq!(f.in_flight().await, 0);
    assert_eq!(
        f.core.finalizations.lock().unwrap()[0].outcome,
        ExecutionOutcome::Cancelled
    );
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.close().await;
}

#[tokio::test]
async fn real_core_adapter_cancel_and_plugin_disable_finish_original_session_without_duplicate_charge()
 {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for (index, revoke) in [false, true].into_iter().enumerate() {
        let mut started = f.start("wait").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), started.session.next_event())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(3), f.server.entered.notified())
            .await
            .unwrap();
        assert_eq!(f.in_flight().await, 1);
        if revoke {
            use gateway_admin::ports::plugins::PluginService;
            let mut command =
                crate::support::update(&f.installed.artifact.sha256, f.installed.revision, false);
            command.configuration = serde_json::json!({"adapter_url":f.server.url});
            command.upstream_adapter = Some(
                gateway_admin::model::plugins::PluginUpstreamAdapterBinding {
                    scope: gateway_admin::model::plugins::PluginExecutionScope::All {},
                },
            );
            f.manager
                .update(Some(&f.installed.id), command, &crate::support::actor())
                .await
                .unwrap();
        } else {
            started.session.cancel();
        }
        assert!(
            tokio::time::timeout(
                Duration::from_secs(3),
                started.session.collect_uncommitted()
            )
            .await
            .unwrap()
            .is_err()
        );
        assert!(started.session.is_finalized());
        started.session.detach_finalize().await;
        f.clean(index + 1).await;
        assert_eq!(f.in_flight().await, 0);
        assert_eq!(f.core.charges.lock().unwrap()[index].amount_usd.scaled(), 0);
        assert_eq!(
            f.core.finalizations.lock().unwrap()[index].outcome,
            ExecutionOutcome::Cancelled
        );
        assert_eq!(f.server.requests.lock().unwrap().len(), index + 1);
    }
    f.close().await;
}
