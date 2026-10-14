use super::fixture::Fixture;
use gateway_core::upstream::UpstreamSendState;
use std::time::Duration;

#[tokio::test]
async fn real_core_bound_credentials_sdk_reads_only_native_selected_account_and_keeps_one_lease() {
    for provider in ["openai", "xai"] {
        let Some(f) = Fixture::for_provider(provider).await else {
            return;
        };
        let mut started = f.start("callback-credentials").await;
        assert!(f.server.requests.lock().unwrap().is_empty());
        let events = tokio::time::timeout(
            Duration::from_secs(20),
            started.session.collect_uncommitted(),
        )
        .await
        .expect("native bound credential probes must finish")
        .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
        started.session.commit_downstream(Some(200)).await.unwrap();
        started.session.detach_finalize().await;
        f.clean(1).await;
        let requests = f.server.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("authorization: Bearer fixture-provider-secret"));
        assert!(!requests[0].contains("fixture-forbidden-secret"));
        assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
        assert_eq!(f.in_flight().await, 0);
        assert!(
            f.env.published.lock().unwrap().is_empty(),
            "bound read probes must not write native accounts"
        );
        f.close().await;
    }
}

async fn barrier(f: &Fixture, started: &mut gateway_core::engine::execution::StartedExecution) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), started.session.next_event())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), f.server.entered.notified())
        .await
        .unwrap();
    assert_eq!(f.in_flight().await, 1);
}
async fn finish(f: Fixture, mut started: gateway_core::engine::execution::StartedExecution) {
    f.server.release.notify_one();
    let events = started.session.collect_uncommitted().await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
    f.clean(1).await;
    assert_eq!(
        f.server.requests.lock().unwrap().len(),
        1,
        "credential rejection must not replay inference"
    );
    f.close().await;
}

#[tokio::test]
async fn real_core_bound_credentials_reject_native_revision_change_while_original_lease_is_live() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let mut started = f.start("callback-credentials-stale").await;
        barrier(&f, &mut started).await;
        f.advance_credential().await;
        finish(f, started).await;
    }
}

#[tokio::test]
async fn real_core_bound_credentials_read_fences_persisted_plugin_disable_and_stale_generation() {
    for enabled in [false, true] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let mut started = f.start("callback-credentials-plugin-fence").await;
        barrier(&f, &mut started).await;
        let store = f.env.store.plugin_store();
        let mut record = store
            .list()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == f.installed.id)
            .unwrap();
        let expected = record.revision;
        record.revision += 1;
        record.enabled = enabled;
        store
            .save(&record, expected, &crate::support::actor())
            .await
            .unwrap();
        // Persist the real row directly, leaving the old SDK generation alive to exercise the DB fence.
        finish(f, started).await;
    }
}
