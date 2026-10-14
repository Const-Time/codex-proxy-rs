//! SDK save uses native preparation, exact receipts, the original graph and account slot.
use super::{capture, fixture::Fixture};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_core::upstream::UpstreamSendState;
use serde_json::{Map, Value, json};
use std::time::Duration;

pub(super) fn fields(f: &Fixture) -> Map<String, Value> {
    let id = f.account.as_str();
    let claims = json!({"sub":format!("subject-{id}"), "exp":(chrono::Utc::now()+chrono::Duration::hours(1)).timestamp(),
        "https://api.openai.com/auth":{"chatgpt_user_id":format!("user-{id}"),"chatgpt_account_id":format!("chatgpt-{id}"),"chatgpt_plan_type":"pro"}});
    json!({"fixture_save_token":format!("e30.{}.fixture", URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap()))}).as_object().unwrap().clone()
}
async fn stored_revision(f: &Fixture) -> u64 {
    f.ports
        .accounts()
        .load_current_credential(&f.account)
        .await
        .unwrap()
        .account
        .revision()
        .get()
}
async fn pending(
    f: &Fixture,
    started: &mut gateway_core::engine::execution::StartedExecution,
    selected: &capture::Selected,
) -> capture::Captured {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), started.session.next_event())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), f.server.entered.notified())
        .await
        .unwrap();
    let captured = selected.lock().unwrap().take().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tokio::time::timeout(Duration::from_millis(20), captured.scope.wait_until_idle())
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bound save must hold a child on the actual Core graph");
    assert_eq!(stored_revision(f).await, 1);
    assert_eq!(f.in_flight().await, 1);
    captured
}

#[tokio::test]
async fn native_bound_save_uses_native_preparation_and_new_headers_with_immutable_sdk_birth_context()
 {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let input = fields(&f);
    let expected = input["fixture_save_token"].as_str().unwrap().to_owned();
    let mut started = f
        .start_with_fields("callback-credentials-save", input, None)
        .await;
    let events = tokio::time::timeout(
        Duration::from_secs(15),
        started.session.collect_uncommitted(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(stored_revision(&f).await, 2);
    assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
    f.clean(1).await;
    let requests = f.server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains(&format!("authorization: Bearer {expected}")));
    assert!(!requests[0].contains("fixture-provider-secret"));
    assert_eq!(f.env.published.lock().unwrap().len(), 1);
    assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
    f.close().await;
}

#[tokio::test]
async fn native_bound_save_waits_for_the_actual_old_send_then_uses_the_new_epoch() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (plan, selected) = capture::plan(&f);
    let input = fields(&f);
    let expected = input["fixture_save_token"].as_str().unwrap().to_owned();
    let mut started = f
        .start_with_fields("callback-credentials-save-gate", input, Some(plan))
        .await;
    let captured = pending(&f, &mut started, &selected).await;
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.server.release.notify_one();
    let events = tokio::time::timeout(
        Duration::from_secs(15),
        started.session.collect_uncommitted(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(captured.account.credential_revision().get(), 2);
    assert_eq!(f.in_flight().await, 1);
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
    captured.scope.wait_until_idle().await;
    drop(captured);
    f.clean(1).await;
    let requests = f.server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("authorization: Bearer fixture-provider-secret"));
    assert!(requests[1].contains(&format!("authorization: Bearer {expected}")));
    assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
    f.close().await;
}

#[tokio::test]
async fn native_bound_save_manager_shutdown_drains_native_commit_and_adoption_on_the_same_graph() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (plan, selected) = capture::plan(&f);
    let mut started = f
        .start_with_fields("callback-credentials-save-gate", fields(&f), Some(plan))
        .await;
    let captured = pending(&f, &mut started, &selected).await;
    let mut lock = f.env.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE provider_accounts IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    f.server.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND wait_event_type='Lock' AND query LIKE '%provider_accounts%'").fetch_one(&f.env.pool).await.unwrap();
            if blocked > 0 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("the original native task must be waiting on the actual account store");
    {
        let shutdown = f.manager.shutdown();
        tokio::pin!(shutdown);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), shutdown.as_mut())
                .await
                .is_err()
        );
        assert_eq!(captured.account.credential_revision().get(), 1);
        assert_eq!(f.in_flight().await, 1);
        lock.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), shutdown.as_mut())
            .await
            .unwrap();
    }
    assert_eq!(captured.account.credential_revision().get(), 2);
    assert_eq!(stored_revision(&f).await, 2);
    assert_eq!(f.env.published.lock().unwrap().len(), 1);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            started.session.collect_uncommitted()
        )
        .await
        .unwrap()
        .is_err()
    );
    started.session.detach_finalize().await;
    captured.scope.wait_until_idle().await;
    drop(captured);
    f.clean(1).await;
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
    f.close().await;
}

#[tokio::test]
async fn native_bound_save_rejects_invalid_material_with_both_original_provider_codecs_without_inference_replay()
 {
    for provider in ["openai", "xai"] {
        let Some(f) = Fixture::for_provider(provider).await else {
            return;
        };
        let mut started = f
            .start_with_fields("callback-credentials-save-invalid", Map::new(), None)
            .await;
        assert!(
            tokio::time::timeout(
                Duration::from_secs(15),
                started.session.collect_uncommitted()
            )
            .await
            .unwrap()
            .is_err()
        );
        started.session.detach_finalize().await;
        f.clean(1).await;
        assert_eq!(stored_revision(&f).await, 1);
        assert!(f.env.published.lock().unwrap().is_empty());
        assert!(f.server.requests.lock().unwrap().is_empty());
        assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
        f.close().await;
    }
}
