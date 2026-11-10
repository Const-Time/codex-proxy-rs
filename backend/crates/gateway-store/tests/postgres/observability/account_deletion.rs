use super::*;
use gateway_admin::{
    model::{MutationActor, MutationContext, accounts::DeleteAccounts},
    ports::store::AccountStore,
};

#[tokio::test]
async fn account_deletion_preserves_history_without_locking_historical_rows() {
    let Some(db) = TestDatabase::create_at_version("delete_history_lock", 51).await else {
        return;
    };
    let now = Utc::now();
    seed_observability_facts(&db.pool, now).await.unwrap();
    clone_request_facts(
        &db.pool,
        "req_observe_success",
        2100,
        now,
        "jsonb_build_object('id','delete_history_'||n)",
    )
    .await;
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(r) from model_requests r order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let ops_before: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(e) from ops_events e order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    // Exercise upgrade from the already-released schema with retained facts.
    super::super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    sqlx::query("delete from usage_hourly_dirty")
        .execute(&db.pool)
        .await
        .unwrap();
    // Contention must fail promptly and leave both configuration and history intact.
    let mut blocker = db.pool.begin().await.unwrap();
    sqlx::query("select id from runtime_settings where id=1 for share")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let store = super::super::admin_account_store(&db.pool);
    let result = store
        .delete_accounts(
            DeleteAccounts {
                account_ids: vec!["acct_observe".into()],
            },
            &MutationContext {
                actor: MutationActor::System,
                request_id: "delete_history_blocked".into(),
            },
        )
        .await;
    assert!(
        result.is_err(),
        "terminal delete must time out on the held revision lock"
    );
    blocker.rollback().await.unwrap();
    let linked: i64 = sqlx::query_scalar(
        "select count(*) from model_requests where provider_account_id is not null",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        linked,
        before.len() as i64,
        "failed deletion must not change historical associations"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from provider_accounts where id='acct_observe'"
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id=1")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from usage_hourly_dirty")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    // A history writer must not delay account deletion. SHARE locks reject any
    // UPDATE/DELETE of these tables, including implicit foreign-key actions.
    let mut history_lock = db.pool.begin().await.unwrap();
    sqlx::query("lock table model_requests, ops_events in share mode")
        .execute(&mut *history_lock)
        .await
        .unwrap();
    store
        .delete_accounts(
            DeleteAccounts {
                account_ids: vec!["acct_observe".into()],
            },
            &MutationContext {
                actor: MutationActor::System,
                request_id: "delete_history_retry".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from provider_accounts")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    history_lock.rollback().await.unwrap();
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(r) from model_requests r order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let ops_after: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(e) from ops_events e order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(ops_before, ops_after);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from usage_hourly_dirty")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0,
        "deletion must not invalidate historical usage totals"
    );
    let detail = observability_repository(&db.pool)
        .usage_record_detail("req_observe_success", None)
        .await
        .unwrap();
    assert_eq!(
        detail.request.provider_account_ref.as_deref(),
        Some("acct_observe")
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "manual synthetic account deletion performance check"]
async fn account_deletion_measures_large_history() {
    let Some(db) = TestDatabase::create("delete_history_perf").await else {
        return;
    };
    let now = Utc::now();
    seed_observability_facts(&db.pool, now).await.unwrap();
    clone_request_facts(
        &db.pool,
        "req_observe_success",
        100_000,
        now,
        "jsonb_build_object('id','delete_perf_'||n)",
    )
    .await;
    sqlx::query("analyze model_requests")
        .execute(&db.pool)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    super::super::admin_account_store(&db.pool)
        .delete_accounts(
            DeleteAccounts {
                account_ids: vec!["acct_observe".into()],
            },
            &MutationContext {
                actor: MutationActor::System,
                request_id: "delete_history_perf".into(),
            },
        )
        .await
        .unwrap();
    eprintln!(
        "DELETE_BENCH: 100000 synthetic historical requests; elapsed_ms={}",
        start.elapsed().as_millis()
    );
    assert!(
        sqlx::query_scalar::<_, i64>("select count(*) from model_requests")
            .fetch_one(&db.pool)
            .await
            .unwrap()
            >= 100_000
    );
    db.close().await;
}
