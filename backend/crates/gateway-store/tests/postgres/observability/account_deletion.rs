use super::*;
use gateway_admin::{
    model::{MutationActor, MutationContext, accounts::DeleteAccounts},
    ports::store::AccountStore,
};

#[tokio::test]
async fn account_deletion_detaches_history_without_holding_the_global_revision_lock() {
    let Some(db) = TestDatabase::create("delete_history_lock").await else {
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
    let before: Vec<serde_json::Value> = sqlx::query_scalar(
        "select to_jsonb(r)-'provider_account_id' from model_requests r order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let ops_before: Vec<serde_json::Value> = sqlx::query_scalar(
        "select to_jsonb(e)-'provider_account_id' from ops_events e order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    sqlx::query("delete from usage_hourly_dirty")
        .execute(&db.pool)
        .await
        .unwrap();
    // Final commit is blocked, but preparatory history batches must complete
    // without taking the configuration lock used by budget admission.
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
        linked, 0,
        "history was detached in bounded committed batches"
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
    let after: Vec<serde_json::Value> = sqlx::query_scalar(
        "select to_jsonb(r)-'provider_account_id' from model_requests r order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let ops_after: Vec<serde_json::Value> = sqlx::query_scalar(
        "select to_jsonb(e)-'provider_account_id' from ops_events e order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(ops_before, ops_after);
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
