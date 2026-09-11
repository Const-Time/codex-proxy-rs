use gateway_core::policy::{ClientApiKeyId, PlaintextClientApiKey, RateLimits};
use gateway_store::postgres::{
    ClientApiKeySnapshot, PgRuntimeSnapshotRepository, RuntimeSnapshotRepository,
};

use super::TestDatabase;

#[test]
fn snapshot_client_policy_contains_only_common_limits() {
    let policy = ClientApiKeySnapshot {
        user_admission: None,
        id: ClientApiKeyId::new("key-1").expect("client key ID"),
        plaintext_key: PlaintextClientApiKey::new("sk_snapshot_secret").expect("plaintext key"),
        group_ids: Vec::new(),
        limits: RateLimits {
            max_concurrency: 3,
            requests_per_minute: 60,
        },
    };
    assert_eq!(policy.limits.max_concurrency, 3);
    assert!(policy.group_ids.is_empty());
    assert!(!format!("{policy:?}").contains("sk_snapshot_secret"));
}

#[tokio::test]
async fn runtime_snapshot_loads_enabled_plaintext_key_without_debug_exposure() {
    let Some(database) = TestDatabase::create("client_snapshot").await else {
        return;
    };
    let plaintext = format!("sk_{}", "s".repeat(43));
    sqlx::query(
        "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
           id, name, key, enabled, max_concurrency, requests_per_minute, created_at, updated_at
         ) values ('test-owner', 'key_snapshot', 'snapshot', $1, true, 2, 60, now(), now()) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
    )
    .bind(&plaintext)
    .execute(&database.pool)
    .await
    .expect("seed client API key");
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone())
        .load_runtime_snapshot()
        .await
        .expect("load runtime snapshot");
    assert_eq!(snapshot.client_api_keys.len(), 1);
    assert_eq!(snapshot.client_api_keys[0].group_ids.len(), 1);
    assert_eq!(
        snapshot.client_api_keys[0].plaintext_key.expose_for_auth(),
        plaintext
    );
    assert!(!format!("{snapshot:?}").contains(&plaintext));
    database.close().await;
}
