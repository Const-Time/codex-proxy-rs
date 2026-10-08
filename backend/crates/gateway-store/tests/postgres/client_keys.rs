use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{TimeZone as _, Utc};
use gateway_admin::{
    model::client_keys::{
        ClientKeyListQuery as AdminClientKeyListQuery, ClientKeyPageSize,
        ClientKeySort as AdminClientKeySort, ClientKeySortField as AdminClientKeySortField,
        SortDirection as AdminSortDirection,
    },
    ports::store::ClientKeyStore as _,
};
use gateway_core::{
    engine::execution::ClientApiKeyUsageSink as _, lifecycle::CancellationToken,
    policy::ClientApiKeyId, task::DaemonTask as _,
};
use gateway_store::postgres::{
    ClientApiKeyCursor, ClientApiKeyCursorValue, ClientApiKeyListQuery, ClientApiKeyRepository,
    ClientApiKeySort, ClientApiKeySortDirection, ClientApiKeySortField, NewClientApiKey,
    PgAdminClientKeyStore, PgClientApiKeyRepository, PgClientApiKeyUsageSink,
};

use super::TestDatabase;

#[test]
fn client_key_requires_the_frozen_plaintext_format() {
    let key = NewClientApiKey {
        id: "key-1".to_owned(),
        name: "default".to_owned(),
        label: None,
        group_ids: Vec::new(),
        key: format!("sk_{}", "a".repeat(43)),
        max_concurrency: 0,
        requests_per_minute: 0,
    };
    assert!(key.validate().is_ok());
}

#[tokio::test]
async fn client_key_list_uses_safe_keyset_search_and_filtered_total() {
    let Some(database) = TestDatabase::create("client_key_page").await else {
        return;
    };
    for (index, label) in ["alpha", "needle", "omega"].into_iter().enumerate() {
        let id = format!("key_page_{index}");
        let key = format!(
            "sk_{}",
            char::from(b'a' + index as u8).to_string().repeat(43)
        );
        sqlx::query(
            "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
               id, name, label, key, enabled, max_concurrency, requests_per_minute,
               created_at, updated_at
             ) values ('test-owner', $1, $2, $2, $3, true, 0, 0,
                       now() - ($4::bigint * interval '1 minute'), now()) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
        )
        .bind(id)
        .bind(label)
        .bind(key)
        .bind(index as i64)
        .execute(&database.pool)
        .await
        .expect("seed paged client key");
    }
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    let first = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            owner_user_id: "test-owner".into(),

            cursor: None,
            page_size: 2,
            search: None,
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("first client key page");
    assert_eq!(first.total, 3);
    assert_eq!(first.items.len(), 2);
    assert!(first.next_cursor.is_some());
    let second = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            owner_user_id: "test-owner".into(),

            cursor: first.next_cursor,
            page_size: 2,
            search: None,
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("second client key page");
    assert_eq!(second.total, 3);
    assert_eq!(second.items.len(), 1);

    let searched = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            owner_user_id: "test-owner".into(),

            cursor: None,
            page_size: 10,
            search: Some("needle".to_owned()),
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("searched client key page");
    assert_eq!(searched.total, 1);
    assert_eq!(searched.items[0].label.as_deref(), Some("needle"));
    assert_eq!(searched.items[0].prefix.len(), 10);
    database.close().await;
}

#[test]
fn client_key_cursor_is_bound_to_one_sort_contract() {
    let created_sort = ClientApiKeySort::default();
    assert!(
        ClientApiKeyListQuery {
            owner_user_id: "test-owner".into(),

            cursor: None,
            page_size: u16::MAX,
            search: None,
            sort: created_sort,
        }
        .validate()
        .is_ok()
    );
    let cursor = ClientApiKeyCursor::new(
        created_sort,
        ClientApiKeyCursorValue::CreatedAt(chrono::Utc::now()),
        "key-cursor",
    )
    .expect("valid cursor");
    let query = ClientApiKeyListQuery {
        owner_user_id: "test-owner".into(),

        cursor: Some(cursor),
        page_size: 10,
        search: None,
        sort: ClientApiKeySort {
            field: ClientApiKeySortField::Name,
            direction: ClientApiKeySortDirection::Asc,
        },
    };
    assert!(query.validate().is_err());
    assert!(
        ClientApiKeyCursor::new(
            created_sort,
            ClientApiKeyCursorValue::Enabled(true),
            "key-cursor",
        )
        .is_err()
    );
}

#[tokio::test]
async fn admin_client_key_adapter_should_preserve_the_full_nonzero_u16_page_size() {
    let Some(database) = TestDatabase::create("admin_client_key_max_page").await else {
        return;
    };
    let page = PgAdminClientKeyStore::new(database.pool.clone())
        .list_client_keys(
            "test-owner",
            AdminClientKeyListQuery {
                cursor: None,
                page_size: ClientKeyPageSize::new(u16::MAX).expect("maximum page size"),
                search: None,
                sort: AdminClientKeySort {
                    field: AdminClientKeySortField::CreatedAt,
                    direction: AdminSortDirection::Desc,
                },
            },
        )
        .await
        .expect("maximum Client Key page size");

    assert_eq!(page.total, 0);
    assert!(page.items.is_empty());
    database.close().await;
}

#[tokio::test]
async fn client_key_database_sort_is_stable_and_keeps_null_last_used_at_last() {
    let Some(database) = TestDatabase::create("client_key_sort").await else {
        return;
    };
    for (index, (id, name, enabled, created_at, last_used_at)) in [
        ("key_sort_a", "Zulu", false, "2026-01-01T00:00:00Z", None),
        (
            "key_sort_b",
            "alpha",
            true,
            "2026-01-02T00:00:00Z",
            Some("2026-01-03T00:00:00Z"),
        ),
        (
            "key_sort_c",
            "Beta",
            false,
            "2026-01-03T00:00:00Z",
            Some("2026-01-01T00:00:00Z"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(
            "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
               id, name, key, enabled, max_concurrency, requests_per_minute,
               last_used_at, created_at, updated_at
             ) values ('test-owner', $1, $2, $3, $4, 0, 0, $5::timestamptz, $6::timestamptz,
                       $6::timestamptz) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
        )
        .bind(id)
        .bind(name)
        .bind(format!(
            "sk_{}",
            char::from(b'a' + index as u8).to_string().repeat(43)
        ))
        .bind(enabled)
        .bind(last_used_at)
        .bind(created_at)
        .execute(&database.pool)
        .await
        .expect("seed sorted client key");
    }
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    for (sort, expected) in [
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::Name,
                direction: ClientApiKeySortDirection::Asc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::Enabled,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::CreatedAt,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_c", "key_sort_b", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::LastUsedAt,
                direction: ClientApiKeySortDirection::Asc,
            },
            vec!["key_sort_c", "key_sort_b", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::LastUsedAt,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
    ] {
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let page = repository
                .list_client_api_keys(ClientApiKeyListQuery {
                    owner_user_id: "test-owner".into(),

                    cursor,
                    page_size: 1,
                    search: None,
                    sort,
                })
                .await
                .expect("sorted client key page");
            ids.extend(page.items.into_iter().map(|item| item.id));
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        assert_eq!(ids, expected);
    }
    database.close().await;
}

#[tokio::test]
async fn client_key_usage_touch_should_preserve_the_latest_observed_timestamp() {
    let Some(database) = TestDatabase::create("client_key_usage_touch").await else {
        return;
    };
    sqlx::query(
        "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('test-owner', 'key_usage_touch', 'usage', $1, true, 0, 0, now(), now()) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
    )
    .bind(format!("sk_{}", "t".repeat(43)))
    .execute(&database.pool)
    .await
    .expect("seed client key");
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    let first = Utc
        .with_ymd_and_hms(2026, 7, 24, 0, 0, 0)
        .single()
        .expect("first timestamp");
    let latest = Utc
        .with_ymd_and_hms(2026, 7, 24, 0, 0, 1)
        .single()
        .expect("latest timestamp");
    repository
        .touch_client_api_keys(&BTreeMap::from([("key_usage_touch".to_owned(), first)]))
        .await
        .expect("record first API Key use");
    repository
        .touch_client_api_keys(&BTreeMap::from([("key_usage_touch".to_owned(), latest)]))
        .await
        .expect("record latest API Key use");

    let record = repository
        .get_client_api_key("key_usage_touch")
        .await
        .expect("load API Key")
        .expect("client key exists");
    assert_eq!(record.last_used_at, Some(latest));
    database.close().await;
}

#[tokio::test]
async fn client_key_usage_daemon_should_flush_pending_touch_on_shutdown() {
    let Some(database) = TestDatabase::create("client_key_usage_daemon").await else {
        return;
    };
    sqlx::query(
        "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('test-owner', 'key_usage_daemon', 'usage', $1, true, 0, 0, now(), now()) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
    )
    .bind(format!("sk_{}", "u".repeat(43)))
    .execute(&database.pool)
    .await
    .expect("seed client key");

    let (sink, writer) =
        PgClientApiKeyUsageSink::with_flush_delay(database.pool.clone(), Duration::from_secs(60));
    let writer = Arc::new(writer);
    let cancellation = CancellationToken::new();
    let task = {
        let writer = Arc::clone(&writer);
        let cancellation = cancellation.clone();
        tokio::spawn(async move { writer.run(cancellation).await })
    };
    sink.record_used(&ClientApiKeyId::new("key_usage_daemon").expect("valid client API Key ID"));
    cancellation.cancel();
    task.await
        .expect("join client key usage daemon")
        .expect("stop client key usage daemon");

    let last_used_at: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "select last_used_at from client_api_keys where id = 'key_usage_daemon'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load client key last-used timestamp");
    assert!(last_used_at.is_some());
    database.close().await;
}

#[tokio::test]
async fn dedicated_reveal_returns_plaintext_without_debug_exposure() {
    let Some(database) = TestDatabase::create("client_key_reveal").await else {
        return;
    };
    let plaintext = format!("sk_{}", "r".repeat(43));
    sqlx::query(
        "with fixture_group as (
           insert into account_groups(id, name, color, created_at, updated_at) values ('grp_ffffffffffffffffffffffffffffffff','Fixture group','#64748BFF',now(),now()) on conflict do nothing
         ), fixture_key as (
           insert into client_api_keys (owner_user_id,
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('test-owner', 'key_reveal', 'reveal', $1, true, 1, 2, now(), now()) returning id
         ) insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
           select id, 'grp_ffffffffffffffffffffffffffffffff', now() from fixture_key",
    )
    .bind(&plaintext)
    .execute(&database.pool)
    .await
    .expect("seed revealed client key");
    let revealed = PgClientApiKeyRepository::new(database.pool.clone())
        .reveal_client_api_key("key_reveal")
        .await
        .expect("reveal client key")
        .expect("revealed key exists");
    assert_eq!(revealed.key, plaintext);
    assert!(!format!("{revealed:?}").contains(&plaintext));
    database.close().await;
}

#[test]
fn client_key_debug_redacts_plaintext() {
    let secret = format!("sk_{}", "s".repeat(43));
    let key = NewClientApiKey {
        id: "key-1".to_owned(),
        name: "default".to_owned(),
        label: None,
        group_ids: Vec::new(),
        key: secret.clone(),
        max_concurrency: 0,
        requests_per_minute: 0,
    };
    assert!(!format!("{key:?}").contains(&secret));
}

#[tokio::test]
async fn plugin_keys_preserve_owner_shared_budgets_idempotency_and_deleted_tombstones() {
    use gateway_admin::model::{
        MutationActor, MutationContext, client_keys::NewClientKey, plugins::*,
    };
    use gateway_admin::ports::store::AdminStoreErrorKind;
    let Some(db) = TestDatabase::create("plugin_keys").await else {
        return;
    };
    let keys = PgAdminClientKeyStore::new(db.pool.clone());
    let group = "grp_00000000000000000000000000000077";
    sqlx::query(
        "insert into plugin_artifacts(sha256,info,archive) values($1,'{}',decode('00','hex'))",
    )
    .bind("a".repeat(64))
    .execute(&db.pool)
    .await
    .unwrap();
    for id in ["plugin-a", "plugin-b"] {
        sqlx::query("insert into plugin_instances(id,revision,name,artifact_sha256,enabled,configuration,secrets) values($1,1,$1,$2,true,'{}','{}')").bind(id).bind("a".repeat(64)).execute(&db.pool).await.unwrap();
    }
    sqlx::query("insert into account_groups(id,name,color,daily_limit_usd,weekly_limit_usd,created_at,updated_at) values($1,'Plugin','#64748BFF',2,10,now(),now())").bind(group).execute(&db.pool).await.unwrap();
    sqlx::query("insert into plugin_managed_groups(instance_id,resource_key,group_id) values('plugin-a','main',$1)").bind(group).execute(&db.pool).await.unwrap();
    sqlx::query("insert into users(id,username,password_hash,role,created_at,updated_at) values('other-owner','other','unused','admin',now(),now())").execute(&db.pool).await.unwrap();
    let scope = |plugin: &str, owner: &str, revision| PluginMutationScope {
        instance_id: plugin.into(),
        revision,
        actor: MutationContext {
            actor: MutationActor::AdminSession {
                admin_user_id: owner.into(),
            },
            request_id: "plugin-key-test".into(),
        },
    };
    let command =
        |resource: &str, id: &str, secret: &str, daily: &str, weekly: &str| PluginKeyCreate {
            resource_key: resource.into(),
            group_resource_key: "main".into(),
            key: NewClientKey {
                id: ClientApiKeyId::new(id).unwrap(),
                name: id.into(),
                label: None,
                group_ids: vec![],
                limits: Default::default(),
                plaintext: format!("sk_{}", secret.repeat(43)),
            },
            budget: gateway_core::engine::budget::ClientBudgetLimits {
                daily_usd: daily.parse().unwrap(),
                weekly_usd: weekly.parse().unwrap(),
            },
        };
    let mismatch = keys
        .ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("key", "bad-budget", "z", "3", "10"),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatch.kind(), AdminStoreErrorKind::Conflict);
    let (a, b) = tokio::join!(
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("key", "key_a", "a", "2", "10")
        ),
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("key", "key_b", "b", "2", "10")
        )
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    let id = a.value.id.clone();
    let stored: (String, String) =
        sqlx::query_as("select owner_user_id,key from client_api_keys where id=$1")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(stored.0, "test-owner");
    let count: i64 = sqlx::query_scalar("select count(*) from client_api_keys")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    sqlx::query("update client_api_keys set name='Admin edited',enabled=false where id=$1")
        .bind(&id)
        .execute(&db.pool)
        .await
        .unwrap();
    let existing = keys
        .ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("key", "other-id", "x", "0", "0"),
        )
        .await
        .unwrap();
    assert_eq!(existing.value.name, "Admin edited");
    assert!(!existing.value.enabled);
    assert!(
        keys.ensure_plugin_key(
            scope("plugin-a", "other-owner", 1),
            command("key", "foreign-owner", "y", "2", "10")
        )
        .await
        .is_err()
    );
    assert!(
        keys.ensure_plugin_key(
            scope("plugin-b", "test-owner", 1),
            command("key", "foreign-plugin", "y", "2", "10")
        )
        .await
        .is_err()
    );
    sqlx::query("update users set group_grants_configured=true where id='test-owner'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("second", "no-grant", "c", "2", "10")
        )
        .await
        .is_err()
    );
    sqlx::query("insert into user_account_groups(user_id,account_group_id,quota_multiplier) values('test-owner',$1,2)").bind(group).execute(&db.pool).await.unwrap();
    assert!(
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("second", "wrong-factor", "c", "2", "10")
        )
        .await
        .is_err()
    );
    keys.ensure_plugin_key(
        scope("plugin-a", "test-owner", 1),
        command("second", "key_second", "c", "4", "20"),
    )
    .await
    .unwrap();
    let limits: (String, String) = sqlx::query_as(
        "select daily_limit_usd::text,weekly_limit_usd::text from account_groups where id=$1",
    )
    .bind(group)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(limits, ("2.0000000000".into(), "10.0000000000".into()));
    sqlx::query("delete from client_api_keys where id=$1")
        .bind(&id)
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("key", "recreate", "d", "4", "20")
        )
        .await
        .unwrap_err()
        .kind(),
        AdminStoreErrorKind::NotFound
    );
    sqlx::query("update plugin_instances set revision=2,enabled=false where id='plugin-a'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        keys.ensure_plugin_key(
            scope("plugin-a", "test-owner", 1),
            command("third", "stale", "e", "4", "20")
        )
        .await
        .is_err()
    );
    sqlx::query("delete from plugin_instances where id='plugin-a'")
        .execute(&db.pool)
        .await
        .unwrap();
    let retained: i64 = sqlx::query_scalar("select count(*) from client_api_keys")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(retained, 1);
    let windows: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(windows, 0);
    let audit: Vec<serde_json::Value> = sqlx::query_scalar(
        "select to_jsonb(a) from admin_audit_events a where action='plugin.key.ensure'",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(audit.len(), 2);
    assert!(!serde_json::to_string(&audit).unwrap().contains(&stored.1));
    db.close().await;
}
