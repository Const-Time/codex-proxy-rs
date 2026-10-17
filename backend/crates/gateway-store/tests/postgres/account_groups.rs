use std::collections::BTreeMap;

use gateway_admin::{
    model::{
        MutationActor, MutationContext, PageSize,
        account_groups::{
            AccountGroupColor, AccountGroupListQuery, DeleteAccountGroup, NewAccountGroup,
        },
        client_keys::NewClientKey,
        client_keys::UpdateClientKey,
    },
    ports::store::{AccountGroupStore, AdminStoreErrorKind, ClientKeyStore},
};
use gateway_core::{
    policy::{ClientApiKeyId, RateLimits},
    routing::AccountGroupId,
};
use gateway_store::postgres::{PgAccountGroupRepository, PgAdminClientKeyStore};

use super::TestDatabase;

const MIXED_GROUP: &str = "grp_00000000000000000000000000000001";
const EMPTY_GROUP: &str = "grp_00000000000000000000000000000002";

fn page_query() -> AccountGroupListQuery {
    AccountGroupListQuery {
        page: 1,
        page_size: PageSize::new(20).unwrap(),
        search: None,
        enabled: None,
    }
}

#[tokio::test]
async fn catalog_needs_neither_request_history_nor_members_and_honors_filters() {
    let Some(db) = TestDatabase::create("group_catalog").await else {
        return;
    };
    sqlx::query(
        "insert into account_groups(id,name,color,enabled,created_at,updated_at) values
        ($1,'Alpha','#2563EBFF',true,now(),now()),($2,'Beta','#2563EBFF',false,now(),now())",
    )
    .bind(MIXED_GROUP)
    .bind(EMPTY_GROUP)
    .execute(&db.pool)
    .await
    .unwrap();
    // These tables are intentionally inaccessible under their production names.
    // A selector must not accidentally regress to the full statistics/member path.
    sqlx::raw_sql(
        "alter table model_requests rename to hidden_requests;
        alter table account_group_accounts rename to hidden_members;
        alter table client_api_key_groups rename to hidden_keys;",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let store = PgAccountGroupRepository::new(db.pool.clone());
    let all = store.account_group_catalog(page_query()).await.unwrap();
    assert_eq!(all.len(), 2);
    let filtered = store
        .account_group_catalog(AccountGroupListQuery {
            search: Some("bet".into()),
            enabled: Some(false),
            ..page_query()
        })
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id.as_str(), EMPTY_GROUP);
    assert!(!filtered[0].enabled);
    for page in 1..=3 {
        let items = store
            .account_group_catalog(AccountGroupListQuery {
                page,
                page_size: PageSize::new(1).unwrap(),
                ..page_query()
            })
            .await
            .unwrap();
        if page <= 2 {
            assert_eq!(items[0].id, all[(page - 1) as usize].id);
        } else {
            assert!(items.is_empty());
        }
    }
    db.close().await;
}

async fn assert_costs_equal_raw(pool: &sqlx::PgPool) {
    let page = PgAccountGroupRepository::new(pool.clone())
        .list_account_groups(page_query())
        .await
        .unwrap();
    for group in page.items {
        let raw: (String,String) = sqlx::query_as("select
            coalesce(sum(billed_cost_amount) filter(where started_at>=date_trunc('day',now() at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai'),0)::text,
            coalesce(sum(billed_cost_amount),0)::text
            from model_requests where routing_group_refs @> array[$1]::text[]
              and started_at>=now()-make_interval(days=>(select usage_retention_days::int from runtime_settings where id=1))
              and outcome='succeeded' and downstream_committed_at is not null
              and (client_status_code between 200 and 399 or (client_transport='websocket' and client_status_code is null))
              and cost_currency='USD' and cost_amount is not null and total_tokens>0")
            .bind(group.id.as_str()).fetch_one(pool).await.unwrap();
        let canonical = |s: &str| {
            s.parse::<gateway_core::metering::Decimal>()
                .unwrap()
                .canonical()
        };
        assert_eq!(group.usage.today_usd.as_str(), canonical(&raw.0));
        assert_eq!(group.usage.retained_total_usd.as_str(), canonical(&raw.1));
    }
}

#[tokio::test]
#[ignore = "manual local PostgreSQL benchmark; synthetic data only"]
async fn group_cost_rollups_measure_large_history() {
    use gateway_store::postgres::PgUsageRollupRepository;
    let Some(db) = TestDatabase::create("group_cost_perf").await else {
        return;
    };
    sqlx::query(
        "insert into account_groups(id,name,color,created_at,updated_at) values
        ($1,'One','#2563EBFF',now(),now()),($2,'Two','#2563EBFF',now(),now())",
    )
    .bind(MIXED_GROUP)
    .bind(EMPTY_GROUP)
    .execute(&db.pool)
    .await
    .unwrap();
    seed_account(&db.pool, "acct_perf", "openai", "Performance").await;
    sqlx::query("insert into model_requests(id,client_api_key_ref,config_revision,protocol,operation,endpoint,
        client_transport,requested_model_id,provider_kind,provider_account_id,provider_account_ref,
        attempt_count,upstream_transport,upstream_send_state,downstream_committed_at,outcome,client_status_code,total_tokens,
        cost_source,cost_amount,cost_currency,started_at,deadline_at,completed_at,
        routing_scope,routing_group_refs,routing_group_names_snapshot)
        select 'perf-'||n,'perf-key',1,'openai','responses','/v1/responses','http_sse','perf-model',
          'openai','acct_perf','acct_perf',1,'http_sse','sent',now(),'succeeded',200,1000,'calculated',0.123,'USD',
          date_trunc('hour',now())-(2+n%168)*interval '1 hour',now()+interval '1 minute',now(),
          'groups',array[$1,$2]::text[],jsonb_build_array($1::text,$2::text) from generate_series(1,100000) n")
        .bind(MIXED_GROUP).bind(EMPTY_GROUP).execute(&db.pool).await.unwrap();
    sqlx::query("analyze model_requests")
        .execute(&db.pool)
        .await
        .unwrap();
    let store = PgAccountGroupRepository::new(db.pool.clone());
    let start = std::time::Instant::now();
    let raw = store.list_account_groups(page_query()).await.unwrap();
    let raw_ms = start.elapsed().as_millis();
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    while worker.refresh_batch(chrono::Utc::now()).await.unwrap() > 0 {}
    sqlx::raw_sql("analyze usage_hourly_stats; analyze usage_hourly_covered;")
        .execute(&db.pool)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let rolled = store.list_account_groups(page_query()).await.unwrap();
    let rolled_ms = start.elapsed().as_millis();
    assert_eq!(raw, rolled);
    assert_costs_equal_raw(&db.pool).await;
    let start = std::time::Instant::now();
    let catalog = store.account_group_catalog(page_query()).await.unwrap();
    let catalog_ms = start.elapsed().as_millis();
    assert_eq!(catalog.len(), 2);
    eprintln!(
        "GROUP_COST_BENCH: 100000 requests; raw_ms={raw_ms}; hybrid_ms={rolled_ms}; catalog_ms={catalog_ms}; exact_values=PASS"
    );
    db.close().await;
}

#[tokio::test]
async fn group_cost_rollups_preserve_raw_semantics_and_repair_late_changes_after_upgrade() {
    use gateway_store::postgres::PgUsageRollupRepository;
    let Some(db) = TestDatabase::create_at_version("group_rollup_upgrade", 43).await else {
        return;
    };
    sqlx::query(
        "insert into account_groups(id,name,color,created_at,updated_at) values
        ($1,'One','#2563EBFF',now(),now()),($2,'Two','#2563EBFF',now(),now())",
    )
    .bind(MIXED_GROUP)
    .bind(EMPTY_GROUP)
    .execute(&db.pool)
    .await
    .unwrap();
    seed_account(&db.pool, "acct_rollup", "openai", "Rollup").await;
    for (id, amount) in [
        ("old", "1"),
        ("recent", "2"),
        ("late", "3"),
        ("websocket", "4"),
        ("http", "8"),
        ("recovered", "16"),
        ("partial", "32"),
        ("expired", "64"),
        ("future", "128"),
    ] {
        seed_group_cost_snapshot(&db.pool, id, "acct_rollup", MIXED_GROUP, amount).await;
    }
    sqlx::query("update runtime_settings set usage_retention_days=31 where id=1")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update model_requests set started_at=case id
        when 'recent' then now()-interval '1 minute' when 'future' then now()+interval '1 hour'
        when 'partial' then now()-interval '31 days'+interval '1 minute'
        when 'expired' then now()-interval '31 days'-interval '1 minute'
        else date_trunc('hour',now())-interval '3 hours' end,
        deadline_at=now()+interval '2 hours',completed_at=now()+interval '1 hour 1 minute',
        routing_group_refs=array[$1,$1,$2]::text[], routing_group_names_snapshot=jsonb_build_array($1::text,$1::text,$2::text)")
        .bind(MIXED_GROUP).bind(EMPTY_GROUP).execute(&db.pool).await.unwrap();
    sqlx::raw_sql("update model_requests set client_status_code=null where id in ('websocket','http');
        update model_requests set client_transport='websocket' where id='websocket';
        update model_requests set recovered_at=completed_at,recovery_request_id='recent',recovery_attempt_count=1,recovery_retry_delay_ms=0,recovery_total_latency_ms=1 where id='recovered';
        update model_requests set cost_amount=null,cost_currency=null,cost_source='unavailable' where id='late';
        insert into usage_hourly_covered select distinct date_bin(interval '1 hour',started_at,timestamptz '1970-01-01') from model_requests;
        delete from usage_hourly_dirty;")
        .execute(&db.pool).await.unwrap();
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    assert!(sqlx::query_scalar::<_,bool>("select not exists(select 1 from usage_hourly_covered c where not exists(select 1 from usage_hourly_dirty d where d.bucket_start=c.bucket_start))")
        .fetch_one(&db.pool).await.unwrap());
    assert_costs_equal_raw(&db.pool).await;
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    while worker.refresh_batch(chrono::Utc::now()).await.unwrap() > 0 {}
    assert_costs_equal_raw(&db.pool).await;
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    // Missing pricing arrives after the stable hour was already projected.
    sqlx::query("update model_requests set cost_amount=7,cost_currency='USD',cost_source='provider_reported' where id='late'").execute(&db.pool).await.unwrap();
    assert_costs_equal_raw(&db.pool).await;
    while worker.refresh_batch(chrono::Utc::now()).await.unwrap() > 0 {}
    assert_costs_equal_raw(&db.pool).await;
    // A later deletion and routing change must also invalidate the old projection.
    sqlx::query("delete from model_requests where id='old'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update model_requests set routing_group_refs=array[$1]::text[],routing_group_names_snapshot=jsonb_build_array($1::text) where id='websocket'")
        .bind(EMPTY_GROUP).execute(&db.pool).await.unwrap();
    assert_costs_equal_raw(&db.pool).await;
    while worker.refresh_batch(chrono::Utc::now()).await.unwrap() > 0 {}
    assert_costs_equal_raw(&db.pool).await;
    db.close().await;
}

#[tokio::test]
async fn user_counts_follow_grants_without_counting_keys_or_deleted_users() {
    let Some(database) = TestDatabase::create("group_user_counts").await else {
        return;
    };
    let groups = PgAccountGroupRepository::new(database.pool.clone());
    for id in [MIXED_GROUP, EMPTY_GROUP] {
        groups
            .create_account_group(
                NewAccountGroup {
                    model_multipliers: Default::default(),
                    id: group_id(id),
                    name: id.into(),
                    description: None,
                    color: group_color("#2563EBFF"),
                    budget: Default::default(),
                },
                &context("create-count-group"),
            )
            .await
            .unwrap();
    }
    sqlx::raw_sql("insert into users(id, username, password_hash, enabled, deleted_at, created_at, updated_at) values
        ('active', 'active@example.com', 'hash', true, null, now(), now()),
        ('disabled', 'disabled@example.com', 'hash', false, null, now(), now()),
        ('deleted', 'deleted@example.com', 'hash', false, now(), now(), now()),
        ('unassigned', 'unassigned@example.com', 'hash', true, null, now(), now())")
        .execute(&database.pool).await.unwrap();
    for id in ["active", "disabled", "deleted", "test-owner"] {
        sqlx::query("insert into user_account_groups(user_id, account_group_id) values ($1, $2)")
            .bind(id)
            .bind(MIXED_GROUP)
            .execute(&database.pool)
            .await
            .unwrap();
    }
    // The same administrator has both an implicit grant and an explicit grant,
    // and owns two keys: it must still count as one user.
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    for id in ["count-key-one", "count-key-two"] {
        keys.create_client_key(new_key(id, vec![group_id(MIXED_GROUP)]), &context(id))
            .await
            .unwrap();
    }
    let query = || AccountGroupListQuery {
        page: 1,
        page_size: PageSize::new(20).unwrap(),
        search: None,
        enabled: None,
    };
    let page = groups.list_account_groups(query()).await.unwrap();
    let mixed = page
        .items
        .iter()
        .find(|group| group.id.as_str() == MIXED_GROUP)
        .unwrap();
    assert_eq!(
        mixed.user_count, 3,
        "active, disabled, and administrator, independently of key count"
    );
    assert_eq!(mixed.client_key_count, 2);
    assert_eq!(
        page.items
            .iter()
            .find(|group| group.id.as_str() == EMPTY_GROUP)
            .unwrap()
            .user_count,
        1,
        "administrator with implicit access"
    );
    sqlx::query("update users set group_grants_configured = true where id = 'test-owner'")
        .execute(&database.pool)
        .await
        .unwrap();
    let page = groups.list_account_groups(query()).await.unwrap();
    assert_eq!(
        page.items
            .iter()
            .find(|group| group.id.as_str() == EMPTY_GROUP)
            .unwrap()
            .user_count,
        0
    );
    sqlx::query("delete from user_account_groups where user_id = 'active'")
        .execute(&database.pool)
        .await
        .unwrap();
    let page = groups.list_account_groups(query()).await.unwrap();
    assert_eq!(
        page.items
            .iter()
            .find(|group| group.id.as_str() == MIXED_GROUP)
            .unwrap()
            .user_count,
        2
    );
    database.close().await;
}

#[tokio::test]
async fn group_deletion_preserves_budget_history_without_blocking_removal() {
    let Some(database) = TestDatabase::create("group_delete_budgets").await else {
        return;
    };
    let groups = PgAccountGroupRepository::new(database.pool.clone());
    for (id, reference_sql) in [
        (
            MIXED_GROUP,
            "insert into user_group_budget_windows(user_id, account_group_id, daily_start, daily_end, weekly_start, weekly_end) values ('test-owner', $1, now(), now() + interval '1 day', now(), now() + interval '7 days')",
        ),
        (
            EMPTY_GROUP,
            "insert into user_group_charge_events(request_id, user_id, account_group_id, client_api_key_ref, amount_usd, completed_at) values ('deleted-key-charge', 'test-owner', $1, 'deleted-key', 0, now())",
        ),
    ] {
        groups
            .create_account_group(
                NewAccountGroup {
                    model_multipliers: Default::default(),
                    id: group_id(id),
                    name: id.to_owned(),
                    description: None,
                    color: group_color("#2563EBFF"),
                    budget: Default::default(),
                },
                &context("create-budget-group"),
            )
            .await
            .expect("create group");
        sqlx::query(reference_sql)
            .bind(id)
            .execute(&database.pool)
            .await
            .expect("seed retained reference");
        let revision = current_revision(&database.pool).await;
        let audits = audit_count(&database.pool).await;
        let result = groups
            .delete_account_group(
                DeleteAccountGroup { id: group_id(id) },
                &context("delete-budget-group"),
            )
            .await
            .expect("history must not prevent deletion of unused configuration");
        assert!(result.record.is_none());
        assert_eq!(current_revision(&database.pool).await, revision + 1);
        assert_eq!(audit_count(&database.pool).await, audits + 1);
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from account_groups where id = $1)")
                .bind(id)
                .fetch_one(&database.pool)
                .await
                .unwrap();
        assert!(!exists);
    }
    for table in ["user_group_budget_windows", "user_group_charge_events"] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("select count(*) from {table}")))
                .fetch_one(&database.pool)
                .await
                .expect("retained reference count");
        assert_eq!(count, 1);
    }
    database.close().await;
}

#[tokio::test]
async fn unreferenced_group_deletion_removes_memberships_but_preserves_accounts() {
    let Some(database) = TestDatabase::create("group_delete_empty").await else {
        return;
    };
    let groups = PgAccountGroupRepository::new(database.pool.clone());
    groups
        .create_account_group(
            NewAccountGroup {
                model_multipliers: Default::default(),
                id: group_id(EMPTY_GROUP),
                name: "Unused group".into(),
                description: None,
                color: group_color("#2563EBFF"),
                budget: Default::default(),
            },
            &context("create-unused-group"),
        )
        .await
        .expect("create group");
    seed_account(
        &database.pool,
        "acct_group_delete",
        "openai",
        "Retained account",
    )
    .await;
    assign_accounts(&database.pool, EMPTY_GROUP, &["acct_group_delete"]).await;
    sqlx::query(
        "insert into user_account_groups(user_id, account_group_id) values ('test-owner', $1)",
    )
    .bind(EMPTY_GROUP)
    .execute(&database.pool)
    .await
    .expect("grant group");
    let revision = current_revision(&database.pool).await;
    let audits = audit_count(&database.pool).await;
    let result = groups
        .delete_account_group(
            DeleteAccountGroup {
                id: group_id(EMPTY_GROUP),
            },
            &context("delete-unused-group"),
        )
        .await
        .expect("delete unreferenced group");
    assert!(result.record.is_none());
    assert_eq!(current_revision(&database.pool).await, revision + 1);
    assert_eq!(audit_count(&database.pool).await, audits + 1);
    for table in [
        "account_groups",
        "user_account_groups",
        "account_group_accounts",
    ] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("select count(*) from {table}")))
                .fetch_one(&database.pool)
                .await
                .expect("membership count");
        assert_eq!(count, 0);
    }
    let accounts: i64 = sqlx::query_scalar("select count(*) from provider_accounts")
        .fetch_one(&database.pool)
        .await
        .expect("account count");
    assert_eq!(accounts, 1);
    database.close().await;
}

#[tokio::test]
async fn groups_aggregate_cross_provider_members_and_key_bindings_without_multiplication() {
    let Some(database) = TestDatabase::create("account_group_aggregate").await else {
        return;
    };
    seed_account(
        &database.pool,
        "acct_group_openai",
        "openai",
        "OpenAI Account",
    )
    .await;
    seed_account(&database.pool, "acct_group_xai", "xai", "xAI Account").await;
    sqlx::query(
        "update provider_accounts set concurrency_limit = 4 where id = 'acct_group_openai'",
    )
    .execute(&database.pool)
    .await
    .expect("set account concurrency override");

    let groups = PgAccountGroupRepository::new(database.pool.clone());
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let mixed_group = group_id(MIXED_GROUP);
    let empty_group = group_id(EMPTY_GROUP);
    groups
        .create_account_group(
            NewAccountGroup {
                model_multipliers: Default::default(),
                budget: Default::default(),

                id: mixed_group.clone(),
                name: "Mixed Production".to_owned(),
                description: Some("cross-provider".to_owned()),
                color: group_color("#2563EBFF"),
            },
            &context("create-mixed"),
        )
        .await
        .expect("create mixed account group");
    groups
        .create_account_group(
            NewAccountGroup {
                model_multipliers: Default::default(),
                budget: Default::default(),

                id: empty_group.clone(),
                name: "Empty Pool".to_owned(),
                description: None,
                color: group_color("#06B6D4CC"),
            },
            &context("create-empty"),
        )
        .await
        .expect("create empty account group");
    assign_accounts(
        &database.pool,
        MIXED_GROUP,
        &["acct_group_openai", "acct_group_xai"],
    )
    .await;

    for (id, group_ids) in [
        ("key_group_one", vec![mixed_group.clone()]),
        ("key_group_two", vec![mixed_group.clone()]),
        ("key_empty_pool", vec![empty_group.clone()]),
    ] {
        keys.create_client_key(new_key(id, group_ids), &context(id))
            .await
            .expect("create scoped client key");
    }
    seed_group_cost_snapshot(
        &database.pool,
        "req_historical_empty_group",
        "acct_group_openai",
        EMPTY_GROUP,
        "1.5",
    )
    .await;

    let page = groups
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).expect("page size"),
            search: None,
            enabled: None,
        })
        .await
        .expect("list account groups");
    let members = groups
        .load_account_group_members(std::slice::from_ref(&mixed_group))
        .await
        .expect("load current-page group members");
    assert_eq!(members.len(), 2);
    assert_eq!(
        members.iter().map(|member| member.total_slots).sum::<u64>(),
        7
    );
    assert_eq!(page.total, 2);
    let by_id = page
        .items
        .into_iter()
        .map(|group| (group.id.to_string(), group))
        .collect::<BTreeMap<_, _>>();
    let mixed = by_id.get(MIXED_GROUP).expect("mixed group");
    assert_eq!(mixed.member_count, 2);
    assert_eq!(
        mixed.provider_counts,
        BTreeMap::from([("openai".to_owned(), 1), ("xai".to_owned(), 1)])
    );
    assert_eq!(mixed.client_key_count, 2);
    // PostgreSQL 返回持久页与 member facts；实时状态/容量由 Admin query service 投影。
    assert_eq!(mixed.account_summary.available, 0);
    assert_eq!(mixed.account_summary.limited, 0);
    assert_eq!(mixed.account_summary.total, 0);
    assert_eq!(mixed.capacity.used_slots, None);
    assert_eq!(mixed.capacity.total_slots, 0);
    assert_eq!(mixed.usage.today_usd.as_str(), "0");
    assert_eq!(mixed.usage.retained_total_usd.as_str(), "0");
    let empty = by_id.get(EMPTY_GROUP).expect("empty group");
    assert_eq!(empty.member_count, 0);
    assert!(empty.provider_counts.is_empty());
    assert_eq!(empty.client_key_count, 1);
    assert_eq!(empty.usage.today_usd.as_str(), "1.5");
    assert_eq!(empty.usage.retained_total_usd.as_str(), "1.5");

    let empty_pool_key = keys
        .reveal_client_key("test-owner", &client_key_id("key_empty_pool"))
        .await
        .expect("reveal empty-pool key")
        .expect("empty-pool key exists");
    assert_eq!(empty_pool_key.record.groups.len(), 1);
    assert!(empty_pool_key.record.provider_kinds.is_empty());

    let revision_before = current_revision(&database.pool).await;
    assert!(
        keys.update_client_key(
            UpdateClientKey {
                id: client_key_id("key_group_one"),
                name: "key_group_one".into(),
                label: None,
                group_ids: Vec::new(),
                limits: RateLimits::unlimited(),
            },
            &context("reject-ungrouped-key")
        )
        .await
        .is_err()
    );
    assert_eq!(current_revision(&database.pool).await, revision_before);
    let (restricted_revision, restricted) = keys
        .update_client_key(
            UpdateClientKey {
                id: client_key_id("key_group_one"),
                name: "key_group_one".to_owned(),
                label: None,
                group_ids: vec![empty_group],
                limits: RateLimits::unlimited(),
            },
            &context("restrict-all-key"),
        )
        .await
        .expect("restrict all-accounts key to groups");
    assert_eq!(restricted.groups.len(), 1);
    let restricted_audit: Vec<String> = sqlx::query_scalar(
        "select changed_fields from admin_audit_events
         where admin_request_id = 'restrict-all-key'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load scope restriction audit");
    assert!(restricted_audit.contains(&"group_ids".to_owned()));
    assert_eq!(
        current_revision(&database.pool).await,
        restricted_revision.get()
    );

    let revision_before_delete = current_revision(&database.pool).await;
    let audit_before_delete = audit_count(&database.pool).await;
    let error = groups
        .delete_account_group(
            DeleteAccountGroup { id: mixed_group },
            &context("delete-referenced"),
        )
        .await
        .expect_err("referenced group must not be deleted");
    assert_eq!(error.kind(), AdminStoreErrorKind::Conflict);
    assert_eq!(
        current_revision(&database.pool).await,
        revision_before_delete
    );
    assert_eq!(audit_count(&database.pool).await, audit_before_delete);

    database.close().await;
}

#[tokio::test]
async fn group_costs_should_include_statusless_websocket_but_reject_statusless_http() {
    let Some(database) = TestDatabase::create("account_group_statusless_websocket_cost").await
    else {
        return;
    };
    seed_account(
        &database.pool,
        "acct_group_statusless",
        "openai",
        "Statusless Account",
    )
    .await;
    let groups = PgAccountGroupRepository::new(database.pool.clone());
    groups
        .create_account_group(
            NewAccountGroup {
                model_multipliers: Default::default(),
                budget: Default::default(),

                id: group_id(EMPTY_GROUP),
                name: "Statusless Costs".to_owned(),
                description: None,
                color: group_color("#06B6D4CC"),
            },
            &context("create-statusless-cost-group"),
        )
        .await
        .expect("create statusless cost group");
    for (request_id, cost_amount) in [
        ("req_group_http_success", "1.5"),
        ("req_group_statusless_websocket", "2"),
        ("req_group_statusless_http", "4"),
    ] {
        seed_group_cost_snapshot(
            &database.pool,
            request_id,
            "acct_group_statusless",
            EMPTY_GROUP,
            cost_amount,
        )
        .await;
    }
    sqlx::query(
        "update model_requests
         set client_transport = case id
               when 'req_group_statusless_websocket' then 'websocket'
               else 'http_sse'
             end,
             client_status_code = null
         where id in ('req_group_statusless_websocket', 'req_group_statusless_http')",
    )
    .execute(&database.pool)
    .await
    .expect("make account group requests statusless");

    let page = groups
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).expect("page size"),
            search: None,
            enabled: None,
        })
        .await
        .expect("list statusless cost group");

    assert_eq!(
        (
            page.items[0].usage.today_usd.as_str(),
            page.items[0].usage.retained_total_usd.as_str(),
        ),
        ("3.5", "3.5"),
    );

    database.close().await;
}

fn new_key(id: &str, group_ids: Vec<AccountGroupId>) -> NewClientKey {
    let marker = char::from(id.as_bytes().last().copied().unwrap_or(b'k'));
    NewClientKey {
        id: client_key_id(id),
        name: id.to_owned(),
        label: None,
        group_ids,
        limits: RateLimits::unlimited(),
        plaintext: format!("sk_{}", marker.to_string().repeat(43)),
    }
}

fn group_id(value: &str) -> AccountGroupId {
    AccountGroupId::new(value).expect("valid account group ID")
}

fn group_color(value: &str) -> AccountGroupColor {
    AccountGroupColor::parse(value).expect("valid account group color")
}

fn client_key_id(value: &str) -> ClientApiKeyId {
    ClientApiKeyId::new(value).expect("valid client key ID")
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: "test-owner".into(),
        },
        request_id: request_id.to_owned(),
    }
}

async fn seed_account(pool: &sqlx::PgPool, id: &str, provider: &str, name: &str) {
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, email, upstream_user_id, upstream_account_id,
           plan_type, authentication_kind, provider_credentials_json, credential_revision,
           has_refresh_token, access_token_expires_at, next_refresh_at, enabled,
           credential_state, credential_observed_at, created_at, updated_at
         ) values (
           $1, $2, $3, null, $1 || '-user', null, null, 'oauth', '{}'::jsonb, 1,
           false, null, null, true, 'ready', now(), now(), now()
         )",
    )
    .bind(id)
    .bind(provider)
    .bind(name)
    .execute(pool)
    .await
    .expect("seed provider account");
}

async fn assign_accounts(pool: &sqlx::PgPool, group_id: &str, account_ids: &[&str]) {
    for account_id in account_ids {
        sqlx::query(
            "insert into account_group_accounts (
               account_group_id, provider_account_id, created_at
             )
             values ($1, $2, now())",
        )
        .bind(group_id)
        .bind(account_id)
        .execute(pool)
        .await
        .expect("seed account group membership");
    }
}

async fn seed_group_cost_snapshot(
    pool: &sqlx::PgPool,
    request_id: &str,
    account_id: &str,
    historical_group_id: &str,
    cost_amount: &str,
) {
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, config_revision, protocol, operation, endpoint,
           client_transport, requested_model_id, provider_kind, provider_account_id,
           provider_account_ref, upstream_model_id, upstream_transport, attempt_count,
           upstream_send_state, downstream_committed_at, outcome, client_status_code,
           upstream_status_code, total_tokens, cost_source, cost_amount, cost_currency,
           started_at, deadline_at, completed_at,
           routing_scope, routing_group_refs, routing_group_names_snapshot
         ) values (
           $1, 'key-group-history', 1, 'openai', 'responses', '/v1/responses',
           'http_sse', 'gpt-group', 'openai', $2, $2, 'gpt-group', 'http_sse', 1,
           'sent', now(), 'succeeded', 200, 200, 10,
           'provider_reported', $4::numeric, 'USD', now() - interval '1 minute',
           now() + interval '5 minutes', now(),
           'groups', array[$3]::text[], jsonb_build_array($3::text)
         )",
    )
    .bind(request_id)
    .bind(account_id)
    .bind(historical_group_id)
    .bind(cost_amount)
    .execute(pool)
    .await
    .expect("seed historical group cost snapshot");
}

async fn current_revision(pool: &sqlx::PgPool) -> u64 {
    let value =
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id = 1")
            .fetch_one(pool)
            .await
            .expect("load config revision");
    u64::try_from(value).expect("positive config revision")
}

async fn audit_count(pool: &sqlx::PgPool) -> u64 {
    let value = sqlx::query_scalar::<_, i64>("select count(*) from admin_audit_events")
        .fetch_one(pool)
        .await
        .expect("count audit rows");
    u64::try_from(value).expect("non-negative audit count")
}

#[tokio::test]
async fn plugin_groups_are_atomic_owned_idempotent_and_tombstoned_after_admin_deletion() {
    use gateway_admin::model::plugins::*;
    let Some(db) = TestDatabase::create("plugin_resources").await else {
        return;
    };
    let groups = PgAccountGroupRepository::new(db.pool.clone());
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
    let scope = |id: &str, revision| PluginMutationScope {
        instance_id: id.into(),
        revision,
        actor: context("plugin-resource-test"),
    };
    let command = |id: &str, name: &str| NewAccountGroup {
        id: group_id(id),
        name: name.into(),
        description: None,
        color: group_color("#2563EBFF"),
        budget: Default::default(),
        model_multipliers: Default::default(),
    };
    let (a, b) = tokio::join!(
        groups.ensure_plugin_group(
            scope("plugin-a", 1),
            "main".into(),
            command(MIXED_GROUP, "First")
        ),
        groups.ensure_plugin_group(
            scope("plugin-a", 1),
            "main".into(),
            command(EMPTY_GROUP, "Second")
        )
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a, b);
    let id = a.value.id.clone();
    let count: i64 = sqlx::query_scalar("select count(*) from account_groups")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let audits: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where action='plugin.group.ensure'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(audits, 1);
    sqlx::query("update account_groups set name='Admin changed',enabled=false where id=$1")
        .bind(&id)
        .execute(&db.pool)
        .await
        .unwrap();
    let existing = groups
        .ensure_plugin_group(
            scope("plugin-a", 1),
            "main".into(),
            command(MIXED_GROUP, "Should not overwrite"),
        )
        .await
        .unwrap();
    assert_eq!(existing.value.name, "Admin changed");
    assert!(!existing.value.enabled);
    let members = |key: &str, add: Vec<&str>, remove: Vec<&str>| PluginGroupMembers {
        resource_key: key.into(),
        add: add.into_iter().map(str::to_owned).collect(),
        remove: remove.into_iter().map(str::to_owned).collect(),
    };
    assert!(
        groups
            .change_plugin_group_members(scope("plugin-b", 1), members("main", vec![], vec![]))
            .await
            .is_err()
    );
    for account in ["acct_001", "acct_002"] {
        seed_account(&db.pool, account, "openai", account).await;
    }
    let added = groups
        .change_plugin_group_members(
            scope("plugin-a", 1),
            members("main", vec!["acct_001", "acct_002"], vec![]),
        )
        .await
        .unwrap();
    assert_eq!(
        added.value,
        PluginMembersChanged {
            added: 2,
            removed: 0
        }
    );
    let repeated = groups
        .change_plugin_group_members(
            scope("plugin-a", 1),
            members("main", vec!["acct_001", "acct_002"], vec![]),
        )
        .await
        .unwrap();
    assert_eq!(
        repeated.value,
        PluginMembersChanged {
            added: 0,
            removed: 0
        }
    );
    assert_eq!(repeated.revision, added.revision);
    assert!(
        groups
            .change_plugin_group_members(
                scope("plugin-a", 1),
                members("main", vec!["missing"], vec!["acct_001"])
            )
            .await
            .is_err()
    );
    let count: i64 =
        sqlx::query_scalar("select count(*) from account_group_accounts where account_group_id=$1")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
    let other = groups
        .ensure_plugin_group(
            scope("plugin-b", 1),
            "main".into(),
            command("grp_00000000000000000000000000000003", "Other"),
        )
        .await
        .unwrap();
    groups
        .change_plugin_group_members(
            scope("plugin-b", 1),
            members("main", vec!["acct_001"], vec![]),
        )
        .await
        .unwrap();
    let removed = groups
        .change_plugin_group_members(
            scope("plugin-a", 1),
            members("main", vec![], vec!["acct_001"]),
        )
        .await
        .unwrap();
    assert_eq!(
        removed.value,
        PluginMembersChanged {
            added: 0,
            removed: 1
        }
    );
    let preserved:bool=sqlx::query_scalar("select exists(select 1 from account_group_accounts where account_group_id=$1 and provider_account_id='acct_001')").bind(&other.value.id).fetch_one(&db.pool).await.unwrap();
    assert!(preserved);
    sqlx::query("update plugin_instances set revision=2 where id='plugin-a'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        groups
            .ensure_plugin_group(
                scope("plugin-a", 1),
                "main".into(),
                command(MIXED_GROUP, "Stale")
            )
            .await
            .is_err()
    );
    groups
        .delete_account_group(
            DeleteAccountGroup { id: group_id(&id) },
            &context("delete-owned-group"),
        )
        .await
        .unwrap();
    assert_eq!(
        groups
            .ensure_plugin_group(
                scope("plugin-a", 2),
                "main".into(),
                command(MIXED_GROUP, "Must not recreate")
            )
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::NotFound
    );
    sqlx::query("update plugin_instances set enabled=false where id='plugin-b'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        groups
            .change_plugin_group_members(
                scope("plugin-b", 1),
                members("main", vec![], vec!["acct_001"])
            )
            .await
            .is_err()
    );
    sqlx::query("delete from plugin_instances where id='plugin-b'")
        .execute(&db.pool)
        .await
        .unwrap();
    let retained: bool =
        sqlx::query_scalar("select exists(select 1 from account_groups where id=$1)")
            .bind(&other.value.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(retained);
    sqlx::query("insert into plugin_managed_groups(instance_id,resource_key) select 'plugin-a','reserved-' || n from generate_series(1,255) as n").execute(&db.pool).await.unwrap();
    let exhausted = groups
        .ensure_plugin_group(
            scope("plugin-a", 2),
            "over-capacity".into(),
            command("grp_00000000000000000000000000000004", "Over capacity"),
        )
        .await
        .unwrap_err();
    assert_eq!(exhausted.kind(), AdminStoreErrorKind::Invalid);
    let budgets: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(budgets, 0);
    let actor:Option<String>=sqlx::query_scalar("select actor_admin_user_id from admin_audit_events where action='plugin.group.ensure' limit 1").fetch_one(&db.pool).await.unwrap();
    assert_eq!(actor.as_deref(), Some("test-owner"));
    db.close().await;
}
