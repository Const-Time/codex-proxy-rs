use std::time::SystemTime;

use chrono::{DateTime, Utc};
use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        account_groups::{AccountGroupColor, UpdateAccountGroup},
    },
    ports::store::AccountGroupStore as _,
};
use gateway_core::{
    engine::{
        ModelRequestId,
        budget::{ClientBudgetCharge, ClientBudgetPort, ClientBudgetStatus},
    },
    error::GatewayErrorKind,
    policy::ClientApiKeyId,
    routing::AccountGroupId,
};
use gateway_store::postgres::{
    ClientApiKeyRepository as _, PgAccountGroupRepository, PgClientApiKeyRepository,
    PgClientBudgetStore,
};

use super::TestDatabase;

fn key_id(key: &str) -> ClientApiKeyId {
    ClientApiKeyId::new(key).unwrap()
}

fn charge(key: &str, request: &str, amount: &str) -> ClientBudgetCharge {
    ClientBudgetCharge {
        components_usd: [gateway_core::metering::Decimal::ZERO; 4],
        scope: Some(gateway_core::engine::budget::ClientBudgetScope {
            billing: Default::default(),
            user_id: "budget-user".into(),
            group_id: group_id(key),
        }),
        key_id: key_id(key),
        request_id: ModelRequestId::new(format!("req_{request}")).unwrap(),
        amount_usd: amount.parse().unwrap(),
        completed_at: SystemTime::now(),
    }
}

fn group_id(key: &str) -> AccountGroupId {
    AccountGroupId::new(format!("grp_{:0<32}", hex::encode(key.as_bytes()))).unwrap()
}

async fn seed(database: &TestDatabase, key: &str, daily: &str, weekly: &str) {
    sqlx::query("insert into users(id, username, password_hash, created_at, updated_at) values ('budget-user', 'budget-user', 'unused', now(), now()) on conflict do nothing")
        .execute(&database.pool).await.unwrap();
    sqlx::query("insert into account_groups(id, name, color, daily_limit_usd, weekly_limit_usd, created_at, updated_at) values ($1, $2, '#64748BFF', $3::text::numeric, $4::text::numeric, now(), now())")
        .bind(group_id(key).as_str()).bind(key).bind(daily).bind(weekly).execute(&database.pool).await.unwrap();
    sqlx::query("insert into user_account_groups values ('budget-user', $1)")
        .bind(group_id(key).as_str())
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("insert into client_api_keys(id, owner_user_id, name, key, created_at, updated_at) values ($1, 'budget-user', $1, $2, now(), now())")
        .bind(key).bind(format!("sk_{key:a<43}")).execute(&database.pool).await.unwrap();
    sqlx::query("insert into client_api_key_groups values ($1, $2, now())")
        .bind(key)
        .bind(group_id(key).as_str())
        .execute(&database.pool)
        .await
        .unwrap();
}

async fn status(database: &TestDatabase, key: &str) -> ClientBudgetStatus {
    PgClientApiKeyRepository::new(database.pool.clone())
        .get_client_api_key(key)
        .await
        .unwrap()
        .unwrap()
        .budget
}

async fn member(database: &TestDatabase, key: &str, suffix: &str) -> String {
    let id = format!("acct_{:0<32}", hex::encode(suffix.as_bytes()));
    sqlx::query("insert into provider_accounts(id, provider_kind, name, authentication_kind, provider_credentials_json, has_refresh_token, credential_observed_at, created_at, updated_at) values ($1, 'openai', 'test', 'oauth', '{}', false, now(), now(), now())")
        .bind(&id).execute(&database.pool).await.unwrap();
    sqlx::query("insert into account_group_accounts values ($1, $2, now())")
        .bind(group_id(key).as_str())
        .bind(&id)
        .execute(&database.pool)
        .await
        .unwrap();
    id
}

async fn second_subscriber(database: &TestDatabase, key: &str) {
    sqlx::query("insert into users(id, username, password_hash, created_at, updated_at) values ('second-user', 'second-user', 'unused', now(), now())")
        .execute(&database.pool).await.unwrap();
    sqlx::query(
        "insert into user_account_groups(user_id, account_group_id) values ('second-user', $1)",
    )
    .bind(group_id(key).as_str())
    .execute(&database.pool)
    .await
    .unwrap();
    sqlx::query("select advance_subscription_windows('second-user', $1, now())")
        .bind(group_id(key).as_str())
        .execute(&database.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn subscription_cycle_upgrade_keeps_historical_windows_counters_and_ledger() {
    let Some(db) = TestDatabase::create_at_version("group_cycle_upgrade", 22).await else {
        return;
    };
    seed(&db, "legacy", "0", "700").await;
    member(&db, "legacy", "legacy").await;
    sqlx::query("insert into user_group_budget_windows values('budget-user',$1,now()-interval '8 hours',now()+interval '16 hours',now()-interval '6 days',now()+interval '1 day',2.5,87.232645,null,null)")
        .bind(group_id("legacy").as_str()).execute(&db.pool).await.unwrap();
    sqlx::query("insert into user_group_charge_events(request_id,user_id,account_group_id,client_api_key_ref,amount_usd,completed_at) values('req_old','budget-user',$1,'legacy',202.60959436,now()-interval '7 days')")
        .bind(group_id("legacy").as_str()).execute(&db.pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(w) from user_group_budget_windows w")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let after: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(w) from user_group_budget_windows w")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        before, after,
        "migration is not an unapproved historical repair"
    );
    PgClientBudgetStore::new(db.pool.clone())
        .admit(key_id("legacy"), Some(group_id("legacy")), None)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "legacy").await.weekly_used_usd.canonical(),
        "87.232645"
    );
    let ledger: String =
        sqlx::query_scalar("select sum(amount_usd)::text from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(ledger, "202.6095943600");
    let events: i64 = sqlx::query_scalar("select count(*) from subscription_reset_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(events, 0);
    db.close().await;
}

#[tokio::test]
async fn single_account_expiry_keeps_usage_until_confirmed_upstream_reset() {
    let Some(db) = TestDatabase::create("single_group_cycle").await else {
        return;
    };
    seed(&db, "single", "0", "10").await;
    let account = member(&db, "single", "single").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    store.settle(charge("single", "first", "9")).await.unwrap();
    sqlx::query("update user_group_budget_windows set weekly_end=now()-interval '1 second'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update subscription_group_cycles set weekly_start=now()-interval '8 days',weekly_end=now()-interval '1 second'").execute(&db.pool).await.unwrap();
    assert_eq!(status(&db, "single").await.weekly_used_usd.canonical(), "9");
    store
        .admit(key_id("single"), Some(group_id("single")), None)
        .await
        .unwrap();
    store
        .settle(charge("single", "after-boundary", "2"))
        .await
        .unwrap();
    assert_eq!(
        status(&db, "single").await.weekly_used_usd.canonical(),
        "11"
    );
    assert_eq!(
        store
            .admit(key_id("single"), Some(group_id("single")), None)
            .await
            .unwrap_err()
            .client_error_code(),
        Some("group_weekly_budget_exceeded")
    );
    let events: i64 = sqlx::query_scalar("select count(*) from subscription_reset_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(events, 0, "no local weekly rollover in linked mode");
    sqlx::query("insert into subscription_quota_observations values($1,'codex:weekly',now(),now()+interval '16 hours',75)")
        .bind(&account).execute(&db.pool).await.unwrap();
    second_subscriber(&db, "single").await;
    let ends: i64 =
        sqlx::query_scalar("select count(distinct weekly_end) from user_group_budget_windows")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(ends, 1, "new subscribers use the same upstream boundary");
    assert_eq!(
        status(&db, "single").await.weekly_used_usd.canonical(),
        "11",
        "first baseline must not repair or clear historical usage"
    );
    db.close().await;
}

#[tokio::test]
async fn multi_account_groups_ignore_individual_resets_and_roll_all_subscribers_together() {
    use gateway_admin::ports::store::AccountStore as _;
    let Some(db) = TestDatabase::create("multi_group_cycle").await else {
        return;
    };
    seed(&db, "multi", "0", "100").await;
    let first = member(&db, "multi", "first").await;
    let second = member(&db, "multi", "second").await;
    // Disabled configured members still count: enable toggles cannot clear budgets.
    sqlx::query("update provider_accounts set enabled=false where id=$1")
        .bind(second)
        .execute(&db.pool)
        .await
        .unwrap();
    let store = PgClientBudgetStore::new(db.pool.clone());
    store.settle(charge("multi", "one", "9")).await.unwrap();
    second_subscriber(&db, "multi").await;
    store
        .settle(ClientBudgetCharge {
            components_usd: [gateway_core::metering::Decimal::ZERO; 4],
            scope: Some(gateway_core::engine::budget::ClientBudgetScope {
                billing: Default::default(),
                user_id: "second-user".into(),
                group_id: group_id("multi"),
            }),
            ..charge("multi", "two", "8")
        })
        .await
        .unwrap();
    super::admin_account_store(&db.pool)
        .reset_account_subscriptions(&first, "one-account-credit")
        .await
        .unwrap();
    assert_eq!(status(&db, "multi").await.weekly_used_usd.canonical(), "9");
    let affected: i64 = sqlx::query_scalar(
        "select affected_count from subscription_reset_events where reason='upstream_manual'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(affected, 0);
    let boundary = Utc::now();
    sqlx::query(
        "update subscription_group_cycles set weekly_start=$1-interval '168 hours',weekly_end=$1",
    )
    .bind(boundary)
    .execute(&db.pool)
    .await
    .unwrap();
    store
        .admit(key_id("multi"), Some(group_id("multi")), None)
        .await
        .unwrap();
    store.settle(charge("multi", "after", "2")).await.unwrap();
    store
        .admit(key_id("multi"), Some(group_id("multi")), None)
        .await
        .unwrap();
    let windows: (i64, i64, String) = sqlx::query_as("select count(distinct weekly_end),count(distinct weekly_start),sum(weekly_used_usd)::text from user_group_budget_windows").fetch_one(&db.pool).await.unwrap();
    assert_eq!(windows, (1, 1, "2.0000000000".into()));
    let events: (i64, i64, i32) = sqlx::query_as("select count(*),max(affected_count),max(jsonb_array_length(targets)) from subscription_reset_events where reason='group_period'").fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        events,
        (1, 2, 2),
        "one recorded group-wide reset, no duplicate after admission"
    );
    let last: String =
        sqlx::query_scalar("select last_reset_reason from user_group_budget_windows limit 1")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(last, "group_period");
    db.close().await;
}

#[tokio::test]
async fn group_membership_and_manual_reset_preserve_shared_cycle_and_other_usage() {
    let Some(db) = TestDatabase::create("group_cycle_transition").await else {
        return;
    };
    seed(&db, "topology", "0", "100").await;
    member(&db, "topology", "only").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    store
        .settle(charge("topology", "before", "7"))
        .await
        .unwrap();
    member(&db, "topology", "additional").await;
    second_subscriber(&db, "topology").await;
    assert_eq!(
        status(&db, "topology").await.weekly_used_usd.canonical(),
        "7"
    );
    let before: DateTime<Utc> =
        sqlx::query_scalar("select weekly_end from subscription_group_cycles")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    let targets =
        serde_json::json!([{"userId":"second-user","groupId":group_id("topology").as_str()}]);
    sqlx::query("select reset_user_subscriptions('selected',null,null,'manual',now(),$1)")
        .bind(sqlx::types::Json(targets))
        .execute(&db.pool)
        .await
        .unwrap();
    let after: DateTime<Utc> =
        sqlx::query_scalar("select weekly_end from subscription_group_cycles")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    let ends: i64 =
        sqlx::query_scalar("select count(distinct weekly_end) from user_group_budget_windows")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(ends, 1);
    assert_eq!(
        status(&db, "topology").await.weekly_used_usd.canonical(),
        "7"
    );
    db.close().await;
}

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "budget-test".to_owned(),
    }
}

#[tokio::test]
async fn user_quota_multiplier_changes_limits_without_repricing_consumption() {
    let Some(db) = TestDatabase::create("quota_multiplier").await else {
        return;
    };
    seed(&db, "factor", "10", "100").await;
    sqlx::query(
        "update user_account_groups set quota_multiplier = 2 where user_id = 'budget-user'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let store = PgClientBudgetStore::new(db.pool.clone());
    store
        .settle(charge("factor", "factor", "15"))
        .await
        .unwrap();
    let current = status(&db, "factor").await;
    assert_eq!(current.limits.daily_usd.canonical(), "20");
    assert_eq!(current.limits.weekly_usd.canonical(), "200");
    assert_eq!(current.daily_used_usd.canonical(), "15");
    store
        .admit(key_id("factor"), Some(group_id("factor")), None)
        .await
        .unwrap();
    sqlx::query(
        "update user_account_groups set quota_multiplier = 1 where user_id = 'budget-user'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .admit(key_id("factor"), Some(group_id("factor")), None)
            .await
            .unwrap_err()
            .client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    assert!(
        sqlx::query("update user_account_groups set quota_multiplier = 0")
            .execute(&db.pool)
            .await
            .is_err()
    );
    sqlx::query("update account_groups set daily_limit_usd = 0, weekly_limit_usd = 0")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "factor").await.limits.daily_usd.canonical(),
        "0"
    );
    db.close().await;
}

#[tokio::test]
async fn subscription_resets_preserve_history_and_retry_does_not_clear_new_usage() {
    let Some(db) = TestDatabase::create("subscription_reset").await else {
        return;
    };
    seed(&db, "reset", "10", "100").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    store
        .settle(charge("reset", "before-reset", "7"))
        .await
        .unwrap();
    let reset = "select reset_user_subscriptions('manual:test', null, null, 'manual', now())";
    let count: i64 = sqlx::query_scalar(reset).fetch_one(&db.pool).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(status(&db, "reset").await.weekly_used_usd.canonical(), "0");
    store
        .settle(charge("reset", "after-reset", "2"))
        .await
        .unwrap();
    sqlx::query(reset).execute(&db.pool).await.unwrap();
    assert_eq!(status(&db, "reset").await.weekly_used_usd.canonical(), "2");
    let historical: String =
        sqlx::query_scalar("select sum(amount_usd)::text from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(historical, "9.0000000000");
    // A delayed observation must retain already settled requests after the upstream boundary.
    let boundary = Utc::now() - chrono::Duration::minutes(1);
    sqlx::query("update user_group_budget_windows set last_reset_at = null")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "select reset_user_subscriptions('observed:test', null, null, 'upstream_window', $1)",
    )
    .bind(boundary)
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(status(&db, "reset").await.weekly_used_usd.canonical(), "9");
    db.close().await;
}

#[tokio::test]
async fn upstream_resets_are_scoped_and_deduplicated_across_observations() {
    use gateway_admin::{
        model::provider_credentials::{
            ProviderQuota, ProviderQuotaWindow, QuotaLocalUsageAttribution,
        },
        ports::store::AccountStore as _,
    };
    let Some(db) = TestDatabase::create("upstream_reset").await else {
        return;
    };
    seed(&db, "linked", "10", "100").await;
    seed(&db, "other", "10", "100").await;
    seed(&db, "pooled", "10", "100").await;
    let account = "acct_00000000000000000000000000000991";
    sqlx::query("insert into provider_accounts(id, provider_kind, name, authentication_kind, provider_credentials_json, has_refresh_token, credential_observed_at, created_at, updated_at) values ($1, 'openai', 'test', 'oauth', '{}', false, now(), now(), now())")
        .bind(account).execute(&db.pool).await.unwrap();
    sqlx::query("insert into account_group_accounts values ($1, $2, now())")
        .bind(group_id("linked").as_str())
        .bind(account)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("insert into account_group_accounts values ($1, $2, now())")
        .bind(group_id("pooled").as_str())
        .bind(account)
        .execute(&db.pool)
        .await
        .unwrap();
    member(&db, "pooled", "pool-other").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    store
        .settle(charge("pooled", "pool-before", "5"))
        .await
        .unwrap();
    store
        .settle(charge("linked", "linked-before", "3"))
        .await
        .unwrap();
    store
        .settle(charge("other", "other-before", "4"))
        .await
        .unwrap();
    let admin = super::admin_account_store(&db.pool);
    let now = Utc::now();
    let mut quota = ProviderQuota {
        plan_type: None,
        observed_at: Some(now),
        refresh_token_expires_at: None,
        limit_reached: false,
        provider_data: None,
        windows: vec![ProviderQuotaWindow {
            key: "codex:weekly".into(),
            group: "shortTerm".into(),
            label: "weekly".into(),
            limit_id: None,
            limit_name: None,
            role: None,
            estimated_quota: None,
            estimate_hint: None,
            local_usage_attribution: QuotaLocalUsageAttribution::AccountWide,
            window_seconds: Some(604800),
            used_percent: Some(75.0),
            reset_at: Some(now + chrono::Duration::days(1)),
            limit_reached: false,
            local_usage: None,
            provider_data: None,
        }],
    };
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "3",
        "first observation is a baseline"
    );
    quota.observed_at = Some(now + chrono::Duration::seconds(1));
    quota.windows[0].used_percent = Some(0.0);
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "3",
        "percentage recovery without a weekly boundary change must not reset"
    );
    let mut short = quota.windows[0].clone();
    short.key = "codex:primary".into();
    short.window_seconds = Some(18_000);
    short.used_percent = Some(90.0);
    quota.windows.push(short);
    quota.observed_at = Some(now + chrono::Duration::milliseconds(1100));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    quota.windows[1].reset_at = Some(now + chrono::Duration::days(2));
    quota.windows[1].used_percent = Some(0.0);
    quota.observed_at = Some(now + chrono::Duration::milliseconds(1200));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "3",
        "five-hour rollover must not clear weekly subscriptions"
    );
    quota.windows[0].reset_at = Some(now + chrono::Duration::days(7));
    quota.observed_at = Some(now + chrono::Duration::milliseconds(1500));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(status(&db, "linked").await.weekly_used_usd.canonical(), "0");
    assert_eq!(status(&db, "other").await.weekly_used_usd.canonical(), "4");
    assert_eq!(
        status(&db, "pooled").await.weekly_used_usd.canonical(),
        "5",
        "the same upstream event resets its single-account group, not its pooled group"
    );
    let mut after = charge("linked", "linked-after", "2");
    after.completed_at = (now + chrono::Duration::seconds(2)).into();
    store.settle(after).await.unwrap();
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    quota.observed_at = Some(now);
    quota.windows[0].used_percent = Some(75.0);
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "2",
        "duplicates and stale snapshots cannot reset again"
    );
    // Natural window rollover is recognized even if the observed usage is higher.
    sqlx::query("update subscription_quota_observations set reset_at = now() - interval '1 hour', observed_at = now() - interval '2 hours'").execute(&db.pool).await.unwrap();
    sqlx::query("update user_group_budget_windows set last_reset_at = null")
        .execute(&db.pool)
        .await
        .unwrap();
    quota.observed_at = Some(now + chrono::Duration::seconds(3));
    quota.windows[0].reset_at = Some(now + chrono::Duration::days(7) - chrono::Duration::hours(1));
    quota.windows[0].used_percent = Some(80.0);
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    let reason: String = sqlx::query_scalar(
        "select last_reset_reason from user_group_budget_windows where account_group_id = $1",
    )
    .bind(group_id("linked").as_str())
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(reason, "upstream_window");
    admin
        .reset_account_subscriptions(account, "credit-test")
        .await
        .unwrap();
    admin
        .reset_account_subscriptions(account, "credit-test")
        .await
        .unwrap();
    let events: i64 = sqlx::query_scalar("select count(*) from subscription_reset_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(events, 3);
    sqlx::query("update runtime_settings set subscription_auto_reset_enabled = false where id = 1")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update user_group_budget_windows set daily_used_usd = 7, weekly_used_usd = 7")
        .execute(&db.pool)
        .await
        .unwrap();
    quota.observed_at = Some(now + chrono::Duration::seconds(4));
    quota.windows[0].reset_at = Some(now + chrono::Duration::days(14));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    admin
        .reset_account_subscriptions(account, "disabled-credit")
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "7",
        "disabled blocks both observed and explicit upstream resets"
    );
    // The administrator's selected-subscription reset remains independent of this switch.
    sqlx::query(
        "select reset_user_subscriptions('selected-while-disabled',null,null,'manual',now(),$1)",
    )
    .bind(sqlx::types::Json(
        serde_json::json!([{"userId":"budget-user","groupId":group_id("linked").as_str()}]),
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    let reset_reason: String = sqlx::query_scalar(
        "select last_reset_reason from user_group_budget_windows where account_group_id=$1",
    )
    .bind(group_id("linked").as_str())
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(reset_reason, "manual");
    sqlx::query("update user_group_budget_windows set daily_used_usd = 5, weekly_used_usd = 5")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update runtime_settings set subscription_auto_reset_enabled = true where id = 1")
        .execute(&db.pool)
        .await
        .unwrap();
    let baselines: i64 = sqlx::query_scalar("select count(*) from subscription_quota_observations")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(baselines, 0, "reenabling discards pre-enable baselines");
    quota.observed_at = Some(now + chrono::Duration::seconds(5));
    quota.windows[0].reset_at = Some(now + chrono::Duration::days(21));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    admin
        .reset_account_subscriptions(account, "disabled-credit")
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "5",
        "no catch-up rollover or replay of disabled redemption"
    );
    quota.observed_at = Some(now + chrono::Duration::seconds(6));
    quota.windows[0].reset_at = Some(now + chrono::Duration::days(28));
    admin
        .observe_subscription_quota(account, &quota)
        .await
        .unwrap();
    assert_eq!(
        status(&db, "linked").await.weekly_used_usd.canonical(),
        "0",
        "new enabled rollover resets normally"
    );
    assert_eq!(status(&db, "other").await.weekly_used_usd.canonical(), "5");
    db.close().await;
}

#[tokio::test]
async fn model_billing_freezes_rates_and_charges_exactly_once() {
    for endpoint in ["/v1/responses", "/api/admin/plugins/models/responses"] {
        model_billing_for_endpoint(endpoint).await;
    }
}

#[tokio::test]
async fn admission_billing_does_not_depend_on_observation_rows_or_live_repricing() {
    let Some(db) = TestDatabase::create("billing_without_observation").await else {
        return;
    };
    for (key, rate, raw, expected) in [
        ("round", "1.5", "0.0000000001", "0.0000000002"),
        ("zero", "0", "10", "0"),
        ("rate", "2", "3", "6"),
    ] {
        seed(&db, key, "100", "1000").await;
        sqlx::query("update account_groups set model_multipliers=$2 where id=$1")
            .bind(group_id(key).as_str())
            .bind(serde_json::json!({"test-model":rate}))
            .execute(&db.pool)
            .await
            .unwrap();
        let store = PgClientBudgetStore::new(db.pool.clone());
        let frozen = store
            .admit(
                key_id(key),
                Some(group_id(key)),
                Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
            )
            .await
            .unwrap();
        sqlx::query("update account_groups set model_multipliers='{}' where id=$1")
            .bind(group_id(key).as_str())
            .execute(&db.pool)
            .await
            .unwrap();
        let charge = ClientBudgetCharge {
            scope: Some(frozen),
            ..charge(key, key, raw)
        };
        store.settle(charge.clone()).await.unwrap();
        store.settle(charge).await.unwrap();
        assert_eq!(status(&db, key).await.daily_used_usd.canonical(), expected);
        let ledger:(String,String,String)=sqlx::query_as("select raw_amount_usd::text,billing_multiplier::text,amount_usd::text from user_group_charge_events where client_api_key_ref=$1")
            .bind(key).fetch_one(&db.pool).await.unwrap();
        assert_eq!(
            ledger
                .0
                .parse::<gateway_core::metering::Decimal>()
                .unwrap()
                .canonical(),
            raw
        );
        assert_eq!(
            ledger
                .1
                .parse::<gateway_core::metering::Decimal>()
                .unwrap()
                .canonical(),
            rate
        );
        assert_eq!(
            ledger
                .2
                .parse::<gateway_core::metering::Decimal>()
                .unwrap()
                .canonical(),
            expected
        );
    }
    let observations: i64 = sqlx::query_scalar("select count(*) from model_requests")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        observations, 0,
        "the original ledger must work while observation writes are absent"
    );
    db.close().await;
}

#[tokio::test]
async fn component_billing_keeps_unknown_retry_fees_raw_without_observation_rows() {
    let Some(db) = TestDatabase::create("component_billing_without_observation").await else {
        return;
    };
    seed(&db, "components", "100", "1000").await;
    sqlx::query("update account_groups set model_multipliers=$2 where id=$1")
        .bind(group_id("components").as_str()).bind(serde_json::json!({"test-model":{"multiplier":"3","scope":{"components":["cacheRead","output"]}}})).execute(&db.pool).await.unwrap();
    let store = PgClientBudgetStore::new(db.pool.clone());
    let frozen = store
        .admit(
            key_id("components"),
            Some(group_id("components")),
            Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
        )
        .await
        .unwrap();
    sqlx::query("update account_groups set model_multipliers='{}'")
        .execute(&db.pool)
        .await
        .unwrap();
    // 10 itemized USD and 4 unitemized USD from a prior attempt. Only the
    // observed output (2) and cache read (1 + image 0.5) receive the multiplier.
    let amount = ClientBudgetCharge {
        scope: Some(frozen),
        components_usd: ["5", "2", "1.5", "1"].map(|v| v.parse().unwrap()),
        ..charge("components", "components", "14")
    };
    store.settle(amount.clone()).await.unwrap();
    store.settle(amount).await.unwrap();
    assert_eq!(
        status(&db, "components").await.daily_used_usd.canonical(),
        "21"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from model_requests")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    db.close().await;
}

#[tokio::test]
async fn delayed_observation_writer_retains_the_admission_rule_for_both_insert_paths() {
    use gateway_core::{
        engine::{AttemptRecord, AttemptTrigger, ExecutionStore as _, NewModelRequest},
        lifecycle::{CancellationToken, Deadline},
        operation::OperationKind,
        routing::{
            AccountRoutingSnapshot, ConfigRevision, ProviderKind, PublicModelId,
            RoutingGroupSnapshot,
        },
        task::DaemonTask as _,
    };
    use gateway_store::postgres::{BufferedExecutionStore, PgExecutionStore};
    use std::{num::NonZeroU32, sync::Arc, time::Duration};
    let Some(db) = TestDatabase::create("delayed_billing_observation").await else {
        return;
    };
    seed(&db, "delayed", "100", "1000").await;
    let account = member(&db, "delayed", "billing").await;
    let budget = PgClientBudgetStore::new(db.pool.clone());
    for first_attempt in [false, true] {
        sqlx::query("update account_groups set model_multipliers=$2 where id=$1")
            .bind(group_id("delayed").as_str())
            .bind(serde_json::json!({"test-model":"1.5"}))
            .execute(&db.pool)
            .await
            .unwrap();
        let frozen = budget
            .admit(
                key_id("delayed"),
                Some(group_id("delayed")),
                Some(PublicModelId::new("test-model").unwrap()),
            )
            .await
            .unwrap();
        let id = ModelRequestId::new(format!("req_delayed_{first_attempt}")).unwrap();
        let started_at = SystemTime::now();
        let request = NewModelRequest {
            parent_request_id: None,
            initiating_plugin_instance_id: None,
            budget_scope: Some(frozen.clone()),
            id: id.clone(),
            client_api_key_id: Some(key_id("delayed")),
            client_api_key_ref: key_id("delayed"),
            config_revision: ConfigRevision::new(1).unwrap(),
            routing: AccountRoutingSnapshot::groups(vec![RoutingGroupSnapshot::new(
                group_id("delayed"),
                "Delayed".into(),
            )]),
            protocol: "openai".into(),
            operation: OperationKind::Generate,
            endpoint: "/v1/responses".into(),
            client_transport: "http_sse".into(),
            requested_model: Some(PublicModelId::new("test-model").unwrap()),
            client_ip: None,
            user_agent: None,
            reasoning_effort: None,
            reasoning_preset: None,
            request_kind: None,
            subagent_kind: None,
            compact: false,
            continuation: Default::default(),
            image_generation_requested: false,
            admission_decision_ms: None,
            started_at,
            deadline_at: Deadline::from_timeout(started_at, Some(Duration::from_secs(60))).unwrap(),
        };
        let (observations, writer) =
            BufferedExecutionStore::new(Arc::new(PgExecutionStore::new(db.pool.clone())));
        if first_attempt {
            observations
                .create_model_request_with_attempt(
                    request,
                    AttemptRecord {
                        request_id: id.clone(),
                        attempt_count: NonZeroU32::new(1).unwrap(),
                        trigger: AttemptTrigger::Initial,
                        provider_kind: ProviderKind::new("openai").unwrap(),
                        provider_account_id: Some(
                            gateway_core::account::ProviderAccountId::new(account.clone()).unwrap(),
                        ),
                        provider_account_ref: Some(
                            gateway_core::account::ProviderAccountId::new(account.clone()).unwrap(),
                        ),
                        upstream_model_id: Some(
                            gateway_core::routing::UpstreamModelId::new("test-model").unwrap(),
                        ),
                        upstream_transport: "http_sse".into(),
                        http_version: None,
                        account_selection_wait_ms: None,
                        capacity_used_slots: None,
                        capacity_total_slots: None,
                    },
                )
                .await
                .unwrap();
        } else {
            observations.create_model_request(request).await.unwrap();
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("select count(*) from model_requests where id=$1")
                .bind(id.as_str())
                .fetch_one(&db.pool)
                .await
                .unwrap(),
            0
        );
        sqlx::query("update account_groups set model_multipliers=$2 where id=$1")
            .bind(group_id("delayed").as_str())
            .bind(serde_json::json!({"test-model":"4"}))
            .execute(&db.pool)
            .await
            .unwrap();
        let amount = ClientBudgetCharge {
            scope: Some(frozen),
            request_id: id.clone(),
            ..charge("delayed", "unused", "3")
        };
        budget.settle(amount.clone()).await.unwrap();
        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let task = tokio::spawn(async move { writer.run(stop).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let rule: Option<String> = sqlx::query_scalar(
                    "select billing_multiplier::text from model_requests where id=$1",
                )
                .bind(id.as_str())
                .fetch_optional(&db.pool)
                .await
                .unwrap();
                if let Some(rule) = rule {
                    assert_eq!(rule, "1.5000000000");
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        task.await.unwrap();
        budget.settle(amount).await.unwrap();
        let ledger: String = sqlx::query_scalar(
            "select amount_usd::text from user_group_charge_events where request_id=$1",
        )
        .bind(id.as_str())
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(ledger, "4.5000000000");
    }
    assert_eq!(status(&db, "delayed").await.daily_used_usd.canonical(), "9");
    db.close().await;
}
async fn model_billing_for_endpoint(endpoint: &str) {
    let Some(db) = TestDatabase::create("model_billing").await else {
        return;
    };
    seed(&db, "priced", "0.5", "5").await;
    sqlx::query(
        "update account_groups set model_multipliers = '{\"test-model\":\"2.5\"}' where id = $1",
    )
    .bind(group_id("priced").as_str())
    .execute(&db.pool)
    .await
    .unwrap();
    let insert = "insert into model_requests(id, client_api_key_ref, user_id, config_revision, protocol, operation, endpoint, client_transport, requested_model_id, routing_scope, routing_group_refs, routing_group_names_snapshot, started_at, deadline_at, outcome, completed_at, cost_source, cost_amount, cost_currency)
        values ($1, 'priced', 'budget-user', 1, 'openai', 'responses', $4, 'http_sse', $2, 'groups', $3, '[\"priced\"]', now(), now() + interval '1 hour', 'failed', now(), 'calculated', 0.2, 'USD')";
    for (id, model) in [
        ("req_rate-old", "test-model"),
        ("req_rate-default", "other-model"),
    ] {
        sqlx::query(insert)
            .bind(id)
            .bind(model)
            .bind(vec![group_id("priced").to_string()])
            .bind(endpoint)
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let store = PgClientBudgetStore::new(db.pool.clone());
    let frozen = store
        .admit(
            key_id("priced"),
            Some(group_id("priced")),
            Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
        )
        .await
        .unwrap();
    sqlx::query(
        "update account_groups set model_multipliers = '{\"test-model\":\"4\"}' where id = $1",
    )
    .bind(group_id("priced").as_str())
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(insert)
        .bind("req_rate-new")
        .bind("test-model")
        .bind(vec![group_id("priced").to_string()])
        .bind(endpoint)
        .execute(&db.pool)
        .await
        .unwrap();
    let rates: Vec<(String, String)> =
        sqlx::query_as("select id, billed_cost_amount::text from model_requests order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        rates,
        vec![
            ("req_rate-default".into(), "0.2000000000".into()),
            ("req_rate-new".into(), "0.8000000000".into()),
            ("req_rate-old".into(), "0.5000000000".into())
        ]
    );
    // The list adapter must carry the original amount separately for provider price validation.
    use gateway_admin::ports::store::ObservabilityStore as _;
    let detail = super::admin_observability_store(&db.pool)
        .usage_record_detail("req_rate-old", None)
        .await
        .unwrap();
    let original = &detail.request;
    let Some(gateway_admin::model::observability::UsageBilling::GroupAdjusted {
        multiplier,
        original,
        total,
        ..
    }) = &original.billing
    else {
        panic!("frozen group billing missing");
    };
    assert_eq!(multiplier.as_str(), "2.5");
    assert_eq!(total.amount.as_str(), "0.5");
    let gateway_admin::model::observability::UsageBilling::Total { total, .. } = original.as_ref()
    else {
        panic!("original bill missing");
    };
    assert_eq!(total.amount.as_str(), "0.2");
    for _ in 0..2 {
        store
            .settle(ClientBudgetCharge {
                scope: Some(frozen.clone()),
                ..charge("priced", "rate-old", "0.2")
            })
            .await
            .unwrap();
    }
    assert_eq!(
        status(&db, "priced").await.daily_used_usd.canonical(),
        "0.5"
    );
    assert_eq!(
        store
            .admit(key_id("priced"), Some(group_id("priced")), None)
            .await
            .unwrap_err()
            .client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    let audit: (String, String, String) = sqlx::query_as("select raw_amount_usd::text, billing_multiplier::text, amount_usd::text from user_group_charge_events").fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        audit,
        (
            "0.2000000000".into(),
            "2.5000000000".into(),
            "0.5000000000".into()
        )
    );
    let summary = super::admin_observability_store(&db.pool)
        .usage_summary(
            gateway_admin::model::observability::TimeRange {
                start: Utc::now() - chrono::Duration::hours(1),
                end: Utc::now() + chrono::Duration::hours(1),
            },
            gateway_admin::model::observability::UsageFilter {
                owner_user_id: Some("budget-user".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        summary.total_cost_usd.as_str(),
        "1.5",
        "includes interrupted billable requests without charging twice"
    );
    db.close().await;
}

#[tokio::test]
async fn budgets_settle_exactly_once_and_enforce_each_threshold_across_store_instances() {
    let Some(database) = TestDatabase::create("budgets_exact").await else {
        return;
    };
    seed(&database, "day", "0.3", "2").await;
    seed(&database, "week", "2", "0.2").await;
    let first = PgClientBudgetStore::new(database.pool.clone());
    let second = PgClientBudgetStore::new(database.pool.clone());
    for (key, prefix, amount) in [("day", "d", "0.1"), ("week", "w", "0.1")] {
        for _ in 0..3 {
            // 已准入请求可完成并超过限额，不预占估算费用。
            first
                .admit(key_id(key), Some(group_id(key)), None)
                .await
                .unwrap();
        }
        for index in 0..3 {
            let id = format!("{prefix}-{index}");
            let (a, b) = tokio::join!(
                first.settle(charge(key, &id, amount)),
                second.settle(charge(key, &id, amount))
            );
            a.unwrap();
            b.unwrap();
        }
    }
    let day = status(&database, "day").await;
    assert_eq!(day.daily_used_usd.canonical(), "0.3");
    assert_eq!(day.weekly_used_usd.canonical(), "0.3");
    let day_error = second
        .admit(key_id("day"), Some(group_id("day")), None)
        .await
        .unwrap_err();
    assert_eq!(day_error.kind(), GatewayErrorKind::RateLimited);
    assert_eq!(
        day_error.client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    assert!(day_error.retry_after().is_some());
    let week_error = first
        .admit(key_id("week"), Some(group_id("week")), None)
        .await
        .unwrap_err();
    assert_eq!(
        week_error.client_error_code(),
        Some("group_weekly_budget_exceeded")
    );
    let event_count: i64 = sqlx::query_scalar("select count(*) from user_group_charge_events")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(event_count, 6, "rejected admission must not create charges");
    database.close().await;
}

#[tokio::test]
async fn window_rollover_is_shanghai_midnight_and_seven_days_with_late_settlement() {
    let Some(database) = TestDatabase::create("budgets_windows").await else {
        return;
    };
    seed(&database, "key", "1", "2").await;
    let store = PgClientBudgetStore::new(database.pool.clone());
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    let (day_start, day_end, week_end): (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) =
        sqlx::query_as("select daily_start, daily_end, weekly_end from user_group_budget_windows where user_id = 'budget-user'")
            .fetch_one(&database.pool).await.unwrap();
    assert_eq!(day_start.timestamp().rem_euclid(86400), 16 * 3600);
    assert_eq!((day_end - day_start).num_hours(), 24);
    assert_eq!((week_end - day_start).num_hours(), 168);
    store.settle(charge("key", "first", "1")).await.unwrap();
    sqlx::query("select advance_subscription_windows('budget-user', $1, $2)")
        .bind(group_id("key").as_str())
        .bind(day_end + chrono::Duration::seconds(1))
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "0"
    );
    assert_eq!(
        status(&database, "key").await.weekly_used_usd.canonical(),
        "1"
    );
    store
        .settle(ClientBudgetCharge {
            components_usd: [gateway_core::metering::Decimal::ZERO; 4],
            completed_at: (day_end + chrono::Duration::seconds(2)).into(),
            ..charge("key", "second", "1")
        })
        .await
        .unwrap();
    sqlx::query("select advance_subscription_windows('budget-user', $1, $2)")
        .bind(group_id("key").as_str())
        .bind(week_end + chrono::Duration::seconds(1))
        .execute(&database.pool)
        .await
        .unwrap();
    let old = ClientBudgetCharge {
        components_usd: [gateway_core::metering::Decimal::ZERO; 4],
        completed_at: (day_start - chrono::Duration::seconds(1)).into(),
        ..charge("key", "after-reset", "0.9")
    };
    store.settle(old).await.unwrap();
    let reset = status(&database, "key").await;
    assert_eq!(reset.daily_used_usd.canonical(), "0");
    assert_eq!(reset.weekly_used_usd.canonical(), "0");
    assert!(reset.weekly_resets_at.is_some());
    database.close().await;
}

#[tokio::test]
async fn zero_cost_and_interrupted_requests_never_block_limited_keys() {
    let Some(database) = TestDatabase::create("budgets_zero").await else {
        return;
    };
    seed(&database, "key", "1", "5").await;
    let store = PgClientBudgetStore::new(database.pool.clone());
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    store.settle(charge("key", "no-cost", "0")).await.unwrap();
    // 模拟准入后进程退出，没有费用可结算；重启后仍应允许同一 Key 使用。
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    let restarted = PgClientBudgetStore::new(database.pool.clone());
    restarted
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    let events: i64 = sqlx::query_scalar("select count(*) from user_group_charge_events")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(events, 1, "admission must not leave pending charges");
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "0"
    );
    restarted
        .settle(charge("key", "known", "0.4"))
        .await
        .unwrap();
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "0.4"
    );
    database.close().await;
}

#[tokio::test]
async fn transient_settlement_failure_rolls_back_and_retries_exact_cost_before_admission() {
    let Some(database) = TestDatabase::create("budgets_retry").await else {
        return;
    };
    seed(&database, "key", "1", "5").await;
    let store = PgClientBudgetStore::new(database.pool.clone());
    sqlx::query("update account_groups set model_multipliers=$2 where id=$1")
        .bind(group_id("key").as_str())
        .bind(serde_json::json!({"test-model":"2"}))
        .execute(&database.pool)
        .await
        .unwrap();
    let frozen = store
        .admit(
            key_id("key"),
            Some(group_id("key")),
            Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
        )
        .await
        .unwrap();
    let pending = ClientBudgetCharge {
        scope: Some(frozen),
        ..charge("key", "retry", "1.25")
    };
    // 在费用事件插入后让窗口写入失败，验证整个事务回滚。
    sqlx::raw_sql(
        "create function reject_budget_update() returns trigger language plpgsql as $$
        begin
            if new.daily_used_usd > 0 then raise exception 'temporary test failure'; end if;
            return new;
        end $$;
        create trigger reject_budget_update before update on user_group_budget_windows
        for each row execute function reject_budget_update();",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    assert!(store.settle(pending.clone()).await.is_err());
    let events: i64 = sqlx::query_scalar("select count(*) from user_group_charge_events")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(events, 0);
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "0"
    );
    sqlx::query("drop trigger reject_budget_update on user_group_budget_windows")
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("update account_groups set model_multipliers='{}'")
        .execute(&database.pool)
        .await
        .unwrap();
    let error = store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap_err();
    assert_eq!(
        error.client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    store.settle(pending.clone()).await.unwrap();
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "2.5"
    );
    database.close().await;
}

#[tokio::test]
async fn group_budget_updates_preserve_usage_and_disable_live_access() {
    let Some(database) = TestDatabase::create("budgets_policy").await else {
        return;
    };
    seed(&database, "key", "0", "0").await;
    let store = PgClientBudgetStore::new(database.pool.clone());
    let admin = PgAccountGroupRepository::new(database.pool.clone());
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    store
        .settle(charge("key", "unlimited", "2.75"))
        .await
        .unwrap();
    let mut update = UpdateAccountGroup {
        model_multipliers: Default::default(),
        id: group_id("key"),
        name: "key".into(),
        description: None,
        color: AccountGroupColor::parse("#64748BFF").unwrap(),
        budget: gateway_core::engine::budget::ClientBudgetLimits {
            daily_usd: "2".parse().unwrap(),
            weekly_usd: "10".parse().unwrap(),
        },
    };
    admin
        .update_account_group(update.clone(), &context())
        .await
        .unwrap();
    let current = status(&database, "key").await;
    assert_eq!(current.limits.daily_usd.canonical(), "2");
    assert_eq!(current.daily_used_usd.canonical(), "2.75");
    assert_eq!(
        store
            .admit(key_id("key"), Some(group_id("key")), None)
            .await
            .unwrap_err()
            .client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    update.budget.daily_usd = gateway_core::metering::Decimal::ZERO;
    admin
        .update_account_group(update, &context())
        .await
        .unwrap();
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    sqlx::query("update users set enabled = false where id = 'budget-user'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .admit(key_id("key"), Some(group_id("key")), None)
            .await
            .unwrap_err()
            .kind(),
        GatewayErrorKind::PolicyDenied
    );
    database.close().await;
}

#[tokio::test]
async fn budget_database_outage_fails_closed() {
    let Some(database) = TestDatabase::create("budgets_outage").await else {
        return;
    };
    let store = PgClientBudgetStore::new(database.pool.clone());
    database.pool.close().await;
    assert_eq!(
        store
            .admit(key_id("key"), Some(group_id("key")), None)
            .await
            .unwrap_err()
            .client_error_code(),
        Some("key_budget_unavailable")
    );
    assert!(store.settle(charge("key", "offline", "1")).await.is_err());
    database.close().await;
}

#[tokio::test]
async fn multiple_keys_share_the_user_group_budget_but_other_users_do_not() {
    let Some(db) = TestDatabase::create("shared_user_budget").await else {
        return;
    };
    seed(&db, "first", "1", "5").await;
    sqlx::query("insert into users(id, username, password_hash, created_at, updated_at) values ('other-user', 'other-user', 'unused', now(), now())")
        .execute(&db.pool).await.unwrap();
    sqlx::query("insert into user_account_groups values ('other-user', $1)")
        .bind(group_id("first").as_str())
        .execute(&db.pool)
        .await
        .unwrap();
    for (key, owner) in [("second", "budget-user"), ("third", "other-user")] {
        sqlx::query("insert into client_api_keys(id, owner_user_id, name, key, created_at, updated_at) values ($1, $2, $1, $3, now(), now())")
            .bind(key).bind(owner).bind(format!("sk_{key:a<43}")).execute(&db.pool).await.unwrap();
        sqlx::query("insert into client_api_key_groups values ($1, $2, now())")
            .bind(key)
            .bind(group_id("first").as_str())
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let store = PgClientBudgetStore::new(db.pool.clone());
    let scope = store
        .admit(key_id("second"), Some(group_id("first")), None)
        .await
        .unwrap();
    assert_eq!(scope.user_id, "budget-user");
    store
        .settle(ClientBudgetCharge {
            components_usd: [gateway_core::metering::Decimal::ZERO; 4],
            scope: Some(scope),
            ..charge("second", "shared", "1")
        })
        .await
        .unwrap();
    for key in ["first", "second"] {
        assert_eq!(
            store
                .admit(key_id(key), Some(group_id("first")), None)
                .await
                .unwrap_err()
                .client_error_code(),
            Some("group_daily_budget_exceeded")
        );
        assert_eq!(status(&db, key).await.daily_used_usd.canonical(), "1");
    }
    assert!(
        store
            .admit(key_id("third"), Some(group_id("first")), None)
            .await
            .is_ok()
    );
    assert_eq!(status(&db, "third").await.daily_used_usd.canonical(), "0");
    db.close().await;
}

#[tokio::test]
async fn deleting_or_reassigning_keys_cannot_erase_or_move_charges() {
    let Some(db) = TestDatabase::create("frozen_budget_scope").await else {
        return;
    };
    seed(&db, "first", "1", "5").await;
    seed(&db, "other", "10", "50").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    let scope = store
        .admit(key_id("first"), Some(group_id("first")), None)
        .await
        .unwrap();
    sqlx::query(
        "update client_api_key_groups set account_group_id = $1 where client_api_key_id = 'first'",
    )
    .bind(group_id("other").as_str())
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(
        store
            .admit(key_id("first"), Some(group_id("first")), None)
            .await
            .is_err(),
        "stale routing scope must be rejected"
    );
    sqlx::query("delete from client_api_keys where id = 'first'")
        .execute(&db.pool)
        .await
        .unwrap();
    let charge = ClientBudgetCharge {
        components_usd: [gateway_core::metering::Decimal::ZERO; 4],
        scope: Some(scope),
        ..charge("first", "deleted", "0.7")
    };
    PgAccountGroupRepository::new(db.pool.clone())
        .delete_account_group(
            gateway_admin::model::account_groups::DeleteAccountGroup {
                id: group_id("first"),
            },
            &MutationContext {
                request_id: "delete-inflight-group".into(),
                actor: MutationActor::System,
            },
        )
        .await
        .expect("group with no keys can be removed while a request is in flight");
    store.settle(charge.clone()).await.unwrap();
    PgClientBudgetStore::new(db.pool.clone())
        .settle(charge)
        .await
        .unwrap();
    let old_used: String = sqlx::query_scalar("select daily_used_usd::text from user_group_budget_windows where user_id = 'budget-user' and account_group_id = $1")
        .bind(group_id("first").as_str()).fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        old_used
            .parse::<gateway_core::metering::Decimal>()
            .unwrap()
            .canonical(),
        "0.7"
    );
    assert_eq!(status(&db, "other").await.daily_used_usd.canonical(), "0");
    db.close().await;
}

#[tokio::test]
async fn revoked_grants_and_disabled_groups_are_checked_without_snapshot_refresh() {
    let Some(db) = TestDatabase::create("live_budget_grants").await else {
        return;
    };
    seed(&db, "key", "1", "5").await;
    let store = PgClientBudgetStore::new(db.pool.clone());
    store
        .admit(key_id("key"), Some(group_id("key")), None)
        .await
        .unwrap();
    sqlx::query("delete from user_account_groups where user_id = 'budget-user'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        store
            .admit(key_id("key"), Some(group_id("key")), None)
            .await
            .is_err()
    );
    sqlx::query("insert into user_account_groups values ('budget-user', $1)")
        .bind(group_id("key").as_str())
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update account_groups set enabled = false")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        store
            .admit(key_id("key"), Some(group_id("key")), None)
            .await
            .is_err()
    );
    assert!(store.admit(key_id("key"), None, None).await.is_err());
    db.close().await;
}

#[tokio::test]
async fn component_billing_freezes_scope_and_uses_the_same_amount_in_budget_ledger() {
    let Some(db) = TestDatabase::create("scoped_billing").await else {
        return;
    };
    seed(&db, "scoped", "100", "1000").await;
    sqlx::query("update account_groups set model_multipliers = $2 where id=$1")
        .bind(group_id("scoped").as_str()).bind(serde_json::json!({"test-model":{"multiplier":"3","scope":{"components":["cacheRead","output"]}}}))
        .execute(&db.pool).await.unwrap();
    let store = PgClientBudgetStore::new(db.pool.clone());
    let frozen = store
        .admit(
            key_id("scoped"),
            Some(group_id("scoped")),
            Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
        )
        .await
        .unwrap();
    let snapshot = serde_json::json!({"version":1,"total":"10","input":"4","output":"2","cacheRead":"1","cacheWrite":"1","image":{"input":"1","cacheRead":"0.5"}});
    sqlx::query("insert into model_requests(id,client_api_key_ref,user_id,config_revision,protocol,operation,endpoint,client_transport,requested_model_id,routing_scope,routing_group_refs,routing_group_names_snapshot,started_at,deadline_at,outcome,completed_at,cost_source,cost_amount,cost_currency,billing_snapshot_json) values('req_scoped','scoped','budget-user',1,'openai','responses','/v1/responses','http_sse','test-model','groups',$1,'[\"scoped\"]',now(),now()+interval '1 hour','failed',now(),'calculated',10,'USD',$2)")
        .bind(vec![group_id("scoped").to_string()]).bind(snapshot.clone()).execute(&db.pool).await.unwrap();
    let billed: String = sqlx::query_scalar(
        "select billed_cost_amount::text from model_requests where id='req_scoped'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(billed, "17.0000000000");
    sqlx::query("update account_groups set model_multipliers='{}'")
        .execute(&db.pool)
        .await
        .unwrap();
    for _ in 0..2 {
        store
            .settle(ClientBudgetCharge {
                scope: Some(frozen.clone()),
                components_usd: ["5", "2", "1.5", "1"].map(|amount| amount.parse().unwrap()),
                ..charge("scoped", "scoped", "10")
            })
            .await
            .unwrap();
    }
    assert_eq!(status(&db, "scoped").await.daily_used_usd.canonical(), "17");
    let ledger: (String,String,serde_json::Value) = sqlx::query_as("select amount_usd::text,raw_amount_usd::text,billing_multiplier_scope from user_group_charge_events where request_id='req_scoped'").fetch_one(&db.pool).await.unwrap();
    assert_eq!(ledger.0, "17.0000000000");
    assert_eq!(ledger.1, "10.0000000000");
    assert_eq!(
        serde_json::from_value::<gateway_core::metering::MultiplierScope>(ledger.2).unwrap(),
        frozen.billing.scope().clone()
    );
    for (scope, expected) in [
        (serde_json::json!("all"), "30.0000000000"),
        (serde_json::json!({"components":["input"]}), "20.0000000000"),
        (
            serde_json::json!({"components":["cacheWrite"]}),
            "12.0000000000",
        ),
        (
            serde_json::json!({"components":["input","output","cacheRead","cacheWrite"]}),
            "29.0000000000",
        ),
    ] {
        let amount: String = sqlx::query_scalar("select scoped_billed_cost(10,3,$1,$2)::text")
            .bind(scope)
            .bind(snapshot.clone())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(amount, expected);
    }
    let unknown: String = sqlx::query_scalar(
        "select scoped_billed_cost(10,3,'{\"components\":[\"cacheRead\"]}',null)::text",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(unknown, "10");
    let zero: String = sqlx::query_scalar(
        "select scoped_billed_cost(10,0,'{\"components\":[\"cacheRead\"]}',$1)::text",
    )
    .bind(snapshot)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(zero, "8.5000000000");
    db.close().await;
}
