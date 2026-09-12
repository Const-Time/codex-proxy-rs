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
        scope: Some(gateway_core::engine::budget::ClientBudgetScope {
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
        .admit(key_id("factor"), Some(group_id("factor")))
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
            .admit(key_id("factor"), Some(group_id("factor")))
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
    let account = "acct_00000000000000000000000000000991";
    sqlx::query("insert into provider_accounts(id, provider_kind, name, authentication_kind, provider_credentials_json, has_refresh_token, credential_observed_at, created_at, updated_at) values ($1, 'openai', 'test', 'oauth', '{}', false, now(), now(), now())")
        .bind(account).execute(&db.pool).await.unwrap();
    sqlx::query("insert into account_group_accounts values ($1, $2, now())")
        .bind(group_id("linked").as_str())
        .bind(account)
        .execute(&db.pool)
        .await
        .unwrap();
    let store = PgClientBudgetStore::new(db.pool.clone());
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
        values ($1, 'priced', 'budget-user', 1, 'openai', 'responses', '/v1/responses', 'http_sse', $2, 'groups', $3, '[\"priced\"]', now(), now() + interval '1 hour', 'failed', now(), 'calculated', 0.2, 'USD')";
    for (id, model) in [
        ("req_rate-old", "test-model"),
        ("req_rate-default", "other-model"),
    ] {
        sqlx::query(insert)
            .bind(id)
            .bind(model)
            .bind(vec![group_id("priced").to_string()])
            .execute(&db.pool)
            .await
            .unwrap();
    }
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
    let store = PgClientBudgetStore::new(db.pool.clone());
    for _ in 0..2 {
        store
            .settle(charge("priced", "rate-old", "0.2"))
            .await
            .unwrap();
    }
    assert_eq!(
        status(&db, "priced").await.daily_used_usd.canonical(),
        "0.5"
    );
    assert_eq!(
        store
            .admit(key_id("priced"), Some(group_id("priced")))
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
            first.admit(key_id(key), Some(group_id(key))).await.unwrap();
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
        .admit(key_id("day"), Some(group_id("day")))
        .await
        .unwrap_err();
    assert_eq!(day_error.kind(), GatewayErrorKind::RateLimited);
    assert_eq!(
        day_error.client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    assert!(day_error.retry_after().is_some());
    let week_error = first
        .admit(key_id("week"), Some(group_id("week")))
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
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    let (day_start, day_end, week_end): (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) =
        sqlx::query_as("select daily_start, daily_end, weekly_end from user_group_budget_windows where user_id = 'budget-user'")
            .fetch_one(&database.pool).await.unwrap();
    assert_eq!(day_start.timestamp().rem_euclid(86400), 16 * 3600);
    assert_eq!((day_end - day_start).num_hours(), 24);
    assert_eq!((week_end - day_start).num_hours(), 168);
    store.settle(charge("key", "first", "1")).await.unwrap();
    sqlx::query("update user_group_budget_windows set daily_start = daily_start - interval '1 day', daily_end = daily_start")
        .execute(&database.pool).await.unwrap();
    store
        .admit(key_id("key"), Some(group_id("key")))
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
    store.settle(charge("key", "second", "1")).await.unwrap();
    sqlx::query("update user_group_budget_windows set daily_end = now() - interval '1 second', weekly_end = now() - interval '1 second'")
        .execute(&database.pool).await.unwrap();
    let virtual_reset = status(&database, "key").await;
    assert_eq!(virtual_reset.daily_used_usd.canonical(), "0");
    assert_eq!(virtual_reset.weekly_used_usd.canonical(), "0");
    store
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    let old = ClientBudgetCharge {
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
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    store.settle(charge("key", "no-cost", "0")).await.unwrap();
    // 模拟准入后进程退出，没有费用可结算；重启后仍应允许同一 Key 使用。
    store
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    let restarted = PgClientBudgetStore::new(database.pool.clone());
    restarted
        .admit(key_id("key"), Some(group_id("key")))
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
    store
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
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
    assert!(store.settle(charge("key", "retry", "1.25")).await.is_err());
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
    let error = store
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap_err();
    assert_eq!(
        error.client_error_code(),
        Some("group_daily_budget_exceeded")
    );
    store.settle(charge("key", "retry", "1.25")).await.unwrap();
    assert_eq!(
        status(&database, "key").await.daily_used_usd.canonical(),
        "1.25"
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
        .admit(key_id("key"), Some(group_id("key")))
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
            .admit(key_id("key"), Some(group_id("key")))
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
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    sqlx::query("update users set enabled = false where id = 'budget-user'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .admit(key_id("key"), Some(group_id("key")))
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
            .admit(key_id("key"), Some(group_id("key")))
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
        .admit(key_id("second"), Some(group_id("first")))
        .await
        .unwrap();
    assert_eq!(scope.user_id, "budget-user");
    store
        .settle(ClientBudgetCharge {
            scope: Some(scope),
            ..charge("second", "shared", "1")
        })
        .await
        .unwrap();
    for key in ["first", "second"] {
        assert_eq!(
            store
                .admit(key_id(key), Some(group_id("first")))
                .await
                .unwrap_err()
                .client_error_code(),
            Some("group_daily_budget_exceeded")
        );
        assert_eq!(status(&db, key).await.daily_used_usd.canonical(), "1");
    }
    assert!(
        store
            .admit(key_id("third"), Some(group_id("first")))
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
        .admit(key_id("first"), Some(group_id("first")))
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
            .admit(key_id("first"), Some(group_id("first")))
            .await
            .is_err(),
        "stale routing scope must be rejected"
    );
    sqlx::query("delete from client_api_keys where id = 'first'")
        .execute(&db.pool)
        .await
        .unwrap();
    let charge = ClientBudgetCharge {
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
        .admit(key_id("key"), Some(group_id("key")))
        .await
        .unwrap();
    sqlx::query("delete from user_account_groups where user_id = 'budget-user'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        store
            .admit(key_id("key"), Some(group_id("key")))
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
            .admit(key_id("key"), Some(group_id("key")))
            .await
            .is_err()
    );
    assert!(store.admit(key_id("key"), None).await.is_err());
    db.close().await;
}
