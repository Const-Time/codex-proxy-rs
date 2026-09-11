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
