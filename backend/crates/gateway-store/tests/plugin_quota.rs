use crate::postgres::TestDatabase;
use gateway_admin::{
    model::{MutationActor, MutationContext, plugins::*, users::SubscriptionTarget},
    ports::store::{AccountGroupStore, AdminStoreErrorKind, AuthStore},
};
use gateway_core::metering::Decimal;
use gateway_store::{
    UserAuthStore, postgres::PgAccountGroupRepository, redis::RedisAdminAuthStateRepository,
};
use sqlx::Row;
const GROUP: &str = "grp_00000000000000000000000000000077";
fn scope() -> PluginMutationScope {
    PluginMutationScope {
        instance_id: "quota-plugin".into(),
        revision: 1,
        actor: MutationContext {
            actor: MutationActor::AdminSession {
                admin_user_id: "admin".into(),
            },
            request_id: "quota-test".into(),
        },
    }
}
fn target(user: &str) -> SubscriptionTarget {
    SubscriptionTarget {
        user_id: user.into(),
        group_id: GROUP.into(),
    }
}
fn amount(s: &str) -> String {
    s.parse::<Decimal>().unwrap().canonical()
}
async fn fixture(label: &str) -> Option<(TestDatabase, UserAuthStore)> {
    let db = TestDatabase::create(label).await?;
    let Some(url) = crate::support::test_env("CPR_TEST_REDIS_URL") else {
        db.close().await;
        return None;
    };
    let client = redis::Client::open(url)
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap();
    let auth = UserAuthStore::new(
        db.pool.clone(),
        RedisAdminAuthStateRepository::new(
            client,
            &format!("plugin-quota-{}", uuid::Uuid::new_v4().simple()),
        )
        .unwrap(),
    );
    sqlx::query(
        "insert into plugin_artifacts(sha256,info,archive) values($1,'{}',decode('00','hex'))",
    )
    .bind("a".repeat(64))
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("insert into plugin_instances(id,revision,name,artifact_sha256,enabled,configuration,secrets) values('quota-plugin',1,'quota',$1,true,'{}','{}')").bind("a".repeat(64)).execute(&db.pool).await.unwrap();
    for (id, role) in [("admin", "admin"), ("alice", "user"), ("bob", "user")] {
        sqlx::query("insert into users(id,username,password_hash,role,created_at,updated_at) values($1,$1,'unused',$2,now(),now())").bind(id).bind(role).execute(&db.pool).await.unwrap();
    }
    sqlx::query("insert into account_groups(id,name,color,daily_limit_usd,weekly_limit_usd,created_at,updated_at) values($1,'Quota','#64748BFF',2,10,now(),now())").bind(GROUP).execute(&db.pool).await.unwrap();
    for user in ["alice", "bob"] {
        sqlx::query("insert into user_account_groups(user_id,account_group_id,quota_multiplier) values($1,$2,2)").bind(user).bind(GROUP).execute(&db.pool).await.unwrap();
    }
    Some((db, auth))
}
#[tokio::test]
async fn plugin_quota_reads_without_writes_and_cas_updates_only_group_limits() {
    let Some((db, auth)) = fixture("quota_limits").await else {
        return;
    };
    let groups = PgAccountGroupRepository::new(db.pool.clone());
    let budget = auth
        .plugin_shared_budget(scope(), target("alice"))
        .await
        .unwrap();
    assert!(budget.enabled);
    assert_eq!(amount(&budget.daily_limit_usd), "4");
    assert_eq!(amount(&budget.group_daily_limit_usd), "2");
    assert_eq!(amount(&budget.quota_multiplier), "2");
    assert!(budget.daily_resets_at.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_budget_windows")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        auth.plugin_shared_budget(scope(), target("admin"))
            .await
            .unwrap()
            .enabled,
        "implicit administrator grants are visible before the first Key"
    );
    let command = || PluginGroupBudgetUpdate {
        group_id: GROUP.into(),
        expected_revision: budget.revision.get(),
        daily_limit_usd: Some("3".into()),
        weekly_limit_usd: None,
    };
    let (a, b) = tokio::join!(
        groups.update_plugin_group_budget(scope(), command()),
        groups.update_plugin_group_budget(scope(), command())
    );
    let success = match (a, b) {
        (Ok(v), Err(e)) | (Err(e), Ok(v)) => {
            assert_eq!(e.kind(), AdminStoreErrorKind::Conflict);
            v
        }
        other => panic!("expected one CAS success: {other:?}"),
    };
    assert_eq!(success.revision.get(), budget.revision.get() + 1);
    let current = auth
        .plugin_shared_budget(scope(), target("alice"))
        .await
        .unwrap();
    assert_eq!(amount(&current.daily_limit_usd), "6");
    assert_eq!(amount(&current.weekly_limit_usd), "20");
    assert_eq!(
        sqlx::query_scalar::<_, String>("select name from account_groups where id=$1")
            .bind(GROUP)
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        "Quota"
    );
    let mut invalid = command();
    invalid.expected_revision = current.revision.get();
    invalid.daily_limit_usd = Some("-1".into());
    assert_eq!(
        groups
            .update_plugin_group_budget(scope(), invalid)
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Invalid
    );
    let mut overflow = command();
    overflow.expected_revision = current.revision.get();
    overflow.daily_limit_usd = Some("9999999999".into());
    assert_eq!(
        groups
            .update_plugin_group_budget(scope(), overflow)
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Invalid,
        "effective limit overflow must roll back, including the revision"
    );
    assert_eq!(
        auth.plugin_shared_budget(scope(), target("alice"))
            .await
            .unwrap(),
        current
    );
    let mut denied = scope();
    denied.actor.actor = MutationActor::AdminSession {
        admin_user_id: "alice".into(),
    };
    assert_eq!(
        auth.plugin_shared_budget(denied.clone(), target("alice"))
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Invalid
    );
    let mut c = command();
    c.expected_revision = current.revision.get();
    assert!(groups.update_plugin_group_budget(denied, c).await.is_err());
    let mut stale = scope();
    stale.revision = 2;
    assert_eq!(
        auth.plugin_shared_budget(stale, target("alice"))
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    sqlx::query("update users set enabled=false where id='admin'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        auth.plugin_shared_budget(scope(), target("alice"))
            .await
            .is_err()
    );
    let mut api = scope();
    api.actor.actor = MutationActor::AdminApiKey;
    assert!(
        auth.plugin_shared_budget(api, target("alice"))
            .await
            .is_ok()
    );
    let audit=sqlx::query("select to_jsonb(a) as value from admin_audit_events a where action='plugin.quota.update_group_limits'").fetch_all(&db.pool).await.unwrap();
    assert_eq!(audit.len(), 1);
    let audit = audit[0].get::<serde_json::Value, _>("value").to_string();
    assert!(audit.contains("admin") && audit.contains("quota-test"));
    assert!(!audit.contains("password_hash"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    db.close().await;
}
#[tokio::test]
async fn plugin_quota_reset_is_atomic_scoped_idempotent_across_upgrade_and_preserves_ledger() {
    let Some((db, auth)) = fixture("quota_reset").await else {
        return;
    };
    for user in ["alice", "bob"] {
        sqlx::query("insert into user_group_budget_windows(user_id,account_group_id,daily_start,daily_end,weekly_start,weekly_end,daily_used_usd,weekly_used_usd) values($1,$2,now()-interval '1 hour',now()+interval '1 day',now()-interval '1 hour',now()+interval '7 days',1,3)").bind(user).bind(GROUP).execute(&db.pool).await.unwrap();
        sqlx::query("insert into user_group_charge_events(request_id,user_id,account_group_id,client_api_key_ref,completed_at,amount_usd) values($1,$1,$2,'fixture-key',now()-interval '1 minute',1)").bind(user).bind(GROUP).execute(&db.pool).await.unwrap();
    }
    let before = auth
        .plugin_shared_budget(scope(), target("bob"))
        .await
        .unwrap();
    let event = uuid::Uuid::new_v4().to_string();
    let reset = || PluginBudgetReset {
        event_id: event.clone(),
        targets: vec![target("alice"), target("alice")],
    };
    let (a, b) = tokio::join!(
        auth.reset_plugin_shared_budget(scope(), reset()),
        auth.reset_plugin_shared_budget(scope(), reset())
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a, b);
    assert_eq!(a.value, 1);
    let alice = auth
        .plugin_shared_budget(scope(), target("alice"))
        .await
        .unwrap();
    assert_eq!(amount(&alice.daily_used_usd), "0");
    assert_eq!(amount(&alice.weekly_used_usd), "0");
    assert!(alice.last_reset_at.is_some());
    let bob = auth
        .plugin_shared_budget(scope(), target("bob"))
        .await
        .unwrap();
    assert_eq!(bob.daily_used_usd, before.daily_used_usd);
    assert_eq!(bob.weekly_resets_at, before.weekly_resets_at);
    assert_eq!(bob.last_reset_at, before.last_reset_at);
    // Later usage survives a retry, even after a new plugin generation is installed.
    sqlx::query("update user_group_budget_windows set daily_used_usd=0.5,weekly_used_usd=0.5 where user_id='alice'").execute(&db.pool).await.unwrap();
    sqlx::query("update plugin_instances set revision=2 where id='quota-plugin'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        auth.reset_plugin_shared_budget(scope(), reset())
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    let mut upgraded = scope();
    upgraded.revision = 2;
    assert_eq!(
        auth.reset_plugin_shared_budget(upgraded.clone(), reset())
            .await
            .unwrap(),
        a
    );
    assert_eq!(
        amount(
            &auth
                .plugin_shared_budget(upgraded.clone(), target("alice"))
                .await
                .unwrap()
                .daily_used_usd
        ),
        "0.5"
    );
    let mut changed = reset();
    changed.targets = vec![target("bob")];
    assert_eq!(
        auth.reset_plugin_shared_budget(upgraded.clone(), changed)
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    let mixed = PluginBudgetReset {
        event_id: uuid::Uuid::new_v4().to_string(),
        targets: vec![target("alice"), target("missing")],
    };
    assert_eq!(
        auth.reset_plugin_shared_budget(upgraded.clone(), mixed)
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::NotFound
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from subscription_reset_events where reason='plugin'"
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from admin_audit_events where action='plugin.quota.reset'"
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        2
    );
    // Retained receipts survive removal of their original scope.
    sqlx::query("delete from user_account_groups where user_id='alice'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        auth.reset_plugin_shared_budget(upgraded.clone(), reset())
            .await
            .unwrap(),
        a
    );
    sqlx::query("update plugin_instances set enabled=false where id='quota-plugin'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        auth.reset_plugin_shared_budget(upgraded, reset())
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    db.close().await;
}
