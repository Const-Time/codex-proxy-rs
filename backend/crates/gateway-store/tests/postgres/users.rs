use super::TestDatabase;
use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        client_keys::{
            ClientKeyListQuery, ClientKeyPageSize, ClientKeySort, ClientKeySortField,
            DeleteClientKey, NewClientKey, SortDirection,
        },
        users::{SubscriptionTarget, UpdateUser, UserRole},
    },
    ports::store::{AdminStoreErrorKind, AuthStore, ClientKeyStore},
};
use gateway_core::{
    policy::{ClientApiKeyId, RateLimits},
    routing::AccountGroupId,
};
use gateway_store::{
    UserAuthStore, postgres::PgAdminClientKeyStore, redis::RedisAdminAuthStateRepository,
};

const GROUP: &str = "grp_00000000000000000000000000000099";
fn context(user: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: user.into(),
        },
        request_id: format!("request-{user}"),
    }
}
async fn auth_store(db: &TestDatabase) -> Option<UserAuthStore> {
    let url = crate::support::test_env("CPR_TEST_REDIS_URL")?;
    let client = redis::Client::open(url)
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap();
    Some(UserAuthStore::new(
        db.pool.clone(),
        RedisAdminAuthStateRepository::new(
            client,
            &format!("users-test-{}", uuid::Uuid::new_v4().simple()),
        )
        .unwrap(),
    ))
}
async fn seed_group(db: &TestDatabase) {
    sqlx::query("insert into account_groups(id, name, color, created_at, updated_at) values ($1, 'Users group', '#64748BFF', now(), now())")
        .bind(GROUP).execute(&db.pool).await.unwrap();
}

#[tokio::test]
async fn optional_display_names_do_not_change_email_identity_or_need_to_be_unique() {
    let Some(db) = TestDatabase::create("display_names").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin@example.com", "admin-hash")
        .await
        .unwrap();
    let admin = context("admin@example.com");
    let first = auth
        .create_user(
            "user-a",
            gateway_admin::model::users::UserIdentity::new("First@example.com", "自定义名字"),
            "hash-a",
            &[],
            RateLimits::unlimited(),
            &admin,
        )
        .await
        .unwrap();
    auth.create_user(
        "user-b",
        gateway_admin::model::users::UserIdentity::new("second@example.com", "自定义名字"),
        "hash-b",
        &[],
        RateLimits::unlimited(),
        &admin,
    )
    .await
    .unwrap();
    assert_eq!(first.display_name, "自定义名字");
    assert_eq!(
        auth.find_user("first@EXAMPLE.com")
            .await
            .unwrap()
            .unwrap()
            .id,
        "user-a"
    );
    assert!(auth.find_user("自定义名字").await.unwrap().is_none());
    let (_, changed) = auth
        .update_user(
            UpdateUser {
                id: first.id,
                username: None,
                display_name: Some(String::new()),
                enabled: true,
                group_ids: vec![],
                quota_multipliers: None,
                limits: RateLimits::unlimited(),
            },
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(changed.username, "First@example.com");
    assert!(changed.display_name.is_empty());
    assert_eq!(
        auth.load_password_hash("user-a").await.unwrap().as_deref(),
        Some("hash-a")
    );
    let legacy = auth.load_user("admin@example.com").await.unwrap().unwrap();
    assert_eq!(legacy.username, "admin@example.com");
    assert!(legacy.display_name.is_empty());
    db.close().await;
}

#[tokio::test]
async fn operation_logs_keep_actor_snapshots_and_bind_filters() {
    use gateway_admin::model::operations::{OperationLog, OperationLogQuery};
    let Some(db) = TestDatabase::create("operation_logs").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin@example.com", "admin-hash")
        .await
        .unwrap();
    let now = chrono::Utc::now();
    for (id, actor, status) in [
        ("audit-ok", Some("admin@example.com".to_owned()), 200),
        ("audit-denied", None, 401),
    ] {
        auth.record_operation(OperationLog {
            id: id.into(),
            occurred_at: now,
            actor_user_id: actor,
            email: None,
            username: None,
            auth_method: if status == 200 {
                "session"
            } else {
                "anonymous"
            }
            .into(),
            method: "POST".into(),
            path: "/api/admin/users/update".into(),
            status,
            duration_ms: 10,
            client_ip: Some("127.0.0.1".into()),
            forwarded_ip: Some("192.0.2.1".into()),
            request_id: id.into(),
        })
        .await
        .unwrap();
    }
    let query = || OperationLogQuery {
        email: None,
        action: None,
        ip: None,
        method: None,
        auth_method: None,
        result: None,
        start_time: Some(now - chrono::Duration::hours(1)),
        end_time: Some(now + chrono::Duration::hours(1)),
        page: Some(1),
    };
    let all = auth.operation_logs(query()).await.unwrap();
    assert_eq!(all.total, 2);
    assert_eq!(
        all.items.iter().find(|i| i["id"] == "audit-ok").unwrap()["email"],
        "admin@example.com"
    );
    let mut filtered = query();
    filtered.result = Some("failure".into());
    let failures = auth.operation_logs(filtered).await.unwrap();
    assert_eq!(failures.total, 1);
    assert_eq!(failures.items[0]["status"], 401);
    assert!(failures.items[0]["email"].is_null());
    let mut filtered = query();
    filtered.email = Some("%' or true --".into());
    assert_eq!(auth.operation_logs(filtered).await.unwrap().total, 0);
    let mut filtered = query();
    filtered.email = Some("ADMIN@".into());
    filtered.ip = Some("192.0.2.1".into());
    assert_eq!(auth.operation_logs(filtered).await.unwrap().total, 1);
    assert!(
        !serde_json::to_string(&all.items)
            .unwrap()
            .contains("admin-hash")
    );
    db.close().await;
}

#[tokio::test]
async fn administrator_can_scope_own_keys_without_losing_management_or_session() {
    let Some(db) = TestDatabase::create("admin_personal_grants").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin", "hash")
        .await
        .unwrap();
    seed_group(&db).await;
    let before = auth.load_user("admin").await.unwrap().unwrap();
    assert_eq!(before.group_ids, vec![GROUP]);
    let keys = PgAdminClientKeyStore::new(db.pool.clone());
    keys.create_client_key(key("admin-own"), &context("admin"))
        .await
        .unwrap();
    let mut update = UpdateUser {
        display_name: None,
        username: None,
        id: "admin".into(),
        enabled: true,
        limits: before.limits,
        group_ids: vec![GROUP.into()],
        quota_multipliers: Some([(GROUP.into(), "2".into())].into()),
    };
    let (_, changed) = auth
        .update_user(update.clone(), &context("admin"))
        .await
        .unwrap();
    assert_eq!(changed.auth_version, before.auth_version);
    assert_eq!(changed.role, UserRole::Admin);
    assert_eq!(changed.quota_multipliers[GROUP], "2.00");
    update.group_ids.clear();
    update.quota_multipliers = None;
    auth.update_user(update, &context("admin")).await.unwrap();
    assert!(auth.user_groups("admin").await.unwrap().is_empty());
    assert!(
        keys.create_client_key(key("admin-denied"), &context("admin"))
            .await
            .is_err()
    );
    use gateway_core::engine::budget::ClientBudgetPort as _;
    assert!(
        gateway_store::postgres::PgClientBudgetStore::new(db.pool.clone())
            .admit(
                ClientApiKeyId::new("admin-own").unwrap(),
                Some(AccountGroupId::new(GROUP).unwrap())
            )
            .await
            .is_err()
    );
    // System administration remains available even with no personal group grants.
    auth.create_user(
        "another",
        gateway_admin::model::users::UserIdentity::new("another", ""),
        "hash",
        &[],
        RateLimits::unlimited(),
        &context("admin"),
    )
    .await
    .unwrap();
    db.close().await;
}

#[tokio::test]
async fn email_rename_preserves_identity_password_keys_and_quota_and_rejects_duplicates() {
    let Some(db) = TestDatabase::create("user_email_rename").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin@example.com", "admin-hash")
        .await
        .unwrap();
    seed_group(&db).await;
    let admin = context("admin@example.com");
    let original = auth
        .create_user(
            "stable-user",
            gateway_admin::model::users::UserIdentity::new("Original@Example.com", ""),
            "existing-password-hash",
            &[GROUP.into()],
            RateLimits::unlimited(),
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(
        auth.find_user("original@example.com")
            .await
            .unwrap()
            .unwrap()
            .id,
        original.id
    );
    let keys = PgAdminClientKeyStore::new(db.pool.clone());
    keys.create_client_key(key("existing-email-key"), &context(&original.id))
        .await
        .unwrap();
    sqlx::query("select reset_user_subscriptions('seed-email',null,null,'manual',now())")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "update user_group_budget_windows set daily_used_usd=7, weekly_used_usd=9 where user_id=$1",
    )
    .bind(&original.id)
    .execute(&db.pool)
    .await
    .unwrap();
    let mut update = UpdateUser {
        display_name: None,
        username: Some("Changed@Example.com".into()),
        id: original.id.clone(),
        enabled: true,
        group_ids: original.group_ids.clone(),
        quota_multipliers: None,
        limits: original.limits,
    };
    let (_, changed) = auth.update_user(update.clone(), &admin).await.unwrap();
    assert_eq!(changed.id, original.id);
    assert_eq!(changed.group_ids, original.group_ids);
    assert_eq!(
        auth.load_password_hash(&original.id)
            .await
            .unwrap()
            .as_deref(),
        Some("existing-password-hash")
    );
    assert!(
        auth.find_user("original@example.com")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        auth.find_user("changed@example.COM")
            .await
            .unwrap()
            .unwrap()
            .id,
        original.id
    );
    assert!(
        keys.reveal_client_key(
            &original.id,
            &ClientApiKeyId::new("existing-email-key").unwrap()
        )
        .await
        .unwrap()
        .is_some()
    );
    let usage: (String, String) = sqlx::query_as("select daily_used_usd::text, weekly_used_usd::text from user_group_budget_windows where user_id=$1").bind(&original.id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(usage.0.parse::<f64>().unwrap(), 7.0);
    assert_eq!(usage.1.parse::<f64>().unwrap(), 9.0);
    update.username = Some("ADMIN@example.com".into());
    assert_eq!(
        auth.update_user(update.clone(), &admin)
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    assert_eq!(
        auth.load_user(&original.id)
            .await
            .unwrap()
            .unwrap()
            .username,
        "Changed@Example.com"
    );
    update.username = None;
    assert_eq!(
        auth.update_user(update, &admin).await.unwrap().1.username,
        "Changed@Example.com"
    );
    let admin_before = auth.load_user("admin@example.com").await.unwrap().unwrap();
    let (_, renamed_admin) = auth
        .update_user(
            UpdateUser {
                display_name: None,
                username: Some("renamed-admin@example.com".into()),
                id: admin_before.id.clone(),
                enabled: true,
                group_ids: admin_before.group_ids.clone(),
                quota_multipliers: None,
                limits: admin_before.limits,
            },
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(renamed_admin.auth_version, admin_before.auth_version);
    assert!(
        !auth
            .create_password_hash_if_absent("admin@example.com", "replacement-hash")
            .await
            .unwrap()
    );
    assert_eq!(
        auth.load_password_hash(&admin_before.id)
            .await
            .unwrap()
            .as_deref(),
        Some("admin-hash")
    );
    assert!(auth.find_user("admin@example.com").await.unwrap().is_none());
    assert_eq!(
        auth.find_user("renamed-admin@example.com")
            .await
            .unwrap()
            .unwrap()
            .id,
        admin_before.id
    );
    db.close().await;
}

#[tokio::test]
async fn selected_resets_use_exact_pairs_and_preserve_unselected_usage() {
    let Some(db) = TestDatabase::create("selected_subscription_pairs").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin", "hash")
        .await
        .unwrap();
    seed_group(&db).await;
    let other = "grp_00000000000000000000000000000098";
    sqlx::query("insert into account_groups(id,name,color,created_at,updated_at) values($1,'Other','#64748BFF',now(),now())").bind(other).execute(&db.pool).await.unwrap();
    for user in ["alice", "bob"] {
        auth.create_user(
            user,
            gateway_admin::model::users::UserIdentity::new(user, ""),
            "hash",
            &[GROUP.into(), other.into()],
            RateLimits::unlimited(),
            &context("admin"),
        )
        .await
        .unwrap();
    }
    sqlx::query("select reset_user_subscriptions('seed',null,null,'manual',now())")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update user_group_budget_windows set daily_used_usd=7, weekly_used_usd=7")
        .execute(&db.pool)
        .await
        .unwrap();
    let targets = vec![
        SubscriptionTarget {
            user_id: "alice".into(),
            group_id: GROUP.into(),
        },
        SubscriptionTarget {
            user_id: "bob".into(),
            group_id: other.into(),
        },
    ];
    assert_eq!(
        auth.reset_subscriptions("selected", &targets, &context("admin"))
            .await
            .unwrap(),
        2
    );
    for row in auth.subscriptions().await.unwrap() {
        let selected = targets
            .iter()
            .any(|t| t.user_id == row.user_id && t.group_id == row.group_id);
        assert_eq!(
            row.weekly_used_usd.parse::<f64>().unwrap(),
            if selected { 0.0 } else { 7.0 }
        );
    }
    sqlx::query("update user_group_budget_windows set daily_used_usd=2, weekly_used_usd=2 where user_id='alice'").execute(&db.pool).await.unwrap();
    assert_eq!(
        auth.reset_subscriptions("selected", &targets, &context("admin"))
            .await
            .unwrap(),
        2
    );
    assert!(
        auth.subscriptions()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.user_id == "alice")
            .all(|r| r.weekly_used_usd.parse::<f64>().unwrap() == 2.0)
    );
    db.close().await;
}
fn key(id: &str) -> NewClientKey {
    NewClientKey {
        id: ClientApiKeyId::new(id).unwrap(),
        name: id.into(),
        label: None,
        group_ids: vec![AccountGroupId::new(GROUP).unwrap()],
        limits: RateLimits::unlimited(),
        plaintext: format!("sk_{id:a<43}"),
    }
}

#[tokio::test]
async fn administrator_creates_regular_users_and_only_owner_can_manage_keys() {
    let Some(db) = TestDatabase::create("users_keys").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin", "initial-hash")
        .await
        .unwrap();
    seed_group(&db).await;
    for user in ["alice", "bob"] {
        let created = auth
            .create_user(
                user,
                gateway_admin::model::users::UserIdentity::new(user, ""),
                "user-hash",
                &[GROUP.into()],
                gateway_core::policy::RateLimits::unlimited(),
                &context("admin"),
            )
            .await
            .unwrap();
        assert_eq!(created.role, UserRole::User);
        assert!(created.enabled);
    }
    assert_eq!(auth.find_user("ALICE").await.unwrap().unwrap().id, "alice");
    sqlx::query(
        "update account_groups set daily_limit_usd = 10, weekly_limit_usd = 100 where id = $1",
    )
    .bind(GROUP)
    .execute(&db.pool)
    .await
    .unwrap();
    let mut update = UpdateUser {
        display_name: None,
        username: None,
        id: "alice".into(),
        enabled: true,
        group_ids: vec![GROUP.into()],
        limits: RateLimits::unlimited(),
        quota_multipliers: Some([(GROUP.into(), "2".into())].into()),
    };
    auth.update_user(update.clone(), &context("admin"))
        .await
        .unwrap();
    update.quota_multipliers = None;
    auth.update_user(update, &context("admin")).await.unwrap();
    assert_eq!(
        auth.load_user("alice")
            .await
            .unwrap()
            .unwrap()
            .quota_multipliers[GROUP],
        "2.00"
    );
    assert_eq!(
        auth.user_groups("alice").await.unwrap()[0]
            .budget
            .limits
            .weekly_usd
            .canonical(),
        "200"
    );
    assert_eq!(
        auth.user_groups("bob").await.unwrap()[0]
            .budget
            .limits
            .weekly_usd
            .canonical(),
        "100"
    );
    let subscriptions = auth.subscriptions().await.unwrap();
    assert_eq!(subscriptions.len(), 2);
    let targets = vec![SubscriptionTarget {
        user_id: "alice".into(),
        group_id: GROUP.into(),
    }];
    assert!(
        auth.reset_subscriptions("test-reset", &targets, &context("alice"))
            .await
            .is_err()
    );
    assert_eq!(
        auth.reset_subscriptions("test-reset", &targets, &context("admin"))
            .await
            .unwrap(),
        1
    );
    assert!(
        auth.reset_subscriptions("empty-reset", &[], &context("admin"))
            .await
            .is_err()
    );
    let windows: Vec<String> =
        sqlx::query_scalar("select user_id from user_group_budget_windows order by user_id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(windows, vec!["alice"]);
    let different = vec![SubscriptionTarget {
        user_id: "bob".into(),
        group_id: GROUP.into(),
    }];
    assert!(
        auth.reset_subscriptions("test-reset", &different, &context("admin"))
            .await
            .is_err()
    );
    assert_eq!(
        auth.create_user(
            "duplicate",
            gateway_admin::model::users::UserIdentity::new("Alice", ""),
            "hash",
            &[],
            gateway_core::policy::RateLimits::unlimited(),
            &context("admin")
        )
        .await
        .unwrap_err()
        .kind(),
        AdminStoreErrorKind::Conflict
    );
    assert!(
        auth.create_user(
            "intruder",
            gateway_admin::model::users::UserIdentity::new("intruder", ""),
            "hash",
            &[],
            gateway_core::policy::RateLimits::unlimited(),
            &context("alice")
        )
        .await
        .is_err()
    );
    let keys = PgAdminClientKeyStore::new(db.pool.clone());
    keys.create_client_key(key("alice-key"), &context("alice"))
        .await
        .unwrap();
    keys.create_client_key(key("bob-key"), &context("bob"))
        .await
        .unwrap();
    let id = ClientApiKeyId::new("alice-key").unwrap();
    assert!(
        keys.reveal_client_key("alice", &id)
            .await
            .unwrap()
            .is_some()
    );
    for viewer in ["bob", "admin"] {
        assert!(keys.reveal_client_key(viewer, &id).await.unwrap().is_none());
        assert_eq!(
            keys.delete_client_key(DeleteClientKey { id: id.clone() }, &context(viewer))
                .await
                .unwrap_err()
                .kind(),
            AdminStoreErrorKind::NotFound
        );
    }
    let page = keys
        .list_client_keys(
            "bob",
            ClientKeyListQuery {
                cursor: None,
                page_size: ClientKeyPageSize::new(10).unwrap(),
                search: None,
                sort: ClientKeySort {
                    field: ClientKeySortField::CreatedAt,
                    direction: SortDirection::Desc,
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id.as_str(), "bob-key");
    let deployment = MutationContext {
        actor: MutationActor::AdminApiKey,
        request_id: "api-key".into(),
    };
    assert!(
        keys.create_client_key(key("impersonation"), &deployment)
            .await
            .is_err()
    );
    let mut no_group = key("no-group");
    no_group.group_ids.clear();
    assert!(
        keys.create_client_key(no_group, &context("alice"))
            .await
            .is_err()
    );
    auth.update_user(
        UpdateUser {
            display_name: None,
            username: None,
            quota_multipliers: Default::default(),
            limits: gateway_core::policy::RateLimits::unlimited(),
            id: "alice".into(),
            enabled: true,
            group_ids: vec![],
        },
        &context("admin"),
    )
    .await
    .unwrap();
    assert!(
        keys.create_client_key(key("ungranted"), &context("alice"))
            .await
            .is_err()
    );
    assert!(auth.user_groups("alice").await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn disabling_and_password_changes_invalidate_versions_and_audit_atomically() {
    let Some(db) = TestDatabase::create("users_versions").await else {
        return;
    };
    let Some(auth) = auth_store(&db).await else {
        db.close().await;
        return;
    };
    auth.create_password_hash_if_absent("admin", "initial-hash")
        .await
        .unwrap();
    let user = auth
        .create_user(
            "alice",
            gateway_admin::model::users::UserIdentity::new("alice", ""),
            "old-hash",
            &[],
            gateway_core::policy::RateLimits::unlimited(),
            &context("admin"),
        )
        .await
        .unwrap();
    assert!(
        auth.change_password("alice", user.auth_version, "new-hash", &context("admin"))
            .await
            .is_err(),
        "admin must not impersonate a user's password change"
    );
    auth.change_password("alice", user.auth_version, "new-hash", &context("alice"))
        .await
        .unwrap();
    assert_eq!(
        auth.load_password_hash("alice").await.unwrap().as_deref(),
        Some("new-hash")
    );
    assert!(
        auth.change_password("alice", user.auth_version, "stale-hash", &context("alice"))
            .await
            .is_err()
    );
    let (_, disabled) = auth
        .update_user(
            UpdateUser {
                display_name: None,
                username: None,
                quota_multipliers: Default::default(),
                limits: gateway_core::policy::RateLimits::unlimited(),
                id: "alice".into(),
                enabled: false,
                group_ids: vec![],
            },
            &context("admin"),
        )
        .await
        .unwrap();
    assert!(!disabled.enabled);
    assert!(disabled.auth_version > user.auth_version);
    assert!(
        auth.update_user(
            UpdateUser {
                display_name: None,
                username: None,
                quota_multipliers: Default::default(),
                limits: gateway_core::policy::RateLimits::unlimited(),
                id: "admin".into(),
                enabled: false,
                group_ids: vec![]
            },
            &context("admin")
        )
        .await
        .is_err()
    );
    let audits: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where action = 'user.password.change'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(audits, 1, "failed changes must roll back their audit event");
    db.close().await;
}

#[tokio::test]
async fn upgrade_preserves_legacy_key_ownership_permissions_and_budget_windows() {
    let Some(db) = TestDatabase::create_at_version("user_upgrade", 5).await else {
        return;
    };
    sqlx::raw_sql("insert into admin_users values ('legacy-admin', 'unchanged-hash', now(), now());
        insert into client_api_keys(id, name, key, daily_limit_usd, weekly_limit_usd, created_at, updated_at)
            values ('legacy-key', 'Legacy key', 'sk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 2, 10, now(), now());
        insert into client_key_budget_windows values ('legacy-key', now(), now() + interval '1 day', now(), now() + interval '7 days', 1.25, 4.75);
        insert into client_key_charge_events values ('legacy-request', 'legacy-key', now(), 1.25);
        insert into model_requests(id, client_api_key_id, client_api_key_ref, config_revision, protocol, operation, endpoint, client_transport, routing_scope, started_at, deadline_at)
            values ('legacy-request', 'legacy-key', 'legacy-key', 1, 'openai', 'responses', '/v1/responses', 'http_sse', 'all', now(), now() + interval '1 hour');")
        .execute(&db.pool).await.unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let user: (String, String, String) =
        sqlx::query_as("select username, role, password_hash from users where id = 'legacy-admin'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        user,
        (
            "legacy-admin".into(),
            "admin".into(),
            "unchanged-hash".into()
        )
    );
    let scope: (String, String) = sqlx::query_as("select k.owner_user_id, kg.account_group_id from client_api_keys k join client_api_key_groups kg on kg.client_api_key_id = k.id where k.id = 'legacy-key'").fetch_one(&db.pool).await.unwrap();
    assert_eq!(scope.0, "legacy-admin");
    let values: (String, String, String, String) = sqlx::query_as("select g.daily_limit_usd::text, g.weekly_limit_usd::text, w.daily_used_usd::text, w.weekly_used_usd::text from account_groups g join user_group_budget_windows w on w.account_group_id = g.id where g.id = $1")
        .bind(&scope.1).fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        [values.0, values.1, values.2, values.3].map(|v| v
            .parse::<gateway_core::metering::Decimal>()
            .unwrap()
            .canonical()),
        ["2", "10", "1.25", "4.75"]
    );
    sqlx::query("delete from client_api_keys where id = 'legacy-key'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select user_id from model_requests where id = 'legacy-request'"
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
        "legacy-admin"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        1
    );
    db.close().await;
}
