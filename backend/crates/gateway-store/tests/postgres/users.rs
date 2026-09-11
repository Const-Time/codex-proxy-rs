use super::TestDatabase;
use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        client_keys::{
            ClientKeyListQuery, ClientKeyPageSize, ClientKeySort, ClientKeySortField,
            DeleteClientKey, NewClientKey, SortDirection,
        },
        users::{UpdateUser, UserRole},
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
            .create_user(user, user, "user-hash", &[GROUP.into()], &context("admin"))
            .await
            .unwrap();
        assert_eq!(created.role, UserRole::User);
        assert!(created.enabled);
    }
    assert_eq!(auth.find_user("ALICE").await.unwrap().unwrap().id, "alice");
    assert_eq!(
        auth.create_user("duplicate", "Alice", "hash", &[], &context("admin"))
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    assert!(
        auth.create_user("intruder", "intruder", "hash", &[], &context("alice"))
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
        .create_user("alice", "alice", "old-hash", &[], &context("admin"))
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
