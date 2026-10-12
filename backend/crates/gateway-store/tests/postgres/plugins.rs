use super::TestDatabase;
use gateway_admin::{
    model::{AdminErrorKind, MutationActor, MutationContext, plugins::*},
    ports::plugins::PluginStore,
};
use gateway_store::postgres::PgPluginStore;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn authentication_bindings_persist_and_only_one_enabled_authenticator_can_commit() {
    let Some(db) = TestDatabase::create("plugin_authentication").await else {
        return;
    };
    let store = PgPluginStore::new(db.pool.clone());
    let actor = MutationContext {
        actor: MutationActor::System,
        request_id: "auth-binding-test".into(),
    };
    let digest = "c".repeat(64);
    store
        .put_artifact(
            PluginArtifact {
                info: PluginArtifactInfo {
                    sha256: digest.clone(),
                    plugin_id: "local.auth".into(),
                    version: "1.0.0".into(),
                    display_name: "auth".into(),
                    manifest: json!({}),
                    unsupported_capabilities: vec![],
                    version_warning: None,
                },
                archive: Arc::from(b"fixture".as_slice()),
            },
            &actor,
        )
        .await
        .unwrap();
    let mut first = PluginRecord {
        retry_policy: None,
        account_scheduler: None,
        model_router: None,
        middleware: Some(PluginMiddlewareBinding {
            order: 12,
            mounts: vec![PluginMiddlewareMount::Request],
            scope: PluginExecutionScope::Keys {
                ids: vec!["key_owner".into()],
            },
        }),
        id: "auth-a".into(),
        revision: 1,
        name: "auth".into(),
        artifact_sha256: digest,
        enabled: true,
        configuration: json!({}),
        secrets: BTreeMap::new(),
        frontend_authentication: Some(PluginAuthenticationBinding {
            failure_policy: PluginAuthenticationPolicy::Reject,
            identities: BTreeMap::from([("private-principal".into(), "key_owner".into())]),
        }),
    };
    let mut second = first.clone();
    second.id = "auth-b".into();
    let (a, b) = tokio::join!(
        store.save(&first, 0, &actor),
        store.save(&second, 0, &actor)
    );
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "DB uniqueness arbitrates concurrent writers"
    );
    let rows = PgPluginStore::new(db.pool.clone()).list().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].frontend_authentication == first.frontend_authentication);
    assert!(rows[0].middleware == first.middleware);
    let winner = rows[0].id.clone();
    if winner != first.id {
        std::mem::swap(&mut first, &mut second);
    }
    second.enabled = false;
    store.save(&second, 0, &actor).await.unwrap();
    second.enabled = true;
    second.revision = 2;
    assert_eq!(
        store.save(&second, 1, &actor).await.err().unwrap().kind(),
        AdminErrorKind::Conflict
    );
    first.enabled = false;
    first.revision = 2;
    store.save(&first, 1, &actor).await.unwrap();
    store.save(&second, 1, &actor).await.unwrap();
    assert_eq!(
        store
            .list()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.enabled && r.frontend_authentication.is_some())
            .count(),
        1
    );
    let audit: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(a) from admin_audit_events a")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert!(
        !serde_json::to_string(&audit)
            .unwrap()
            .contains("private-principal")
    );
    db.close().await;
}
#[tokio::test]
async fn plugin_facts_project_current_bindings_and_paginate_without_credentials() {
    use gateway_admin::ports::store::{AccountStore as _, ClientKeyStore as _};
    use gateway_core::policy::ClientApiKeyId;
    use gateway_store::postgres::PgAdminClientKeyStore;
    let Some(db) = TestDatabase::create("plugin_facts").await else {
        return;
    };
    let group = "grp_00000000000000000000000000000099";
    sqlx::query("insert into users(id,username,password_hash,created_at,updated_at) values('facts-user','facts-user','unused',now(),now())").execute(&db.pool).await.unwrap();
    sqlx::query("insert into account_groups(id,name,color,created_at,updated_at) values($1,'facts','#64748BFF',now(),now())").bind(group).execute(&db.pool).await.unwrap();
    sqlx::query(
        "insert into user_account_groups(user_id,account_group_id) values('facts-user',$1)",
    )
    .bind(group)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("insert into client_api_keys(id,owner_user_id,name,key,enabled,created_at,updated_at) values('key_facts','facts-user','facts',$1,false,now(),now())").bind(format!("sk_{}","x".repeat(43))).execute(&db.pool).await.unwrap();
    sqlx::query("insert into client_api_key_groups(client_api_key_id,account_group_id,created_at) values('key_facts',$1,now())").bind(group).execute(&db.pool).await.unwrap();
    for (id, provider) in [
        ("acct_001", "openai"),
        ("acct_002", "xai"),
        ("acct_003", "openai"),
    ] {
        sqlx::query("insert into provider_accounts(id,provider_kind,name,authentication_kind,provider_credentials_json,has_refresh_token,credential_observed_at,created_at,updated_at) values($1,$2,'Facts','oauth','{\"test-secret\":\"never-return\"}',false,now(),now(),now())").bind(id).bind(provider).execute(&db.pool).await.unwrap();
    }
    sqlx::query("insert into account_group_accounts(account_group_id,provider_account_id,created_at) values($1,'acct_001',now())").bind(group).execute(&db.pool).await.unwrap();
    let keys = PgAdminClientKeyStore::new(db.pool.clone());
    let key = ClientApiKeyId::new("key_facts").unwrap();
    let facts = keys.plugin_key_facts(&key).await.unwrap().unwrap();
    assert!(!facts.enabled);
    assert_eq!(facts.group_ids, [group]);
    assert!(
        keys.plugin_key_facts(&ClientApiKeyId::new("missing").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let accounts = super::admin_account_store(&db.pool);
    let query = |provider: Option<&str>, cursor: Option<String>, limit| PluginAccountQuery {
        provider: provider.map(str::to_owned),
        cursor,
        limit,
    };
    let first = accounts
        .plugin_account_facts(query(Some("openai"), None, 1))
        .await
        .unwrap();
    assert_eq!(first.accounts.len(), 1);
    assert_eq!(first.accounts[0].id, "acct_001");
    assert_eq!(first.accounts[0].group_ids, [group]);
    assert_eq!(first.next_cursor.as_deref(), Some("acct_001"));
    let next = accounts
        .plugin_account_facts(query(Some("openai"), first.next_cursor, 1))
        .await
        .unwrap();
    assert_eq!(next.accounts[0].id, "acct_003");
    assert!(next.next_cursor.is_none());
    assert!(next.accounts[0].group_ids.is_empty());
    let all = accounts
        .plugin_account_facts(query(None, None, 100))
        .await
        .unwrap();
    assert_eq!(
        all.accounts
            .iter()
            .map(|a| a.id.as_str())
            .collect::<Vec<_>>(),
        ["acct_001", "acct_002", "acct_003"]
    );
    assert!(all.next_cursor.is_none());
    assert!(
        accounts
            .plugin_account_facts(query(None, None, 0))
            .await
            .is_err()
    );
    sqlx::query("delete from client_api_key_groups where client_api_key_id='key_facts'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        keys.plugin_key_facts(&key)
            .await
            .unwrap()
            .unwrap()
            .group_ids
            .is_empty()
    );
    db.close().await;
}
#[tokio::test]
async fn plugin_state_is_cas_scoped_bounded_and_revoked_on_disable_without_touching_budgets() {
    let Some(db) = TestDatabase::create("plugins").await else {
        return;
    };
    let store = PgPluginStore::new(db.pool.clone());
    let actor = MutationContext {
        actor: MutationActor::System,
        request_id: "plugin-store-test".into(),
    };
    let info = PluginArtifactInfo {
        sha256: "a".repeat(64),
        plugin_id: "local.fixture".into(),
        version: "1.0.0".into(),
        display_name: "fixture".into(),
        manifest: json!({}),
        unsupported_capabilities: vec![],
        version_warning: None,
    };
    store
        .put_artifact(
            PluginArtifact {
                info: info.clone(),
                archive: Arc::from(b"fixture".as_slice()),
            },
            &actor,
        )
        .await
        .unwrap();
    assert_eq!(
        store.artifact_info(&info.sha256).await.unwrap().plugin_id,
        "local.fixture"
    );
    let mut record = PluginRecord {
        retry_policy: Some(PluginRetryPolicyBinding {
            order: -9,
            scope: PluginExecutionScope::Keys {
                ids: vec!["key_retry".into()],
            },
        }),
        account_scheduler: Some(PluginAccountSchedulerBinding {
            order: -5,
            scope: PluginExecutionScope::Keys {
                ids: vec!["key_test".into()],
            },
            failure_policy: PluginPolicyFailurePolicy::Delegate,
        }),
        model_router: Some(PluginModelRouterBinding {
            order: 4,
            scope: PluginExecutionScope::All {},
            failure_policy: PluginPolicyFailurePolicy::Reject,
        }),
        middleware: Some(PluginMiddlewareBinding {
            order: -1,
            mounts: vec![
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
            scope: PluginExecutionScope::All {},
        }),
        frontend_authentication: None,
        id: "plugin-a".into(),
        revision: 1,
        name: "test".into(),
        artifact_sha256: info.sha256.clone(),
        enabled: true,
        configuration: json!({}),
        secrets: BTreeMap::from([("token".into(), "not-in-audit".into())]),
    };
    store.save(&record, 0, &actor).await.unwrap();
    assert!(store.list().await.unwrap()[0].retry_policy == record.retry_policy);
    for invalid in [
        PluginRetryPolicyBinding {
            order: 10_001,
            scope: PluginExecutionScope::All {},
        },
        PluginRetryPolicyBinding {
            order: 0,
            scope: PluginExecutionScope::Keys { ids: vec![] },
        },
    ] {
        let mut invalid_retry = record.clone();
        invalid_retry.revision += 1;
        invalid_retry.retry_policy = Some(invalid);
        assert_eq!(
            store
                .save(&invalid_retry, record.revision, &actor)
                .await
                .err()
                .unwrap()
                .kind(),
            AdminErrorKind::Invalid
        );
        assert!(store.list().await.unwrap()[0].retry_policy == record.retry_policy);
        assert_eq!(store.list().await.unwrap()[0].revision, record.revision);
    }
    assert!(store.list().await.unwrap()[0].account_scheduler == record.account_scheduler);
    let mut invalid_scheduler = record.clone();
    invalid_scheduler.revision += 1;
    invalid_scheduler.account_scheduler.as_mut().unwrap().order = 10_001;
    assert_eq!(
        store
            .save(&invalid_scheduler, record.revision, &actor)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Invalid
    );
    assert!(store.list().await.unwrap()[0].account_scheduler == record.account_scheduler);
    assert!(store.list().await.unwrap()[0].model_router == record.model_router);
    let mut invalid_router = record.clone();
    invalid_router.revision += 1;
    invalid_router.model_router.as_mut().unwrap().scope =
        PluginExecutionScope::Keys { ids: vec![] };
    assert_eq!(
        store
            .save(&invalid_router, record.revision, &actor)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Invalid
    );
    assert!(store.list().await.unwrap()[0].model_router == record.model_router);
    assert!(store.list().await.unwrap()[0].middleware == record.middleware);
    let mut invalid_binding = record.clone();
    invalid_binding.revision += 1;
    invalid_binding.middleware.as_mut().unwrap().mounts.clear();
    assert_eq!(
        store
            .save(&invalid_binding, record.revision, &actor)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Invalid
    );
    assert!(store.list().await.unwrap()[0].middleware == record.middleware);
    assert!(store.delete_artifact(&info.sha256, &actor).await.is_err());
    let scope = PluginStateScope {
        instance_id: record.id.clone(),
        revision: 1,
        namespace: "private".into(),
        schema_version: 1,
        maximum_records: 1,
        maximum_bytes: 64,
    };
    let v = store
        .state_put(&scope, "key", None, Some(json!(1)))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        store.state_put(&scope, "key", Some(v), Some(json!(2))),
        store.state_put(&scope, "key", Some(v), Some(json!(3)))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(
        store
            .state_put(&scope, "second", None, Some(json!(4)))
            .await
            .is_err()
    );
    let current = store.state_get(&scope, "key").await.unwrap().unwrap();
    assert!(
        store
            .state_put(
                &scope,
                "key",
                Some(current.version),
                Some(json!("x".repeat(128)))
            )
            .await
            .is_err()
    );
    store
        .state_put(&scope, "key", Some(current.version), None)
        .await
        .unwrap();
    let recreated = store
        .state_put(&scope, "key", None, Some(json!(5)))
        .await
        .unwrap();
    assert!(recreated > current.version);
    assert!(
        store
            .state_put(&scope, "key", Some(current.version), Some(json!(6)))
            .await
            .is_err()
    );
    record.id = "plugin-b".into();
    store.save(&record, 0, &actor).await.unwrap();
    assert!(store.list().await.unwrap()[0].middleware == record.middleware);
    let mut invalid_binding = record.clone();
    invalid_binding.revision += 1;
    invalid_binding.middleware.as_mut().unwrap().mounts.clear();
    assert_eq!(
        store
            .save(&invalid_binding, record.revision, &actor)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Invalid
    );
    assert!(store.list().await.unwrap()[0].middleware == record.middleware);
    let other = PluginStateScope {
        instance_id: "plugin-b".into(),
        ..scope
    };
    assert!(store.state_get(&other, "key").await.unwrap().is_none());
    record.id = "plugin-a".into();
    record.revision = 2;
    record.enabled = false;
    store.save(&record, 1, &actor).await.unwrap();
    let stale = PluginStateScope {
        instance_id: record.id.clone(),
        ..other
    };
    assert_eq!(
        store
            .state_put(&stale, "key", Some(recreated), Some(json!(7)))
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Conflict
    );
    assert!(store.delete("plugin-a", 1, &actor).await.is_err());
    store.delete("plugin-a", 2, &actor).await.unwrap();
    let remaining: i64 = sqlx::query_scalar(
        "select count(*) from plugin_private_state where instance_id='plugin-a'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    let budgets: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(budgets, 0);
    let audits: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(a) from admin_audit_events a")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert!(
        !serde_json::to_string(&audits)
            .unwrap()
            .contains("not-in-audit")
    );
    record.id = "plugin-b".into();
    store.save(&record, 1, &actor).await.unwrap();
    store.delete("plugin-b", 2, &actor).await.unwrap();
    store.delete_artifact(&info.sha256, &actor).await.unwrap();
    store.delete_artifact(&info.sha256, &actor).await.unwrap();
    assert!(store.artifacts().await.unwrap().is_empty());
    db.close().await;
}
