use super::TestDatabase;
use gateway_admin::{
    model::{AdminErrorKind, MutationActor, MutationContext, plugins::*},
    ports::plugins::PluginStore,
};
use gateway_store::postgres::PgPluginStore;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
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
        id: "plugin-a".into(),
        revision: 1,
        name: "test".into(),
        artifact_sha256: info.sha256.clone(),
        enabled: true,
        configuration: json!({}),
        secrets: BTreeMap::from([("token".into(), "not-in-audit".into())]),
    };
    store.save(&record, 0, &actor).await.unwrap();
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
