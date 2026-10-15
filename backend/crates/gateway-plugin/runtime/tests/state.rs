//! Original SDK subprocesses and PostgreSQL prove atomic schema/revision switching.
use crate::support::database::Database;
use crate::{services::upstream_sdk::environment::Environment, support::*};
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        account_groups::{AccountGroupColor, CreateAccountGroup},
        auth::LoginCommand,
        client_keys::CreateClientKey,
    },
    ports::backup::DumpRequest,
};
use gateway_core::policy::RateLimits;
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::BTreeMap, process::Stdio};
use std::{io::Read, sync::Arc, time::Duration};

fn namespace(version: u32, schema: Value, from: &[u32]) -> Value {
    json!({"namespace":"counter","schemaVersion":version,"schema":schema,"migratesFrom":from,
        "maximumRecords":128,"maximumBytes":1024*1024,"maximumValueBytes":256*1024})
}
fn package(version: &str, namespaces: Value) -> Arc<[u8]> {
    let original = archive(version, false);
    let decoder = flate2::read::GzDecoder::new(original.as_ref());
    let mut source = tar::Archive::new(decoder);
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut target = tar::Builder::new(encoder);
    for entry in source.entries().unwrap() {
        let mut entry = entry.unwrap();
        let name = entry.path().unwrap().into_owned();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if name == std::path::Path::new("plugin.json") {
            let mut manifest: Value = serde_json::from_slice(&bytes).unwrap();
            manifest["state"] = namespaces.clone();
            bytes = serde_json::to_vec(&manifest).unwrap();
        }
        let mut header = entry.header().clone();
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        target
            .append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    Arc::from(target.into_inner().unwrap().finish().unwrap())
}
fn manager(env: &Environment) -> Arc<PluginManager> {
    let store = env.store.plugin_store();
    Arc::new(PluginManager::new(
        store.clone(),
        Arc::new(PluginEngine::new(
            env.cache.path().join("state-plugins"),
            "3.11.2".parse().unwrap(),
            store,
        )),
    ))
}
async fn fixture() -> Option<(Environment, Arc<PluginManager>, PluginView)> {
    let env = Environment::new().await?;
    let manager = manager(&env);
    let artifact = manager
        .upload(
            package(
                "1.0.0",
                json!([namespace(1, json!({"type":"integer"}), &[])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    Some((env, manager, installed))
}
fn scope(id: &str, revision: u64) -> PluginStateScope {
    PluginStateScope {
        instance_id: id.into(),
        revision,
        namespace: "counter".into(),
        schema_version: 1,
        maximum_records: 128,
        maximum_bytes: 1024 * 1024,
    }
}
async fn read(manager: &PluginManager, installed: &PluginView) -> Value {
    let response = manager
        .handle(&installed.id, installed.revision, request("state_read"))
        .await
        .unwrap();
    serde_json::from_slice(&response.body).unwrap()
}
async fn wait_for_signal(path: &std::path::Path) -> u32 {
    // Candidate package validation and extraction precede migration entry.
    // The migration deadline and process-reaping bounds are checked separately.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(text) = tokio::fs::read_to_string(path).await
                && let Ok(pid) = text.parse()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate reaches native migration before testing its lifecycle")
}
async fn reaped(pid: u32) {
    if cfg!(target_os = "linux") {
        tokio::time::timeout(Duration::from_secs(5), async {
            while std::path::Path::new(&format!("/proc/{pid}")).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled candidate process must be reaped");
    }
}

#[tokio::test]
async fn native_state_migration_batches_keep_replace_delete_commit_with_revision_and_restore() {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    let store = env.store.plugin_store();
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    for (key, value) in [("keep", 2), ("delete", 3)] {
        store
            .state_put(&scope(&installed.id, 1), key, None, Some(json!(value)))
            .await
            .unwrap();
    }
    for index in 0..70 {
        store
            .state_put(
                &scope(&installed.id, 1),
                &format!("row-{index:03}"),
                None,
                Some(json!(index)),
            )
            .await
            .unwrap();
    }
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, true);
    change.configuration = json!({"state_migration":"actions"});
    let updated = manager
        .update(Some(&installed.id), change, &actor())
        .await
        .unwrap();
    let after = store.state_snapshot(&installed.id, 2).await.unwrap();
    assert_eq!(after.records.len(), 72);
    assert!(after.records.iter().all(|row| {
        row.schema_version == 2
            && row.version
                > before
                    .records
                    .iter()
                    .find(|old| old.key == row.key)
                    .unwrap()
                    .version
    }));
    assert!(!after.records.iter().any(|row| row.key == "delete"));
    assert_eq!(
        after
            .records
            .iter()
            .find(|row| row.key == "keep")
            .unwrap()
            .value,
        json!(2)
    );
    assert_eq!(read(&manager, &updated).await["record"]["value"], json!(10));
    assert!(
        store
            .state_put(&scope(&installed.id, 1), "stale", None, Some(json!(1)))
            .await
            .is_err()
    );
    manager.shutdown().await;
    drop(manager);
    let restored = self::manager(&env);
    restored.restore().await.unwrap();
    assert_eq!(
        read(&restored, &updated).await["record"]["schema_version"],
        json!(2)
    );
    assert!(store.state_snapshot(&installed.id, 2).await.unwrap() == after);
    restored
        .update(
            Some(&installed.id),
            update(&artifact.sha256, 2, false),
            &actor(),
        )
        .await
        .unwrap();
    restored.delete(&installed.id, 3, &actor()).await.unwrap();
    let removed = store.state_snapshot(&installed.id, 0).await.unwrap();
    assert!(removed.records.is_empty() && removed.namespaces.is_empty());
    restored.shutdown().await;
    drop(restored);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn invalid_migration_results_schema_changes_and_quota_shrink_preserve_original_data_and_process()
 {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    let store = env.store.plugin_store();
    for key in ["value", "second", "third"] {
        store
            .state_put(&scope(&installed.id, 1), key, None, Some(json!(1)))
            .await
            .unwrap();
    }
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    for mode in [
        "fail",
        "bad_keys",
        "missing",
        "reorder",
        "invalid_value",
        "envelope",
        "malformed",
    ] {
        let mut change = update(&artifact.sha256, 1, true);
        change.configuration = json!({"state_migration":mode});
        assert!(
            manager
                .update(Some(&installed.id), change, &actor())
                .await
                .is_err(),
            "{mode}"
        );
        assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
        assert_eq!(
            read(&manager, &installed).await["record"]["value"],
            json!(1)
        );
    }
    let mut quota = namespace(2, json!({"type":"integer"}), &[1]);
    quota["maximumRecords"] = json!(1);
    for (version, ns) in [
        ("2.1.0", namespace(2, json!({"type":"integer"}), &[])),
        ("2.2.0", namespace(1, json!({"type":"object"}), &[])),
        ("2.3.0", quota),
    ] {
        let artifact = manager
            .upload(package(version, json!([ns])), &actor())
            .await
            .unwrap();
        assert!(
            manager
                .update(
                    Some(&installed.id),
                    update(&artifact.sha256, 1, true),
                    &actor()
                )
                .await
                .is_err()
        );
        assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
    }
    manager.shutdown().await;
    drop(manager);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn disabled_artifact_changes_preserve_source_schema_until_enable_and_downgrade_requires_migration()
 {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    let store = env.store.plugin_store();
    store
        .state_put(&scope(&installed.id, 1), "value", None, Some(json!(7)))
        .await
        .unwrap();
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let old_sha = installed.artifact.sha256.clone();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"object"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, false);
    change.configuration = json!({"state_migration":"wrap"});
    manager
        .update(Some(&installed.id), change, &actor())
        .await
        .unwrap();
    assert!(store.state_snapshot(&installed.id, 2).await.unwrap() == before);
    let mut change = update(&artifact.sha256, 2, true);
    change.configuration = json!({"state_migration":"wrap"});
    let enabled = manager
        .update(Some(&installed.id), change, &actor())
        .await
        .unwrap();
    assert_eq!(
        read(&manager, &enabled).await["record"]["value"],
        json!({"value":7})
    );
    assert!(
        manager
            .update(Some(&installed.id), update(&old_sha, 3, true), &actor())
            .await
            .is_err()
    );
    let artifact = manager
        .upload(
            package(
                "1.0.1",
                json!([namespace(1, json!({"type":"integer"}), &[2])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 3, true);
    change.configuration = json!({"state_migration":"unwrap"});
    let downgraded = manager
        .update(Some(&installed.id), change, &actor())
        .await
        .unwrap();
    assert_eq!(
        read(&manager, &downgraded).await["record"]["value"],
        json!(7)
    );
    manager.shutdown().await;
    drop(manager);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn live_source_writes_conflict_and_cancelled_candidate_is_reaped_without_replacing_original_process()
 {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    for cancel in [false, true] {
        let signal = env.cache.path().join(format!("migration-{cancel}"));
        let mut change = update(&artifact.sha256, 1, true);
        change.configuration = json!({"state_migration":"wait","state_migration_signal":signal});
        let running = {
            let manager = manager.clone();
            let id = installed.id.clone();
            tokio::spawn(async move { manager.update(Some(&id), change, &actor()).await })
        };
        let pid = wait_for_signal(&signal).await;
        manager
            .handle(&installed.id, 1, request("state"))
            .await
            .unwrap();
        if cancel {
            running.abort();
            assert!(matches!(running.await, Err(error) if error.is_cancelled()));
        } else {
            tokio::fs::write(format!("{}.release", signal.display()), b"release")
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(5), running)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
        }
        reaped(pid).await;
        assert_eq!(
            read(&manager, &installed).await["record"]["value"],
            json!(if cancel { 3 } else { 2 })
        );
    }
    manager.shutdown().await;
    drop(manager);
    env.close().await;
}

#[tokio::test]
async fn database_jsonb_quota_failure_rolls_back_state_and_instance_revision_together() {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    let store = env.store.plugin_store();
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let mut ns = namespace(2, json!({"type":"object"}), &[1]);
    ns["maximumBytes"] = json!(11);
    ns["maximumValueBytes"] = json!(11);
    let artifact = manager
        .upload(package("2.0.0", json!([ns])), &actor())
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, true);
    change.configuration = json!({"state_migration":"wrap"});
    let error = manager
        .update(Some(&installed.id), change, &actor())
        .await
        .err()
        .unwrap();
    assert_eq!(error.message(), "plugin state quota exceeded");
    assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
    assert_eq!(
        read(&manager, &installed).await["record"]["value"],
        json!(1)
    );
    manager.shutdown().await;
    drop(manager);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn existing_private_state_schema_is_backfilled_without_changing_values_or_cas_versions() {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    manager.shutdown().await;
    drop(manager);
    let store = env.store.plugin_store();
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    sqlx::query("drop table plugin_state_schemas")
        .execute(&env.pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../migrations/0041_plugin_state_schemas.sql"
    ))
    .execute(&env.pool)
    .await
    .unwrap();
    assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
    let restored = self::manager(&env);
    restored.restore().await.unwrap();
    assert_eq!(
        read(&restored, &installed).await["record"]["value"],
        json!(1)
    );
    restored.shutdown().await;
    drop(restored);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn migration_total_deadline_stops_candidate_and_preserves_original_process_and_state() {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    let store = env.store.plugin_store();
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let signal = env.cache.path().join("migration-deadline");
    let mut change = update(&artifact.sha256, 1, true);
    change.configuration = json!({"state_migration":"wait","state_migration_signal":signal});
    let running = {
        let manager = manager.clone();
        let id = installed.id.clone();
        tokio::spawn(async move { manager.update(Some(&id), change, &actor()).await })
    };
    let pid = wait_for_signal(&signal).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(36), running)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    reaped(pid).await;
    assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
    assert_eq!(
        read(&manager, &installed).await["record"]["value"],
        json!(1)
    );
    manager.shutdown().await;
    drop(manager);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn nonempty_namespace_cannot_be_removed_until_plugin_explicitly_migrates_its_records_to_deleted()
 {
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    let store = env.store.plugin_store();
    let before = store.state_snapshot(&installed.id, 1).await.unwrap();
    let empty = manager
        .upload(package("3.0.0", json!([])), &actor())
        .await
        .unwrap();
    assert!(
        manager
            .update(
                Some(&installed.id),
                update(&empty.sha256, 1, true),
                &actor()
            )
            .await
            .is_err()
    );
    assert!(store.state_snapshot(&installed.id, 1).await.unwrap() == before);
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, true);
    change.configuration = json!({"state_migration":"delete_all"});
    manager
        .update(Some(&installed.id), change, &actor())
        .await
        .unwrap();
    let removed = manager
        .update(
            Some(&installed.id),
            update(&empty.sha256, 2, true),
            &actor(),
        )
        .await
        .unwrap();
    let state = store.state_snapshot(&installed.id, 3).await.unwrap();
    assert!(state.namespaces.is_empty() && state.records.is_empty());
    assert!(
        manager
            .handle(&removed.id, 3, request("state_read"))
            .await
            .is_err()
    );
    let mut added = namespace(1, json!({"type":"object"}), &[]);
    added["namespace"] = json!("fresh");
    let artifact = manager
        .upload(package("4.0.0", json!([added])), &actor())
        .await
        .unwrap();
    manager
        .update(
            Some(&installed.id),
            update(&artifact.sha256, 3, true),
            &actor(),
        )
        .await
        .unwrap();
    let state = store.state_snapshot(&installed.id, 4).await.unwrap();
    assert_eq!(state.namespaces.len(), 1);
    assert_eq!(state.namespaces[0].namespace, "fresh");
    assert!(state.records.is_empty());
    manager.shutdown().await;
    drop(manager);
    drop(store);
    env.close().await;
}

#[tokio::test]
async fn unchanged_schema_configuration_accepts_native_writes_after_preparation_without_snapshot_conflicts()
 {
    use gateway_admin::ports::plugins::PluginRuntime;
    let Some((env, manager, installed)) = fixture().await else {
        return;
    };
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    let store = env.store.plugin_store();
    let mut record = store
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == installed.id)
        .unwrap();
    record.revision = 2;
    record.name = "Reconfigured with live writes".into();
    let artifact = store.artifact(&record.artifact_sha256).await.unwrap();
    let engine = PluginEngine::new(
        env.cache.path().join("ordinary-config"),
        "3.11.2".parse().unwrap(),
        store.clone(),
    );
    let state = engine
        .prepare_state(&record, &artifact, None)
        .await
        .unwrap();
    assert!(state.is_none());
    manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    store
        .save(&record, 1, &actor(), state.as_ref())
        .await
        .unwrap();
    manager.shutdown().await;
    drop(manager);
    let restored = self::manager(&env);
    restored.restore().await.unwrap();
    let current = restored
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == installed.id)
        .unwrap();
    assert_eq!(current.revision, 2);
    assert_eq!(current.name, record.name);
    assert_eq!(read(&restored, &current).await["record"]["value"], json!(2));
    restored.shutdown().await;
    drop(restored);
    drop(store);
    env.close().await;
}

// Native archive restoration also verifies private data ownership across restart.
async fn table_facts(pool: &PgPool) -> BTreeMap<String, (i64, String)> {
    let tables: Vec<String> = sqlx::query_scalar(
        "select tablename from pg_tables where schemaname='public' order by tablename",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut facts = BTreeMap::new();
    for table in tables {
        assert!(
            table
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        );
        // Keep private values out of assertion diagnostics. Order-independent row
        // digests also cover native tables outside the plugin owner.
        let query = format!(
            "select count(*),md5(coalesce(string_agg(row::text,E'\\n' order by row::text),'')) \
             from (select to_jsonb(t) as row from public.\"{table}\" t) rows"
        );
        let fact = sqlx::query_as(sqlx::AssertSqlSafe(query))
            .fetch_one(pool)
            .await
            .unwrap();
        facts.insert(table, fact);
    }
    facts
}

#[tokio::test]
#[ignore = "requires pg_dump and pg_restore clients matching the test PostgreSQL server"]
async fn native_archive_restores_plugin_data_secrets_and_pending_upgrade_without_original_cache() {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let login = env
        .admin
        .services()
        .auth()
        .login(LoginCommand {
            username: Some("sdk-fixture@fixture.test".into()),
            password: "sdk-business-fixture-password".into(),
        })
        .await
        .unwrap();
    let owner = env
        .admin
        .services()
        .auth()
        .resolve_admin_user_id(Some(&login.session_id))
        .await
        .unwrap()
        .unwrap();
    let context = MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: owner.clone(),
        },
        request_id: "native-backup-fixture".into(),
    };
    let group = env
        .admin
        .services()
        .account_groups()
        .create(
            &context,
            CreateAccountGroup {
                budget: Default::default(),
                model_multipliers: BTreeMap::new(),
                name: "Backup native group".into(),
                description: None,
                color: AccountGroupColor::parse("#2563EBFF").unwrap(),
            },
        )
        .await
        .unwrap();
    let key = env
        .admin
        .services()
        .client_keys()
        .create(
            &context,
            CreateClientKey {
                name: "Backup native key".into(),
                label: None,
                group_ids: vec![group.id],
                limits: RateLimits::unlimited(),
            },
        )
        .await
        .unwrap();
    let manager = self::manager(&env);
    let first_artifact = manager
        .upload(
            package(
                "1.0.0",
                json!([namespace(1, json!({"type":"integer"}), &[])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let original = first_artifact.sha256;
    let first = manager
        .update(None, update(&original, 0, true), &actor())
        .await
        .unwrap();
    manager
        .handle(&first.id, 1, request("state"))
        .await
        .unwrap();
    let second = manager
        .update(None, update(&original, 0, true), &actor())
        .await
        .unwrap();
    manager
        .handle(&second.id, 1, request("state"))
        .await
        .unwrap();
    let artifact = manager
        .upload(
            package(
                "2.0.0",
                json!([namespace(2, json!({"type":"integer"}), &[1])]),
            ),
            &actor(),
        )
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, true);
    change.configuration = json!({"state_migration":"actions", "public_resource":true});
    manager
        .update(Some(&first.id), change, &actor())
        .await
        .unwrap();
    // Restoration must read the committed schema, rather than invoke migration
    // again. This active configuration deliberately rejects migration calls.
    let mut change = update(&artifact.sha256, 2, true);
    change.name = "Restored active instance".into();
    change.configuration = json!({"state_migration":"fail", "public_resource":true});
    let active = manager
        .update(Some(&first.id), change, &actor())
        .await
        .unwrap();
    let mut change = update(&artifact.sha256, 1, false);
    change.configuration = json!({"state_migration":"actions"});
    let inactive = manager
        .update(Some(&second.id), change, &actor())
        .await
        .unwrap();
    let store = env.store.plugin_store();
    let expected_records = store.list().await.unwrap();
    let expected_archive = store.artifact(&artifact.sha256).await.unwrap().archive;
    let active_state = store
        .state_snapshot(&active.id, active.revision)
        .await
        .unwrap();
    let inactive_state = store
        .state_snapshot(&inactive.id, inactive.revision)
        .await
        .unwrap();
    assert_eq!(active_state.records[0].value, json!(10));
    assert_eq!(inactive_state.records[0].value, json!(1));
    assert_eq!(inactive_state.namespaces[0].schema_version, 1);
    let maximum_version = active_state
        .records
        .iter()
        .chain(&inactive_state.records)
        .map(|row| row.version)
        .max()
        .unwrap();
    let resource = manager
        .public_resource(&active.id, active.revision, "index.html")
        .await
        .unwrap()
        .body;
    manager.shutdown().await;
    drop(manager);

    let before = table_facts(&env.pool).await;
    assert_eq!(before["plugin_instances"].0, 2);
    assert_eq!(before["plugin_state_schemas"].0, 2);
    assert_eq!(before["plugin_private_state"].0, 2);
    let dump = env.store.admin_ports().backup().dump();
    let backup_id = format!("backup_{}", uuid::Uuid::new_v4().simple());
    let archive = dump
        .dump(DumpRequest {
            backup_id: backup_id.clone(),
            cancellation: gateway_core::lifecycle::CancellationToken::new(),
        })
        .await
        .unwrap();
    let staged = dump.inspect_staging(&backup_id).await.unwrap().unwrap();
    assert_eq!(staged.sha256, archive.sha256);
    assert_eq!(staged.size_bytes, archive.size_bytes);
    let files = tempfile::tempdir().unwrap();
    let archive_path = files.path().join("native.dump");
    tokio::fs::copy(&archive.path, &archive_path).await.unwrap();
    let bytes = tokio::fs::read(&archive_path).await.unwrap();
    assert!(bytes.starts_with(b"PGDMP"));
    assert_eq!(bytes.len() as u64, archive.size_bytes);
    assert_eq!(hex::encode(Sha256::digest(&bytes)), archive.sha256);
    drop(bytes);
    dump.cleanup_staging(&backup_id).await.unwrap();
    drop(dump);
    let original_cache = env.cache.path().to_owned();
    drop(store);
    // Closing the original fixture removes its database and unpacked cache.
    env.close().await;
    assert!(!original_cache.exists());

    let (database, database_url, redis_url) = Database::create().await.unwrap();
    let mut public_url = url::Url::parse(&database_url).unwrap();
    let password = public_url.password().unwrap_or_default().to_owned();
    public_url.set_password(None).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new("pg_restore")
            .args([
                "--exit-on-error",
                "--no-owner",
                "--no-privileges",
                "--dbname",
            ])
            .arg(public_url.as_str())
            .arg(&archive_path)
            .env("PGPASSWORD", password)
            .env("PGCONNECT_TIMEOUT", "10")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("native restore must finish within its deadline")
    .expect("pg_restore must be installed for explicit backup acceptance");
    assert!(
        output.status.success(),
        "pg_restore failed: {}",
        output.status
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    assert!(
        table_facts(&pool).await == before,
        "every public table must retain its rows"
    );
    pool.close().await;
    // The ordinary Store initializer verifies restored migration checksums;
    // Admin/Core and Runtime use a new cache and the restored native facts.
    let env = Environment::from_database(
        database,
        database_url,
        redis_url,
        "sdk-fixture@fixture.test",
    )
    .await;
    assert!(
        table_facts(&env.pool).await == before,
        "ordinary initialization must retain restored facts"
    );
    let restored_key = env
        .admin
        .services()
        .client_keys()
        .reveal(&owner, &key.secret.record.id)
        .await
        .unwrap();
    assert!(restored_key.expose_for_response() == key.secret.expose_for_response());
    assert_eq!(restored_key.record.groups, key.secret.record.groups);
    let store = env.store.plugin_store();
    assert!(store.list().await.unwrap() == expected_records);
    assert!(
        store
            .state_snapshot(&active.id, active.revision)
            .await
            .unwrap()
            == active_state
    );
    assert!(
        store
            .state_snapshot(&inactive.id, inactive.revision)
            .await
            .unwrap()
            == inactive_state
    );
    assert!(
        store
            .artifact(&artifact.sha256)
            .await
            .unwrap()
            .archive
            .as_ref()
            == expected_archive.as_ref()
    );
    assert!(
        std::fs::read_dir(env.cache.path())
            .unwrap()
            .all(|entry| entry.unwrap().file_name() != "state-plugins")
    );
    let restored = self::manager(&env);
    restored.restore().await.unwrap();
    assert_eq!(read(&restored, &active).await["record"]["value"], json!(10));
    assert_eq!(
        restored
            .public_resource(&active.id, active.revision, "index.html")
            .await
            .unwrap()
            .body,
        resource
    );
    assert!(
        restored
            .handle(&inactive.id, inactive.revision, request("state_read"))
            .await
            .is_err()
    );
    restored
        .handle(&active.id, active.revision, request("state"))
        .await
        .unwrap();
    let written = store
        .state_snapshot(&active.id, active.revision)
        .await
        .unwrap();
    assert_eq!(written.records[0].value, json!(11));
    assert!(
        written.records[0].version > maximum_version,
        "native CAS sequence must be restored"
    );
    let mut change = update(&artifact.sha256, inactive.revision, true);
    change.configuration = json!({"state_migration":"actions"});
    let enabled = restored
        .update(Some(&inactive.id), change, &actor())
        .await
        .unwrap();
    assert_eq!(
        read(&restored, &enabled).await["record"]["value"],
        json!(10)
    );
    assert_eq!(
        read(&restored, &enabled).await["record"]["schema_version"],
        json!(2)
    );
    restored.shutdown().await;
    drop(restored);
    drop(store);
    env.close().await;
}
