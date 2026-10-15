//! Original SDK subprocesses and PostgreSQL prove atomic schema/revision switching.
use crate::{services::upstream_sdk::environment::Environment, support::*};
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
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
    tokio::time::timeout(Duration::from_secs(5), async {
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
    .unwrap()
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
