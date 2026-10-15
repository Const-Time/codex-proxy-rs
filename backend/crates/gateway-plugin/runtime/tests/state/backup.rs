//! A real native archive is restored into an empty, independently owned database.
use super::*;
use crate::support::database::Database;
use gateway_admin::model::{
    MutationActor, MutationContext,
    account_groups::{AccountGroupColor, CreateAccountGroup},
    auth::LoginCommand,
    client_keys::CreateClientKey,
};
use gateway_admin::ports::backup::DumpRequest;
use gateway_core::policy::RateLimits;
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::BTreeMap, process::Stdio};

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
