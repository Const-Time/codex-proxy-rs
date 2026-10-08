use super::support::*;
use gateway_admin::{
    model::AdminErrorKind,
    plugins::PluginManager,
    ports::plugins::{PluginRuntime, PluginService},
};
use gateway_plugin_runtime::{PackageLimits, PluginEngine, ValidatedPackage};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn installs_runs_callbacks_upgrades_and_revokes_old_revision_without_leaking_secrets() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().to_owned(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store.clone(), runtime.clone());
    let package = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    let old_access = manager.access(&installed.id, 1).unwrap();
    assert_eq!(installed.status, "ready");
    assert_eq!(installed.pages.len(), 1);
    assert!(
        !serde_json::to_string(&installed)
            .unwrap()
            .contains("fixture-secret-must-not-leak")
    );
    assert_eq!(installed.configured_secrets, ["token"]);
    let response = manager
        .handle(&installed.id, 1, request("state"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        json!({"count":1})
    );
    let response = manager
        .handle(&installed.id, 1, request("unsupported"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["code"],
        "unsupported"
    );
    assert_eq!(
        manager
            .resource(&installed.id, 1, "index.html")
            .await
            .unwrap()
            .content_type,
        "text/html"
    );
    assert!(
        manager
            .resource(&installed.id, 1, "../plugin.json")
            .await
            .is_err()
    );

    let mut invalid = update(&package.sha256, 1, true);
    invalid.configuration = json!({"fail":true});
    assert!(
        manager
            .update(Some(&installed.id), invalid, &actor())
            .await
            .is_err()
    );
    assert_eq!(manager.list().await.unwrap()[0].revision, 1);
    assert_eq!(manager.list().await.unwrap()[0].status, "ready");
    let v2 = manager
        .upload(archive("1.1.0", false), &actor())
        .await
        .unwrap();
    let next = manager
        .update(Some(&installed.id), update(&v2.sha256, 1, true), &actor())
        .await
        .unwrap();
    assert_eq!(next.revision, 2);
    assert!(old_access.is_revoked());
    assert!(
        manager
            .handle(&installed.id, 1, request("state"))
            .await
            .is_err()
    );
    let response = manager
        .handle(&installed.id, 2, request("state"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["count"],
        2
    );
    assert!(
        manager
            .update(Some(&installed.id), update(&v2.sha256, 1, true), &actor())
            .await
            .is_err()
    );
    manager.shutdown().await;
    let restored = PluginManager::new(store, runtime);
    restored.restore().await.unwrap();
    assert_eq!(restored.list().await.unwrap()[0].status, "ready");
    restored
        .update(Some(&installed.id), update(&v2.sha256, 2, false), &actor())
        .await
        .unwrap();
    assert!(
        restored
            .handle(&installed.id, 2, request("state"))
            .await
            .is_err()
    );
    restored.delete(&installed.id, 3, &actor()).await.unwrap();
    assert!(restored.list().await.unwrap().is_empty());
    restored.shutdown().await;
}

#[tokio::test]
async fn unsupported_extensions_require_adaptation_and_plaintext_secret_configuration_is_rejected()
{
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().to_owned(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store, runtime);
    let artifact = manager
        .upload(archive("1.0.0", true), &actor())
        .await
        .unwrap();
    assert_eq!(artifact.unsupported_capabilities, ["middleware"]);
    let error = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), AdminErrorKind::Invalid);
    let mut bad = update(&artifact.sha256, 0, false);
    bad.configuration = json!({"token":"secret"});
    assert!(manager.update(None, bad, &actor()).await.is_err());
    let other_runtime = PluginEngine::new(
        cache.path().to_owned(),
        "4.0.0".parse().unwrap(),
        Arc::new(MemoryStore::default()),
    );
    assert!(
        other_runtime
            .describe(artifact.clone())
            .unwrap()
            .version_warning
            .is_some()
    );
    let disabled = manager
        .update(None, update(&artifact.sha256, 0, false), &actor())
        .await
        .unwrap();
    assert_eq!(disabled.status, "disabled");
}

#[tokio::test]
async fn corrupted_archive_and_digest_mismatch_fail_before_process_start() {
    let package = archive("1.0.0", false);
    assert!(
        ValidatedPackage::read(package, Some(&"0".repeat(64)), PackageLimits::default()).is_err()
    );
    let store = Arc::new(MemoryStore::default());
    let cache = tempfile::tempdir().unwrap();
    let runtime = PluginEngine::new(cache.path().to_owned(), "3.11.2".parse().unwrap(), store);
    assert!(
        runtime
            .inspect(Arc::from(b"not a tar.gz".as_slice()))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_dir(cache.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn one_crashed_plugin_does_not_break_another_instance() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().to_owned(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store, runtime);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let second = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let crashed_access = manager.access(&first.id, 1).unwrap();
    assert!(
        manager
            .handle(&first.id, 1, request("crash"))
            .await
            .is_err()
    );
    assert!(
        manager
            .handle(&second.id, 1, request("state"))
            .await
            .is_ok()
    );
    assert_eq!(
        manager
            .list()
            .await
            .unwrap()
            .iter()
            .find(|r| r.id == first.id)
            .unwrap()
            .status,
        "failed"
    );
    assert!(crashed_access.is_revoked());
    manager.shutdown().await;
}

#[tokio::test]
async fn uncertain_commit_reconciles_confirmed_state_and_revokes_when_unreadable() {
    use std::sync::atomic::Ordering;
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().to_owned(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store.clone(), runtime);
    let package = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    store.save_fault.store(1, Ordering::SeqCst);
    assert!(
        manager
            .update(Some(&first.id), update(&package.sha256, 1, true), &actor())
            .await
            .is_err()
    );
    assert!(manager.handle(&first.id, 1, request("state")).await.is_ok());
    store.save_fault.store(2, Ordering::SeqCst);
    let second = manager
        .update(Some(&first.id), update(&package.sha256, 1, true), &actor())
        .await
        .unwrap();
    assert_eq!(second.revision, 2);
    assert!(
        manager
            .handle(&first.id, 1, request("state"))
            .await
            .is_err()
    );
    assert!(manager.handle(&first.id, 2, request("state")).await.is_ok());
    store.save_fault.store(3, Ordering::SeqCst);
    let error = manager
        .update(Some(&first.id), update(&package.sha256, 2, true), &actor())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), AdminErrorKind::UpstreamResultUnknown);
    assert!(
        manager
            .handle(&first.id, 2, request("state"))
            .await
            .is_err()
    );
    assert!(
        manager
            .handle(&first.id, 3, request("state"))
            .await
            .is_err()
    );
    let current = manager.list().await.unwrap().pop().unwrap();
    assert_eq!(current.revision, 3);
    assert_eq!(current.status, "failed");
    let recovered = manager
        .update(Some(&first.id), update(&package.sha256, 3, true), &actor())
        .await
        .unwrap();
    assert_eq!(recovered.status, "ready");
    manager.shutdown().await;
}

#[test]
fn archive_rejects_traversal_links_duplicate_paths_and_expansion_bombs() {
    use gateway_plugin_runtime::PackageError;
    fn raw(entries: &[(&str, tar::EntryType, &[u8])]) -> Arc<[u8]> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        let mut tar = tar::Builder::new(encoder);
        for (path, kind, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o700);
            header.set_entry_type(*kind);
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            header.set_cksum();
            tar.append(&header, *data).unwrap();
        }
        Arc::from(tar.into_inner().unwrap().finish().unwrap())
    }
    for entries in [
        vec![("../escape", tar::EntryType::Regular, b"".as_slice())],
        vec![("link", tar::EntryType::Symlink, b"".as_slice())],
        vec![
            ("index.html", tar::EntryType::Regular, b"".as_slice()),
            ("INDEX.html", tar::EntryType::Regular, b"".as_slice()),
        ],
    ] {
        assert_eq!(
            ValidatedPackage::read(raw(&entries), None, PackageLimits::default())
                .err()
                .unwrap(),
            PackageError::Path
        );
    }
    let bomb = raw(&[("large", tar::EntryType::Regular, &vec![0; 64 * 1024])]);
    let limits = PackageLimits {
        expanded_bytes: 4096,
        ..PackageLimits::default()
    };
    assert_eq!(
        ValidatedPackage::read(bomb, None, limits).err().unwrap(),
        PackageError::Limit
    );
}
