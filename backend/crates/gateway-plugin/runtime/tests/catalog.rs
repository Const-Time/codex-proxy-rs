use crate::support::*;
use gateway_admin::{
    plugins::PluginManager,
    ports::plugins::{PluginRuntime, PluginService, PluginStore},
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::json;
use std::sync::Arc;
#[tokio::test]
async fn sdk_catalog_registration_retains_host_owner_and_compiles_against_native_target_facts() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().into(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store.clone(), runtime.clone());
    let package = manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    assert!(
        !package
            .unsupported_capabilities
            .contains(&"model_catalog".into())
    );
    assert!(
        manager
            .update(None, update(&package.sha256, 0, true), &actor())
            .await
            .is_err()
    );
    let installed = manager
        .update(None, update(&package.sha256, 0, false), &actor())
        .await
        .unwrap();
    let record = store.list().await.unwrap().remove(0);
    let artifact = store.artifact(&package.sha256).await.unwrap();
    let process = runtime.prepare(&record, artifact).await.unwrap();
    process.activate();
    let aliases = process.model_aliases();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].owner, installed.id);
    assert_eq!(aliases[0].id.as_str(), "plugin-model");
    assert_eq!(aliases[0].target.as_str(), "gpt-native");
    use gateway_core::{account::*, operation::OperationKind, routing::*};
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).unwrap(),
            std::time::Duration::ZERO,
        ),
        vec![ProviderKind::new("openai").unwrap()],
        vec![ProviderModel::new(
            ProviderKind::new("openai").unwrap(),
            UpstreamModelId::new("gpt-native").unwrap(),
            ModelCapabilities::new([OperationKind::Generate].into(), None),
        )],
        vec![],
    )
    .unwrap()
    .with_model_aliases(aliases)
    .unwrap();
    assert_eq!(snapshot.mapped_model("plugin-model"), "gpt-native");
    process.stop().await;
    assert!(process.access().is_revoked());
    manager.shutdown().await;
}
#[tokio::test]
async fn sdk_catalog_invalid_replacements_leave_the_previous_generation_and_record_unchanged() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let manager = PluginManager::new(
        store.clone(),
        Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )),
    )
    .with_model_catalog(Arc::new(gateway_core::runtime::RuntimeSnapshotHandle::new(
        catalog_snapshot(),
    )));
    let package = manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    let old = manager.access(&installed.id, 1).unwrap();
    for mode in [
        "duplicate",
        "self",
        "provider",
        "bad-id",
        "oversized",
        "large-payload",
        "unknown-field",
        "unknown-alias-field",
        "envelope",
    ] {
        let mut command = update(&package.sha256, 1, true);
        command.configuration = json!({"catalog_mode":mode});
        assert!(
            manager
                .update(Some(&installed.id), command, &actor())
                .await
                .is_err(),
            "{mode}"
        );
        assert!(!old.is_revoked(), "{mode}");
        assert_eq!(store.list().await.unwrap()[0].revision, 1);
        assert_eq!(manager.list().await.unwrap()[0].status, "ready");
    }
    let mut command = update(&package.sha256, 1, true);
    command.configuration = json!({"catalog_mode":"empty"});
    manager
        .update(Some(&installed.id), command, &actor())
        .await
        .unwrap();
    assert!(old.is_revoked());
    manager.shutdown().await;
}

fn catalog_snapshot() -> gateway_core::routing::RuntimeSnapshot {
    use gateway_core::{account::*, operation::OperationKind, routing::*};
    RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::new(1).unwrap(),
            std::time::Duration::ZERO,
        ),
        vec![ProviderKind::new("openai").unwrap()],
        vec![ProviderModel::new(
            ProviderKind::new("openai").unwrap(),
            UpstreamModelId::new("gpt-native").unwrap(),
            ModelCapabilities::new([OperationKind::Generate].into(), None),
        )],
        vec![],
    )
    .unwrap()
}

#[tokio::test]
async fn sdk_catalog_native_model_calls_and_listing_share_published_aliases() {
    let f = crate::models::fixture::Fixture::new().await;
    let package = f
        .manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let installed = f
        .manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    let catalog=f.call(json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}})).await;
    assert_eq!(
        catalog["result"]["models"],
        json!(["gpt-native", "plugin-model"])
    );
    for (index, mode) in ["unary", "stream", "small"].into_iter().enumerate() {
        let result = f
            .call(json!({"key":"key_alice","model":"plugin-model","mode":mode}))
            .await;
        assert!(result.get("error").is_none(), "{mode}: {result}");
        f.clean(index + 1).await;
    }
    assert_eq!(f.core.starts.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(f.core.charges.lock().unwrap().len(), 3);
    let frozen = f
        .manager
        .middleware_registry()
        .unwrap()
        .model_catalog()
        .unwrap();
    // A duplicate from a different instance cannot change the live generation.
    assert!(
        f.manager
            .update(None, update(&package.sha256, 0, true), &actor())
            .await
            .is_err()
    );
    assert!(!frozen.cancellation().is_cancelled());
    f.manager
        .update(
            Some(&installed.id),
            update(&package.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(frozen.cancellation().is_cancelled());
    let catalog=f.call(json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}})).await;
    assert_eq!(catalog["result"]["models"], json!(["gpt-native"]));
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_catalog_uncertain_commit_revokes_old_publication_and_explicit_update_recovers() {
    let f = crate::models::fixture::Fixture::new().await;
    let package = f
        .manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let installed = f
        .manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    let frozen = f
        .manager
        .middleware_registry()
        .unwrap()
        .model_catalog()
        .unwrap();
    let old = f.manager.access(&installed.id, 1).unwrap();
    f.store
        .save_fault
        .store(3, std::sync::atomic::Ordering::SeqCst);
    assert!(
        f.manager
            .update(
                Some(&installed.id),
                update(&package.sha256, 1, true),
                &actor()
            )
            .await
            .is_err()
    );
    assert!(old.is_revoked());
    assert!(frozen.cancellation().is_cancelled());
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_err()
    );
    let result = f
        .call(json!({"key":"key_alice","model":"plugin-model"}))
        .await;
    assert!(result.get("error").is_some(), "{result}");
    assert_eq!(f.core.starts.load(std::sync::atomic::Ordering::SeqCst), 0);
    f.manager
        .update(
            Some(&installed.id),
            update(&package.sha256, 2, true),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_ok()
    );
    assert!(
        f.call(json!({"key":"key_alice","model":"plugin-model"}))
            .await
            .get("error")
            .is_none()
    );
    f.clean(1).await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_catalog_disable_during_native_lookup_rejects_the_frozen_result() {
    use std::sync::atomic::Ordering;
    let f = crate::models::fixture::Fixture::new().await;
    let package = f
        .manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let installed = f
        .manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *f.core.catalog_gate.lock().unwrap() = Some(receiver);
    let call=f.call(json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}}));
    tokio::pin!(call);
    tokio::select! {_=&mut call=>panic!("catalog should wait"),_=async {while f.core.catalogs.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    f.manager
        .update(
            Some(&installed.id),
            update(&package.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    let _ = sender.send(());
    assert_eq!(call.await["error"]["code"], "permission_denied");
    assert_eq!(f.core.active.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_catalog_restore_validates_current_targets_and_failed_registration_remains_a_gate() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().into(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let snapshot = Arc::new(gateway_core::runtime::RuntimeSnapshotHandle::new(
        catalog_snapshot(),
    ));
    let manager =
        PluginManager::new(store.clone(), runtime.clone()).with_model_catalog(snapshot.clone());
    let package = manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    manager.shutdown().await;
    let restored =
        PluginManager::new(store.clone(), runtime.clone()).with_model_catalog(snapshot.clone());
    restored.restore().await.unwrap();
    assert!(
        restored
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_ok()
    );
    assert_eq!(restored.list().await.unwrap()[0].status, "ready");
    restored.shutdown().await;
    store
        .records
        .lock()
        .unwrap()
        .get_mut(&installed.id)
        .unwrap()
        .configuration = json!({"catalog_mode":"duplicate"});
    let failed =
        PluginManager::new(store.clone(), runtime.clone()).with_model_catalog(snapshot.clone());
    failed.restore().await.unwrap();
    assert_eq!(failed.list().await.unwrap()[0].status, "failed");
    assert!(
        failed
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_err()
    );
    failed
        .update(
            Some(&installed.id),
            update(&package.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        failed
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_ok()
    );
    failed.shutdown().await;
    // A formerly valid target may disappear between host restarts.
    store
        .records
        .lock()
        .unwrap()
        .get_mut(&installed.id)
        .unwrap()
        .enabled = true;
    snapshot.publish(
        catalog_snapshot().with_model_mappings(std::collections::BTreeMap::from([(
            "gpt-native".into(),
            "gone".into(),
        )])),
    );
    let missing = PluginManager::new(store, runtime).with_model_catalog(snapshot);
    missing.restore().await.unwrap();
    assert_eq!(missing.list().await.unwrap()[0].status, "failed");
    assert!(
        missing
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_err()
    );
    missing.shutdown().await;
}

#[tokio::test]
async fn sdk_catalog_failed_restore_does_not_stop_other_instances_from_starting() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().into(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let snapshot = Arc::new(gateway_core::runtime::RuntimeSnapshotHandle::new(
        catalog_snapshot(),
    ));
    let manager =
        PluginManager::new(store.clone(), runtime.clone()).with_model_catalog(snapshot.clone());
    let package = manager
        .upload(catalog_archive("1.0.0"), &actor())
        .await
        .unwrap();
    manager
        .update(None, update(&package.sha256, 0, true), &actor())
        .await
        .unwrap();
    manager.shutdown().await;
    {
        let mut records = store.records.lock().unwrap();
        let mut valid = records.values().next().unwrap().clone();
        records.clear();
        valid.id = "b-ready".into();
        let mut invalid = valid.clone();
        invalid.id = "a-broken".into();
        invalid.configuration = json!({"catalog_mode":"duplicate"});
        records.insert(invalid.id.clone(), invalid);
        records.insert(valid.id.clone(), valid);
    }
    let restored = PluginManager::new(store, runtime).with_model_catalog(snapshot);
    restored.restore().await.unwrap();
    let rows = restored.list().await.unwrap();
    assert_eq!(rows[0].id, "a-broken");
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[1].id, "b-ready");
    assert_eq!(rows[1].status, "ready");
    assert!(
        restored
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_err()
    );
    // Recovering with duplicate IDs is still rejected, while disabling the failed instance opens the existing valid catalog.
    assert!(
        restored
            .update(Some("a-broken"), update(&package.sha256, 1, true), &actor())
            .await
            .is_err()
    );
    restored
        .update(
            Some("a-broken"),
            update(&package.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        restored
            .middleware_registry()
            .unwrap()
            .model_catalog()
            .is_ok()
    );
    restored.shutdown().await;
}
