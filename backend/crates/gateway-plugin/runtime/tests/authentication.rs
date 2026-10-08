use super::support::*;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_plugin_runtime::PluginEngine;
use std::{collections::BTreeMap, sync::Arc};
fn binding(policy: PluginAuthenticationPolicy) -> PluginAuthenticationBinding {
    PluginAuthenticationBinding {
        failure_policy: policy,
        identities: BTreeMap::from([("alice".into(), "key_alice".into())]),
    }
}
#[tokio::test]
async fn authentication_commit_uncertainty_never_reopens_builtin_access() {
    use std::sync::atomic::Ordering;
    for fault in [1, 2, 3] {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let engine = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = PluginManager::new(store.clone(), engine);
        let artifact = manager
            .upload(authentication_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut create = update(&artifact.sha256, 0, true);
        create.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
        let installed = manager.update(None, create, &actor()).await.unwrap();
        let PluginAuthenticationDecision::Authenticated { access: old, .. } = manager
            .authenticate("Bearer external-alice".into())
            .await
            .unwrap()
        else {
            panic!()
        };
        store.save_fault.store(fault, Ordering::SeqCst);
        let mut disable = update(&artifact.sha256, 1, false);
        disable.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
        let result = manager.update(Some(&installed.id), disable, &actor()).await;
        if fault == 1 {
            assert!(result.is_err());
            assert!(!old.is_revoked());
            assert!(matches!(
                manager
                    .authenticate("Bearer sk_local".into())
                    .await
                    .unwrap(),
                PluginAuthenticationDecision::Rejected
            ));
        } else if fault == 2 {
            result.unwrap();
            assert!(old.is_revoked());
            assert!(matches!(
                manager
                    .authenticate("Bearer sk_local".into())
                    .await
                    .unwrap(),
                PluginAuthenticationDecision::Builtin { .. }
            ));
        } else {
            assert!(result.is_err());
            assert!(old.is_revoked());
            assert!(
                manager
                    .authenticate("Bearer sk_local".into())
                    .await
                    .is_err()
            );
            assert!(
                manager
                    .authenticate("Bearer external-alice".into())
                    .await
                    .is_err()
            );
            // Repair from the persisted revision is an explicit lifecycle operation.
            let mut repair = update(&artifact.sha256, 2, true);
            repair.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
            manager
                .update(Some(&installed.id), repair, &actor())
                .await
                .unwrap();
            assert!(matches!(
                manager
                    .authenticate("Bearer external-alice".into())
                    .await
                    .unwrap(),
                PluginAuthenticationDecision::Authenticated { .. }
            ));
            assert!(old.is_revoked());
        }
        manager.shutdown().await;
    }
}
#[tokio::test]
async fn authentication_obeys_host_mapping_delegation_lifecycle_and_failed_upgrade() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let engine = Arc::new(PluginEngine::new(
        cache.path().into(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store.clone(), engine.clone());
    let PluginAuthenticationDecision::Builtin { access: before } = manager
        .authenticate("Bearer sk_local".into())
        .await
        .unwrap()
    else {
        panic!()
    };
    let artifact = manager
        .upload(authentication_archive("1.0.0"), &actor())
        .await
        .unwrap();
    assert!(artifact.unsupported_capabilities.is_empty());
    let mut command = update(&artifact.sha256, 0, true);
    command.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Delegate));
    let installed = manager.update(None, command, &actor()).await.unwrap();
    assert!(before.is_revoked());
    let PluginAuthenticationDecision::Authenticated { key, access } = manager
        .authenticate("Bearer external-alice".into())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(key.as_str(), "key_alice");
    assert!(matches!(
        manager
            .authenticate("Bearer external-state".into())
            .await
            .unwrap(),
        PluginAuthenticationDecision::Authenticated { .. }
    ));
    for header in ["Bearer external-unknown", "Bearer external-rejected"] {
        assert!(matches!(
            manager.authenticate(header.into()).await.unwrap(),
            PluginAuthenticationDecision::Rejected
        ));
    }
    for header in ["Bearer external-forged", "Bearer external-fault"] {
        assert!(manager.authenticate(header.into()).await.is_err());
    }
    let PluginAuthenticationDecision::Builtin { access: fallback } = manager
        .authenticate("Bearer sk_local".into())
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut duplicate = update(&artifact.sha256, 0, true);
    duplicate.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Delegate));
    assert!(manager.update(None, duplicate, &actor()).await.is_err());
    let mut failed = update(&artifact.sha256, 1, true);
    failed.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
    failed.configuration = serde_json::json!({"fail":true});
    assert!(
        manager
            .update(Some(&installed.id), failed, &actor())
            .await
            .is_err()
    );
    assert!(!access.is_revoked());
    assert!(!fallback.is_revoked());
    let mut strict = update(&artifact.sha256, 1, true);
    strict.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
    manager
        .update(Some(&installed.id), strict, &actor())
        .await
        .unwrap();
    assert!(access.is_revoked());
    assert!(fallback.is_revoked());
    assert!(matches!(
        manager
            .authenticate("Bearer sk_local".into())
            .await
            .unwrap(),
        PluginAuthenticationDecision::Rejected
    ));
    let PluginAuthenticationDecision::Authenticated {
        access: current, ..
    } = manager
        .authenticate("Bearer external-alice".into())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(
        manager
            .authenticate("Bearer external-crash".into())
            .await
            .is_err()
    );
    assert!(current.is_revoked());
    assert!(
        manager
            .authenticate("Bearer sk_local".into())
            .await
            .is_err(),
        "crash must not delegate"
    );
    manager.shutdown().await;
    store
        .records
        .lock()
        .unwrap()
        .get_mut(&installed.id)
        .unwrap()
        .configuration = serde_json::json!({"fail":true});
    let restored = PluginManager::new(store, engine);
    restored.restore().await.unwrap();
    assert!(
        restored
            .authenticate("Bearer sk_local".into())
            .await
            .is_err(),
        "failed startup must retain the authentication gate"
    );
    let mut disable = update(&artifact.sha256, 2, false);
    disable.frontend_authentication = Some(binding(PluginAuthenticationPolicy::Reject));
    restored
        .update(Some(&installed.id), disable, &actor())
        .await
        .unwrap();
    assert!(matches!(
        restored
            .authenticate("Bearer sk_local".into())
            .await
            .unwrap(),
        PluginAuthenticationDecision::Builtin { .. }
    ));
    restored.shutdown().await;
}
