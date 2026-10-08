use super::support::*;
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationActor, plugins::*},
    plugins::PluginManager,
    ports::plugins::{PluginResourceService, PluginService},
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Resources(Mutex<Vec<PluginMutationScope>>);
#[async_trait]
impl PluginResourceService for Resources {
    async fn ensure_group(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupEnsure,
    ) -> Result<PluginManagedResource, AdminError> {
        assert_eq!(command.resource_key, "main");
        let id = scope.actor.request_id.clone();
        self.0.lock().unwrap().push(scope);
        tokio::task::yield_now().await;
        Ok(PluginManagedResource {
            id,
            name: command.name,
            enabled: true,
        })
    }
    async fn change_members(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupMembers,
    ) -> Result<PluginMembersChanged, AdminError> {
        assert_eq!(command.resource_key, "main");
        assert_eq!(command.add, ["acct_001"]);
        self.0.lock().unwrap().push(scope);
        Ok(PluginMembersChanged {
            added: 1,
            removed: 0,
        })
    }
}
#[tokio::test]
async fn resource_callbacks_keep_concurrent_host_callers_separate_and_reject_forged_fields() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let resources = Arc::new(Resources::default());
    let runtime = Arc::new(
        PluginEngine::new(
            cache.path().to_owned(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_resources(resources.clone()),
    );
    let manager = PluginManager::new(store, runtime);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let req = |id: &str| {
        let mut r = request("resources");
        r.actor.request_id = id.into();
        r.actor.actor = MutationActor::AdminSession {
            admin_user_id: id.into(),
        };
        r
    };
    let (a, b) = tokio::join!(
        manager.handle(&installed.id, 1, req("admin-a")),
        manager.handle(&installed.id, 1, req("admin-b"))
    );
    for (response, id) in [(a.unwrap(), "admin-a"), (b.unwrap(), "admin-b")] {
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["group"]["id"], id);
        assert_eq!(body["members"], json!({"added":1,"removed":0}));
    }
    {
        let scopes = resources.0.lock().unwrap();
        assert_eq!(scopes.len(), 4);
        for s in scopes.iter() {
            assert_eq!(s.instance_id, installed.id);
            assert_eq!(s.revision, 1);
            assert_eq!(
                s.actor.actor,
                MutationActor::AdminSession {
                    admin_user_id: s.actor.request_id.clone()
                }
            );
        }
    }
    let mut r = request("facts-raw");
    r.body=serde_json::to_vec(&json!({"operation":"facts-raw","method":"host.groups.ensure","params":{},"payload":{"resource_key":"main","name":"Forged","color":"#2563EBFF","actor":"admin-a"}})).unwrap();
    let response = manager.handle(&installed.id, 1, r).await.unwrap();
    let value: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(value["error"]["code"], "invalid_input");
    assert_eq!(resources.0.lock().unwrap().len(), 4);
    manager
        .update(
            Some(&installed.id),
            update(&artifact.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        manager
            .handle(&installed.id, 1, req("admin-a"))
            .await
            .is_err()
    );
    manager.shutdown().await;
}
