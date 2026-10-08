use super::support::*;
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationActor, Revision, plugins::*, users::SubscriptionTarget},
    plugins::PluginManager,
    ports::plugins::{PluginQuotaService, PluginService},
};
use gateway_plugin_runtime::PluginEngine;
use gateway_plugin_sdk::call::shared_budgets as wire;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Quota(Mutex<Vec<PluginMutationScope>>);
#[async_trait]
impl PluginQuotaService for Quota {
    async fn get(
        &self,
        scope: PluginMutationScope,
        target: SubscriptionTarget,
    ) -> Result<PluginSharedBudget, AdminError> {
        self.0.lock().unwrap().push(scope);
        Ok(PluginSharedBudget {
            revision: Revision::new(10).unwrap(),
            target,
            enabled: true,
            group_daily_limit_usd: "2".into(),
            group_weekly_limit_usd: "10".into(),
            quota_multiplier: "2".into(),
            daily_limit_usd: "4".into(),
            weekly_limit_usd: "20".into(),
            daily_used_usd: "1".into(),
            weekly_used_usd: "3".into(),
            daily_resets_at: None,
            weekly_resets_at: None,
            last_reset_at: None,
        })
    }
    async fn update_group_limits(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupBudgetUpdate,
    ) -> Result<PluginResourceMutation<PluginGroupBudget>, AdminError> {
        assert_eq!(command.expected_revision, 10);
        assert_eq!(command.daily_limit_usd.as_deref(), Some("3"));
        assert!(command.weekly_limit_usd.is_none());
        self.0.lock().unwrap().push(scope);
        Ok(PluginResourceMutation {
            revision: Revision::new(11).unwrap(),
            value: PluginGroupBudget {
                group_id: command.group_id,
                daily_limit_usd: "3".into(),
                weekly_limit_usd: "10".into(),
            },
        })
    }
    async fn reset(
        &self,
        scope: PluginMutationScope,
        command: PluginBudgetReset,
    ) -> Result<PluginResourceMutation<u64>, AdminError> {
        assert_eq!(command.targets.len(), 1);
        assert_eq!(command.targets[0].user_id, "alice");
        self.0.lock().unwrap().push(scope);
        Ok(PluginResourceMutation {
            revision: Revision::new(12).unwrap(),
            value: 1,
        })
    }
}
#[tokio::test]
async fn shared_quota_typed_sdk_calls_keep_host_identity_and_reject_key_semantics_and_spoofing() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let quota = Arc::new(Quota::default());
    let engine = Arc::new(
        PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_quota(quota.clone()),
    );
    let manager = PluginManager::new(store, engine);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let req = |id: &str| {
        let mut r = request("quota");
        r.actor.request_id = id.into();
        r.actor.actor = MutationActor::AdminSession {
            admin_user_id: id.into(),
        };
        r
    };
    let (a, b) = tokio::join!(
        manager.handle(&installed.id, 1, req("a")),
        manager.handle(&installed.id, 1, req("b"))
    );
    for r in [a.unwrap(), b.unwrap()] {
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["reset"]["affected"], 1);
        assert_eq!(v["budget"]["daily_limit_usd"], "4");
        assert_eq!(v["updated"]["revision"], 11);
    }
    {
        let calls = quota.0.lock().unwrap();
        assert_eq!(calls.len(), 6);
        for c in calls.iter() {
            assert_eq!(c.instance_id, installed.id);
            assert_eq!(c.revision, 1);
            assert_eq!(
                c.actor.actor,
                MutationActor::AdminSession {
                    admin_user_id: c.actor.request_id.clone()
                }
            );
        }
    }
    for (method, params, payload, code) in [
        (
            wire::GET,
            json!({}),
            json!({"user_id":"alice","group_id":"group","actor":"admin"}),
            "invalid_input",
        ),
        (
            wire::RESET,
            json!({}),
            json!({"client_key_id":"key","event_id":"id"}),
            "invalid_input",
        ),
        (
            wire::GET,
            json!({"user_id":"alice"}),
            json!({"user_id":"alice","group_id":"group"}),
            "invalid_input",
        ),
        (
            "host.keys.reset_budget",
            json!({}),
            json!({"client_key_id":"key","period":"all"}),
            "unsupported",
        ),
    ] {
        let mut r = request("facts-raw");
        r.body = serde_json::to_vec(
            &json!({"operation":"facts-raw","method":method,"params":params,"payload":payload}),
        )
        .unwrap();
        let v: Value =
            serde_json::from_slice(&manager.handle(&installed.id, 1, r).await.unwrap().body)
                .unwrap();
        assert_eq!(v["error"]["code"], code);
    }
    assert_eq!(quota.0.lock().unwrap().len(), 6);
    manager
        .update(
            Some(&installed.id),
            update(&artifact.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(manager.handle(&installed.id, 1, req("a")).await.is_err());
    manager.shutdown().await;
}
