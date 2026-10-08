use super::support::*;
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, plugins::*, provider_credentials::*},
    plugins::PluginManager,
    ports::plugins::{PluginDataService, PluginService},
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Facts {
    reads: AtomicUsize,
}
#[async_trait]
impl PluginDataService for Facts {
    async fn key(&self, id: &str) -> Result<PluginKeyFacts, AdminError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if id != "key_test" {
            return Err(AdminError::not_found("secret error detail"));
        }
        Ok(PluginKeyFacts {
            id: id.into(),
            enabled: false,
            group_ids: vec!["group_test".into()],
        })
    }
    async fn accounts(&self, q: PluginAccountQuery) -> Result<PluginAccountPage, AdminError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        assert_eq!(q.provider.as_deref(), Some("openai"));
        assert_eq!(q.limit, 1);
        assert!(q.cursor.is_none());
        Ok(PluginAccountPage {
            accounts: vec![PluginAccountFacts {
                id: "acct_test".into(),
                provider: "openai".into(),
                name: "Account".into(),
                email: None,
                group_ids: vec![],
                enabled: true,
                updated_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            }],
            next_cursor: Some("acct_test".into()),
        })
    }
    async fn quota(&self, id: &str) -> Result<ProviderQuota, AdminError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        assert_eq!(id, "acct_test");
        Ok(ProviderQuota {
            plan_type: Some("not-in-facts".into()),
            observed_at: None,
            refresh_token_expires_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
            windows: vec![ProviderQuotaWindow {
                key: "weekly".into(),
                group: "not-in-facts".into(),
                label: "not-in-facts".into(),
                limit_id: None,
                limit_name: None,
                role: None,
                local_usage_attribution: QuotaLocalUsageAttribution::Unavailable,
                estimated_quota: None,
                estimate_hint: None,
                window_seconds: Some(604800),
                used_percent: None,
                reset_at: None,
                limit_reached: false,
                local_usage: None,
                provider_data: None,
            }],
            limit_reached: false,
            provider_data: None,
        })
    }
}

#[tokio::test]
async fn facts_cross_real_process_sdk_without_credentials_refresh_or_error_details() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let facts = Arc::new(Facts::default());
    let runtime = Arc::new(
        PluginEngine::new(
            cache.path().to_owned(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_data(facts.clone()),
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
    let reply = manager
        .handle(&installed.id, 1, request("facts"))
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(
        value["key"],
        json!({"schema_version":1,"client_key_id":"key_test","enabled":false,"group_ids":["group_test"]})
    );
    assert_eq!(value["accounts"]["next_cursor"], "acct_test");
    assert_eq!(
        value["accounts"]["accounts"][0]["updated_at_ms"],
        1767225600000_i64
    );
    assert_eq!(
        value["quota"],
        json!({"schema_version":1,"account_id":"acct_test","observed_at_ms":null,"windows":[{"key":"weekly","window_seconds":604800,"used_percent":null,"reset_at_ms":null}]})
    );
    assert!(
        !String::from_utf8(reply.body)
            .unwrap()
            .contains("not-in-facts")
    );
    assert_eq!(facts.reads.load(Ordering::SeqCst), 3);
    for (method, params, payload, code) in [
        (
            "host.data.keys.get",
            json!({"unknown":true}),
            json!({"client_key_id":"key_test"}),
            "invalid_input",
        ),
        (
            "host.data.keys.get",
            json!({}),
            json!({"client_key_id":"key_test","unknown":true}),
            "invalid_input",
        ),
        (
            "host.data.keys.get",
            json!({}),
            json!({"client_key_id":"x".repeat(8192)}),
            "invalid_input",
        ),
        (
            "host.quota_observations.refresh",
            json!({}),
            json!({"account_id":"acct_test"}),
            "unsupported",
        ),
    ] {
        let mut req = request("facts-raw");
        req.body = serde_json::to_vec(
            &json!({"operation":"facts-raw","method":method,"params":params,"payload":payload}),
        )
        .unwrap();
        let reply = manager.handle(&installed.id, 1, req).await.unwrap();
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(value["error"]["code"], code, "{value}");
    }
    assert_eq!(facts.reads.load(Ordering::SeqCst), 3);
    let mut req = request("facts-raw");
    req.body=serde_json::to_vec(&json!({"operation":"facts-raw","method":"host.data.keys.get","params":{},"payload":{"client_key_id":"missing"}})).unwrap();
    let reply = manager.handle(&installed.id, 1, req).await.unwrap();
    let value: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(value["error"]["code"], "rejected");
    assert!(
        !String::from_utf8(reply.body)
            .unwrap()
            .contains("secret error detail")
    );
    manager.shutdown().await;
}
