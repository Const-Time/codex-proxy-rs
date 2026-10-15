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
    async fn refresh_quota(
        &self,
        _: PluginMutationScope,
        _: gateway_admin::model::MutationContext,
        _: gateway_core::account::ProviderAccountId,
        _: gateway_core::engine::nested::ExtensionCallScope,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Result<ProviderQuota, AdminError> {
        Err(AdminError::unavailable("quota refresh is not configured"))
    }
    async fn keys(&self, _: PluginKeyQuery) -> Result<PluginKeyPage, AdminError> {
        Err(AdminError::unavailable(
            "global Key listing is not configured",
        ))
    }
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
            "permission_denied",
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

// Explicit observation refresh exercises the same production data owner.
use crate::support::quota_server::Server;
use crate::{
    command_line::credentials::{Native, create},
    services::upstream_sdk::environment::Environment,
    support,
};
use gateway_admin::{model::MutationActor, ports::plugins::PluginProcess};
use gateway_core::{engine::nested::ExtensionCallScope, lifecycle::CancellationToken};
use std::time::Duration;

struct NativeFixture {
    native: Native,
    server: Server,
    account: String,
}
impl NativeFixture {
    async fn new(wait: bool) -> Option<Self> {
        let server = Server::new(wait).await;
        let env = Environment::with_openai_url(Some(&server.url)).await?;
        let native =
            Native::with_environment(json!({}), None, support::command_archive("1.0.0"), env).await;
        let mut request = support::request("models");
        request.actor.actor = MutationActor::AdminApiKey;
        request.body = serde_json::to_vec(&json!({"operation":"models","mode":"credentials","method":"host.auth.save","request":create("quota-native-user")})).unwrap();
        let response = native.process.handle(request).await.unwrap();
        let result: Value = serde_json::from_slice(&response.body).unwrap();
        let account = result["account_id"]
            .as_str()
            .expect("native account creation must succeed")
            .to_owned();
        Some(Self {
            native,
            server,
            account,
        })
    }
    fn input(&self, method: &str) -> Value {
        json!({"operation":"models", "mode":"quota", "method":method, "params":{}, "request":{"account_id":self.account}})
    }
    async fn call(&self, actor: MutationActor, input: Value) -> Value {
        call(self.native.process.clone(), actor, input).await
    }
    async fn cached(&self) -> Value {
        self.call(
            MutationActor::AdminApiKey,
            self.input("host.data.quota.get"),
        )
        .await
    }
    async fn invariant(&self) {
        let revision: i64 =
            sqlx::query_scalar("SELECT credential_revision FROM provider_accounts WHERE id=$1")
                .bind(&self.account)
                .fetch_one(&self.native.env.pool)
                .await
                .unwrap();
        assert_eq!(revision, 1);
        let charges: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_group_charge_events")
            .fetch_one(&self.native.env.pool)
            .await
            .unwrap();
        assert_eq!(charges, 0);
        let windows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_group_budget_windows")
            .fetch_one(&self.native.env.pool)
            .await
            .unwrap();
        assert_eq!(windows, 0);
        assert_eq!(
            self.native.env.published.lock().unwrap().len(),
            1,
            "quota observation does not publish a new configuration"
        );
    }
    async fn close(self) {
        self.native.close().await;
    }
}
async fn call(process: Arc<dyn PluginProcess>, actor: MutationActor, input: Value) -> Value {
    let mut request = support::request("facts-raw");
    request.actor.actor = actor;
    request.body = serde_json::to_vec(&input).unwrap();
    let response = process.handle(request).await.unwrap();
    serde_json::from_slice(&response.body).unwrap()
}
fn observed(value: &Value) {
    assert!(value["observed_at_ms"].is_i64(), "{value}");
    let windows = value["windows"]
        .as_array()
        .expect("native windows are projected");
    assert!(windows.iter().any(|v| v["used_percent"] == 25.0));
    assert!(windows.iter().any(|v| v["used_percent"] == 50.0));
}

#[tokio::test]
async fn native_quota_refresh_management_and_system_command_use_original_observations_without_passive_network()
 {
    let Some(f) = NativeFixture::new(false).await else {
        return;
    };
    assert!(f.cached().await["observed_at_ms"].is_null());
    assert!(f.server.requests.lock().unwrap().is_empty());
    let refreshed = f
        .call(
            MutationActor::AdminApiKey,
            f.input("host.quota_observations.refresh"),
        )
        .await;
    observed(&refreshed);
    observed(&f.cached().await);
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    let root = ExtensionCallScope::default();
    let output = f
        .native
        .process
        .command(
            PluginCommandRequest::system(
                "probe".into(),
                vec![
                    "--input".into(),
                    f.input("host.quota_observations.refresh").to_string(),
                ],
                CancellationToken::new(),
                root.clone(),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(output.saved_accounts, 0);
    observed(&serde_json::from_str::<Value>(&output.stdout).unwrap());
    root.wait_until_idle().await;
    assert_eq!(f.server.requests.lock().unwrap().len(), 2);
    f.invariant().await;
    f.close().await;
}

#[tokio::test]
async fn native_quota_refresh_checks_actual_admin_role_scope_and_input_before_external_work() {
    let Some(f) = NativeFixture::new(false).await else {
        return;
    };
    let id: String = sqlx::query_scalar("SELECT id FROM users WHERE username='sdk-fixture'")
        .fetch_one(&f.native.env.pool)
        .await
        .unwrap();
    for actor in [
        MutationActor::System,
        MutationActor::Plugin {
            instance_id: f.native.record.id.clone(),
            revision: std::num::NonZeroU64::new(1).unwrap(),
        },
        MutationActor::AdminSession {
            admin_user_id: "missing-user".into(),
        },
    ] {
        let value = f
            .call(actor, f.input("host.quota_observations.refresh"))
            .await;
        assert_eq!(value["error"]["code"], "permission_denied", "{value}");
    }
    for (role, enabled) in [("user", true), ("admin", false)] {
        sqlx::query("UPDATE users SET role=$2,enabled=$3 WHERE id=$1")
            .bind(&id)
            .bind(role)
            .bind(enabled)
            .execute(&f.native.env.pool)
            .await
            .unwrap();
        let value = f
            .call(
                MutationActor::AdminSession {
                    admin_user_id: id.clone(),
                },
                f.input("host.quota_observations.refresh"),
            )
            .await;
        assert_eq!(value["error"]["code"], "permission_denied", "{value}");
    }
    for input in [
        json!({"operation":"models","mode":"quota","method":"host.quota_observations.refresh","params":{"actor":"system"},"request":{"account_id":f.account}}),
        json!({"operation":"models","mode":"quota","method":"host.quota_observations.refresh","params":{},"request":{"account_id":f.account,"extra":true}}),
    ] {
        assert_eq!(
            f.call(MutationActor::AdminApiKey, input).await["error"]["code"],
            "invalid_input"
        );
    }
    assert!(f.server.requests.lock().unwrap().is_empty());
    f.invariant().await;
    f.close().await;
}

#[tokio::test]
async fn native_quota_refresh_admin_revocation_suppresses_result_after_native_observation() {
    let Some(f) = NativeFixture::new(true).await else {
        return;
    };
    let id: String = sqlx::query_scalar("SELECT id FROM users WHERE username='sdk-fixture'")
        .fetch_one(&f.native.env.pool)
        .await
        .unwrap();
    let process = f.native.process.clone();
    let input = f.input("host.quota_observations.refresh");
    let actor = MutationActor::AdminSession {
        admin_user_id: id.clone(),
    };
    let pending = tokio::spawn(async move { call(process, actor, input).await });
    tokio::time::timeout(Duration::from_secs(5), f.server.entered.notified())
        .await
        .unwrap();
    sqlx::query("UPDATE users SET enabled=false WHERE id=$1")
        .bind(&id)
        .execute(&f.native.env.pool)
        .await
        .unwrap();
    f.server.release.notify_one();
    let value = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["error"]["code"], "permission_denied", "{value}");
    observed(&f.cached().await);
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.invariant().await;
    f.close().await;
}

#[tokio::test]
async fn native_quota_refresh_cancel_and_plugin_stop_drain_the_original_native_task_without_replay()
{
    let Some(f) = NativeFixture::new(true).await else {
        return;
    };
    let root = ExtensionCallScope::default();
    let cancellation = CancellationToken::new();
    let request = PluginCommandRequest::system(
        "probe".into(),
        vec![
            "--input".into(),
            f.input("host.quota_observations.refresh").to_string(),
        ],
        cancellation.clone(),
        root.clone(),
    );
    let process = f.native.process.clone();
    let pending = tokio::spawn(async move { process.command(request, None).await });
    tokio::time::timeout(Duration::from_secs(5), f.server.entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), root.wait_until_idle())
            .await
            .is_err()
    );
    {
        let stop = f.native.process.stop();
        tokio::pin!(stop);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stop.as_mut())
                .await
                .is_err()
        );
        f.server.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), stop.as_mut())
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), root.wait_until_idle())
        .await
        .unwrap();
    let quota = f
        .native
        .env
        .admin
        .services()
        .plugin_data()
        .quota(&f.account)
        .await
        .unwrap();
    assert!(quota.observed_at.is_some());
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.invariant().await;
    f.close().await;
}

#[tokio::test]
async fn native_quota_refresh_plugin_revision_change_rejects_stale_result_and_new_external_work() {
    let Some(f) = NativeFixture::new(true).await else {
        return;
    };
    let process = f.native.process.clone();
    let input = f.input("host.quota_observations.refresh");
    let pending =
        tokio::spawn(async move { call(process, MutationActor::AdminApiKey, input).await });
    tokio::time::timeout(Duration::from_secs(5), f.server.entered.notified())
        .await
        .unwrap();
    let mut replacement = f.native.record.clone();
    replacement.revision += 1;
    f.native
        .env
        .store
        .plugin_store()
        .save(&replacement, 1, &support::actor(), None)
        .await
        .unwrap();
    f.server.release.notify_one();
    let value = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["error"]["code"], "conflict", "{value}");
    let value = f
        .call(
            MutationActor::AdminApiKey,
            f.input("host.quota_observations.refresh"),
        )
        .await;
    assert_eq!(value["error"]["code"], "conflict", "{value}");
    let quota = f
        .native
        .env
        .admin
        .services()
        .plugin_data()
        .quota(&f.account)
        .await
        .unwrap();
    assert!(quota.observed_at.is_some());
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.invariant().await;
    f.close().await;
}

#[tokio::test]
async fn native_quota_refresh_rechecks_admin_after_native_metadata_wait_before_external_work() {
    let Some(f) = NativeFixture::new(false).await else {
        return;
    };
    let id: String = sqlx::query_scalar("SELECT id FROM users WHERE username='sdk-fixture'")
        .fetch_one(&f.native.env.pool)
        .await
        .unwrap();
    let mut lock = f.native.env.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE plugin_instances IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let process = f.native.process.clone();
    let input = f.input("host.quota_observations.refresh");
    let actor = MutationActor::AdminSession {
        admin_user_id: id.clone(),
    };
    let pending = tokio::spawn(async move { call(process, actor, input).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND wait_event_type='Lock' AND query LIKE '%plugin_instances%'").fetch_one(&f.native.env.pool).await.unwrap();
            if blocked > 0 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("native account metadata lookup must wait on the actual plugin row");
    sqlx::query("UPDATE users SET enabled=false WHERE id=$1")
        .bind(&id)
        .execute(&f.native.env.pool)
        .await
        .unwrap();
    lock.rollback().await.unwrap();
    let value = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["error"]["code"], "permission_denied", "{value}");
    assert!(f.server.requests.lock().unwrap().is_empty());
    f.invariant().await;
    f.close().await;
}
