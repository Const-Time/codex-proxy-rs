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

// Native maintenance runs through the published Manager worker and the original
// Provider/Admin/Store observation pipeline, without a synthetic refresh port.
struct MaintenanceRefresh {
    env: Environment,
    manager: Arc<PluginManager>,
    instance: PluginView,
    account: String,
    server: RefreshServer,
}
enum RefreshServer {
    OpenAi(Server),
    Xai(support::xai::Proxy),
}
impl RefreshServer {
    fn count(&self) -> usize {
        match self {
            Self::OpenAi(server) => server.requests.lock().unwrap().len(),
            Self::Xai(server) => server
                .events()
                .iter()
                .filter(|e| e["kind"] == "catalog" && e["path"] == "/v1/billing?format=credits")
                .count(),
        }
    }
    async fn entered(&self) {
        match self {
            Self::OpenAi(server) => {
                tokio::time::timeout(Duration::from_secs(5), server.entered.notified())
                    .await
                    .unwrap()
            }
            Self::Xai(server) => tokio::time::timeout(Duration::from_secs(5), async {
                while self.count() == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert!(!server.events().iter().any(|e| e["kind"] == "forbidden"));
            })
            .await
            .unwrap(),
        }
    }
    async fn release(&self) {
        match self {
            Self::OpenAi(server) => server.release.notify_one(),
            Self::Xai(server) => {
                let reply = reqwest::Client::new()
                    .post(format!("{}/__fixture/release", server.url))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(reply.status(), 200);
            }
        }
    }
    async fn close(self) {
        if let Self::Xai(server) = self {
            server.stop().await;
        }
    }
}
impl MaintenanceRefresh {
    async fn new(provider: &str, root: &std::path::Path, wait: bool) -> Self {
        use gateway_core::account::{OutboundProxy, ProviderAccountId};
        let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
        let (env, server) = if provider == "xai" {
            let proxy = support::xai::Proxy::start(
                root,
                &format!("subject-{account}"),
                "http://127.0.0.1:9",
                if wait { "quota-wait" } else { "quota" },
            )
            .await;
            (Environment::new().await.unwrap(), RefreshServer::Xai(proxy))
        } else {
            let server = Server::new(wait).await;
            (
                Environment::with_openai_url(Some(&server.url))
                    .await
                    .unwrap(),
                RefreshServer::OpenAi(server),
            )
        };
        let prepared = if let RefreshServer::Xai(proxy) = &server {
            let mut prepared = support::xai::account(
                &ProviderAccountId::new(account.clone()).unwrap(),
                "fixture-provider-secret",
            );
            prepared.account = prepared
                .account
                .with_outbound_proxy(Some(OutboundProxy::parse(&proxy.url).unwrap()));
            prepared
        } else {
            use provider_openai::credential::*;
            CodexCredentialAdmin
                .prepare_import(ImportCodexOAuthCredential {
                    account_id: account.clone(),
                    name: account.clone(),
                    enabled: true,
                    secret: CodexOAuthSecret {
                        access_token: secrecy::SecretString::from("fixture-provider-secret"),
                        refresh_token: None,
                        id_token: None,
                    },
                    verified_account: CodexAccountProfile {
                        oauth_subject: "maintenance-native-subject".into(),
                        poid: None,
                        chatgpt_account_id: "maintenance-native-account".into(),
                        chatgpt_user_id: "maintenance-native-user".into(),
                        email: None,
                        plan_type: Some("pro".into()),
                        access_token_expires_at: Some(
                            chrono::Utc::now() + chrono::Duration::hours(1),
                        ),
                    },
                    next_refresh_at: None,
                })
                .unwrap()
        };
        env.store
            .provider_ports()
            .accounts()
            .create_account(prepared)
            .await
            .unwrap();
        let store = env.store.plugin_store();
        let manager = Arc::new(PluginManager::new(
            store.clone(),
            Arc::new(
                PluginEngine::new(
                    env.cache.path().join("quota-maintenance"),
                    "3.11.2".parse().unwrap(),
                    store,
                )
                .with_data(env.admin.services().plugin_data()),
            ),
        ));
        let artifact = manager
            .upload(support::maintenance_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut command = support::update(&artifact.sha256, 0, true);
        command.configuration = json!({"adapter_mode":"maintenance-quota", "command_result":{"operation":"models","mode":"quota","method":"host.quota_observations.refresh","params":{},"request":{"account_id":account}}});
        let instance = manager.update(None, command, &actor()).await.unwrap();
        Self {
            env,
            manager,
            instance,
            account,
            server,
        }
    }
    fn worker(&self) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        use gateway_core::task::{WorkerContribution, WorkerRunnable};
        let WorkerContribution::Registration(registration) = self
            .manager
            .maintenance_worker(self.env.core.snapshot_publications())
            .unwrap()
        else {
            panic!("maintenance registration")
        };
        let WorkerRunnable::Daemon { task, .. } = registration.runnable else {
            panic!("maintenance daemon")
        };
        let stop = CancellationToken::new();
        let cancellation = stop.clone();
        (
            stop,
            tokio::spawn(async move {
                task.run(cancellation).await.unwrap();
            }),
        )
    }
    async fn completed(&self) {
        let store = self.env.store.plugin_store();
        let scope = PluginStateScope {
            instance_id: self.instance.id.clone(),
            revision: self.instance.revision,
            namespace: "counter".into(),
            schema_version: 1,
            maximum_records: 10,
            maximum_bytes: 1024,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store
                    .state_get(&scope, "reconcile")
                    .await
                    .unwrap()
                    .is_some_and(|r| r.value == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(
            "real native refresh and passive read must finish before maintenance state commits",
        );
    }
    async fn invariant(&self) {
        let quota = self
            .env
            .admin
            .services()
            .plugin_data()
            .quota(&self.account)
            .await
            .unwrap();
        assert!(quota.observed_at.is_some());
        assert!(quota.windows.iter().any(|w| w.used_percent == Some(25.0)));
        assert_eq!(
            self.server.count(),
            1,
            "maintenance must not replay an accepted refresh"
        );
        assert!(self.env.published.lock().unwrap().is_empty());
        for table in [
            "model_requests",
            "user_group_charge_events",
            "user_group_budget_windows",
        ] {
            let count: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                    .fetch_one(&self.env.pool)
                    .await
                    .unwrap();
            assert_eq!(
                count, 0,
                "quota observation cannot create model execution or local charges"
            );
        }
        let ports = self.env.store.provider_ports();
        let id = gateway_core::account::ProviderAccountId::new(self.account.clone()).unwrap();
        assert_eq!(
            ports
                .accounts()
                .load_current_credential(&id)
                .await
                .unwrap()
                .account
                .revision()
                .get(),
            1
        );
        if let RefreshServer::Xai(proxy) = &self.server {
            let events = proxy.events();
            let requests: Vec<_> = events.iter().filter(|e| e["kind"] == "catalog").collect();
            assert_eq!(
                requests.len(),
                2,
                "one billing read and one subscription read"
            );
            assert!(requests.iter().all(|e| e["authorization"] == true
                && e["subject"] == true
                && e["token_auth"] == true));
            assert!(!events.iter().any(|e| matches!(
                e["kind"].as_str(),
                Some("forbidden" | "inference" | "refresh")
            )));
            let observations = ports.accounts().get_quotas(&[id]).await.unwrap();
            assert_eq!(observations.len(), 1);
            let document = observations[0].quota.expose_to_provider();
            assert_eq!(document["config"]["futureWindow"]["kind"], "rolling");
            assert!(document.get("userId").is_none() && document.get("email").is_none());
            assert_eq!(quota.plan_type.as_deref(), Some("SuperGrokPro"));
            assert!(
                quota
                    .windows
                    .iter()
                    .any(|w| w.window_seconds == Some(604800))
            );
        }
    }
    async fn close(self) {
        self.manager.shutdown().await;
        drop(self.manager);
        self.env.close().await;
        self.server.close().await;
    }
}
async fn maintenance_trust(name: &str, timeout: Duration) -> Option<std::path::PathBuf> {
    if let Some(root) = std::env::var_os("CPR_NATIVE_XAI_TRUST") {
        return Some(root.into());
    }
    for key in ["CPR_TEST_DATABASE_URL", "CPR_TEST_REDIS_URL"] {
        if std::env::var_os(key).is_none() {
            let ci = std::env::var("CI")
                .ok()
                .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
            assert!(
                !ci,
                "{key} is required for native maintenance integration tests in CI"
            );
            return None;
        }
    }
    support::xai::Trust::new()
        .run_with_timeout(&format!("data::{name}"), timeout)
        .await;
    None
}
#[tokio::test]
async fn native_maintenance_refresh_uses_published_worker_and_original_openai_xai_observations() {
    let Some(root) = maintenance_trust(
        "native_maintenance_refresh_uses_published_worker_and_original_openai_xai_observations",
        Duration::from_secs(60),
    )
    .await
    else {
        return;
    };
    for provider in ["openai", "xai"] {
        let f = MaintenanceRefresh::new(provider, &root, false).await;
        assert!(
            f.env
                .admin
                .services()
                .plugin_data()
                .quota(&f.account)
                .await
                .unwrap()
                .observed_at
                .is_none()
        );
        assert_eq!(f.server.count(), 0);
        let (stop, worker) = f.worker();
        f.completed().await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        f.invariant().await;
        f.close().await;
    }
}
#[tokio::test]
async fn native_maintenance_refresh_drains_retired_openai_generations() {
    retired_maintenance(
        "openai",
        "native_maintenance_refresh_drains_retired_openai_generations",
    )
    .await;
}
#[tokio::test]
async fn native_maintenance_refresh_drains_retired_xai_generations() {
    retired_maintenance(
        "xai",
        "native_maintenance_refresh_drains_retired_xai_generations",
    )
    .await;
}
async fn retired_maintenance(provider: &str, name: &str) {
    let Some(root) = maintenance_trust(name, Duration::from_secs(120)).await else {
        return;
    };
    for action in ["shutdown", "disable", "upgrade", "delete"] {
        let f = MaintenanceRefresh::new(provider, &root, true).await;
        let sha = if action == "upgrade" {
            f.manager
                .upload(support::maintenance_archive("1.0.1"), &actor())
                .await
                .unwrap()
                .sha256
        } else {
            f.instance.artifact.sha256.clone()
        };
        let (stop, worker) = f.worker();
        f.server.entered().await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        let manager = f.manager.clone();
        let access = manager.access(&f.instance.id, f.instance.revision).unwrap();
        let (id, revision, mut configuration) = (
            f.instance.id.clone(),
            f.instance.revision,
            f.instance.configuration.clone(),
        );
        let registration_signal = (action == "upgrade")
            .then(|| f.env.cache.path().join("replacement-registration-started"));
        if let Some(path) = &registration_signal {
            configuration["registration_signal"] = json!(path);
        }
        let preparation_started = std::time::Instant::now();
        let finishing = tokio::spawn(async move {
            match action {
                "disable" | "upgrade" => {
                    let mut command = support::update(&sha, revision, action == "upgrade");
                    command.configuration = configuration;
                    manager.update(Some(&id), command, &actor()).await.unwrap();
                }
                "delete" => {
                    let mut command = support::update(&sha, revision, false);
                    command.configuration = configuration;
                    manager.update(Some(&id), command, &actor()).await.unwrap();
                    manager.delete(&id, revision + 1, &actor()).await.unwrap();
                }
                _ => {}
            }
            manager.shutdown().await;
        });
        let mut finishing = Box::pin(finishing);
        if let Some(path) = &registration_signal {
            // The old publication stays valid during candidate preparation.
            // Measure revocation only after the real replacement SDK is ready.
            tokio::time::timeout(Duration::from_secs(30), async {
                while !tokio::fs::try_exists(path).await.unwrap() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("replacement reaches SDK registration before testing retirement drain");
            eprintln!(
                "{provider}, action={action}: replacement setup_elapsed={:?}",
                preparation_started.elapsed()
            );
        }
        tokio::time::timeout(Duration::from_secs(5), access.cancellation().cancelled())
            .await
            .expect("old published generation must be revoked before its native work finishes");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), finishing.as_mut())
                .await
                .is_err(),
            "{provider}, action={action}: Manager shutdown must retain accepted native work from retired generations"
        );
        if action == "upgrade" {
            // Losing a shutdown waiter must not lose the original stop future.
            finishing.as_ref().get_ref().abort();
            assert!(finishing.await.unwrap_err().is_cancelled());
            let manager = f.manager.clone();
            finishing = Box::pin(tokio::spawn(async move {
                manager.shutdown().await;
            }));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), finishing.as_mut())
                    .await
                    .is_err()
            );
        }
        f.server.release().await;
        tokio::time::timeout(Duration::from_secs(10), finishing)
            .await
            .unwrap()
            .unwrap();
        f.invariant().await;
        let result = f
            .manager
            .handle(
                &f.instance.id,
                f.instance.revision,
                support::request("facts"),
            )
            .await;
        assert!(
            result.is_err(),
            "old maintenance generation must remain revoked"
        );
        f.close().await;
    }
}
