use super::*;
use crate::services::upstream_sdk::environment::Environment;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::ports::plugins::PluginCredentialService;
use gateway_core::engine::nested::ExtensionCallScope;
use sqlx::Row as _;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::Notify;

pub(crate) struct Native {
    pub(crate) env: Environment,
    engine: PluginEngine,
    pub(crate) record: PluginRecord,
    artifact: PluginArtifact,
    pub(crate) process: Arc<dyn PluginProcess>,
}
impl Native {
    async fn new(
        accounts: Vec<Value>,
        port: Option<Arc<dyn PluginCredentialService>>,
    ) -> Option<Self> {
        Self::with_configuration(configuration(accounts), port).await
    }
    pub(crate) async fn with_configuration(
        configuration: Value,
        port: Option<Arc<dyn PluginCredentialService>>,
    ) -> Option<Self> {
        Self::with_archive(configuration, port, command_archive("1.0.0")).await
    }
    pub(crate) async fn with_archive(
        configuration: Value,
        port: Option<Arc<dyn PluginCredentialService>>,
        archive: Arc<[u8]>,
    ) -> Option<Self> {
        let env = Environment::new().await?;
        Some(Self::with_environment(configuration, port, archive, env).await)
    }
    pub(crate) async fn with_environment(
        configuration: Value,
        port: Option<Arc<dyn PluginCredentialService>>,
        archive: Arc<[u8]>,
        env: Environment,
    ) -> Self {
        let store = env.store.plugin_store();
        let engine = PluginEngine::new(
            env.cache.path().join("command-accounts"),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_credentials(port.unwrap_or_else(|| env.admin.services().plugin_credentials()))
        .with_data(env.admin.services().plugin_data());
        let artifact = engine.inspect(archive).await.unwrap();
        store
            .put_artifact(artifact.clone(), &actor())
            .await
            .unwrap();
        let record = PluginRecord {
            id: "command-fixture".into(),
            revision: 1,
            name: "Native command".into(),
            artifact_sha256: artifact.info.sha256.clone(),
            enabled: true,
            configuration,
            secrets: Default::default(),
            frontend_authentication: None,
            middleware: None,
            model_router: None,
            account_scheduler: None,
            retry_policy: None,
            upstream_adapter: None,
            observer: None,
        };
        store.save(&record, 0, &actor(), None).await.unwrap();
        engine.validate(&record, &artifact, None).await.unwrap();
        let process = engine.prepare(&record, artifact.clone()).await.unwrap();
        process.activate();
        Self {
            env,
            engine,
            record,
            artifact,
            process,
        }
    }
    async fn replace_result(&mut self, accounts: Vec<Value>) {
        stop(&self.process).await;
        let expected = self.record.revision;
        self.record.revision += 1;
        self.record.configuration = configuration(accounts);
        self.env
            .store
            .plugin_store()
            .save(&self.record, expected, &actor(), None)
            .await
            .unwrap();
        self.process = self
            .engine
            .prepare(&self.record, self.artifact.clone())
            .await
            .unwrap();
        self.process.activate();
    }
    pub(crate) async fn count(&self) -> i64 {
        sqlx::query_scalar("select count(*) from provider_accounts")
            .fetch_one(&self.env.pool)
            .await
            .unwrap()
    }
    pub(crate) async fn close(self) {
        stop(&self.process).await;
        drop(self.process);
        drop(self.engine);
        self.env.close().await;
    }
}
fn configuration(accounts: Vec<Value>) -> Value {
    json!({"command_result":{"stdout":"confirmed output\n", "stderr":"", "exit_code":0, "accounts":accounts}})
}
fn facts(identity: &str) -> Value {
    let payload = json!({"exp":(chrono::Utc::now()+chrono::TimeDelta::hours(1)).timestamp(),
        "https://api.openai.com/auth":{"chatgpt_user_id":identity,"chatgpt_account_id":format!("account-{identity}"),"chatgpt_plan_type":"plus"}});
    json!({"name":format!("Fixture {identity}"), "authentication_kind":"oauth", "has_refresh_token":true,
        "upstream_user_id":identity, "upstream_account_id":format!("account-{identity}"),
        "access_token_expires_at_ms":1, "next_refresh_at_ms":2,
        "material":{"schema_version":1,"installation_id":uuid::Uuid::new_v4().to_string(),
            "access_token":format!("e30.{}.fixture",URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap())),
            "refresh_token":format!("controlled-fixture-refresh-{identity}"), "id_token":null, "cookies":[]}})
}
pub(crate) fn create(identity: &str) -> Value {
    json!({"action":"create", "provider_id":"openai", "facts":facts(identity)})
}
fn call(root: &ExtensionCallScope, cancellation: CancellationToken) -> PluginCommandRequest {
    PluginCommandRequest::system("probe".into(), vec![], cancellation, root.clone())
}

#[tokio::test]
async fn command_accounts_use_native_provider_transactions_publication_and_plugin_audit() {
    let Some(fixture) = Native::new(vec![create("user-a"), create("user-b")], None).await else {
        return;
    };
    let root = ExtensionCallScope::default();
    let before: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&fixture.env.pool)
        .await
        .unwrap();
    let output = fixture
        .process
        .command(call(&root, CancellationToken::new()), None)
        .await
        .unwrap();
    assert_eq!(output.saved_accounts, 2);
    assert_eq!(output.stdout, "confirmed output\n");
    assert_eq!(fixture.count().await, 2);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 2);
    root.wait_until_idle().await;
    let rows = sqlx::query("select authentication_kind,has_refresh_token,access_token_expires_at,next_refresh_at,plan_type from provider_accounts order by id").fetch_all(&fixture.env.pool).await.unwrap();
    for row in rows {
        assert_eq!(row.get::<String, _>("authentication_kind"), "oauth");
        assert!(row.get::<bool, _>("has_refresh_token"));
        assert!(
            row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("access_token_expires_at")
                .unwrap()
                > chrono::Utc::now()
        );
        assert_eq!(
            row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("next_refresh_at"),
            None
        );
        assert_eq!(
            row.get::<Option<String>, _>("plan_type").as_deref(),
            Some("plus")
        );
    }
    let audits: Vec<(String, String, Vec<String>)> = sqlx::query_as("select actor_kind,actor_ref,changed_fields from admin_audit_events where actor_kind='plugin' order by created_at").fetch_all(&fixture.env.pool).await.unwrap();
    assert_eq!(audits.len(), 2);
    assert!(audits.iter().all(|(kind, actor, fields)| kind == "plugin"
        && actor == "plugin:command-fixture@1"
        && fields == &["credentials"]));
    let after: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&fixture.env.pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    fixture.close().await;
}

#[tokio::test]
async fn command_account_replacement_checks_native_identity_revision_and_keeps_settings() {
    let Some(mut fixture) = Native::new(vec![create("replace-user")], None).await else {
        return;
    };
    fixture
        .process
        .command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        )
        .await
        .unwrap();
    let (id, revision): (String, i64) =
        sqlx::query_as("select id,credential_revision from provider_accounts")
            .fetch_one(&fixture.env.pool)
            .await
            .unwrap();
    sqlx::query(
        "update provider_accounts set name='Native name',enabled=false,weight=7 where id=$1",
    )
    .bind(&id)
    .execute(&fixture.env.pool)
    .await
    .unwrap();
    let request = |facts| json!({"action":"replace", "account_id":id,"credential_revision":revision,"facts":facts});
    fixture
        .replace_result(vec![request(facts("replace-user"))])
        .await;
    let output = fixture
        .process
        .command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(output.saved_accounts, 1);
    let row = sqlx::query(
        "select name,enabled,weight,credential_revision from provider_accounts where id=$1",
    )
    .bind(&id)
    .fetch_one(&fixture.env.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("name"), "Native name");
    assert!(!row.get::<bool, _>("enabled"));
    assert_eq!(row.get::<i16, _>("weight"), 7);
    assert_eq!(row.get::<i64, _>("credential_revision"), revision + 1);
    // The same expected revision cannot be submitted again.
    let error = fixture
        .process
        .command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        PluginCommandError::Incomplete {
            saved_accounts: 0,
            ..
        }
    ));
    fixture.replace_result(vec![json!({"action":"replace","account_id":id,"credential_revision":revision+1,"facts":facts("other-user")})]).await;
    let error = fixture
        .process
        .command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        PluginCommandError::Incomplete {
            saved_accounts: 0,
            ..
        }
    ));
    assert_eq!(fixture.count().await, 1);
    fixture.close().await;
}

#[tokio::test]
async fn command_account_invalid_later_result_reports_only_confirmed_native_commits() {
    let mut invalid = create("bad-user");
    invalid["facts"]["upstream_user_id"] = json!("forged-user");
    let Some(mut fixture) = Native::new(vec![create("good-user"), invalid], None).await else {
        return;
    };
    let error = fixture
        .process
        .command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        PluginCommandError::Incomplete {
            saved_accounts: 1,
            ..
        }
    ));
    assert!(!error.to_string().contains("controlled-fixture-refresh"));
    assert_eq!(fixture.count().await, 1);
    for invalid in [
        {
            let mut v = create("other");
            v["facts"]["authentication_kind"] = json!("api_key");
            v
        },
        {
            let mut v = create("other");
            v["facts"]["material"]["schema_version"] = json!(99);
            v
        },
        {
            let mut v = create("other");
            v["facts"]["has_refresh_token"] = json!(false);
            v
        },
        {
            let mut v = create("other");
            v["facts"]["access_token_expires_at_ms"] = json!(i64::MAX);
            v
        },
    ] {
        fixture.replace_result(vec![invalid]).await;
        assert!(
            fixture
                .process
                .command(
                    call(&ExtensionCallScope::default(), CancellationToken::new()),
                    None
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.count().await, 1);
    }
    fixture.close().await;
}

struct Observed {
    inner: Arc<dyn PluginCredentialService>,
    calls: AtomicUsize,
    first: Notify,
    proceed: Notify,
    second: Notify,
    hold_before_start: AtomicBool,
}
#[async_trait]
impl PluginCredentialService for Observed {
    async fn accounts(
        &self,
        scope: PluginMutationScope,
        initiator: gateway_admin::model::MutationContext,
        query: PluginCredentialQuery,
    ) -> Result<PluginCredentialPage, AdminError> {
        self.inner.accounts(scope, initiator, query).await
    }
    async fn credential(
        &self,
        scope: PluginMutationScope,
        initiator: gateway_admin::model::MutationContext,
        account: gateway_core::account::ProviderAccountId,
    ) -> Result<gateway_admin::model::provider_credentials::ProviderExportCredentialInput, AdminError>
    {
        self.inner.credential(scope, initiator, account).await
    }

    async fn save(
        &self,
        scope: PluginMutationScope,
        initiator: gateway_admin::model::MutationContext,
        command: PluginCredentialSave,
        cleanup: ExtensionCallScope,
        cancellation: CancellationToken,
    ) -> Result<PluginCredentialSaved, AdminError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        if index == 1 && self.hold_before_start.load(Ordering::SeqCst) {
            self.second.notify_one();
            self.proceed.notified().await;
        }
        let mut saving =
            Box::pin(
                self.inner
                    .save(scope, initiator, command, cleanup, cancellation),
            );
        if index == 1 && !self.hold_before_start.load(Ordering::SeqCst) {
            assert!(futures::poll!(saving.as_mut()).is_pending());
            self.second.notify_one();
        }
        let result = saving.await;
        if index == 0 {
            self.first.notify_one();
            self.proceed.notified().await;
        }
        result
    }
}

#[tokio::test]
async fn command_cancel_and_total_deadline_leave_inflight_native_commit_on_original_graph() {
    for deadline in [false, true] {
        let Some(mut fixture) = Native::new(vec![], None).await else {
            return;
        };
        let observed = Arc::new(Observed {
            inner: fixture.env.admin.services().plugin_credentials(),
            calls: AtomicUsize::new(0),
            first: Notify::new(),
            proceed: Notify::new(),
            second: Notify::new(),
            hold_before_start: AtomicBool::new(false),
        });
        fixture.engine = fixture.engine.with_credentials(observed.clone());
        fixture
            .replace_result(vec![create("committed-first"), create("inflight-second")])
            .await;
        let root = ExtensionCallScope::default();
        let cancellation = CancellationToken::new();
        let mut command = Box::pin(
            fixture
                .process
                .command(call(&root, cancellation.clone()), None),
        );
        tokio::select! { _=observed.first.notified()=>{}, _=&mut command=>panic!("command must wait for first receipt") };
        let mut lock = fixture.env.pool.begin().await.unwrap();
        let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        sqlx::query("select id from runtime_settings where id=1 for update")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        observed.proceed.notify_one();
        tokio::select! { _=observed.second.notified()=>{}, _=&mut command=>panic!("command must start second save") };
        // Wait for the real native transaction to reach its commit fence. A
        // spawned task alone does not prove preparation has actually started.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar("select exists(select 1 from pg_stat_activity where datname=current_database() and query like 'update runtime_settings%' and $1=any(pg_blocking_pids(pid)))")
                    .bind(lock_pid).fetch_one(&fixture.env.pool).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        if deadline {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(121)).await;
        } else {
            cancellation.cancel();
        }
        let error = command.await.err().unwrap();
        if deadline {
            tokio::time::resume();
        }
        assert!(matches!(
            error,
            PluginCommandError::Incomplete {
                saved_accounts: 1,
                ..
            }
        ));
        let mut drain = Box::pin(root.wait_until_idle());
        assert!(
            futures::poll!(drain.as_mut()).is_pending(),
            "original account task must retain its graph permit"
        );
        assert_eq!(fixture.count().await, 1);
        lock.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .unwrap();
        assert_eq!(
            fixture.count().await,
            2,
            "native task must finish once, without re-running SDK command"
        );
        assert_eq!(observed.calls.load(Ordering::SeqCst), 2);
        fixture.close().await;
    }
}

#[tokio::test]
async fn command_later_save_is_fenced_by_original_plugin_row_after_disable() {
    for replacing in [false, true] {
        let Some(mut fixture) = Native::new(vec![], None).await else {
            return;
        };
        let mut requests = vec![create("before-disable"), create("after-disable")];
        let mut original_revision = None;
        if replacing {
            fixture
                .replace_result(vec![create("replace-disabled")])
                .await;
            fixture
                .process
                .command(
                    call(&ExtensionCallScope::default(), CancellationToken::new()),
                    None,
                )
                .await
                .unwrap();
            let (id, revision): (String, i64) =
                sqlx::query_as("select id,credential_revision from provider_accounts")
                    .fetch_one(&fixture.env.pool)
                    .await
                    .unwrap();
            requests[1] = json!({"action":"replace","account_id":id,"credential_revision":revision,"facts":facts("replace-disabled")});
            original_revision = Some((id, revision));
        }
        let observed = Arc::new(Observed {
            inner: fixture.env.admin.services().plugin_credentials(),
            calls: AtomicUsize::new(0),
            first: Notify::new(),
            proceed: Notify::new(),
            second: Notify::new(),
            hold_before_start: AtomicBool::new(true),
        });
        fixture.engine = fixture.engine.with_credentials(observed.clone());
        fixture.replace_result(requests).await;
        let mut command = Box::pin(fixture.process.command(
            call(&ExtensionCallScope::default(), CancellationToken::new()),
            None,
        ));
        tokio::select! { _=observed.first.notified()=>{}, _=&mut command=>panic!("first save must finish") };
        observed.proceed.notify_one();
        tokio::select! { _=observed.second.notified()=>{}, _=&mut command=>panic!("second save must wait") };
        let mut disabled = fixture.record.clone();
        disabled.enabled = false;
        disabled.revision += 1;
        fixture
            .env
            .store
            .plugin_store()
            .save(&disabled, fixture.record.revision, &actor(), None)
            .await
            .unwrap();
        observed.proceed.notify_one();
        let error = command.await.err().unwrap();
        assert!(matches!(
            error,
            PluginCommandError::Incomplete {
                saved_accounts: 1,
                ..
            }
        ));
        assert_eq!(fixture.count().await, if replacing { 2 } else { 1 });
        if let Some((id, revision)) = original_revision {
            let current: i64 =
                sqlx::query_scalar("select credential_revision from provider_accounts where id=$1")
                    .bind(id)
                    .fetch_one(&fixture.env.pool)
                    .await
                    .unwrap();
            assert_eq!(
                current, revision,
                "disabled generation cannot rotate the original credential"
            );
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn command_personal_actor_cannot_use_native_global_account_port() {
    let Some(fixture) = Native::new(vec![create("unauthorized")], None).await else {
        return;
    };
    let root = ExtensionCallScope::default();
    let mut request = call(&root, CancellationToken::new());
    request.actor.actor = MutationActor::AdminSession {
        admin_user_id: "personal-command-user".into(),
    };
    assert!(!matches!(request.actor.actor, MutationActor::System));
    let error = fixture.process.command(request, None).await.err().unwrap();
    assert!(matches!(
        error,
        PluginCommandError::Incomplete {
            saved_accounts: 0,
            ..
        }
    ));
    root.wait_until_idle().await;
    assert_eq!(fixture.count().await, 0);
    fixture.close().await;
}

#[tokio::test]
async fn command_xai_result_uses_native_userinfo_before_committing_replacement() {
    xai(
        "command_xai_result_uses_native_userinfo_before_committing_replacement",
        "save",
    )
    .await;
}
#[tokio::test]
async fn command_xai_result_commits_refreshed_material_from_native_provider() {
    xai(
        "command_xai_result_commits_refreshed_material_from_native_provider",
        "refresh",
    )
    .await;
}
#[tokio::test]
async fn command_xai_result_rejects_foreign_verified_subject_without_committing() {
    xai(
        "command_xai_result_rejects_foreign_verified_subject_without_committing",
        "subject",
    )
    .await;
}
#[tokio::test]
async fn command_xai_result_rejects_foreign_discovery_issuer_without_committing() {
    xai(
        "command_xai_result_rejects_foreign_discovery_issuer_without_committing",
        "issuer",
    )
    .await;
}

async fn xai(name: &str, mode: &str) {
    use crate::support::xai::{Proxy, Trust};
    use gateway_core::account::{CredentialState, ProviderAccountId};
    use provider_xai::{
        CreateGrokCredential, GrokAccountProfile, GrokCredentialAdmin, GrokOAuthSecret,
        OFFICIAL_SCOPES, SecretValue,
    };
    let root = match std::env::var_os("CPR_NATIVE_XAI_TRUST") {
        Some(root) => std::path::PathBuf::from(root),
        None => {
            for key in ["CPR_TEST_DATABASE_URL", "CPR_TEST_REDIS_URL"] {
                if std::env::var_os(key).is_none() {
                    let ci = std::env::var("CI")
                        .ok()
                        .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
                    assert!(
                        !ci,
                        "{key} is required for native command integration tests in CI"
                    );
                    return;
                }
            }
            Trust::new()
                .run(&format!("command_line::credentials::{name}"))
                .await;
            return;
        }
    };
    let env = Environment::new().await.unwrap();
    let ports = env.store.provider_ports();
    let id = ProviderAccountId::new(format!("acct_{}", uuid::Uuid::new_v4().simple())).unwrap();
    let subject = format!("subject-{id}");
    let prepared = GrokCredentialAdmin
        .prepare_import(&CreateGrokCredential {
            account_id: id.clone(),
            name: "Native xAI name".into(),
            enabled: true,
            initial_credential_state: CredentialState::Ready,
            initial_error_reason: None,
            initial_error_message: None,
            secret: GrokOAuthSecret {
                access_token: SecretValue::new("fixture-original-xai-token"),
                refresh_token: SecretValue::new("fixture-refresh-token"),
                id_token: None,
                scope: OFFICIAL_SCOPES.join(" "),
            },
            account: GrokAccountProfile {
                subject: subject.clone(),
                email: None,
                upstream_account_id: None,
                plan_type: Some("pro".into()),
                access_token_expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                refresh_token_expires_at: None,
            },
        })
        .unwrap();
    ports.accounts().create_account(prepared).await.unwrap();
    let proxy = Proxy::start(&root, &subject, "http://127.0.0.1:9", mode).await;
    sqlx::query(
        "update provider_accounts set enabled=false,weight=7,outbound_proxy_url=$1 where id=$2",
    )
    .bind(&proxy.url)
    .bind(id.as_str())
    .execute(&env.pool)
    .await
    .unwrap();
    let original = ports.accounts().load_current_credential(&id).await.unwrap();
    let mut material = original.credential.expose_to_provider().clone();
    material["access_token"] = json!("fixture-xai-candidate-token");
    let expiry =
        chrono::Utc::now() + chrono::Duration::seconds(if mode == "refresh" { 20 } else { 3600 });
    let account = json!({"action":"replace", "account_id":id.as_str(), "credential_revision":original.account.revision().get(),
        "facts":{"name":"SDK candidate name", "authentication_kind":"oauth", "has_refresh_token":true,
            "upstream_user_id":subject, "access_token_expires_at_ms":expiry.timestamp_millis(), "next_refresh_at_ms":1,
            "material":material}});
    let f = Native::with_environment(
        configuration(vec![account]),
        None,
        command_archive("1.0.0"),
        env,
    )
    .await;
    let before: i64 = sqlx::query_scalar("select config_revision from runtime_settings where id=1")
        .fetch_one(&f.env.pool)
        .await
        .unwrap();
    let root = ExtensionCallScope::default();
    let result = f
        .process
        .command(call(&root, CancellationToken::new()), None)
        .await;
    root.wait_until_idle().await;
    let success = matches!(mode, "save" | "refresh");
    if success {
        let output = result.unwrap();
        assert_eq!(output.saved_accounts, 1);
        assert_eq!(output.stdout, "confirmed output\n");
        assert!(matches!(
            f.process
                .command(call(&root, CancellationToken::new()), None)
                .await
                .err()
                .unwrap(),
            PluginCommandError::Incomplete {
                saved_accounts: 0,
                ..
            }
        ));
    } else {
        assert!(matches!(
            result.err().unwrap(),
            PluginCommandError::Incomplete {
                saved_accounts: 0,
                ..
            }
        ));
    }
    let stored = ports.accounts().load_current_credential(&id).await.unwrap();
    assert_eq!(
        stored.account.revision().get(),
        original.account.revision().get() + u64::from(success)
    );
    let secret = stored.credential.expose_to_provider();
    assert_eq!(
        secret["access_token"],
        if mode == "refresh" {
            "fixture-xai-verified-token"
        } else if success {
            "fixture-xai-candidate-token"
        } else {
            "fixture-original-xai-token"
        }
    );
    assert_eq!(
        secret["refresh_token"],
        if mode == "refresh" {
            "fixture-xai-verified-refresh"
        } else {
            "fixture-refresh-token"
        }
    );
    let row = sqlx::query(
        "select name,enabled,weight,outbound_proxy_url from provider_accounts where id=$1",
    )
    .bind(id.as_str())
    .fetch_one(&f.env.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("name"), "Native xAI name");
    assert!(!row.get::<bool, _>("enabled"));
    assert_eq!(row.get::<i16, _>("weight"), 7);
    assert_eq!(row.get::<String, _>("outbound_proxy_url"), proxy.url);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id=1")
            .fetch_one(&f.env.pool)
            .await
            .unwrap(),
        before + i64::from(success)
    );
    assert_eq!(f.env.published.lock().unwrap().len(), usize::from(success));
    let audits: Vec<(String, Vec<String>)> = sqlx::query_as(
        "select actor_ref,changed_fields from admin_audit_events where actor_kind='plugin'",
    )
    .fetch_all(&f.env.pool)
    .await
    .unwrap();
    assert_eq!(
        audits,
        if success {
            vec![(
                "plugin:command-fixture@1".into(),
                vec!["credentials".into()],
            )]
        } else {
            vec![]
        }
    );
    let events = proxy.events();
    assert_eq!(
        events.iter().filter(|e| e["kind"] == "discovery").count(),
        1
    );
    let userinfo: Vec<_> = events.iter().filter(|e| e["kind"] == "userinfo").collect();
    assert_eq!(userinfo.len(), usize::from(mode != "issuer"));
    assert!(userinfo.iter().all(|e| e["authorization"] == true));
    let refresh: Vec<_> = events.iter().filter(|e| e["kind"] == "refresh").collect();
    assert_eq!(refresh.len(), usize::from(mode == "refresh"));
    assert!(refresh.iter().all(|e| e["validated"] == true));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e["kind"].as_str(), Some("forbidden" | "inference")))
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_budget_windows")
            .fetch_one(&f.env.pool)
            .await
            .unwrap(),
        0
    );
    f.close().await;
    proxy.stop().await;
}
