use super::*;
use crate::services::upstream_sdk::environment::Environment;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::ports::plugins::PluginCredentialService;
use gateway_core::engine::nested::ExtensionCallScope;
use sqlx::Row as _;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::Notify;

struct Native {
    env: Environment,
    engine: PluginEngine,
    record: PluginRecord,
    artifact: PluginArtifact,
    process: Arc<dyn PluginProcess>,
}
impl Native {
    async fn new(
        accounts: Vec<Value>,
        port: Option<Arc<dyn PluginCredentialService>>,
    ) -> Option<Self> {
        let env = Environment::new().await?;
        let store = env.store.plugin_store();
        let engine = PluginEngine::new(
            env.cache.path().join("command-accounts"),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_credentials(port.unwrap_or_else(|| env.admin.services().plugin_credentials()));
        let artifact = engine.inspect(command_archive("1.0.0")).await.unwrap();
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
            configuration: configuration(accounts),
            secrets: Default::default(),
            frontend_authentication: None,
            middleware: None,
            model_router: None,
            account_scheduler: None,
            retry_policy: None,
            upstream_adapter: None,
            observer: None,
        };
        store.save(&record, 0, &actor()).await.unwrap();
        // The kernel fixture uses actual SDK processes and native stores while
        // the production command gate remains explicit in inspect/validate.
        let process = engine.prepare(&record, artifact.clone()).await.unwrap();
        process.activate();
        Some(Self {
            env,
            engine,
            record,
            artifact,
            process,
        })
    }
    async fn replace_result(&mut self, accounts: Vec<Value>) {
        stop(&self.process).await;
        let expected = self.record.revision;
        self.record.revision += 1;
        self.record.configuration = configuration(accounts);
        self.env
            .store
            .plugin_store()
            .save(&self.record, expected, &actor())
            .await
            .unwrap();
        self.process = self
            .engine
            .prepare(&self.record, self.artifact.clone())
            .await
            .unwrap();
        self.process.activate();
    }
    async fn count(&self) -> i64 {
        sqlx::query_scalar("select count(*) from provider_accounts")
            .fetch_one(&self.env.pool)
            .await
            .unwrap()
    }
    async fn close(self) {
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
fn create(identity: &str) -> Value {
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
    async fn save(
        &self,
        scope: PluginMutationScope,
        command: PluginCredentialSave,
        cleanup: ExtensionCallScope,
        cancellation: CancellationToken,
    ) -> Result<PluginCredentialSaved, AdminError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        if index == 1 && self.hold_before_start.load(Ordering::SeqCst) {
            self.second.notify_one();
            self.proceed.notified().await;
        }
        let mut saving = Box::pin(self.inner.save(scope, command, cleanup, cancellation));
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
            .save(&disabled, fixture.record.revision, &actor())
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
