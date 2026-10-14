mod duration;
mod parameters;

use crate::{
    services::{Probe, factory},
    support::*,
};
use gateway_admin::{
    model::{AdminError, AdminErrorKind, MutationActor, plugins::*},
    ports::plugins::{PluginProcess, PluginRuntime, PluginService, PluginStore},
};
use gateway_core::lifecycle::CancellationToken;
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

struct Fixture {
    engine: PluginEngine,
    artifact: PluginArtifact,
    store: Arc<MemoryStore>,
    global: Arc<Probe>,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        Self::build(None).await
    }
    async fn build(models: Option<&crate::models::fixture::Fixture>) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let global = Arc::new(Probe::default());
        let port = factory(&global);
        let mut engine = PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_services(Arc::new(move || Ok(Some(port.clone()))));
        if let Some(models) = models {
            engine = engine.with_http(models.http.clone()).with_models(
                Arc::new(gateway_api::openai::middleware::PluginModelExecution::new(
                    models.execution.clone(),
                    Arc::downgrade(&models.manager),
                )),
                Arc::new(crate::models::fixture::Keys),
            );
        }
        let artifact = engine.inspect(command_archive("1.0.0")).await.unwrap();
        Self {
            engine,
            artifact,
            store,
            global,
            _cache: cache,
        }
    }
    async fn prepare(&self, configuration: Value) -> Result<Arc<dyn PluginProcess>, AdminError> {
        let mut record = PluginRecord {
            id: "command-fixture".into(),
            revision: 1,
            name: "Command fixture".into(),
            artifact_sha256: self.artifact.info.sha256.clone(),
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
        // Inspect/validate continue to expose the production gate; the test runs
        // the actual prepared process explicitly, without a production test hook.
        assert!(
            self.artifact
                .info
                .unsupported_capabilities
                .iter()
                .any(|c| c == "command_line")
        );
        assert_eq!(
            self.engine
                .validate(&record, &self.artifact, None)
                .await
                .err()
                .unwrap()
                .kind(),
            AdminErrorKind::Invalid
        );
        record.enabled = false;
        self.engine
            .validate(&record, &self.artifact, None)
            .await
            .unwrap();
        record.enabled = true;
        self.store
            .records
            .lock()
            .unwrap()
            .insert(record.id.clone(), record.clone());
        self.engine.prepare(&record, self.artifact.clone()).await
    }
    async fn count(&self) -> Option<u64> {
        self.store
            .state_get(
                &PluginStateScope {
                    instance_id: "command-fixture".into(),
                    revision: 1,
                    namespace: "counter".into(),
                    schema_version: 1,
                    maximum_records: 10,
                    maximum_bytes: 1024,
                },
                "commands",
            )
            .await
            .unwrap()
            .map(|v| v.value.as_u64().unwrap())
    }
}
fn request(arguments: &[&str]) -> PluginCommandRequest {
    PluginCommandRequest {
        name: "probe".into(),
        arguments: arguments.iter().map(|s| (*s).into()).collect(),
        actor: actor(),
        cancellation: CancellationToken::new(),
        call_scope: gateway_core::engine::nested::ExtensionCallScope::default(),
    }
}
async fn stop(process: &Arc<dyn PluginProcess>) {
    process.revoke();
    process.stop().await;
}

#[tokio::test]
async fn system_command_dispatch_resolves_actual_revision_and_help_without_executing() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    assert_eq!(
        fixture.manager.command_revision(&fixture.id).await.unwrap(),
        1
    );
    for name in [None, Some("probe".into())] {
        let result = fixture
            .manager
            .run_system_command(
                &fixture.id,
                PluginCommandAction::Help(name),
                CancellationToken::new(),
                Default::default(),
            )
            .await
            .unwrap();
        assert!(result.stdout.contains("probe"));
        assert_eq!(result.exit_code, 0);
    }
    let artifact = {
        let records = fixture.store.records.lock().unwrap();
        records[&fixture.id].artifact_sha256.clone()
    };
    let scope = PluginStateScope {
        instance_id: fixture.id.clone(),
        revision: 1,
        namespace: "counter".into(),
        schema_version: 1,
        maximum_records: 10,
        maximum_bytes: 1024,
    };
    assert!(
        fixture
            .store
            .state_get(&scope, "commands")
            .await
            .unwrap()
            .is_none()
    );
    let disabled = fixture
        .manager
        .update(Some(&fixture.id), update(&artifact, 1, false), &actor())
        .await
        .unwrap();
    assert_eq!(disabled.revision, 2);
    assert!(fixture.manager.command_revision(&fixture.id).await.is_err());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn system_command_dispatch_rejects_borrowed_graphs_before_rpc() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    let root = gateway_core::engine::nested::ExtensionCallScope::default();
    let borrowed = root.fork_http("command-parent".into()).unwrap();
    let error = fixture
        .manager
        .run_system_command(
            &fixture.id,
            PluginCommandAction::Execute {
                name: "probe".into(),
                arguments: vec![],
            },
            CancellationToken::new(),
            borrowed,
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(error, PluginCommandError::Invalid(_)));
    root.wait_until_idle().await;
    assert!(fixture.core.requests.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn host_command_graph_drain_waits_for_original_fees_after_command_cancellation() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.settle_gate.lock().unwrap() = Some(receiver);
    let root = gateway_core::engine::nested::ExtensionCallScope::default();
    let cancellation = CancellationToken::new();
    let operation = PluginCommandAction::Execute {
        name: "probe".into(),
        arguments: vec![
            "--input".into(),
            json!({"mode":"unary","key":"key_system"}).to_string(),
        ],
    };
    let mut call = Box::pin(fixture.manager.run_system_command(
        &fixture.id,
        operation,
        cancellation.clone(),
        root.clone(),
    ));
    tokio::select! { _=&mut call=>panic!("fee settlement must wait"), _=async { while fixture.core.finalizations.lock().unwrap().is_empty() { tokio::task::yield_now().await; } }=>{} }
    cancellation.cancel();
    assert!(matches!(
        call.await.err().unwrap(),
        PluginCommandError::Incomplete { .. }
    ));
    let mut drain = Box::pin(root.wait_until_idle());
    assert!(futures::poll!(drain.as_mut()).is_pending());
    assert!(fixture.core.charges.lock().unwrap().is_empty());
    sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .unwrap();
    fixture.clean(1).await;
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        123
    );
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 1);
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}
fn descriptor(parameters: Value) -> Value {
    json!({"commands":[{"name":"probe","description":"Inspect command execution","parameters":parameters}]})
}
fn parameter(name: &str, kind: &str) -> Value {
    json!({"name":name,"description":"Parameter","value_type":kind})
}

#[tokio::test]
async fn registration_is_strict_and_keeps_commands_in_the_actual_process() {
    let fixture = Fixture::new().await;
    let valid = fixture.prepare(json!({})).await.unwrap();
    let help = valid.command_help(None).unwrap().unwrap();
    assert!(help.contains("codex-proxy-rs plugin command-fixture <命令>"));
    assert!(help.contains("probe"));
    assert_eq!(
        valid.command_help(Some("missing")).err().unwrap().kind(),
        AdminErrorKind::NotFound
    );
    let error = valid.command(request(&[]), None).await.err().unwrap();
    assert!(matches!(error, PluginCommandError::Invalid(_)));
    assert_eq!(fixture.count().await, None);
    stop(&valid).await;
    let mut invalid = vec![
        json!({"commands":[]}),
        json!({"commands":[{"name":"serve","description":"Reserved"}]}),
        json!({"commands":[{"name":"probe","description":"Valid"},{"name":"probe","description":"Duplicate"}]}),
        json!({"commands":[{"name":"probe","description":"Valid","unknown":true}]}),
        json!({"commands":[{"name":"Bad","description":"Invalid name"}]}),
        json!({"commands":[{"name":"probe","description":"Bad\ntext"}]}),
    ];
    invalid.push(json!({"commands":(0..33).map(|i|json!({"name":format!("probe-{i}"),"description":"Too many"})).collect::<Vec<_>>()}));
    for registration in invalid {
        let error = fixture
            .prepare(json!({"command_registration":registration}))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), AdminErrorKind::Invalid);
    }
    for mode in ["command-register-envelope", "command-register-size"] {
        assert_eq!(
            fixture
                .prepare(json!({"adapter_mode":mode}))
                .await
                .err()
                .unwrap()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
}

#[tokio::test]
async fn command_callbacks_use_one_frozen_service_scope_and_cleanup_between_calls() {
    let fixture = Fixture::new().await;
    let process = fixture.prepare(json!({})).await.unwrap();
    process.activate();
    let selected = Arc::new(Probe::default());
    for _ in 0..40 {
        let result = process
            .command(
                request(&["--input", r#"{"mode":"service"}"#]),
                Some(factory(&selected)),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&result.stdout).unwrap()["exists"],
            true
        );
        assert_eq!(result.saved_accounts, 0);
    }
    {
        let facts = selected.facts.lock().unwrap();
        assert_eq!(facts.len(), 40);
        assert!(facts.iter().all(|f| f.instance == "command-fixture"
            && f.revision == 1
            && f.context.request_id == "plugin-test"
            && f.context.operation == "command_line.execute"
            && f.context.parent_call_id.is_none()));
        assert!(
            facts
                .windows(2)
                .all(|f| f[0].context.call_id != f[1].context.call_id)
        );
    }
    assert!(fixture.global.facts.lock().unwrap().is_empty());
    let absent = process
        .command(request(&["--input", r#"{"mode":"service"}"#]), None)
        .await
        .unwrap();
    assert!(
        serde_json::from_str::<Value>(&absent.stdout)
            .unwrap()
            .get("error")
            .is_some()
    );
    assert!(
        fixture.global.facts.lock().unwrap().is_empty(),
        "explicit absence must not query the engine's later source"
    );
    assert_eq!(fixture.count().await, Some(41));
    stop(&process).await;
}

#[tokio::test]
async fn cancellation_and_revocation_close_active_callbacks_without_repeating_effects() {
    for revoke in [false, true] {
        let fixture = Fixture::new().await;
        let process = fixture.prepare(json!({})).await.unwrap();
        process.activate();
        let probe = Arc::new(Probe::default());
        probe.wait.store(true, Ordering::SeqCst);
        let call = request(&["--input", r#"{"mode":"service"}"#]);
        let cancel = call.cancellation.clone();
        let pending = process.command(call, Some(factory(&probe)));
        tokio::pin!(pending);
        tokio::select! {
            _ = probe.entered.notified() => {},
            result = &mut pending => panic!("command completed before waiting: {}",result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("service callback did not start"),
        }
        if revoke {
            process.revoke();
        } else {
            cancel.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .unwrap()
            .err()
            .unwrap();
        assert!(matches!(
            error,
            PluginCommandError::Incomplete {
                saved_accounts: 0,
                ..
            }
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !probe.dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fixture.count().await, Some(1));
        assert_eq!(probe.facts.lock().unwrap().len(), 1);
        if !revoke {
            probe.wait.store(false, Ordering::SeqCst);
            assert_eq!(
                process.command(request(&[]), None).await.unwrap().exit_code,
                0
            );
            assert_eq!(fixture.count().await, Some(2));
        }
        stop(&process).await;
    }
}

#[tokio::test]
async fn malformed_or_failed_results_are_uncertain_after_exactly_one_execution() {
    for mode in [
        "command-envelope",
        "command-invalid",
        "command-size",
        "command-unknown",
        "command-fault",
        "command-crash",
    ] {
        let fixture = Fixture::new().await;
        let process = fixture.prepare(json!({"adapter_mode":mode})).await.unwrap();
        process.activate();
        let error = process.command(request(&[]), None).await.err().unwrap();
        assert!(
            matches!(
                error,
                PluginCommandError::Incomplete {
                    saved_accounts: 0,
                    ..
                }
            ),
            "{mode}: {error}"
        );
        assert!(!error.to_string().contains("private command failure"));
        assert_eq!(fixture.count().await, Some(1), "{mode}");
        stop(&process).await;
    }
}

#[tokio::test]
async fn native_manager_rejects_old_revisions_and_failed_candidates_keep_the_original_command() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    assert!(
        fixture
            .manager
            .command_help(&fixture.id, 1, None)
            .await
            .unwrap()
            .contains("probe")
    );
    assert_eq!(
        fixture
            .manager
            .command_help(&fixture.id, 0, None)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Conflict
    );
    let artifact = fixture.store.records.lock().unwrap()[&fixture.id]
        .artifact_sha256
        .clone();
    let mut candidate = update(&artifact, 1, true);
    candidate.configuration = json!({"command_registration":{"commands":[]}});
    assert!(
        fixture
            .manager
            .update(Some(&fixture.id), candidate, &actor())
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .manager
            .execute_command(&fixture.id, 1, request(&[]))
            .await
            .unwrap()
            .exit_code,
        0
    );
    let disabled = fixture
        .manager
        .update(Some(&fixture.id), update(&artifact, 1, false), &actor())
        .await
        .unwrap();
    assert_eq!(disabled.revision, 2);
    assert!(matches!(
        fixture
            .manager
            .execute_command(&fixture.id, 1, request(&[]))
            .await
            .err()
            .unwrap(),
        PluginCommandError::Invalid(_)
    ));
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn command_model_calls_reuse_native_personal_keys_costs_and_stream_cleanup() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    for (index, mode) in ["unary", "stream", "leak"].into_iter().enumerate() {
        let input = json!({"mode":mode,"key":"key_alice"}).to_string();
        let mut call = request(&["--input", &input]);
        call.actor.actor = MutationActor::AdminSession {
            admin_user_id: "alice".into(),
        };
        let result = fixture
            .manager
            .execute_command(&fixture.id, 1, call)
            .await
            .unwrap();
        let output: Value = serde_json::from_str(&result.stdout).unwrap();
        assert!(output.get("error").is_none(), "{output}");
        fixture.clean(index + 1).await;
    }
    // The unread third stream is cancelled before Provider execute is polled.
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.core.requests.lock().unwrap().len(), 3);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 3);
    assert_eq!(fixture.core.charges.lock().unwrap().len(), 3);
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    for identity in [MutationActor::AdminSession {
        admin_user_id: "bob".into(),
    }] {
        let mut call = request(&["--input", r#"{"mode":"unary","key":"key_alice"}"#]);
        call.actor.actor = identity;
        let output = fixture
            .manager
            .execute_command(&fixture.id, 1, call)
            .await
            .unwrap();
        assert!(
            serde_json::from_str::<Value>(&output.stdout)
                .unwrap()
                .get("error")
                .is_some()
        );
    }
    assert_eq!(
        fixture.core.starts.load(Ordering::SeqCst),
        2,
        "a personal command cannot select another user's Key"
    );
    fixture.manager.shutdown().await;
}

async fn system_command(fixture: &crate::models::fixture::Fixture, input: Value) -> Value {
    let input = input.to_string();
    let mut call = request(&["--input", &input]);
    call.actor.actor = MutationActor::System;
    let result = fixture
        .manager
        .execute_command(&fixture.id, 1, call)
        .await
        .unwrap();
    serde_json::from_str(&result.stdout).unwrap()
}

#[tokio::test]
async fn system_commands_bind_native_key_ids_and_keep_original_costs_and_cleanup() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    for (index, mode) in ["unary", "stream", "leak"].into_iter().enumerate() {
        let output = system_command(&fixture, json!({"mode":mode,"key":"key_system"})).await;
        assert!(output.get("error").is_none(), "{output}");
        fixture.clean(index + 1).await;
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 3);
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    for row in fixture.core.requests.lock().unwrap().iter() {
        assert_eq!(row.client_api_key_ref.as_str(), "key_system");
        assert!(row.parent_request_id.is_none());
        assert!(row.initiating_plugin_instance_id.is_none());
    }
    for key in ["missing", "key_disabled"] {
        let output = system_command(&fixture, json!({"mode":"unary","key":key})).await;
        assert_eq!(output["error"]["code"], "permission_denied");
    }
    let output = system_command(&fixture, json!({"mode":"unary"})).await;
    assert_eq!(output["error"]["code"], "invalid_input");
    let input = json!({"mode":"unary","key":"key_system"}).to_string();
    let mut personal = request(&["--input", &input]);
    personal.actor.actor = MutationActor::AdminSession {
        admin_user_id: "alice".into(),
    };
    let output = fixture
        .manager
        .execute_command(&fixture.id, 1, personal)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&output.stdout).unwrap()["error"]["code"],
        "permission_denied"
    );
    assert_eq!(fixture.core.requests.lock().unwrap().len(), 3);
    assert_eq!(fixture.key_data.reads.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn system_command_key_listing_is_paged_non_secret_and_validated_before_store_reads() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    let mut cursor = Value::Null;
    for (id, enabled) in [
        ("key_alice", true),
        ("key_disabled", false),
        ("key_system", true),
    ] {
        let output = system_command(
            &fixture,
            json!({"mode":"raw","method":"host.keys.list","params":{"cursor":cursor,"limit":1}}),
        )
        .await;
        assert_eq!(
            output["result"]["keys"],
            json!([{"id":id,"name":id,"enabled":enabled}])
        );
        cursor = output["result"]["next_cursor"].clone();
        if id != "key_system" {
            assert!(!cursor.is_null());
        }
    }
    assert!(cursor.is_null());
    assert_eq!(fixture.key_data.reads.load(Ordering::SeqCst), 3);
    for limit in [101, 200] {
        let output = system_command(
            &fixture,
            json!({"mode":"raw","method":"host.keys.list","params":{"limit":limit}}),
        )
        .await;
        assert_eq!(output["result"]["keys"].as_array().unwrap().len(), 3);
        assert!(output["result"]["next_cursor"].is_null());
    }
    for params in [
        json!({"limit":0}),
        json!({"limit":201}),
        json!({"limit":1,"cursor":"private-token"}),
        json!({"limit":1,"cursor":""}),
        json!({"limit":1,"cursor":"ff"}),
        json!({"limit":1,"cursor":"00".repeat(2049)}),
    ] {
        let output = system_command(
            &fixture,
            json!({"mode":"raw","method":"host.keys.list","params":params}),
        )
        .await;
        assert_eq!(output["error"]["code"], "invalid_input");
        assert!(!output.to_string().contains("private-token"));
    }
    assert_eq!(fixture.key_data.reads.load(Ordering::SeqCst), 5);
    for actor in [
        MutationActor::AdminApiKey,
        MutationActor::Plugin {
            instance_id: fixture.id.clone(),
            revision: std::num::NonZeroU64::new(1).unwrap(),
        },
    ] {
        let input =
            json!({"mode":"raw","method":"host.keys.list","params":{"limit":1}}).to_string();
        let mut call = request(&["--input", &input]);
        call.actor.actor = actor;
        let output = fixture
            .manager
            .execute_command(&fixture.id, 1, call)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&output.stdout).unwrap()["error"]["code"],
            "permission_denied"
        );
    }
    assert_eq!(fixture.key_data.reads.load(Ordering::SeqCst), 5);
    assert!(fixture.core.requests.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn system_command_key_authorization_is_revoked_during_catalog_lookup() {
    let fixture = crate::models::fixture::Fixture::with_command().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.catalog_gate.lock().unwrap() = Some(receiver);
    let call = system_command(
        &fixture,
        json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_system","protocol":"openai","client_version":"fixture"}}),
    );
    tokio::pin!(call);
    tokio::select! { _=&mut call => panic!("catalog should wait"), _=async { while fixture.core.catalogs.load(Ordering::SeqCst)==0 { tokio::task::yield_now().await; } }=>{} }
    fixture
        .snapshot
        .publish(crate::models::fixture::snapshot(2, false));
    sender.send(()).unwrap();
    assert_eq!(call.await["error"]["code"], "permission_denied");
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}

struct GraphFactory;
impl gateway_core::engine::middleware::service::Factory for GraphFactory {
    fn bind(
        &self,
        context: &gateway_core::engine::middleware::service::Context,
        _: gateway_core::engine::middleware::service::Caller,
    ) -> Result<
        Arc<dyn gateway_core::engine::middleware::service::Dispatcher>,
        gateway_core::engine::middleware::service::Error,
    > {
        Ok(Arc::new(crate::models::http::GraphServices(
            context.call_scope.clone(),
        )))
    }
}

#[tokio::test]
async fn command_model_http_and_service_callbacks_share_the_original_graph_budget() {
    let native = crate::models::fixture::Fixture::new().await;
    let (_http, calls) = crate::models::http::attach(&native, None, false);
    let fixture = Fixture::build(Some(&native)).await;
    let process = fixture.prepare(json!({})).await.unwrap();
    process.activate();
    let mut call = request(&["--input", r#"{"mode":"service-budget"}"#]);
    call.actor.actor = MutationActor::AdminSession {
        admin_user_id: "alice".into(),
    };
    let result = process
        .command(call, Some(Arc::new(GraphFactory)))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result.stdout).unwrap(),
        json!({"completed":62,"kind":"capacity"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    native.clean(1).await;
    assert_eq!(native.core.starts.load(Ordering::SeqCst), 1);
    stop(&process).await;
    native.manager.shutdown().await;
}

#[tokio::test]
async fn dropped_command_future_closes_its_callback_and_the_next_command_is_independent() {
    let fixture = Fixture::new().await;
    let process = fixture.prepare(json!({})).await.unwrap();
    process.activate();
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    {
        let pending = process.command(
            request(&["--input", r#"{"mode":"service"}"#]),
            Some(factory(&probe)),
        );
        tokio::pin!(pending);
        tokio::select! {
            _ = probe.entered.notified() => {},
            result = &mut pending => panic!("unexpected completion: {}",result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("service callback did not start"),
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.count().await, Some(1));
    assert_eq!(
        process.command(request(&[]), None).await.unwrap().exit_code,
        0
    );
    assert_eq!(fixture.count().await, Some(2));
    stop(&process).await;
}

#[tokio::test]
async fn command_rpc_timeout_closes_the_native_service_call_without_retry() {
    let fixture = Fixture::new().await;
    let process = fixture.prepare(json!({})).await.unwrap();
    process.activate();
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let pending = process.command(
        request(&["--input", r#"{"mode":"service"}"#]),
        Some(factory(&probe)),
    );
    tokio::pin!(pending);
    tokio::select! {
        _ = probe.entered.notified() => {},
        result = &mut pending => panic!("unexpected completion: {}",result.is_ok()),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("service callback did not start"),
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(121)).await;
    let error = tokio::time::timeout(Duration::from_secs(5), &mut pending)
        .await
        .unwrap()
        .err()
        .unwrap();
    tokio::time::resume();
    assert!(matches!(
        error,
        PluginCommandError::Incomplete {
            saved_accounts: 0,
            ..
        }
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.count().await, Some(1));
    stop(&process).await;
}

#[tokio::test]
async fn credential_results_remain_explicitly_incomplete_until_native_account_commit_is_connected()
{
    let account = json!({"action":"create","provider_id":"openai","facts":{"name":"Fixture","authentication_kind":"oauth","material":{"access_token":"fake-command-fixture-token"}}});
    for (exit, count) in [(0, 1), (7, 1), (0, 33)] {
        let fixture = Fixture::new().await;
        let process=fixture.prepare(json!({"command_result":{"stdout":"fixture-output","stderr":"fixture-error","exit_code":exit,"accounts":vec![account.clone();count]}})).await.unwrap();
        process.activate();
        let result = process.command(request(&[]), None).await;
        if exit == 0 {
            let error = result.err().unwrap();
            assert!(matches!(
                error,
                PluginCommandError::Incomplete {
                    saved_accounts: 0,
                    ..
                }
            ));
            assert!(!error.to_string().contains("fake-command-fixture-token"));
        } else {
            let output = result.unwrap();
            assert_eq!(output.exit_code, 7);
            assert_eq!(output.saved_accounts, 0);
            assert_eq!(output.stdout, "fixture-output");
            assert_eq!(output.stderr, "fixture-error");
        }
        assert_eq!(fixture.count().await, Some(1));
        stop(&process).await;
    }
}

#[tokio::test]
async fn command_external_http_uses_the_native_outbound_port_and_releases_streams() {
    let server = crate::outbound::Server::new().await;
    let fixture = Fixture::new().await;
    let process = fixture
        .prepare(json!({"adapter_url":format!("{}/binary",server.url)}))
        .await
        .unwrap();
    process.activate();
    let result = process
        .command(request(&["--input", r#"{"mode":"external"}"#]), None)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result.stdout).unwrap()["bytes"],
        json!([0, 255, 1, 0])
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert_eq!(fixture.count().await, Some(1));
    stop(&process).await;
}
