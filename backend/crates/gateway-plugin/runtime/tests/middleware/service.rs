use super::*;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_core::{engine::middleware::service as core, middleware::compose};

struct Fixture {
    store: Arc<crate::support::MemoryStore>,
    manager: PluginManager,
    digest: String,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let store = Arc::new(crate::support::MemoryStore::default());
        let cache = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(
            store.clone(),
            Arc::new(gateway_plugin_runtime::PluginEngine::new(
                cache.path().into(),
                "3.11.2".parse().unwrap(),
                store.clone(),
            )),
        );
        let digest = manager
            .upload(
                crate::support::http_archive("1.0.0"),
                &crate::support::actor(),
            )
            .await
            .unwrap()
            .sha256;
        let f = Self {
            store,
            manager,
            digest,
            _cache: cache,
        };
        f.install(0).await;
        f
    }
    async fn install(&self, order: i32) -> String {
        let mut command = crate::support::update(&self.digest, 0, true);
        command.middleware = Some(PluginMiddlewareBinding {
            order,
            mounts: vec![PluginMiddlewareMount::Service],
            scope: PluginExecutionScope::All {},
        });
        self.manager
            .update(None, command, &crate::support::actor())
            .await
            .unwrap()
            .id
    }
    async fn change(&self, id: &str, enabled: bool) {
        let record = self
            .manager
            .list()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
        let mut command = crate::support::update(&self.digest, record.revision, enabled);
        command.middleware = record.middleware;
        self.manager
            .update(Some(id), command, &crate::support::actor())
            .await
            .unwrap();
    }
    fn plan(
        &self,
        context: &core::Context,
    ) -> gateway_admin::plugins::service::PluginServiceMiddlewarePlan {
        self.manager
            .middleware_registry()
            .unwrap()
            .freeze_service(context)
            .unwrap()
    }
}
fn ctx(mode: &str) -> core::Context {
    core::Context {
        dispatcher: None,
        operation: "settings.admin_api_key_exists",
        request_id: mode.into(),
        call_id: "service-fixture-call".into(),
        parent_call_id: Some("service-fixture-parent".into()),
        cancellation: Default::default(),
        deadline_at: Default::default(),
        call_scope: Default::default(),
    }
}
fn terminal(calls: Arc<AtomicUsize>) -> core::Next {
    compose(vec![], move |value| {
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(value)
        })
    })
}

#[derive(Clone, Default)]
struct HostServices {
    callers: Arc<std::sync::Mutex<Vec<(String, u64)>>>,
    contexts: Arc<std::sync::Mutex<Vec<core::Context>>>,
    replies: Arc<std::sync::Mutex<std::collections::BTreeMap<String, serde_json::Value>>>,
    calls: Arc<std::sync::Mutex<Vec<(String, String, serde_json::Value)>>>,
    wait: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
struct WaitingCall(Arc<std::sync::atomic::AtomicBool>);
impl Drop for WaitingCall {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl core::Dispatcher for HostServices {
    fn for_caller(
        &self,
        context: &core::Context,
        caller: core::Caller,
    ) -> Result<Arc<dyn core::Dispatcher>, core::Error> {
        self.callers
            .lock()
            .unwrap()
            .push((caller.instance_id().into(), caller.revision().get()));
        Ok(self.scoped(context))
    }
    fn scoped(&self, context: &core::Context) -> Arc<dyn core::Dispatcher> {
        let mut observed = context.clone();
        observed.dispatcher = None;
        self.contexts.lock().unwrap().push(observed);
        Arc::new(self.clone())
    }
    fn call(
        &self,
        instance: String,
        operation: String,
        input: serde_json::Value,
    ) -> futures::future::BoxFuture<'static, Result<serde_json::Value, core::Error>> {
        let host = self.clone();
        Box::pin(async move {
            host.calls
                .lock()
                .unwrap()
                .push((instance, operation.clone(), input));
            if host.wait.load(Ordering::SeqCst) {
                let _waiting = WaitingCall(host.dropped.clone());
                host.entered.notify_one();
                return futures::future::pending().await;
            }
            Ok(host
                .replies
                .lock()
                .unwrap()
                .get(&operation)
                .cloned()
                .unwrap_or(json!(true)))
        })
    }
}
fn invocation(host: &HostServices) -> PluginInvocationContext {
    PluginInvocationContext {
        context: gateway_core::engine::middleware::http::Context {
            request_id: "http-source".into(),
            call_id: "host-http-parent".into(),
            parent_call_id: None,
            call_scope: Default::default(),
            model_parent: None,
            cancellation: Default::default(),
            deadline_at: Default::default(),
        },
        http: None,
        services: Some(Arc::new(host.clone())),
    }
}
#[tokio::test]
async fn sdk_host_service_management_calls_keep_host_context_and_release_each_rpc_scope() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let host = HostServices::default();
    for _ in 0..35 {
        let mut request = crate::support::request("services");
        request.invocation = Some(invocation(&host));
        let response = f.manager.handle(&id, 1, request).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
            json!({"exists":true})
        );
    }
    let contexts = host.contexts.lock().unwrap().clone();
    assert_eq!(contexts.len(), 35);
    for context in contexts {
        assert_eq!(context.request_id, "plugin-test");
        assert_eq!(context.parent_call_id.as_deref(), Some("host-http-parent"));
        assert!(context.cancellation.is_cancelled());
    }
    let calls = host.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 35);
    for (instance, operation, input) in calls {
        assert_eq!(instance, id);
        assert_eq!(operation, "settings.admin_api_key_exists");
        assert_eq!(input, json!(null));
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_host_service_requires_host_authority_and_validates_binary_and_unknown_fields() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let host = HostServices::default();
    for (bound, binary, extra) in [
        (false, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let mut request = crate::support::request("services");
        if bound {
            request.invocation = Some(invocation(&host));
        }
        let mut call = json!({"operation":"settings.admin_api_key_exists","input":null});
        if extra {
            call["actor"] = json!("system");
        }
        request.body = serde_json::to_vec(
            &json!({"operation":"services","mode":"raw","request":call,"binary":binary}),
        )
        .unwrap();
        let response = f.manager.handle(&id, 1, request).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            value["fault"]["code"],
            if bound {
                "invalid_input"
            } else {
                "permission_denied"
            }
        );
    }
    assert!(host.calls.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_host_service_in_service_stage_uses_its_bound_dispatcher_without_consuming_next() {
    let f = Fixture::new().await;
    let host = HostServices::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut context = ctx("host-service");
    context.dispatcher = Some(Arc::new(host.clone()));
    assert_eq!(
        f.plan(&context)
            .run(context, json!(null), terminal(calls.clone()))
            .await
            .unwrap(),
        json!(true)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.calls.lock().unwrap().len(), 1);
    let instance = f.manager.list().await.unwrap()[0].id.clone();
    assert_eq!(*host.callers.lock().unwrap(), vec![(instance, 1)]);
    assert_eq!(host.contexts.lock().unwrap().len(), 1);
    assert!(host.contexts.lock().unwrap()[0].cancellation.is_cancelled());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_host_service_wait_is_closed_when_management_caller_is_dropped() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let host = HostServices::default();
    host.wait.store(true, Ordering::SeqCst);
    let mut request = crate::support::request("services");
    request.invocation = Some(invocation(&host));
    let mut call = Box::pin(f.manager.handle(&id, 1, request));
    tokio::time::timeout(Duration::from_secs(3),async {tokio::select! {result=&mut call=>panic!("service must wait: {}",result.is_ok()),()=host.entered.notified()=>{}}}).await.unwrap();
    drop(call);
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(host.contexts.lock().unwrap()[0].cancellation.is_cancelled());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn service_rpc_fault_keeps_original_rejection_details_and_ambiguous_send_fact() {
    let f = Fixture::new().await;
    let context = ctx("reject");
    let calls = Arc::new(AtomicUsize::new(0));
    let error = f
        .plan(&context)
        .run(context, json!(null), terminal(calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.kind, "unavailable");
    assert_eq!(error.message, "fixture service rejection");
    let fault: PluginFault = serde_json::from_value(error.details.unwrap()).unwrap();
    assert_eq!(fault.code, ErrorCode::Rejected);
    assert_eq!(fault.send_state, gateway_plugin_sdk::SendState::Ambiguous);
    assert_eq!(fault.details, Some(json!({"fixture":"original"})));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let context = ctx("typed");
    assert!(
        f.plan(&context)
            .call(context, (), |()| Box::pin(async { Ok(true) }))
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_parent_cancellation_and_deadline_stop_native_terminal_waits() {
    let f = Fixture::new().await;
    for expired in [false, true] {
        let mut context = ctx("forward");
        let calls = Arc::new(AtomicUsize::new(0));
        if expired {
            context.deadline_at = gateway_core::lifecycle::Deadline::from_timeout(
                SystemTime::now(),
                Some(Duration::ZERO),
            )
            .unwrap();
        } else {
            context.cancellation.cancel();
        }
        assert!(
            f.plan(&context)
                .run(context, json!(null), terminal(calls.clone()))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let context = ctx("forward");
    let cancellation = context.cancellation.clone();
    let plan = f.plan(&context);
    let (sent, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        plan.run(
            context,
            json!(null),
            compose(vec![], move |_| {
                Box::pin(async move {
                    sent.send(()).unwrap();
                    futures::future::pending().await
                })
            }),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), received)
        .await
        .unwrap()
        .unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    f.manager.shutdown().await;
}
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Trace {
    value: u64,
    steps: Vec<String>,
}

#[derive(serde::Deserialize)]
struct NativeOnly;
impl serde::Serialize for NativeOnly {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("native call must not cross JSON"))
    }
}
#[tokio::test]
async fn empty_service_plan_keeps_native_types_and_cancellation_without_a_json_round_trip() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let mut context = ctx("typed");
    context.call_scope = context.call_scope.fork_http(id).unwrap();
    let plan = f.plan(&context);
    assert!(plan.is_empty());
    let output = plan
        .call(context.clone(), NativeOnly, |value| {
            Box::pin(async move { Ok(value) })
        })
        .await;
    assert!(output.is_ok());
    context.cancellation.cancel();
    assert!(
        plan.call::<NativeOnly, NativeOnly>(context, NativeOnly, |_| panic!(
            "cancelled native terminal must not run"
        ))
        .await
        .is_err()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_service_typed_result_and_business_errors_preserve_their_contract_and_release_scopes() {
    let f = Fixture::new().await;
    // Exceed the simultaneous callback registry capacity across sequential calls.
    for _ in 0..35 {
        let context = ctx("typed");
        assert!(
            f.plan(&context)
                .call(context, (), |()| Box::pin(async { Ok(true) }))
                .await
                .unwrap()
        );
    }
    let failure = core::Error {
        kind: "conflict".into(),
        message: "revision changed".into(),
        details: Some(json!({"revision":2})),
    };
    let error = failure.clone();
    let context = ctx("forward");
    let result = f
        .plan(&context)
        .run(
            context,
            json!(null),
            compose(vec![], move |_| Box::pin(async move { Err(error) })),
        )
        .await
        .unwrap_err();
    assert_eq!(result, failure);
    let context = ctx("typed");
    assert!(
        !f.plan(&context)
            .call(context, (), |()| Box::pin(async { Ok(false) }))
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_layers_follow_publication_order_and_preserve_single_typed_terminal() {
    let f = Fixture::new().await;
    let last = f.manager.list().await.unwrap()[0].id.clone();
    let first = f.install(-1).await;
    let context = ctx("rewrite");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let value = f
        .plan(&context)
        .call(
            context,
            Trace {
                value: 0,
                steps: vec![],
            },
            move |input: Trace| {
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(input.value, 2);
                    Ok(input)
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(
        value,
        Trace {
            value: 202,
            steps: vec![
                format!("{first}:in"),
                format!("{last}:in"),
                format!("{last}:out"),
                format!("{first}:out")
            ]
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_short_circuit_input_and_output_validation_do_not_run_an_invalid_terminal() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    for mode in ["short", "bad-output"] {
        let context = ctx(mode);
        let count = calls.clone();
        let result: Result<bool, _> = f
            .plan(&context)
            .call(context, (), move |()| {
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                })
            })
            .await;
        if mode == "short" {
            assert!(!result.unwrap());
        } else {
            assert_eq!(result.unwrap_err().kind, "invalid");
        }
    }
    let context = ctx("bad-input");
    let count = calls.clone();
    let result = f
        .plan(&context)
        .call(
            context,
            Trace {
                value: 0,
                steps: vec![],
            },
            move |input: Trace| {
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(input)
                })
            },
        )
        .await;
    assert_eq!(result.unwrap_err().kind, "invalid");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_next_is_single_use_binary_input_does_not_consume_it_and_no_principal_is_forged() {
    let f = Fixture::new().await;
    for mode in ["double", "no-authority"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let context = ctx(mode);
        let output = f
            .plan(&context)
            .run(context, json!({"raw":"input"}), terminal(calls.clone()))
            .await
            .unwrap();
        assert_eq!(output, json!({"raw":"input"}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn malformed_service_reply_rejects_only_the_call() {
    let f = Fixture::new().await;
    for mode in ["bad-reply", "binary-reply"] {
        let context = ctx(mode);
        assert!(
            f.plan(&context)
                .run(context, json!(null), terminal(Arc::default()))
                .await
                .is_err()
        );
        assert!(
            f.manager
                .middleware_registry()
                .unwrap()
                .freeze_service(&ctx("typed"))
                .is_ok()
        );
        let context = ctx("typed");
        assert!(
            f.plan(&context)
                .call(context, (), |()| Box::pin(async { Ok(true) }))
                .await
                .unwrap()
        );
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_disable_revokes_entered_rpc_and_replacement_cannot_rebind_a_frozen_plan() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let context = ctx("wait");
    let plan = f.plan(&context);
    let old = plan.clone();
    let old_context = context.clone();
    let (sent, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        plan.run(
            context,
            json!(null),
            compose(vec![], move |value| {
                Box::pin(async move {
                    sent.send(()).unwrap();
                    Ok(value)
                })
            }),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), received)
        .await
        .unwrap()
        .unwrap();
    f.change(&id, false).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    f.change(&id, true).await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        old.run(old_context, json!(null), terminal(calls.clone()))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut excluded = ctx("typed");
    excluded.call_scope = excluded.call_scope.fork_http(id).unwrap();
    assert!(f.plan(&excluded).is_empty());
    assert!(
        f.plan(&excluded)
            .call(excluded, (), |()| Box::pin(async { Ok(true) }))
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_local_services_preserve_subscription_and_pricing_extension_fields() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let host = HostServices::default();
    let command = json!({"subscription_auto_reset_enabled":false,"model_mappings":{"alias":"upstream"},"refresh_margin_seconds":1800,"refresh_concurrency":4,"max_concurrent_per_account":5,"request_interval_ms":25,"rotation_strategy":"round_robin","min_codex_desktop_version":null,"min_codex_cli_version":null,"usage_retention_days":32,"ops_event_retention_days":31,"audit_retention_days":91});
    let mut settings = command.clone();
    settings["config_revision"] = json!(7);
    settings["updated_at"] = json!("2026-10-12T00:00:00Z");
    let actor = json!({"actor":"system","request_id":"sdk-input"});
    let policy = json!({"enabled":false,"updateExisting":true});
    let preview = json!({"prices":{},"skipped":["unknown-provider"],"sourceHash":"a".repeat(64)});
    let catalog = json!({"defaults":{},"overrides":{},"synced":{},"synced_at":null,"sync_policy":policy,"source_hash":"a".repeat(64)});
    let cases = [
        ("load", json!(null), settings.clone()),
        ("replace", json!([actor, command]), settings),
        ("pricing", json!(null), catalog),
        ("preview_pricing_sync", json!(null), preview.clone()),
        ("sync_pricing", json!([actor, preview]), json!(null)),
        (
            "update_pricing",
            json!([actor,{"provider":"openai","models":["gpt-5.4"],"change":{"Multiplier":[15000,{"components":["input","cacheRead"]}]}}]),
            json!(null),
        ),
        (
            "update_pricing_sync_policy",
            json!([actor, policy]),
            json!(null),
        ),
    ];
    for (operation, input, output) in cases {
        let name = format!("local.settings.v1.{operation}");
        host.replies
            .lock()
            .unwrap()
            .insert(name.clone(), output.clone());
        let mut request = crate::support::request("services");
        request.invocation = Some(invocation(&host));
        request.body=serde_json::to_vec(&json!({"operation":"services","mode":"local","local_operation":operation,"input":input})).unwrap();
        let response = f.manager.handle(&id, 1, request).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(value, json!({"result":output}), "{operation}");
        assert_eq!(
            host.calls.lock().unwrap().last().unwrap(),
            &(id.clone(), name, input)
        );
    }
    assert_eq!(host.calls.lock().unwrap().len(), 7);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_local_service_preview_rejects_missing_source_identity() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
    let host = HostServices::default();
    host.replies.lock().unwrap().insert(
        "local.settings.v1.preview_pricing_sync".into(),
        json!({"prices":{},"skipped":[]}),
    );
    let mut request = crate::support::request("services");
    request.invocation = Some(invocation(&host));
    request.body=serde_json::to_vec(&json!({"operation":"services","mode":"local","local_operation":"preview_pricing_sync","input":null})).unwrap();
    let response = f.manager.handle(&id, 1, request).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert!(value.get("error").is_some(), "{value}");
    assert!(value.get("result").is_none());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn private_state_callbacks_follow_service_invocation() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    for value in 1..=3 {
        if value == 3 {
            let context = ctx("private-state");
            let cancellation = context.cancellation.clone();
            let plan = f.plan(&context);
            crate::support::cancel_middleware_state(
                &f.store,
                &cancellation,
                plan.run(context, json!({"value":7}), terminal(calls.clone())),
            )
            .await;
            crate::support::assert_middleware_state(&f.store, &f.manager, "service", 2).await;
        }
        let context = ctx("private-state");
        assert_eq!(
            f.plan(&context)
                .run(context, json!({"value":7}), terminal(calls.clone()))
                .await
                .unwrap(),
            json!({"value":7})
        );
        crate::support::assert_middleware_state(&f.store, &f.manager, "service", value).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    f.manager.shutdown().await;
}
