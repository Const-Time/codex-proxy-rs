use super::*;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_core::{engine::middleware::service as core, middleware::compose};

struct Fixture {
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
            Arc::new(super::plan::KernelRuntime(
                gateway_plugin_runtime::PluginEngine::new(
                    cache.path().into(),
                    "3.11.2".parse().unwrap(),
                    store,
                ),
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
async fn malformed_service_reply_faults_generation_and_explicit_restart_recovers() {
    let f = Fixture::new().await;
    let id = f.manager.list().await.unwrap()[0].id.clone();
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
                .is_err()
        );
        f.change(&id, true).await;
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
