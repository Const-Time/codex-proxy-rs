mod local_settings;
mod maintenance;
mod roots;
mod transport;
pub(crate) mod upstream_sdk;
use crate::{models::fixture::Fixture, support::*};
use gateway_admin::{model::plugins::*, ports::plugins::PluginService};
use gateway_core::{
    engine::{execution::*, middleware::service as core, middleware::*},
    middleware::compose,
    operation::{GenerateRequest, Operation, ProtocolPayload},
    routing::PublicModelId,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Clone)]
pub(crate) struct Fact {
    pub(crate) instance: String,
    pub(crate) revision: u64,
    pub(crate) context: core::Context,
}
#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) facts: Mutex<Vec<Fact>>,
    pub(crate) wait: AtomicBool,
    pub(crate) reject: AtomicBool,
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) dropped: Arc<AtomicBool>,
}
struct Factory(Arc<Probe>);
impl core::Factory for Factory {
    fn bind(
        &self,
        context: &core::Context,
        caller: core::Caller,
    ) -> Result<Arc<dyn core::Dispatcher>, core::Error> {
        assert!(!caller.cancellation().is_cancelled());
        if self.0.reject.load(Ordering::SeqCst) {
            return Err(core::Error::unavailable("frozen service unavailable"));
        }
        Ok(Arc::new(Port {
            probe: self.0.clone(),
            caller,
            context: context.clone(),
        }))
    }
}
#[derive(Clone)]
struct Port {
    probe: Arc<Probe>,
    caller: core::Caller,
    context: core::Context,
}
struct Waiting(Arc<AtomicBool>);
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl core::Dispatcher for Port {
    fn scoped(&self, context: &core::Context) -> Arc<dyn core::Dispatcher> {
        Arc::new(Self {
            context: context.clone(),
            ..self.clone()
        })
    }
    fn call(
        &self,
        instance: String,
        operation: String,
        input: Value,
    ) -> futures::future::BoxFuture<'static, Result<Value, core::Error>> {
        let port = self.clone();
        Box::pin(async move {
            assert_eq!(instance, port.caller.instance_id());
            assert_eq!(operation, "settings.admin_api_key_exists");
            assert_eq!(input, Value::Null);
            port.probe.facts.lock().unwrap().push(Fact {
                instance,
                revision: port.caller.revision().get(),
                context: port.context,
            });
            if port.probe.wait.load(Ordering::SeqCst) {
                let _waiting = Waiting(port.probe.dropped.clone());
                port.probe.entered.notify_one();
                futures::future::pending::<()>().await;
            }
            Ok(json!(true))
        })
    }
}
pub(crate) fn factory(probe: &Arc<Probe>) -> Arc<dyn core::Factory> {
    Arc::new(Factory(probe.clone()))
}
fn parent(f: &Fixture, probe: &Arc<Probe>) -> PreparedRootExecution {
    f.execution
        .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap()
        .with_service_factory(factory(probe))
}
fn context(parent: &PreparedRootExecution) -> MiddlewareContext {
    MiddlewareContext {
        request_id: parent.request_id().clone(),
        client_key_id: parent.client().policy().key_id().clone(),
        account_group_ids: vec![],
        excluded_plugin_instances: Default::default(),
        mount: MiddlewareMount::Request,
        attempt_index: None,
        operation: gateway_core::operation::OperationKind::Generate,
        endpoint: "/v1/responses".into(),
        transport: ClientTransport::HttpJson,
        provider: None,
        model: Some("gpt-native".into()),
        account_id: None,
        cancellation: parent.cancellation(),
        deadline_at: parent.deadline_at(),
    }
}
async fn request(f: &Fixture, prepared: &PreparedRootExecution) -> Result<Value, MiddlewareError> {
    request_body(
        f,
        prepared,
        json!({"mode":"native-model","native":{"mode":"service"}}),
    )
    .await
}
async fn request_body(
    f: &Fixture,
    prepared: &PreparedRootExecution,
    input: Value,
) -> Result<Value, MiddlewareError> {
    let request = MiddlewareRequest::new(
        "openai",
        vec![],
        bytes::Bytes::from(serde_json::to_vec(&input).unwrap()),
    )
    .with_execution_context(prepared.nested_context())
    .with_settings(prepared.settings().clone());
    let response = f
        .manager
        .freeze_middleware(context(prepared))
        .unwrap()
        .run(
            request,
            compose(vec![], |_| {
                Box::pin(async { panic!("fixture short circuits") })
            }),
        )
        .await?;
    let mut body = response.into_parts().3;
    let frame = body.next_frame().await?.unwrap();
    let result = serde_json::from_slice(frame.bytes()).unwrap();
    body.close().await;
    Ok(result)
}
async fn execute(f: &Fixture, prepared: PreparedRootExecution, body: Value) -> StartedExecution {
    f.execution
        .start_prepared(
            prepared,
            PreparedExecutionRequest {
                public_model: PublicModelId::new("gpt-native").unwrap(),
                operation: Operation::Generate(GenerateRequest::from_protocol_payload(
                    ProtocolPayload::json_object("openai", serde_json::from_value(body).unwrap())
                        .unwrap(),
                )),
                metadata: ExecutionRequestMetadata {
                    protocol: "openai".into(),
                    endpoint: "/v1/responses".into(),
                    transport: ClientTransport::HttpJson,
                    stream: false,
                    client_ip: None,
                    user_agent: None,
                    previous_response_id: None,
                },
            },
        )
        .await
        .unwrap()
}
async fn settle(mut started: StartedExecution) {
    started.session.collect_uncommitted().await.unwrap();
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
}

#[tokio::test]
async fn sdk_request_services_use_native_generation_and_release_each_original_scope() {
    let f = Fixture::with_middleware().await;
    let probe = Arc::new(Probe::default());
    let prepared = parent(&f, &probe);
    for _ in 0..35 {
        assert_eq!(
            request(&f, &prepared).await.unwrap(),
            json!({"exists":true})
        );
    }
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 35);
        for fact in facts.iter() {
            assert_eq!(fact.instance, f.id);
            assert_eq!(fact.revision, 1);
            assert_eq!(fact.context.request_id, prepared.request_id().as_str());
            assert_eq!(fact.context.operation, "host.services.call");
            assert!(fact.context.cancellation.is_cancelled());
        }
    }
    let unbound = f
        .execution
        .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    assert!(request(&f, &unbound).await.unwrap().get("error").is_some());
    assert_eq!(probe.facts.lock().unwrap().len(), 35);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_request_service_wait_obeys_original_cancellation_and_drops_callback() {
    let f = Fixture::with_middleware().await;
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let prepared = parent(&f, &probe);
    let cancellation = prepared.cancellation();
    let call = request(&f, &prepared);
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("service must wait: {result:?}"), () = probe.entered.notified() => {} }
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), &mut call)
            .await
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        probe.facts.lock().unwrap()[0]
            .context
            .cancellation
            .is_cancelled()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_attempt_services_share_native_request_and_keep_original_single_settlement() {
    let f = Fixture::with_middleware().await;
    let id = f.install_layers().await;
    let probe = Arc::new(Probe::default());
    let prepared = parent(&f, &probe);
    let ctx = MiddlewareContext {
        mount: MiddlewareMount::Attempt,
        ..context(&prepared)
    };
    let prepared = prepared.with_attempt_middleware(
        gateway_api::openai::middleware::responses_attempt_middleware(
            Arc::new(f.manager.middleware_registry().unwrap()),
            ctx,
        ),
    );
    let request_id = prepared.request_id().as_str().to_owned();
    settle(execute(&f, prepared, json!({"model":"gpt-native","input":"fixture","mode":"native-transform","service_probe":true})).await).await;
    f.clean(1).await;
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].instance, id);
        assert_eq!(facts[0].context.request_id, request_id);
        assert!(facts[0].context.cancellation.is_cancelled());
    }
    assert_eq!(f.core.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn sdk_routing_scheduling_and_retry_services_keep_native_identity_and_scope() {
    for stage in ["routing", "scheduling", "retry"] {
        let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
        let artifact = f
            .manager
            .upload(
                match stage {
                    "routing" => routing_archive("1.0.0"),
                    "scheduling" => scheduling_archive("1.0.0"),
                    _ => retry_archive("1.0.0"),
                },
                &actor(),
            )
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        match stage {
            "routing" => {
                command.model_router = Some(PluginModelRouterBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                    failure_policy: PluginPolicyFailurePolicy::Reject,
                })
            }
            "scheduling" => {
                command.configuration = json!({"schedule":"service-probe"});
                command.account_scheduler = Some(PluginAccountSchedulerBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                    failure_policy: PluginPolicyFailurePolicy::Reject,
                });
            }
            _ => {
                command.configuration = json!({"retry":"service-probe"});
                command.retry_policy = Some(PluginRetryPolicyBinding {
                    order: 0,
                    scope: PluginExecutionScope::All {},
                });
            }
        }
        let id = f.manager.update(None, command, &actor()).await.unwrap().id;
        let probe = Arc::new(Probe::default());
        let registry = Arc::new(f.manager.middleware_registry().unwrap());
        let prepared = parent(&f, &probe)
            .with_model_router(registry.clone())
            .with_account_scheduler(registry.clone())
            .with_retry_policy(registry);
        let request_id = prepared.request_id().as_str().to_owned();
        settle(execute(&f, prepared, json!({"model":"gpt-native","input":"fixture","service_probe":true,"failure":if stage == "retry" {json!("retry-once")}else{Value::Null}})).await).await;
        f.clean(1).await;
        {
            let facts = probe.facts.lock().unwrap();
            assert_eq!(facts.len(), 1, "{stage}");
            assert_eq!(facts[0].instance, id);
            assert_eq!(facts[0].revision, 1);
            assert_eq!(facts[0].context.request_id, request_id);
            assert!(facts[0].context.cancellation.is_cancelled());
        }
        assert_eq!(f.core.charges.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn sdk_upstream_services_keep_selected_execution_and_original_ledger() {
    let (f, server) = crate::upstream_adapter::callbacks::fixture().await;
    let probe = Arc::new(Probe::default());
    let started = crate::upstream_adapter::callbacks::start_with_factory(
        &f,
        "service-probe",
        None,
        Some(factory(&probe)),
    )
    .await;
    let request_id = started.request_id.as_str().to_owned();
    settle(started).await;
    f.clean(1).await;
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].instance, f.id);
        assert_eq!(facts[0].revision, 2);
        assert_eq!(facts[0].context.request_id, request_id);
        assert!(facts[0].context.cancellation.is_cancelled());
    }
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 500);
    assert!(f.core.leases.lock().unwrap().is_empty());
    f.manager.shutdown().await;
    drop(server);
}

#[tokio::test]
async fn native_children_inherit_original_service_factory_and_exclude_initiating_plugin() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let child_plugin = f.install_layers().await;
    let probe = Arc::new(Probe::default());
    let prepared = parent(&f, &probe);
    let result = request_body(&f, &prepared, json!({"mode":"native-model","native":{"provider_mode":"native-transform","service_probe":true}})).await.unwrap();
    assert!(result.get("error").is_none(), "{result}");
    f.clean(1).await;
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 2);
        for fact in facts.iter() {
            assert_eq!(fact.instance, child_plugin);
            assert_eq!(fact.revision, 1);
            assert_ne!(fact.context.request_id, prepared.request_id().as_str());
            assert!(fact.context.call_scope.excluded_instances().contains(&f.id));
            assert_eq!(
                fact.context.request_id,
                f.core.requests.lock().unwrap()[0].id.as_str()
            );
            assert!(fact.context.cancellation.is_cancelled());
        }
    }
    let stale_parent = prepared.nested_context();
    drop(prepared);
    assert!(
        stale_parent
            .service_context("stale-scope".into(), Default::default())
            .is_err()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn plugin_revocation_cancels_service_without_changing_model_request_authority() {
    let f = Fixture::with_middleware().await;
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let prepared = parent(&f, &probe);
    let root_cancellation = prepared.cancellation();
    let call = request(&f, &prepared);
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("service must wait: {result:?}"), () = probe.entered.notified() => {} }
    let record = f.store.records.lock().unwrap()[&f.id].clone();
    f.manager
        .update(
            Some(&f.id),
            update(&record.artifact_sha256, record.revision, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), &mut call)
            .await
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        probe.facts.lock().unwrap()[0]
            .context
            .cancellation
            .is_cancelled()
    );
    assert!(!root_cancellation.is_cancelled());
    assert!(f.core.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_binding_failure_keeps_frozen_authority_and_releases_prepared_scopes() {
    let f = Fixture::with_middleware().await;
    let probe = Arc::new(Probe::default());
    probe.reject.store(true, Ordering::SeqCst);
    let prepared = parent(&f, &probe);
    for _ in 0..35 {
        assert!(request(&f, &prepared).await.is_err());
    }
    assert!(probe.facts.lock().unwrap().is_empty());
    probe.reject.store(false, Ordering::SeqCst);
    assert_eq!(
        request(&f, &prepared).await.unwrap(),
        json!({"exists":true})
    );
    assert_eq!(probe.facts.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
