use super::*;
use gateway_core::engine::middleware::http as core;
use http_body_util::BodyExt;
use std::sync::{Arc, atomic::AtomicUsize};

pub(crate) struct GraphServices(pub(crate) gateway_core::engine::nested::ExtensionCallScope);
impl gateway_core::engine::middleware::service::Dispatcher for GraphServices {
    fn scoped(
        &self,
        context: &gateway_core::engine::middleware::service::Context,
    ) -> Arc<dyn gateway_core::engine::middleware::service::Dispatcher> {
        Arc::new(Self(context.call_scope.clone()))
    }
    fn call(
        &self,
        instance: String,
        _: String,
        _: serde_json::Value,
    ) -> futures::future::BoxFuture<
        'static,
        Result<serde_json::Value, gateway_core::engine::middleware::service::Error>,
    > {
        let scope = self.0.clone();
        Box::pin(async move {
            let _permit = scope.fork_http(instance).map_err(|_| {
                gateway_core::engine::middleware::service::Error {
                    kind: "capacity".into(),
                    message: "shared graph capacity".into(),
                    details: None,
                }
            })?;
            Ok(json!(true))
        })
    }
}
#[tokio::test]
async fn management_model_http_and_service_callbacks_inherit_one_host_graph_and_frozen_http_port() {
    let f = Fixture::new().await;
    let global_calls = Arc::new(AtomicUsize::new(0));
    let bound_calls = Arc::new(AtomicUsize::new(0));
    let global: Arc<dyn core::Dispatcher> = Arc::new(Echo {
        calls: global_calls.clone(),
        parent: None,
        pending: false,
    });
    f.http.attach(&global);
    let frozen: Arc<dyn core::Dispatcher> = Arc::new(Echo {
        calls: bound_calls.clone(),
        parent: None,
        pending: false,
    });
    let scope = gateway_core::engine::nested::ExtensionCallScope::default();
    let mut request = crate::support::request("models");
    request.actor.actor = gateway_admin::model::MutationActor::AdminSession {
        admin_user_id: "alice".into(),
    };
    request.body =
        serde_json::to_vec(&json!({"operation":"models","mode":"service-budget"})).unwrap();
    request.invocation = Some(gateway_admin::model::plugins::PluginInvocationContext {
        context: core::Context {
            request_id: "host-request".into(),
            call_id: "host-http".into(),
            parent_call_id: None,
            call_scope: scope.clone(),
            model_parent: None,
            cancellation: Default::default(),
            deadline_at: Default::default(),
        },
        http: Some(frozen),
        services: Some(Arc::new(GraphServices(Default::default()))),
    });
    let response = f.manager.handle(&f.id, 1, request).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(value, json!({"completed":62,"kind":"capacity"}));
    assert_eq!(bound_calls.load(Ordering::SeqCst), 1);
    assert_eq!(global_calls.load(Ordering::SeqCst), 0);
    assert!(scope.fork_http("exhausted".into()).is_err());
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
    f.manager.shutdown().await;
}

struct Echo {
    calls: Arc<AtomicUsize>,
    parent: Option<gateway_core::engine::ModelRequestId>,
    pending: bool,
}
impl core::Dispatcher for Echo {
    fn dispatch(
        &self,
        context: core::Context,
        request: core::Request,
    ) -> futures::future::BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        context.validate().unwrap();
        assert!(context.call_scope.is_http_child());
        assert_eq!(context.call_scope.parent_request_id(), self.parent.as_ref());
        assert_eq!(
            context.model_parent.as_ref().map(|p| p.request_id()),
            self.parent.as_ref()
        );
        assert_eq!(request.headers()["x-explicit"], "plugin-value");
        assert!(!request.headers().contains_key("authorization"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let pending = self.pending;
        Box::pin(async move {
            if pending {
                return futures::future::pending().await;
            }
            let body = futures::stream::unfold(
                (request.into_body(), context),
                |(mut body, context)| async move {
                    body.frame().await.map(|frame| (frame, (body, context)))
                },
            );
            Ok(core::Response::new(
                http_body_util::StreamBody::new(body).boxed_unsync(),
            ))
        })
    }
}
pub(crate) fn attach(
    f: &Fixture,
    parent: Option<gateway_core::engine::ModelRequestId>,
    pending: bool,
) -> (Arc<dyn core::Dispatcher>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher: Arc<dyn core::Dispatcher> = Arc::new(Echo {
        calls: calls.clone(),
        parent,
        pending,
    });
    f.http.attach(&dispatcher);
    (dispatcher, calls)
}
#[tokio::test]
async fn management_http_uses_raw_sdk_resources_and_weak_host_ownership() {
    let f = Fixture::new().await;
    let (dispatcher, calls) = attach(&f, None, false);
    let result = f.call(json!({"mode":"http"})).await;
    assert_eq!(result["bytes"], json!([0, 255, 1, 0]), "{result}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&dispatcher),
        1,
        "finished callbacks must release the host port"
    );
    drop(dispatcher);
    let result = f.call(json!({"mode":"http"})).await;
    assert_eq!(result["error"]["code"], "unsupported", "{result}");
    f.manager.shutdown().await;
}
#[tokio::test]
async fn management_http_total_budget_is_per_call_and_invalid_uri_does_not_dispatch() {
    let f = Fixture::new().await;
    let (_dispatcher, calls) = attach(&f, None, false);
    let result = f
        .call(json!({"mode":"http", "uri":"https://external.test/"}))
        .await;
    assert_eq!(result["error"]["code"], "invalid_input", "{result}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let result = f.call(json!({"mode":"http", "count":65})).await;
    assert_eq!(result["error"]["code"], "capacity", "{result}");
    assert_eq!(calls.load(Ordering::SeqCst), 64);
    assert_eq!(
        f.call(json!({"mode":"http"})).await["bytes"],
        json!([0, 255, 1, 0])
    );
    let before = calls.load(Ordering::SeqCst);
    let result = f
        .call(json!({"mode":"http", "count":64, "model_first":true}))
        .await;
    assert_eq!(result["error"]["code"], "capacity", "{result}");
    assert_eq!(
        calls.load(Ordering::SeqCst) - before,
        63,
        "models and HTTP must share one total budget"
    );
    f.clean(1).await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn management_revocation_cancels_pending_http_and_releases_host_references() {
    let f = Fixture::new().await;
    let (dispatcher, calls) = attach(&f, None, true);
    let manager = f.manager.clone();
    let id = f.id.clone();
    let call = tokio::spawn(async move {
        let mut request = crate::support::request("models");
        request.body = serde_json::to_vec(&json!({"operation":"models", "mode":"http"})).unwrap();
        manager.handle(&id, 1, request).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let record = f.manager.list().await.unwrap().remove(0);
    let command = crate::support::update(&record.artifact.sha256, record.revision, false);
    f.manager
        .update(Some(&f.id), command, &crate::support::actor())
        .await
        .unwrap();
    if let Ok(response) = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .unwrap()
        .unwrap()
    {
        let result: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert!(
            matches!(
                result["error"]["code"].as_str(),
                Some("cancelled" | "permission_denied")
            ),
            "the fixture may catch cancellation, but may not return success: {result}"
        );
        assert_eq!(
            result["error"]["send_state"], "ambiguous",
            "a lost HTTP reply does not prove the operation was unsent"
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&dispatcher) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.manager.shutdown().await;
}
#[tokio::test]
async fn request_and_attempt_http_callbacks_receive_only_host_issued_parent_ancestry() {
    use gateway_admin::model::plugins::PluginMiddlewareMount;
    for mount in [MiddlewareMount::Request, MiddlewareMount::Attempt] {
        let f = Fixture::with_middleware().await;
        let mut record = f.manager.list().await.unwrap().remove(0);
        let mut update = crate::support::update(&record.artifact.sha256, record.revision, true);
        let binding = record.middleware.as_mut().unwrap();
        binding.mounts = vec![if mount == MiddlewareMount::Request {
            PluginMiddlewareMount::Request
        } else {
            PluginMiddlewareMount::Attempt
        }];
        update.middleware = record.middleware;
        f.manager
            .update(Some(&f.id), update, &crate::support::actor())
            .await
            .unwrap();
        let parent = f
            .execution
            .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
            .unwrap();
        let (dispatcher, calls) = attach(&f, Some(parent.request_id().clone()), false);
        let parent = parent.with_http_dispatcher(dispatcher);
        let (_new_dispatcher, new_calls) = attach(&f, None, false);
        let mut context = native_context(&parent);
        context.mount = mount;
        let request = MiddlewareRequest::new(
            "openai",
            vec![],
            bytes::Bytes::from_static(br#"{"mode":"native-model","native":{"mode":"http"}}"#),
        )
        .with_execution_context(parent.nested_context())
        .with_settings(parent.settings().clone());
        let response = f
            .manager
            .freeze_middleware(context)
            .unwrap()
            .run(
                request,
                gateway_core::middleware::compose(vec![], |_| {
                    Box::pin(async { panic!("short circuit") })
                }),
            )
            .await
            .unwrap();
        let mut body = response.into_parts().3;
        let frame = body.next_frame().await.unwrap().unwrap();
        let result: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
        assert_eq!(
            result["bytes"],
            json!([0, 255, 1, 0]),
            "{mount:?}: {result}"
        );
        body.close().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            new_calls.load(Ordering::SeqCst),
            0,
            "a native parent must keep its frozen HTTP port"
        );
        assert!(!parent.cancellation().is_cancelled());
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn native_http_adoption_excludes_live_ancestor_accounts_and_keeps_single_ledger() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let parent = f
        .execution
        .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    let context = core::Context::from_execution(parent.nested_context())
        .child("http-origin".into())
        .unwrap();
    let parent_id = parent.request_id().clone();
    let child = f
        .execution
        .prepare_http_execution(
            f.execution.authenticate("sk_native_fixture").unwrap(),
            context,
        )
        .unwrap();
    let mut parent = f.start(parent).await;
    parent.session.collect_uncommitted().await.unwrap();
    let mut child = f.start(child).await;
    child.session.collect_uncommitted().await.unwrap();
    assert_eq!(
        f.core
            .selected
            .lock()
            .unwrap()
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        ["acct_allowed", "acct_alternative"]
    );
    for mut call in [child, parent] {
        call.session.commit_downstream(Some(200)).await.unwrap();
        call.session.detach_finalize().await;
    }
    f.clean(2).await;
    assert!(f.core.leases.lock().unwrap().is_empty());
    assert_eq!(
        f.core.requests.lock().unwrap()[1]
            .parent_request_id
            .as_ref(),
        Some(&parent_id)
    );
    assert_eq!(f.core.charges.lock().unwrap().len(), 2);
    f.manager.shutdown().await;
}
