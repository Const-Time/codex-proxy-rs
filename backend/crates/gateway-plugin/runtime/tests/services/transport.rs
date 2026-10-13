use super::*;
use gateway_admin::plugins::PluginManager;
use gateway_core::engine::middleware::{http, websocket as ws};
use gateway_plugin_runtime::PluginEngine;
use http_body_util::BodyExt;

struct TransportFixture {
    manager: PluginManager,
    _cache: tempfile::TempDir,
}
impl TransportFixture {
    async fn new(mount: PluginMiddlewareMount, root: &Arc<Probe>) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let root_factory = factory(root);
        let engine = PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_services(Arc::new(move || Ok(Some(root_factory.clone()))));
        let manager = PluginManager::new(
            store,
            Arc::new(crate::middleware::plan::KernelRuntime(engine)),
        );
        let artifact = manager
            .upload(http_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        command.middleware = Some(PluginMiddlewareBinding {
            order: 0,
            mounts: vec![mount],
            scope: PluginExecutionScope::All {},
        });
        manager.update(None, command, &actor()).await.unwrap();
        Self {
            manager,
            _cache: cache,
        }
    }
    async fn http(
        &self,
        context: http::Context,
        port: Option<Arc<dyn http::Dispatcher>>,
    ) -> Result<Value, MiddlewareError> {
        let mut request = http::Request::new(http::empty_body());
        request
            .headers_mut()
            .insert("x-fixture-mode", "service-probe".parse().unwrap());
        if let Some(port) = port {
            request.extensions_mut().insert(port);
        }
        let response = self
            .manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context)
            .unwrap()
            .run(
                request,
                compose(vec![], |_| {
                    Box::pin(async { panic!("plugin short circuits") })
                }),
            )
            .await?;
        Ok(
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap(),
        )
    }
}
struct HttpPort(Option<Arc<dyn core::Factory>>);
impl http::Dispatcher for HttpPort {
    fn service_factory(&self) -> Option<Arc<dyn core::Factory>> {
        self.0.clone()
    }
    fn dispatch(
        &self,
        _: http::Context,
        _: http::Request,
    ) -> futures::future::BoxFuture<'static, Result<http::Response, MiddlewareError>> {
        Box::pin(async { panic!("service callback must not dispatch HTTP") })
    }
}
fn ingress() -> http::Context {
    http::Context {
        request_id: "original-ingress".into(),
        call_id: "host-call".into(),
        parent_call_id: None,
        call_scope: Default::default(),
        model_parent: None,
        cancellation: Default::default(),
        deadline_at: Default::default(),
    }
}
struct Sender;
impl ws::Sender for Sender {
    fn send(&self, _: ws::Message) -> futures::future::BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async { panic!("fixture does not write a socket") })
    }
}
fn websocket(context: http::Context, factory: Option<Arc<dyn core::Factory>>) -> ws::Context {
    ws::Context {
        origin: Some(core::Origin {
            context: context.service_context().unwrap(),
            factory,
        }),
        connection_id: "original-connection".into(),
        direction: ws::Direction::Incoming,
        headers: vec![MiddlewareHeader::new(
            "x-fixture-mode",
            bytes::Bytes::from_static(b"service-probe"),
        )]
        .into(),
        excluded_plugin_instances: Default::default(),
        cancellation: context.cancellation,
        deadline_at: context.deadline_at,
        sender: Arc::new(Sender),
    }
}
async fn ws_call(
    plan: &gateway_admin::plugins::websocket::PluginWebSocketMiddlewarePlan,
    context: ws::Context,
) -> Result<Value, MiddlewareError> {
    let result = plan
        .run(
            context,
            ws::Message {
                kind: ws::Kind::Text,
                payload: bytes::Bytes::from_static(b"original"),
            },
            compose(vec![], |message| Box::pin(async { Ok(Some(message)) })),
        )
        .await?
        .unwrap();
    assert_eq!(result.kind, ws::Kind::Binary);
    Ok(serde_json::from_slice(&result.payload).unwrap())
}

#[tokio::test]
async fn http_services_use_actual_frozen_dispatcher_and_never_root_source_fallback() {
    let root = Arc::new(Probe::default());
    let f = TransportFixture::new(PluginMiddlewareMount::Http, &root).await;
    let probe = Arc::new(Probe::default());
    let port = Arc::new(HttpPort(Some(factory(&probe))));
    for _ in 0..35 {
        assert_eq!(
            f.http(ingress(), Some(port.clone())).await.unwrap(),
            json!({"exists":true})
        );
    }
    assert!(f.http(ingress(), None).await.is_err());
    assert!(
        f.http(ingress(), Some(Arc::new(HttpPort(None))))
            .await
            .is_err()
    );
    assert!(root.facts.lock().unwrap().is_empty());
    let instance = f.manager.list().await.unwrap().remove(0).id;
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 35);
        for fact in facts.iter() {
            assert_eq!(fact.instance, instance);
            assert_eq!(fact.context.request_id, "original-ingress");
            assert_eq!(fact.context.call_id, "host-call");
            assert!(fact.context.cancellation.is_cancelled());
        }
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn native_model_http_services_inherit_original_factory_and_parent_graph() {
    let native = Fixture::with_middleware().await;
    let root = Arc::new(Probe::default());
    let f = TransportFixture::new(PluginMiddlewareMount::Http, &root).await;
    let original = Arc::new(Probe::default());
    let replacement = Arc::new(Probe::default());
    let prepared = parent(&native, &original);
    let context = http::Context::from_execution(prepared.nested_context())
        .child(native.id.clone())
        .unwrap();
    let scope = context.call_scope.clone();
    assert_eq!(
        f.http(
            context,
            Some(Arc::new(HttpPort(Some(factory(&replacement)))))
        )
        .await
        .unwrap(),
        json!({"exists":true})
    );
    assert!(replacement.facts.lock().unwrap().is_empty());
    assert!(root.facts.lock().unwrap().is_empty());
    {
        let facts = original.facts.lock().unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].context.request_id, prepared.request_id().as_str());
        assert!(
            facts[0]
                .context
                .call_scope
                .excluded_instances()
                .contains(&native.id)
        );
        let service_child = facts[0]
            .context
            .call_scope
            .fork_http("service-child".into())
            .unwrap();
        assert!(service_child.is_direct_child_of(&scope));
    }
    let unbound = native
        .execution
        .prepare_execution(native.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    let context = http::Context::from_execution(unbound.nested_context())
        .child(native.id.clone())
        .unwrap();
    assert!(
        f.http(
            context,
            Some(Arc::new(HttpPort(Some(factory(&replacement)))))
        )
        .await
        .is_err()
    );
    assert!(replacement.facts.lock().unwrap().is_empty());
    assert!(native.core.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
    native.manager.shutdown().await;
}

#[tokio::test]
async fn http_service_wait_keeps_ingress_cancellation_and_drops_active_callback() {
    let f = TransportFixture::new(PluginMiddlewareMount::Http, &Arc::new(Probe::default())).await;
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let context = ingress();
    let cancellation = context.cancellation.clone();
    let call = f.http(context, Some(Arc::new(HttpPort(Some(factory(&probe))))));
    tokio::pin!(call);
    tokio::select! { result=&mut call=>panic!("callback must wait: {result:?}"), ()=probe.entered.notified()=>{} }
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
    f.manager.shutdown().await;
}

#[tokio::test]
async fn websocket_services_reuse_connection_and_release_each_message_scope_without_fallback() {
    let root = Arc::new(Probe::default());
    let f = TransportFixture::new(PluginMiddlewareMount::WebSocket, &root).await;
    let probe = Arc::new(Probe::default());
    let context = ingress();
    let plan = f
        .manager
        .middleware_registry()
        .unwrap()
        .freeze_websocket(&Default::default(), context.cancellation.clone())
        .unwrap();
    for _ in 0..70 {
        assert_eq!(
            ws_call(
                &plan,
                websocket(context.transport_event(), Some(factory(&probe)))
            )
            .await
            .unwrap(),
            json!({"exists":true})
        );
    }
    assert!(
        ws_call(&plan, websocket(context.transport_event(), None))
            .await
            .is_err()
    );
    assert!(root.facts.lock().unwrap().is_empty());
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 70);
        for fact in facts.iter() {
            assert_eq!(fact.context.request_id, context.request_id);
            assert!(fact.context.cancellation.is_cancelled());
        }
        assert_ne!(facts[0].context.call_id, facts[1].context.call_id);
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn websocket_service_wait_keeps_connection_cancel_and_cleans_callback() {
    let f = TransportFixture::new(
        PluginMiddlewareMount::WebSocket,
        &Arc::new(Probe::default()),
    )
    .await;
    let probe = Arc::new(Probe::default());
    probe.wait.store(true, Ordering::SeqCst);
    let context = ingress();
    let cancellation = context.cancellation.clone();
    let plan = f
        .manager
        .middleware_registry()
        .unwrap()
        .freeze_websocket(&Default::default(), Default::default())
        .unwrap();
    let mut message_context = websocket(context.transport_event(), Some(factory(&probe)));
    // A host-issued parent must remain authoritative even when the raw caller
    // supplies an independent transport token.
    message_context.cancellation = Default::default();
    let call = ws_call(&plan, message_context);
    tokio::pin!(call);
    tokio::select! { result=&mut call=>panic!("callback must wait: {result:?}"), ()=probe.entered.notified()=>{} }
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
    f.manager.shutdown().await;
}
