use super::*;

struct Dispatcher {
    calls: Arc<AtomicUsize>,
    root: core::Context,
    pending: bool,
}
impl core::Dispatcher for Dispatcher {
    fn dispatch(
        &self,
        context: core::Context,
        request: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        assert_eq!(context.request_id, self.root.request_id);
        assert_eq!(
            context.parent_call_id.as_deref(),
            Some(self.root.call_id.as_str())
        );
        assert_ne!(context.call_id, self.root.call_id);
        assert!(context.call_scope.is_direct_child_of(&self.root.call_scope));
        assert_eq!(context.call_scope.excluded_instances().len(), 1);
        assert!(request.extensions().get::<Marker>().is_none());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let pending = self.pending;
        Box::pin(async move {
            if pending {
                return futures::future::pending().await;
            }
            // The real API response owns this graph scope through its body. The
            // isolated dispatcher mirrors that lifetime without executing models.
            let (parts, body) = request.into_parts();
            let body = futures::stream::unfold((body, context), |(mut body, context)| async move {
                body.frame().await.map(|frame| (frame, (body, context)))
            });
            let mut response =
                core::Response::new(http_body_util::StreamBody::new(body).boxed_unsync());
            *response.status_mut() = http::StatusCode::CREATED;
            *response.version_mut() = parts.version;
            response.extensions_mut().insert(Marker);
            Ok(response)
        })
    }
}
fn no_next() -> core::Next {
    compose(vec![], |_| {
        Box::pin(async { panic!("dispatch must not consume the original next") })
    })
}

struct BrokenDispatcher;
impl core::Dispatcher for BrokenDispatcher {
    fn dispatch(
        &self,
        _: core::Context,
        _: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        Box::pin(async {
            Err(MiddlewareError::Engine(
                gateway_core::engine::EngineError::Deadline,
            ))
        })
    }
}
struct BrokenBodyDispatcher;
impl core::Dispatcher for BrokenBodyDispatcher {
    fn dispatch(
        &self,
        _: core::Context,
        _: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, MiddlewareError>> {
        Box::pin(async {
            let frames = futures::stream::iter([Err::<Frame<Bytes>, _>(Box::new(
                std::io::Error::other("independent child source failed"),
            )
                as Box<dyn std::error::Error + Send + Sync>)]);
            Ok(core::Response::new(
                http_body_util::StreamBody::new(frames).boxed_unsync(),
            ))
        })
    }
}
#[tokio::test]
async fn sdk_can_handle_child_body_failure_while_original_next_and_body_faults_stay_protected() {
    let f = Fixture::new().await;
    let response = f
        .run_with_dispatcher(
            "dispatch-body-error",
            core::empty_body(),
            context(),
            no_next(),
            Some(Arc::new(BrokenBodyDispatcher)),
        )
        .await
        .unwrap();
    assert_eq!(
        collect(response.into_body()).await.0,
        b"caught child body error"
    );
    f.stop().await;
}
#[tokio::test]
async fn sdk_can_handle_an_independent_child_dispatch_failure_without_replacing_original_next_error()
 {
    let f = Fixture::new().await;
    let response = f
        .run_with_dispatcher(
            "dispatch-error",
            core::empty_body(),
            context(),
            no_next(),
            Some(Arc::new(BrokenDispatcher)),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"caught child error");
    f.stop().await;
}
#[tokio::test]
async fn sdk_dispatch_preserves_raw_data_and_trailers_with_host_issued_ancestry_and_response_ownership()
 {
    let f = Fixture::new().await;
    let root = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(Dispatcher {
        calls: calls.clone(),
        root: root.clone(),
        pending: false,
    });
    let (input, _, drops) = source(vec![Bytes::from_static(b"raw\0child\xff")], None);
    let response = f
        .run_with_dispatcher("dispatch", input, root, no_next(), Some(dispatcher))
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::CREATED);
    assert_eq!(response.version(), http::Version::HTTP_2);
    assert!(response.extensions().get::<Marker>().is_some());
    assert_eq!(
        collect(response.into_body()).await,
        (b"raw\0child\xff".to_vec(), vec!["one".into(), "two".into()])
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_dispatch_rejects_external_uris_before_consuming_resources_or_entering_dispatcher() {
    let f = Fixture::new().await;
    let root = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(Dispatcher {
        calls: calls.clone(),
        root: root.clone(),
        pending: false,
    });
    let response = f
        .run_with_dispatcher(
            "dispatch-invalid",
            core::empty_body(),
            root,
            no_next(),
            Some(dispatcher),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"rejected");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}
#[tokio::test]
async fn sdk_dispatch_without_host_port_is_explicitly_unsupported_and_preserves_process() {
    let f = Fixture::new().await;
    for _ in 0..2 {
        let response = f
            .run("dispatch-missing", core::empty_body(), context(), no_next())
            .await
            .unwrap();
        assert_eq!(collect(response.into_body()).await.0, b"rejected");
    }
    f.stop().await;
}
#[tokio::test]
async fn sdk_dispatch_total_budget_is_shared_across_closed_child_bodies() {
    let f = Fixture::new().await;
    let root = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(Dispatcher {
        calls: calls.clone(),
        root: root.clone(),
        pending: false,
    });
    let response = f
        .run_with_dispatcher(
            "dispatch-total",
            core::empty_body(),
            root,
            no_next(),
            Some(dispatcher),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"bounded");
    assert_eq!(calls.load(Ordering::SeqCst), 64);
    f.stop().await;
}
#[tokio::test]
async fn revoking_sdk_parent_cancels_pending_dispatch_before_original_next() {
    let f = Fixture::new().await;
    let root = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(Dispatcher {
        calls: calls.clone(),
        root: root.clone(),
        pending: true,
    });
    let mut run = Box::pin(f.run_with_dispatcher(
        "dispatch",
        core::empty_body(),
        root.clone(),
        no_next(),
        Some(dispatcher),
    ));
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),_=async {while calls.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    root.cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), run)
            .await
            .unwrap()
            .is_err()
    );
    f.stop().await;
}
