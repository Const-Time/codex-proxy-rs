//! Real SDK processes retain next ownership while the RPC processing budget is paused.
use super::*;

#[tokio::test]
async fn slow_next_releases_execution_capacity_but_retains_inflight_limit() {
    let fixture = Fixture::with_limits(RpcLimits {
        maximum_calls: 64,
        maximum_executing_calls: 1,
        maximum_queued_calls: 1,
        maximum_callbacks: 1,
        ..RpcLimits::default()
    })
    .await;
    let (entered, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut tasks = Vec::new();
    for index in 0..64 {
        let callbacks = fixture.callbacks.clone();
        let session = fixture.session.clone();
        let entered = entered.clone();
        let release = release.clone();
        tasks.push(tokio::spawn(async move {
            let next = gateway_core::middleware::compose(vec![], move |input| {
                Box::pin(async move {
                    entered.send(()).unwrap();
                    release.acquire().await.unwrap().forget();
                    terminal(Arc::new(Trace::default()), false).run(input).await
                })
            });
            let mut context = context(false);
            context.request_id =
                gateway_core::engine::ModelRequestId::new(format!("req_capacity_{index}")).unwrap();
            callbacks
                .invoke(session, context, request("pass"), next)
                .await
        }));
        tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
            .await
            .unwrap()
            .unwrap();
    }
    // Waiting downstream requests still own their inflight lease: no bypass at the cap.
    let trace = Arc::new(Trace::default());
    let result = fixture.run("pass", false, trace.clone()).await;
    assert!(matches!(result, Err(MiddlewareError::Gateway(error))
        if error.kind() == gateway_core::error::GatewayErrorKind::MiddlewareCapacityUnavailable));
    assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    release.add_permits(64);
    for task in tasks {
        drop(
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    fixture.stop().await;
}

#[tokio::test]
async fn request_and_attempt_next_can_outlive_rpc_budget_without_resetting_core_deadline() {
    let fixture = Fixture::with_limits(RpcLimits {
        maximum_call_timeout: Duration::from_millis(300),
        ..RpcLimits::default()
    })
    .await;
    for mount in [MiddlewareMount::Request, MiddlewareMount::Attempt] {
        let trace = Arc::new(Trace::default());
        let downstream = trace.clone();
        let next = gateway_core::middleware::compose(vec![], move |input| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(900)).await;
                terminal(downstream, false).run(input).await
            })
        });
        let mut context = context(false);
        context.mount = mount;
        context.attempt_index = (mount == MiddlewareMount::Attempt).then_some(1);
        context.deadline_at = (SystemTime::now() + Duration::from_secs(3)).into();
        let response = fixture
            .callbacks
            .invoke(fixture.session.clone(), context, request("pass"), next)
            .await
            .unwrap();
        drop(response);
        trace.closed_once().await;
        assert_eq!(trace.next.load(Ordering::SeqCst), 1);
        assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    }
    fixture.stop().await;
}

#[tokio::test]
async fn processing_budget_resumes_after_next_and_cleans_up_unfinished_plugin_work() {
    let fixture = Fixture::with_limits(RpcLimits {
        maximum_call_timeout: Duration::from_millis(300),
        ..RpcLimits::default()
    })
    .await;
    let trace = Arc::new(Trace::default());
    let downstream = trace.clone();
    let next = gateway_core::middleware::compose(vec![], move |input| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(900)).await;
            terminal(downstream, false).run(input).await
        })
    });
    let mut context = context(false);
    context.deadline_at = (SystemTime::now() + Duration::from_secs(5)).into();
    let started = tokio::time::Instant::now();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .callbacks
                .invoke(fixture.session.clone(), context, request("wait"), next)
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(started.elapsed() >= Duration::from_millis(900));
    trace.closed_once().await;
    assert_eq!(trace.next.load(Ordering::SeqCst), 1);
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    fixture.stop().await;
}

#[tokio::test]
async fn paused_next_still_obeys_core_deadline_and_explicit_cancellation() {
    let fixture = Fixture::with_limits(RpcLimits {
        maximum_call_timeout: Duration::from_millis(300),
        ..RpcLimits::default()
    })
    .await;
    for explicit_cancel in [false, true] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let next = gateway_core::middleware::compose(vec![], move |_| {
            Box::pin(async move {
                signal.notify_one();
                std::future::pending::<Result<MiddlewareResponse, MiddlewareError>>().await
            })
        });
        let mut context = context(false);
        context.deadline_at = (SystemTime::now() + Duration::from_millis(800)).into();
        let cancellation = context.cancellation.clone();
        let callbacks = fixture.callbacks.clone();
        let session = fixture.session.clone();
        let invocation = tokio::spawn(async move {
            callbacks
                .invoke(session, context, request("pass"), next)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        if explicit_cancel {
            cancellation.cancel();
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(2), invocation)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
    fixture.stop().await;
}
