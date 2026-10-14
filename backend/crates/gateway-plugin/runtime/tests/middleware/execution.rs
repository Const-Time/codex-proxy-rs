//! Real SDK processes retain next ownership while the RPC processing budget is paused.
use super::*;

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
