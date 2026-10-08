use super::*;

#[tokio::test]
async fn json_envelope_keeps_all_original_events_until_outer_commit() {
    let trace = Arc::new(Trace::default());
    let response = execution_middleware_response(Box::new(Session::json(trace.clone())), false)
        .await
        .unwrap();
    assert!(trace.commits.lock().unwrap().is_empty());
    let (_, _, _, mut body, _) = response.into_parts();
    let frame = body.next_frame().await.unwrap().unwrap();
    let (_, framing, terminal, envelope) = frame.into_parts();
    assert_eq!(framing, MiddlewareFraming::JsonDocument);
    assert!(terminal);
    let envelope = envelope.unwrap();
    assert_eq!(envelope.events().count(), 2);
    assert_eq!(
        envelope.batch_end(),
        Some(CommitRequirement::CommitBeforeDelivery)
    );
    assert!(body.next_frame().await.unwrap().is_none());
    assert!(!body.is_finalized());
    body.commit_downstream(Some(200)).await.unwrap();
    assert!(body.is_finalized());
    assert_eq!(*trace.commits.lock().unwrap(), vec![200]);
}

#[tokio::test]
async fn dropping_prefetched_response_retains_original_cleanup_owner() {
    let trace = Arc::new(Trace::default());
    let response = execution_middleware_response(Box::new(Session::json(trace.clone())), false)
        .await
        .unwrap();
    drop(response);
    tokio::task::yield_now().await;
    assert!(trace.cancelled.load(Ordering::SeqCst));
    assert!(trace.finalized.load(Ordering::SeqCst));
    assert!(trace.commits.lock().unwrap().is_empty());
}
