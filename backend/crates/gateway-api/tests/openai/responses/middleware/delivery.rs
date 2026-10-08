use super::*;
use axum::http::StatusCode;
use futures::StreamExt;

#[tokio::test]
async fn rewritten_json_commits_original_execution_once() {
    let trace = Arc::new(Trace::default());
    let response = execution_middleware_response(Box::new(Session::json(trace.clone())), false)
        .await
        .unwrap();
    let response = deliver_middleware_response(rewrite(response, Rewrite::Json), false, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes(response).await).unwrap()["id"],
        "resp_rewritten"
    );
    assert_eq!(*trace.commits.lock().unwrap(), vec![200]);
    assert!(trace.finalized.load(Ordering::SeqCst));
    assert!(!trace.cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn invalid_json_never_commits_and_records_actual_error_status() {
    let trace = Arc::new(Trace::default());
    let response = execution_middleware_response(Box::new(Session::json(trace.clone())), false)
        .await
        .unwrap();
    let response =
        deliver_middleware_response(rewrite(response, Rewrite::Invalid), false, None).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(trace.commits.lock().unwrap().is_empty());
    assert_eq!(*trace.statuses.lock().unwrap(), vec![502]);
    assert!(trace.cancelled.load(Ordering::SeqCst));
    assert!(trace.finalized.load(Ordering::SeqCst));
}

#[tokio::test]
async fn atomic_first_sse_batch_is_validated_before_any_commit() {
    let trace = Arc::new(Trace::default());
    let session = Session::sse(
        trace.clone(),
        vec![vec![event("response.created"), event("response.completed")]],
    );
    let response = execution_middleware_response(Box::new(session), true)
        .await
        .unwrap();
    let response =
        deliver_middleware_response(rewrite(response, Rewrite::InvalidSecond), true, None).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(trace.commits.lock().unwrap().is_empty());
    assert!(trace.cancelled.load(Ordering::SeqCst));
    assert!(trace.finalized.load(Ordering::SeqCst));
}

#[tokio::test]
async fn empty_first_sse_batch_cannot_acknowledge_delivery() {
    let trace = Arc::new(Trace::default());
    let session = Session::sse(
        trace.clone(),
        vec![
            vec![event("response.created")],
            vec![event("response.completed")],
        ],
    );
    let response = execution_middleware_response(Box::new(session), true)
        .await
        .unwrap();
    let response = deliver_middleware_response(rewrite(response, Rewrite::Empty), true, None).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(trace.commits.lock().unwrap().is_empty());
    assert!(trace.finalized.load(Ordering::SeqCst));
}

#[tokio::test]
async fn sse_terminal_requires_client_consumable_terminal() {
    let trace = Arc::new(Trace::default());
    let session = Session::sse(trace.clone(), vec![vec![event("response.completed")]]);
    let response = execution_middleware_response(Box::new(session), true)
        .await
        .unwrap();
    let response =
        deliver_middleware_response(rewrite(response, Rewrite::MissingTerminal), true, None).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(trace.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn valid_sse_commits_once_for_single_and_multiple_batches() {
    for batches in [
        vec![vec![event("response.created"), event("response.completed")]],
        vec![
            vec![event("response.created")],
            vec![event("response.completed")],
        ],
    ] {
        let trace = Arc::new(Trace::default());
        let response =
            execution_middleware_response(Box::new(Session::sse(trace.clone(), batches)), true)
                .await
                .unwrap();
        let response =
            deliver_middleware_response(rewrite(response, Rewrite::Pass), true, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let output = String::from_utf8(bytes(response).await.to_vec()).unwrap();
        assert!(output.contains("response.completed"), "{output}");
        assert!(!output.contains("middleware_failed"), "{output}");
        assert_eq!(output.matches("[DONE]").count(), 1);
        assert_eq!(*trace.commits.lock().unwrap(), vec![200]);
        assert!(trace.finalized.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn midstream_failure_closes_source_before_emitting_failure() {
    let trace = Arc::new(Trace::default());
    let session = Session::sse(
        trace.clone(),
        vec![
            vec![event("response.created")],
            vec![event("response.output_text.delta")],
            vec![event("response.completed")],
        ],
    );
    let response = execution_middleware_response(Box::new(session), true)
        .await
        .unwrap();
    let response =
        deliver_middleware_response(rewrite(response, Rewrite::InvalidSecond), true, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("response.created"));
    let failure = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&failure).contains("middleware_failed"));
    assert!(trace.cancelled.load(Ordering::SeqCst));
    assert!(trace.finalized.load(Ordering::SeqCst));
    assert_eq!(*trace.commits.lock().unwrap(), vec![200]);
}

#[tokio::test]
async fn failed_original_commit_does_not_expose_rewritten_json() {
    let trace = Arc::new(Trace::default());
    let mut session = Session::json(trace.clone());
    session.fail_commit = true;
    let response = execution_middleware_response(Box::new(session), false)
        .await
        .unwrap();
    let response = deliver_middleware_response(rewrite(response, Rewrite::Json), false, None).await;
    assert!(response.status().is_server_error());
    assert!(!String::from_utf8_lossy(&bytes(response).await).contains("resp_rewritten"));
    assert!(trace.commits.lock().unwrap().is_empty());
    assert!(trace.finalized.load(Ordering::SeqCst));
}

#[tokio::test]
async fn finalized_terminal_expansion_is_validated_as_one_batch() {
    let trace = Arc::new(Trace::default());
    let session = Session::sse(
        trace.clone(),
        vec![
            vec![event("response.created")],
            vec![event("response.completed")],
        ],
    );
    let response = execution_middleware_response(Box::new(session), true)
        .await
        .unwrap();
    let response = deliver_middleware_response(
        rewrite(response, Rewrite::ExpandInvalidTerminal),
        true,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let output = String::from_utf8(bytes(response).await.to_vec()).unwrap();
    assert!(output.contains("middleware_failed"));
    assert!(!output.contains("must-not-leak"));
    assert_eq!(*trace.commits.lock().unwrap(), vec![200]);
    assert!(trace.finalized.load(Ordering::SeqCst));
}
