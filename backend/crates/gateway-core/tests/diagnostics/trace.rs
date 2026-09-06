use gateway_core::diagnostics::TraceContext;
use serde_json::json;

#[test]
fn bounded_history_preserves_start_failure_and_final_result() {
    let trace = TraceContext::new("req_trace");
    trace.record("request.started", json!({}));
    for index in 0..400 {
        trace.record(
            "test.event",
            json!({"index": index, "detail": "x".repeat(2000)}),
        );
    }
    trace
        .attempt(1)
        .record("attempt.failed", json!({"code": "websocket_close_1000"}));
    trace
        .attempt(2)
        .record("request.finished", json!({"outcome": "succeeded"}));
    let snapshot = trace.snapshot().unwrap();
    let events = snapshot["events"].as_array().unwrap();
    assert_eq!(events[0]["stage"], "request.started");
    assert_eq!(events[events.len() - 2]["attemptIndex"], 1);
    assert_eq!(events.last().unwrap()["attemptIndex"], 2);
    assert!(snapshot["droppedEvents"].as_u64().unwrap() > 0);
    assert!(events.len() <= 128);
    assert!(snapshot.to_string().len() < 64 * 1024);
    let kept: u64 = events
        .iter()
        .map(|event| event["count"].as_u64().unwrap())
        .sum();
    assert_eq!(
        kept + snapshot["droppedEvents"].as_u64().unwrap(),
        snapshot["totalEvents"]
    );
}

#[test]
fn shared_attempts_and_exchanges_coalesce_delta_without_losing_metadata() {
    let trace = TraceContext::new("req_trace");
    let ws = trace.attempt(1).exchange("websocket");
    ws.capture(
        "upstream.event",
        br#"{"type":"codex.response.metadata","headers":{"x-request-id":"up-1"}}"#,
    );
    trace
        .attempt(1)
        .wire_event("openai", Some("codex.response.metadata"));
    for _ in 0..1000 {
        ws.capture(
            "upstream.event",
            br#"{"type":"response.output_text.delta","delta":"secret"}"#,
        );
        trace
            .attempt(1)
            .wire_event("openai", Some("response.output_text.delta"));
    }
    let http = trace.attempt(1).exchange("http_sse");
    http.capture("upstream.event", br#"{"type":"response.completed"}"#);
    let snapshot = trace.snapshot().unwrap();
    let events = snapshot["events"].as_array().unwrap();
    assert_eq!(snapshot["droppedEvents"], 0);
    assert_eq!(
        events
            .iter()
            .find(|e| e["data"]["eventType"] == "response.output_text.delta")
            .unwrap()["count"],
        1000
    );
    assert_eq!(events.last().unwrap()["exchangeId"], 2);
    assert!(
        events
            .iter()
            .any(|e| e["data"]["metadata"]["headers"]["x-request-id"] == "up-1")
    );
    assert!(!snapshot.to_string().contains("secret"));
    assert!(
        events
            .iter()
            .filter(|event| event["stage"] == "provider.event")
            .all(|event| event["data"].get("metadata").is_none())
    );
}

#[test]
fn header_capture_keeps_duplicates_but_excludes_credentials_from_timeline() {
    let trace = TraceContext::new("req_headers");
    trace.headers(
        "upstream.response.headers",
        json!({"status": 101}),
        [
            ("authorization", b"Bearer SECRET".as_slice()),
            ("x-request-id", b"upstream-one".as_slice()),
            ("x-request-id", b"upstream-two".as_slice()),
            ("set-cookie", b"PRIVATE_COOKIE".as_slice()),
        ],
    );
    let snapshot = trace.snapshot().unwrap();
    assert_eq!(
        snapshot["events"][0]["data"]["headers"]["x-request-id"],
        json!(["upstream-one", "upstream-two"])
    );
    assert!(!snapshot.to_string().contains("SECRET"));
    assert!(!snapshot.to_string().contains("PRIVATE_COOKIE"));
}

#[test]
fn disabled_context_does_not_allocate_a_history() {
    let trace = TraceContext::default().attempt(2).exchange("http");
    trace.capture("upstream.event", b"not json");
    assert!(trace.snapshot().is_none());
}

#[test]
fn a_long_successful_retry_keeps_the_earlier_failure_ahead_of_routine_events() {
    let trace = TraceContext::new("req_retry");
    for _ in 0..12 {
        trace.record("setup", json!({}));
    }
    trace
        .attempt(1)
        .record("attempt.failed", json!({"kind": "unavailable"}));
    trace
        .attempt(1)
        .record("retry.decided", json!({"retryable": true}));
    for index in 0..500 {
        trace
            .attempt(2)
            .record("upstream.event", json!({"index": index}));
    }
    trace.record("request.finished", json!({"outcome": "succeeded"}));
    let snapshot = trace.snapshot().unwrap();
    let events = snapshot["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["stage"] == "attempt.failed" && event["attemptIndex"] == 1)
    );
    assert!(events.iter().any(|event| event["stage"] == "retry.decided"));
    assert_eq!(events.last().unwrap()["stage"], "request.finished");
    assert!(events.len() <= 128);
}
