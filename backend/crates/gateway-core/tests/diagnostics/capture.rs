use gateway_core::diagnostics::{diagnostic_headers, diagnostic_json};
use serde_json::json;

#[test]
fn credentials_and_content_never_enter_diagnostics_but_trace_ids_survive() {
    let headers = diagnostic_headers([
        ("Authorization", "Bearer never-persist-this"),
        ("Set-Cookie", "session=never-persist-this"),
        ("X-OAI-Request-ID", "upstream-123"),
        ("x-codex-turn-state", "opaque-never-persist-this"),
        ("cf-ray", "ray-one"),
        ("cf-ray", "ray-two"),
    ]);
    assert_eq!(headers["x-oai-request-id"][0], "upstream-123");
    assert_eq!(headers["cf-ray"].as_array().unwrap().len(), 2);
    assert!(!headers.to_string().contains("never-persist-this"));
    let content = diagnostic_json(&json!({
        "type": "codex.response.metadata", "headers": headers,
        "access_token": "never-persist-this", "refresh_token": "never-persist-this",
        "response": {"output": [{"text": "private model output"}]},
        "input": "private user prompt", "unknown": "private extension",
    }));
    assert_eq!(content["type"], "codex.response.metadata");
    let text = content.to_string();
    for secret in [
        "never-persist-this",
        "private model output",
        "private user prompt",
        "private extension",
    ] {
        assert!(!text.contains(secret), "leaked {secret}");
    }
}

#[test]
fn arbitrary_event_type_text_is_fingerprinted_instead_of_logged() {
    let trace = gateway_core::diagnostics::TraceContext::new("req_type");
    trace.capture(
        "upstream.event",
        br#"{"type":"secret@example.com","message":"private-content"}"#,
    );
    let snapshot = trace.snapshot().unwrap();
    assert_eq!(
        snapshot["events"][0]["data"]["eventType"],
        serde_json::Value::Null
    );
    assert!(!snapshot.to_string().contains("secret@example.com"));
    assert!(!snapshot.to_string().contains("private-content"));
}
