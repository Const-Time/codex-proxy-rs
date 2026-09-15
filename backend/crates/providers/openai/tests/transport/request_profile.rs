use gateway_core::{
    account::{OutboundProxy, ProxyRequestContext, RequestLocation},
    diagnostics::TraceContext,
};
use provider_openai::transport::{
    location::apply_request_location, request_profile::record_request_profile,
};
use serde_json::json;

#[test]
fn request_profile_captures_applied_fields_without_credentials_or_other_message_content() {
    let location = RequestLocation {
        country: "US".into(),
        region: "California".into(),
        city: "Los Angeles".into(),
        timezone: "America/Los_Angeles".into(),
    };
    let proxy = OutboundProxy::parse("http://username:proxy-password@127.0.0.1:8888")
        .unwrap()
        .with_request_context(Some(ProxyRequestContext {
            proxy_id: "proxy_one".into(),
            mode: "auto".into(),
            detected_ip: Some("203.0.113.5".into()),
            detected_at: Some("2026-09-15T01:00:00Z".into()),
            location: Some(location.clone()),
        }));
    let mut body = json!({
        "input": [{"role":"user","content":[{"type":"input_text","text":"<environment_context><timezone>Asia/Shanghai</timezone><current_date>2026-09-15</current_date><cwd>private-directory</cwd></environment_context>"}],
            "internal_chat_message_metadata_passthrough":{"content_item_kinds":["environments.environment_context"]}},
            {"role":"user","content":[{"type":"input_text","text":"private-prompt"}]}],
        "tools":[{"type":"web_search","user_location":{"country":"CN"}}]
    }).as_object().unwrap().clone();
    let report = apply_request_location(
        &mut body,
        Some(&location),
        "2026-09-15T01:00:00Z".parse().unwrap(),
    );
    assert_eq!(report.environment_changed, 1);
    assert_eq!(report.search_changed, 1);
    let again = apply_request_location(
        &mut body,
        Some(&location),
        "2026-09-15T01:00:00Z".parse().unwrap(),
    );
    assert_eq!(again.environment_changed, 0);
    assert_eq!(again.search_changed, 0);
    let trace = TraceContext::new("req_profile").attempt(2);
    let headers = [
        ("user-agent", "codex/0.154.0"),
        ("version", "0.154.0"),
        ("authorization", "Bearer private-access-token"),
        ("cookie", "private-cookie"),
        ("session-id", "private-session"),
        ("x-unknown", "private-unknown"),
    ];
    record_request_profile(
        &trace,
        "http_sse",
        "zstd",
        Some(&proxy),
        headers
            .iter()
            .map(|(name, value)| (*name, value.as_bytes())),
        Some(&body),
    );
    let snapshot = trace.snapshot().unwrap();
    let event = &snapshot["events"][0];
    assert_eq!(event["attemptIndex"], 2);
    assert_eq!(event["data"]["proxy"]["mode"], "auto");
    assert_eq!(event["data"]["proxy"]["detectedIp"], "203.0.113.5");
    assert_eq!(
        event["data"]["structuredLocation"]["environments"][0]["timezone"],
        "America/Los_Angeles"
    );
    assert_eq!(
        event["data"]["structuredLocation"]["environments"][0]["current_date"],
        "2026-09-14"
    );
    assert_eq!(
        event["data"]["structuredLocation"]["searches"][0]["country"],
        "US"
    );
    assert_eq!(event["data"]["headers"]["user-agent"], "codex/0.154.0");
    assert_eq!(
        event["data"]["sensitiveFieldsPresent"]["authorization"],
        true
    );
    assert!(
        event["data"]["identifierDigests"]["session-id"]["sha256"]
            .as_str()
            .is_some()
    );
    let serialized = snapshot.to_string();
    for secret in [
        "private-access-token",
        "private-cookie",
        "private-session",
        "private-unknown",
        "proxy-password",
        "username",
        "private-prompt",
        "private-directory",
    ] {
        assert!(!serialized.contains(secret), "leaked {secret}");
    }
}

#[test]
fn passthrough_profile_preserves_body_and_bounds_location_entries() {
    let trace = TraceContext::new("req_profile");
    let body = json!({"tools": (0..20).map(|_| json!({"type":"web_search","user_location":{"country":"CN","timezone":"Asia/Shanghai"}})).collect::<Vec<_>>()});
    record_request_profile(
        &trace,
        "websocket",
        "permessage-deflate_requested",
        None,
        std::iter::empty(),
        body.as_object(),
    );
    let snapshot = trace.snapshot().unwrap();
    let data = &snapshot["events"][0]["data"];
    assert_eq!(data["viaProxy"], false);
    assert_eq!(data["compression"], "none_requested");
    assert!(data["effectiveLocation"].is_null());
    assert_eq!(
        data["structuredLocation"]["searches"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        data["structuredLocation"]["searches"][0]["timezone"],
        "Asia/Shanghai"
    );
    assert_eq!(body["tools"].as_array().unwrap().len(), 20);
}
