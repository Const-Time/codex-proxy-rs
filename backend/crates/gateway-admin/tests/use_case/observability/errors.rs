use gateway_admin::model::observability::{UsageErrorEvidence, UsageErrorOrigin};

#[test]
fn error_origin_preserves_unknown_and_prioritizes_recorded_upstream_evidence() {
    use UsageErrorOrigin::{Connection, Proxy, Unknown, Upstream};
    let empty = UsageErrorEvidence::default();
    for kind in [
        "invalid_request",
        "protocol",
        "rate_limited",
        "unauthorized",
        "timeout",
    ] {
        assert_eq!(empty.origin(None, Some(200), Some(kind)), Unknown);
    }
    for kind in [
        "no_available_provider",
        "account_capacity_unavailable",
        "key_budget_unavailable",
        "user_rate_limited",
        "group_daily_budget_exceeded",
    ] {
        assert_eq!(empty.origin(None, None, Some(kind)), Proxy);
    }
    assert_eq!(empty.origin(None, None, Some("transport")), Connection);
    assert_eq!(empty.origin(None, Some(429), Some("transport")), Upstream);
    for raw in [
        r#"{"error":{"message":"rate limit"}}"#,
        r#"{"response":{"error":{"code":"quota"}}}"#,
        r#"{"type":"websocket.close","code":1009}"#,
    ] {
        let facts = UsageErrorEvidence::from_recorded(Some(raw), None, None);
        assert_eq!(
            facts.origin(None, Some(200), Some("account_capacity_unavailable")),
            Upstream
        );
    }
    for raw in [
        "invalid JSON",
        r#"{"error":{"message":5}}"#,
        r#"{"error":{"code":""}}"#,
        r#"{"type":"websocket.close","code":"1009"}"#,
    ] {
        assert_eq!(
            UsageErrorEvidence::from_recorded(Some(raw), None, None).origin(None, None, None),
            Unknown
        );
    }
    assert_eq!(
        UsageErrorEvidence::from_recorded(None, Some("connection_busy".into()), None)
            .origin(None, None, None),
        Proxy
    );
    assert_eq!(
        UsageErrorEvidence::from_recorded(None, None, Some("connection_reset".into()))
            .origin(None, None, None),
        Connection
    );
}
