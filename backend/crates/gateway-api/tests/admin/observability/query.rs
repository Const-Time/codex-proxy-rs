//! 查询 wire 校验的固定合同测试。

use gateway_api::admin::observability::{
    DashboardQuery, DiagnosticDimension, DiagnosticsQuery, OpsQuery, TrendKind, UsageQuery,
    parse_attempt_index, parse_datetime, parse_status,
};
use serde_json::json;

#[test]
fn dashboard_query_should_parse_terminal_trend_kinds() {
    let query: DashboardQuery = serde_json::from_value(json!({"kind": "errors"})).unwrap();
    assert_eq!(query.trend_kind().unwrap(), TrendKind::Errors);
}

#[test]
fn dashboard_query_should_reject_unknown_trend_kind() {
    let query: DashboardQuery = serde_json::from_value(json!({"kind": "secret"})).unwrap();
    assert_eq!(query.trend_kind().unwrap_err().field(), "kind");
}

#[test]
fn usage_query_should_accept_element_plus_pagination() {
    let query: UsageQuery = serde_json::from_value(json!({
        "currentPage": 129,
        "pageSize": 10
    }))
    .unwrap();
    assert_eq!(query.validate_pagination().unwrap(), (129, 10));
}

#[test]
fn usage_query_should_reject_removed_total_toggle() {
    assert!(serde_json::from_value::<UsageQuery>(json!({"includeTotal": true})).is_err());
}

#[test]
fn usage_query_should_reject_removed_cursor_contract() {
    assert!(serde_json::from_value::<UsageQuery>(json!({"cursor": "opaque"})).is_err());
}

#[test]
fn usage_query_should_reject_nonstandard_page_name() {
    assert!(serde_json::from_value::<UsageQuery>(json!({"page": 1})).is_err());
}

#[test]
fn ops_query_should_reject_page_size_above_terminal_limit() {
    let query: OpsQuery = serde_json::from_value(json!({"pageSize": 101})).unwrap();
    assert_eq!(query.validate_pagination().unwrap_err().field(), "pageSize");
}

#[test]
fn ops_query_should_reject_removed_filters() {
    assert!(serde_json::from_value::<OpsQuery>(json!({"route": "/v1/responses"})).is_err());
    assert!(serde_json::from_value::<OpsQuery>(json!({"failureClass": "rate_limited"})).is_err());
}

#[test]
fn diagnostics_query_should_keep_wire_dimension_name() {
    let query: DiagnosticsQuery =
        serde_json::from_value(json!({"dimension": "failure_class"})).unwrap();
    assert_eq!(query.dimension().unwrap(), DiagnosticDimension::Failure);
    assert_eq!(DiagnosticDimension::Failure.display_name(), "failureClass");
    let key_model: DiagnosticsQuery =
        serde_json::from_value(json!({"dimension": "keyModel"})).unwrap();
    assert_eq!(
        key_model.dimension().unwrap(),
        DiagnosticDimension::KeyModel
    );
    assert_eq!(DiagnosticDimension::KeyModel.display_name(), "keyModel");
}

#[test]
fn scalar_query_parsers_should_reject_out_of_range_values_without_echoing_input() {
    assert_eq!(parse_status(Some(99)).unwrap_err().field(), "statusCode");
    assert_eq!(
        parse_attempt_index(Some(0)).unwrap_err().field(),
        "attemptIndex"
    );
    assert_eq!(
        parse_datetime(Some("not-a-time")).unwrap_err().field(),
        "timeRange"
    );
}

#[test]
fn usage_cards_validate_metric_combinations_and_preserve_filters() {
    use gateway_admin::model::observability::{
        UsageInsightSelection as S, UsagePerformanceMetric as M,
    };
    use gateway_api::admin::observability::UsageQuery;
    for (card, metric, expected) in [
        ("health", None, S::Health),
        ("cost", None, S::Cost),
        ("performance", Some("total"), S::Performance(M::Total)),
        (
            "performance",
            Some("firstToken"),
            S::Performance(M::FirstToken),
        ),
        (
            "performance",
            Some("throughput"),
            S::Performance(M::Throughput),
        ),
        (
            "performance",
            Some("scheduling"),
            S::Performance(M::Scheduling),
        ),
        (
            "performance",
            Some("concurrency"),
            S::Performance(M::Concurrency),
        ),
    ] {
        let query: UsageQuery = serde_json::from_value(json!({"card":card,"metric":metric,"personal":true,"groupId":"g1","startTime":"2026-11-01T00:00:00Z"})).unwrap();
        assert_eq!(query.selection().unwrap(), expected);
        assert!(query.personal);
        assert_eq!(query.group_id.as_deref(), Some("g1"));
    }
    for value in [
        json!({"card":"health","metric":"total"}),
        json!({"metric":"total"}),
        json!({"card":"performance","metric":"bad"}),
        json!({"card":"bad"}),
    ] {
        assert!(
            serde_json::from_value::<UsageQuery>(value)
                .unwrap()
                .selection()
                .is_err()
        );
    }
    assert_eq!(UsageQuery::default().selection().unwrap(), S::All);
    assert!(serde_json::from_value::<UsageQuery>(json!({"unknown":"bad"})).is_err());
}

#[test]
fn usage_card_url_query_accepts_personal_and_numeric_filters() {
    let query = axum::extract::Query::<gateway_api::admin::observability::UsageQuery>::try_from_uri(&"/api/admin/usage/insights/overview?card=performance&metric=concurrency&personal=true&statusCode=200&startTime=2026-11-01T00%3A00%3A00Z".parse().unwrap()).unwrap().0;
    assert!(query.personal);
    assert_eq!(query.status_code, Some(200));
    assert!(query.selection().is_ok());
}
