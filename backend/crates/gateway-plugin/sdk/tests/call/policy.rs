//! 验证插件路由和账号调度决策使用显式且严格的线协议值

use gateway_plugin_sdk::call::policy::{
    AccountScheduleCandidate, AccountScheduleDecision, AccountScheduleRequest, ModelRouteDecision,
    ModelRouteRequest, PolicyHeader,
};
use serde_json::json;

#[test]
fn route_and_scheduler_decisions_are_explicit_and_strict() {
    let route = ModelRouteRequest {
        request_id: "req_policy".into(),
        operation: "generate".into(),
        protocol: "openai".into(),
        model: "public-model".into(),
        available_providers: vec!["openai".into()],
        headers: vec![PolicyHeader {
            name: "x-feature".into(),
            value_base64: "b24=".into(),
        }],
    };
    assert_eq!(
        serde_json::from_value::<ModelRouteRequest>(serde_json::to_value(&route).unwrap()).unwrap(),
        route
    );
    assert_eq!(
        serde_json::to_value(ModelRouteDecision::Route {
            provider: Some("openai".into()),
            model: None,
        })
        .unwrap(),
        json!({"decision":"route","provider":"openai"})
    );
    assert!(
        serde_json::from_value::<ModelRouteDecision>(
            json!({"decision":"route","provider":"openai","unexpected":true})
        )
        .is_err()
    );

    let schedule = AccountScheduleRequest {
        request_id: "req_policy".into(),
        attempt_index: 2,
        provider: "openai".into(),
        model: Some("gpt-5".into()),
        candidates: vec![AccountScheduleCandidate {
            account_id: "acct_candidate".into(),
            available: true,
            session_bound: false,
            role: gateway_plugin_sdk::call::policy::AccountScheduleRole::Ordinary,
            weight: 10,
            in_flight: 1,
            maximum_concurrency: 4,
            last_started_at_ms: None,
            quota_reset_at_ms: None,
            quota_remaining_rank: Some(9_000),
            failure_rate_basis_points: None,
            first_output_latency_ms: Some(20),
        }],
    };
    assert_eq!(
        serde_json::from_value::<AccountScheduleRequest>(serde_json::to_value(&schedule).unwrap())
            .unwrap(),
        schedule
    );
    for required in ["available", "session_bound", "role"] {
        let mut wire = serde_json::to_value(&schedule).unwrap();
        wire["candidates"][0]
            .as_object_mut()
            .unwrap()
            .remove(required);
        assert!(serde_json::from_value::<AccountScheduleRequest>(wire).is_err());
    }
    assert_eq!(
        gateway_plugin_sdk::Capability::Scheduler.contract_versions(),
        &[4]
    );
    assert_eq!(
        serde_json::from_value::<AccountScheduleDecision>(json!({
            "decision":"delegate_filtered", "exclude_account_ids":["acct_candidate"]
        }))
        .unwrap(),
        AccountScheduleDecision::DelegateFiltered {
            exclude_account_ids: vec!["acct_candidate".into()]
        }
    );
    assert_eq!(
        serde_json::to_value(AccountScheduleDecision::Pick {
            account_id: "acct_candidate".into(),
        })
        .unwrap(),
        json!({"decision":"pick","account_id":"acct_candidate"})
    );
}
