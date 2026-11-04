use gateway_plugin_sdk::call::services::{local_settings_v1 as local, settings as upstream};
use serde_json::json;

#[test]
fn common_settings_actor_contract_keeps_upstream_variants_and_rejects_local_extensions() {
    for actor in [
        json!({"admin_session":{"admin_user_id":"admin"}}),
        json!("admin_api_key"),
        json!("system"),
    ] {
        let input = json!({"actor":actor,"request_id":"host-request"});
        let decoded: upstream::MutationContext = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), input);
    }
    assert!(
        serde_json::from_value::<upstream::MutationContext>(json!({
            "actor":{"plugin":{"instance_id":"plugin_fixture","revision":7}},
            "request_id":"host-request"
        }))
        .is_err()
    );
}

#[test]
fn local_v1_settings_preserve_plugin_audit_identity_and_require_positive_generation() {
    let input = json!({"actor":{"plugin":{"instance_id":"plugin_fixture","revision":7}},"request_id":"host-request"});
    let decoded: local::MutationContext = serde_json::from_value(input.clone()).unwrap();
    assert!(
        matches!(decoded.actor, local::MutationActor::Plugin { revision, .. } if revision.get() == 7)
    );
    assert_eq!(serde_json::to_value(decoded).unwrap(), input);
    assert!(
        serde_json::from_value::<local::MutationContext>(json!({
            "actor":{"plugin":{"instance_id":"plugin_fixture","revision":0}},
            "request_id":"host-request"
        }))
        .is_err()
    );
}

#[test]
fn local_settings_v3_roundtrip_preserves_optional_policies_and_rejects_unknown_fields() {
    use gateway_plugin_sdk::call::services::local_settings_v3 as v3;
    let mut value = json!({"subscription_auto_reset_enabled":false,"model_mappings":{},"refresh_margin_seconds":1800,"refresh_concurrency":4,"max_concurrent_per_account":5,"request_interval_ms":25,"rotation_strategy":"smart","min_codex_desktop_version":null,"min_codex_cli_version":null,"usage_retention_days":31,"ops_event_retention_days":30,"audit_retention_days":90});
    let command: v3::ReplaceRuntimeSettings = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(command.max_retry_after_seconds, None);
    assert_eq!(command.reset_credit_max_weekly_remaining_percent, None);
    value["max_retry_after_seconds"] = json!(0);
    value["reset_credit_max_weekly_remaining_percent"] = json!(null);
    let command: v3::ReplaceRuntimeSettings = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(command.max_retry_after_seconds, Some(0));
    assert_eq!(
        command.reset_credit_max_weekly_remaining_percent,
        Some(None)
    );
    assert_eq!(serde_json::to_value(command).unwrap(), value);
    value["extra"] = json!(true);
    assert!(serde_json::from_value::<v3::ReplaceRuntimeSettings>(value).is_err());
}
