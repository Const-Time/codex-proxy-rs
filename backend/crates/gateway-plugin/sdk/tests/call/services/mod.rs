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
