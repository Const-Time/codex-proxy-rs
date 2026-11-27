use gateway_core::account::OpaqueProviderData;
use provider_antigravity::Selection;
use serde_json::json;

use super::configuration;

#[test]
fn selection_rejects_injected_versions_and_unexpected_fields() {
    let invalid = configuration("desktop", "macos", Some("1.2.3\r\nInjected: value"));
    assert!(Selection::parse(&invalid).is_err());

    let mut invalid = configuration("desktop", "macos", None).into_inner();
    invalid.insert("oauthClientSecret".into(), json!("fixture"));
    assert!(Selection::parse(&OpaqueProviderData::new(invalid)).is_err());
}
