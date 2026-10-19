#[path = "../src/schema.rs"]
mod implementation;
use serde_json::json;
use std::sync::Arc;

#[test]
fn compiled_schema_is_shared_bounded_and_keeps_live_validators_usable() {
    let value = json!({"type":"integer", "minimum":0});
    let first = implementation::schema(&value).unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &implementation::schema(&value).unwrap()
    ));
    for minimum in 1..=64 {
        implementation::schema(&json!({"type":"integer", "minimum":minimum})).unwrap();
    }
    assert!(first.is_valid(&json!(0)));
    assert!(!Arc::ptr_eq(
        &first,
        &implementation::schema(&value).unwrap()
    ));
    assert!(implementation::schema(&json!({"$ref":"https://example.invalid/schema"})).is_err());
}
