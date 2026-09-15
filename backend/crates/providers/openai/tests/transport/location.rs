use chrono::{TimeZone, Utc};
use gateway_core::account::RequestLocation;
use provider_openai::transport::location::apply_request_location;
use serde_json::json;

#[test]
fn proxy_location_rewrites_only_structured_client_environment_and_search() {
    let now = Utc.with_ymd_and_hms(2026, 9, 15, 1, 0, 0).unwrap();
    let environment = "<environment_context><current_date>2026-09-15</current_date><timezone>Asia/Shanghai</timezone><cwd>/keep/me</cwd></environment_context>";
    let original = json!({
        "input": [
            {"role": "user", "content": [{"type":"input_text","text":environment}],
             "internal_chat_message_metadata_passthrough": {"content_item_kinds": ["environments.environment_context"]}},
            {"role": "user", "content": [{"type":"input_text","text":environment}]},
            {"role": "developer", "content": [{"type":"input_text","text":environment}],
             "internal_chat_message_metadata_passthrough": {"content_item_kinds": ["environments.environment_context"]}}
        ],
        "tools": [{"type":"web_search"}, {"type":"function","name":"unchanged"}],
        "timestamp": 1789434000
    }).as_object().unwrap().clone();
    let mut body = original.clone();
    apply_request_location(&mut body, None, now);
    assert_eq!(body, original);
    let mut location = RequestLocation {
        country: "US".to_owned(),
        region: "California".to_owned(),
        city: "Los Angeles".to_owned(),
        timezone: "America/Los_Angeles".to_owned(),
    };
    apply_request_location(&mut body, Some(&location), now);
    assert!(
        body["input"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("<current_date>2026-09-14</current_date>")
    );
    assert!(
        body["input"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("<cwd>/keep/me</cwd>")
    );
    assert_eq!(body["input"][1], original["input"][1]);
    assert_eq!(body["input"][2], original["input"][2]);
    assert_eq!(
        body["tools"][0]["user_location"]["timezone"],
        "America/Los_Angeles"
    );
    assert_eq!(body["tools"][1], original["tools"][1]);
    assert_eq!(body["timestamp"], original["timestamp"]);
    location.timezone = "not/a/zone".to_owned();
    let unchanged = body.clone();
    apply_request_location(&mut body, Some(&location), now);
    assert_eq!(body, unchanged);
}
