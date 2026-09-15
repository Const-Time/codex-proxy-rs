//! Proxy-selected request metadata. Adapted from upstream #88 and cc2dfab1.
use chrono::{DateTime, Utc};
use gateway_core::account::RequestLocation;
use roxmltree::Document;
use serde_json::{Map, Value, json};
const ENVIRONMENT_CONTEXT_CONTENT_KIND: &str = "environments.environment_context";
struct CodexRequestLocation {
    country: String,
    region: String,
    city: String,
    timezone: chrono_tz::Tz,
}
/// None means preserve the client payload, including missing fields.
pub fn apply_request_location(
    body: &mut Map<String, Value>,
    location: Option<&RequestLocation>,
    now: DateTime<Utc>,
) {
    let Some(location) = location else { return };
    let Ok(timezone) = location.timezone.parse() else {
        return;
    };
    let location = CodexRequestLocation {
        country: location.country.clone(),
        region: location.region.clone(),
        city: location.city.clone(),
        timezone,
    };
    align_structured_location_fields(body, now, &location);
}
fn align_structured_location_fields(
    body: &mut Map<String, Value>,
    now: DateTime<Utc>,
    location: &CodexRequestLocation,
) {
    // 只改写带环境标记的日期和时区；epoch 时间戳保持绝对时间原值。
    let current_date = now
        .with_timezone(&location.timezone)
        .format("%Y-%m-%d")
        .to_string();
    if let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) {
        for item in input {
            align_environment_context(item, &current_date, location.timezone.name());
        }
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            align_web_search_location(tool, location);
        }
    }
}

fn align_environment_context(item: &mut Value, current_date: &str, timezone: &str) {
    let Some(item) = item.as_object_mut() else {
        return;
    };
    if item.get("role").and_then(Value::as_str) != Some("user") {
        return;
    }
    let content_kinds = item
        .get("internal_chat_message_metadata_passthrough")
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("content_item_kinds"))
        .and_then(Value::as_array)
        .map(|kinds| {
            kinds
                .iter()
                .map(|kind| kind.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        });
    let Some(content) = item.get_mut("content").and_then(Value::as_array_mut) else {
        return;
    };
    for (index, part) in content.iter_mut().enumerate() {
        if content_kinds
            .as_ref()
            .and_then(|kinds| kinds.get(index))
            .and_then(Option::as_deref)
            != Some(ENVIRONMENT_CONTEXT_CONTENT_KIND)
        {
            continue;
        }
        let Some(part) = part.as_object_mut() else {
            continue;
        };
        if part.get("type").and_then(Value::as_str) != Some("input_text") {
            continue;
        }
        let Some(Value::String(text)) = part.get_mut("text") else {
            continue;
        };
        if let Some(aligned) = aligned_environment_context(text, current_date, timezone) {
            *text = aligned;
        }
    }
}

fn aligned_environment_context(text: &str, current_date: &str, timezone: &str) -> Option<String> {
    let trimmed = text.trim();
    if !trimmed.starts_with("<environment_context>") || !trimmed.ends_with("</environment_context>")
    {
        return None;
    }
    let document = Document::parse(text).ok()?;
    let root = document.root_element();
    if !root.has_tag_name("environment_context") {
        return None;
    }
    let mut replacements = root
        .children()
        .filter(|node| node.is_element())
        .filter_map(|node| {
            let replacement = match node.tag_name().name() {
                "current_date" => format!("<current_date>{current_date}</current_date>"),
                "timezone" => format!("<timezone>{timezone}</timezone>"),
                _ => return None,
            };
            Some((node.range(), replacement))
        })
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        return None;
    }
    replacements.sort_unstable_by_key(|(range, _)| std::cmp::Reverse(range.start));
    let mut aligned = text.to_owned();
    for (range, replacement) in replacements {
        aligned.replace_range(range, &replacement);
    }
    Some(aligned)
}

fn align_web_search_location(tool: &mut Value, location: &CodexRequestLocation) {
    let Some(tool) = tool.as_object_mut() else {
        return;
    };
    let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
        return;
    };
    if tool_type != "web_search" && !tool_type.starts_with("web_search_") {
        return;
    }
    tool.insert(
        "user_location".to_owned(),
        json!({
            "type": "approximate",
            "country": location.country,
            "region": location.region,
            "city": location.city,
            "timezone": location.timezone.name(),
        }),
    );
}
