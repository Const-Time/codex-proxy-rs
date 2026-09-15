//! Bounded allowlisted facts at the prepared upstream boundary; no raw bodies or credentials.
use gateway_core::{
    account::OutboundProxy,
    diagnostics::{TraceContext, body_fingerprint},
};
use serde_json::{Map, Value, json};

pub fn record_request_profile<'a>(
    trace: &TraceContext,
    transport: &'static str,
    compression: &'static str,
    proxy: Option<&OutboundProxy>,
    headers: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    body: Option<&Map<String, Value>>,
) {
    if !trace.is_enabled() {
        return;
    }
    let mut visible = Map::new();
    let mut identifiers = Map::new();
    let mut presence: Map<String, Value> =
        ["authorization", "cookie", "x-oai-is", "x-oai-is-update"]
            .into_iter()
            .map(|name| (name.to_owned(), Value::Bool(false)))
            .collect();
    for (name, bytes) in headers {
        let name = name.to_ascii_lowercase();
        match name.as_str() {
            "user-agent"
            | "originator"
            | "version"
            | "openai-beta"
            | "content-type"
            | "content-encoding"
            | "sec-websocket-extensions" => {
                visible.insert(
                    name,
                    Value::String(String::from_utf8_lossy(bytes).chars().take(128).collect()),
                );
            }
            "session-id"
            | "thread-id"
            | "x-codex-window-id"
            | "x-client-request-id"
            | "x-codex-routing-hint"
            | "x-codex-turn-metadata"
            | "x-codex-turn-state"
            | "x-codex-installation-id"
            | "x-codex-parent-thread-id"
            | "chatgpt-account-id" => {
                identifiers.insert(name, body_fingerprint(bytes));
            }
            "authorization" | "cookie" | "x-oai-is" | "x-oai-is-update" => {
                presence.insert(name, Value::Bool(!bytes.is_empty()));
            }
            _ => {}
        }
    }
    let compression = if transport == "websocket" {
        if visible
            .get("sec-websocket-extensions")
            .and_then(Value::as_str)
            .is_some_and(|value| value.contains("permessage-deflate"))
        {
            "permessage-deflate_requested"
        } else {
            "none_requested"
        }
    } else {
        compression
    };
    trace.record(
        "upstream.request.profile",
        json!({
            "phase": "prepared_request",
            "transport": transport,
            "compression": compression,
            "viaProxy": proxy.is_some(),
            "proxy": proxy.and_then(OutboundProxy::request_context),
            "effectiveLocation": proxy.and_then(OutboundProxy::location),
            "headers": visible,
            "identifierDigests": identifiers,
            "sensitiveFieldsPresent": presence,
            "structuredLocation": body.map(structured_location),
        }),
    );
}

fn structured_location(body: &Map<String, Value>) -> Value {
    let mut environments = Vec::new();
    let mut searches = Vec::new();
    if let Some(input) = body.get("input").and_then(Value::as_array) {
        for item in input {
            if item.get("role").and_then(Value::as_str) != Some("user") {
                continue;
            }
            let kinds = item
                .pointer("/internal_chat_message_metadata_passthrough/content_item_kinds")
                .and_then(Value::as_array);
            if let Some(parts) = item.get("content").and_then(Value::as_array) {
                for (index, part) in parts.iter().enumerate() {
                    if environments.len() >= 2 {
                        break;
                    }
                    if kinds
                        .and_then(|kinds| kinds.get(index))
                        .and_then(Value::as_str)
                        != Some("environments.environment_context")
                    {
                        continue;
                    }
                    if part.get("type").and_then(Value::as_str) != Some("input_text") {
                        continue;
                    }
                    let Some(text) = part
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|text| text.len() <= 64 * 1024)
                    else {
                        continue;
                    };
                    let Ok(document) = roxmltree::Document::parse(text) else {
                        continue;
                    };
                    let root = document.root_element();
                    if !root.has_tag_name("environment_context") {
                        continue;
                    }
                    let mut fields = Map::new();
                    for node in root.children().filter(roxmltree::Node::is_element) {
                        let name = node.tag_name().name();
                        if matches!(name, "timezone" | "current_date") {
                            fields.insert(
                                name.to_owned(),
                                Value::String(
                                    node.text().unwrap_or_default().chars().take(48).collect(),
                                ),
                            );
                        }
                    }
                    if !fields.is_empty() {
                        environments.push(fields);
                    }
                }
            }
        }
    }
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        for tool in tools {
            if searches.len() >= 2 {
                break;
            }
            let kind = tool.get("type").and_then(Value::as_str).unwrap_or_default();
            if kind != "web_search" && !kind.starts_with("web_search_") {
                continue;
            }
            let mut fields = Map::new();
            if let Some(location) = tool.get("user_location").and_then(Value::as_object) {
                for key in ["country", "region", "city", "timezone"] {
                    if let Some(value) = location.get(key).and_then(Value::as_str) {
                        fields.insert(
                            key.to_owned(),
                            Value::String(value.chars().take(48).collect()),
                        );
                    }
                }
            }
            searches.push(fields);
        }
    }
    json!({"environments": environments, "searches": searches, "maxEntriesPerKind": 2})
}
