//! 诊断内容的统一筛选边界。未知字符串只保留长度和摘要。

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

/// 原始字节的长度与 SHA-256，不复制或保留正文。
#[must_use]
pub fn body_fingerprint(bytes: &[u8]) -> Value {
    let digest = Sha256::digest(bytes);
    let hex: String = digest
        .iter()
        .flat_map(|byte| {
            let alphabet = b"0123456789abcdef";
            [
                char::from(alphabet[usize::from(byte >> 4)]),
                char::from(alphabet[usize::from(byte & 15)]),
            ]
        })
        .collect();
    json!({"bytes": bytes.len(), "sha256": hex})
}

/// 保留可诊断的协议结构；凭据永不进入普通日志或数据库。
#[must_use]
pub fn diagnostic_json(value: &Value) -> Value {
    capture(value, "", 0, &mut 96)
}

fn capture(value: &Value, key: &str, depth: usize, budget: &mut usize) -> Value {
    if *budget == 0 || depth > 6 {
        return json!({"omitted": true});
    }
    *budget -= 1;
    let key = key.to_ascii_lowercase();
    if secret_key(&key) {
        return Value::String("<redacted>".to_owned());
    }
    match value {
        Value::Object(object) => {
            let mut result = Map::new();
            for (name, value) in object.iter().take(48) {
                if *budget == 0 {
                    break;
                }
                result.insert(bounded(name, 96), capture(value, name, depth + 1, budget));
            }
            if result.len() < object.len() {
                result.insert(
                    "_omittedFields".to_owned(),
                    json!(object.len() - result.len()),
                );
            }
            Value::Object(result)
        }
        Value::Array(items) => json!({
            "length": items.len(),
            "sample": items.iter().take(4)
                .map(|item| capture(item, key.as_str(), depth + 1, budget))
                .collect::<Vec<_>>()
        }),
        Value::String(text)
            if safe_header(&key) || safe_value_key(&key) && protocol_label(text) =>
        {
            Value::String(bounded(text, 256))
        }
        Value::String(text) => body_fingerprint(text.as_bytes()),
        other => other.clone(),
    }
}

/// 请求/响应头保持多值；安全 trace ID 原样保存，未知值只保存摘要。
#[must_use]
pub fn diagnostic_headers<'a>(headers: impl IntoIterator<Item = (&'a str, &'a str)>) -> Value {
    let mut result = Map::new();
    for (name, value) in headers.into_iter().take(64) {
        let name = bounded(&name.to_ascii_lowercase(), 96);
        let value = if secret_key(&name) {
            Value::String("<redacted>".to_owned())
        } else if safe_header(&name) {
            Value::String(bounded(value, 256))
        } else {
            body_fingerprint(value.as_bytes())
        };
        let values = result
            .entry(name)
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(values) = values {
            values.push(value);
        }
    }
    Value::Object(result)
}

fn secret_key(key: &str) -> bool {
    key.contains("authorization")
        || key.contains("cookie")
        || key.contains("api_key")
        || key.contains("api-key")
        || key.contains("token") && !key.ends_with("tokens")
        || key.contains("password")
        || key.contains("secret")
        || key.contains("attestation")
        || matches!(key, "x-oai-is" | "x-oai-is-update")
}

fn safe_header(key: &str) -> bool {
    matches!(
        key,
        "x-oai-request-id"
            | "x-client-request-id"
            | "x-request-id"
            | "request-id"
            | "openai-request-id"
            | "x-openai-request-id"
            | "cf-ray"
            | "traceparent"
            | "tracestate"
            | "content-type"
            | "content-length"
            | "content-encoding"
            | "retry-after"
            | "openai-processing-ms"
            | "x-processing-ms"
            | "date"
            | "server"
            | "sec-websocket-extensions"
            | "x-codex-allowed"
            | "x-codex-limit-reached"
            | "x-codex-active-limit"
            | "x-models-etag"
    )
}

fn safe_value_key(key: &str) -> bool {
    safe_header(key)
        || matches!(
            key,
            "type" | "event" | "status" | "code" | "model" | "role" | "object" | "service_tier"
        )
}

pub(super) fn bounded(value: &str, limit: usize) -> String {
    value
        .chars()
        .take(limit)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Event names/codes are protocol labels, never arbitrary upstream text.
pub(super) fn protocol_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}
