//! Opt-in wire identity projection. Business input and local affinity keys are never rewritten.

use serde_json::{Map, Value};
use uuid::Uuid;

use super::{
    protocol::responses::CodexResponsesRequest, request::encode_turn_metadata, session::hmac_sha256,
};

/// Derive per-account, per-client identities with stable UUID/composite shapes.
/// The caller must apply the account boundary first and retain original local turn/affinity IDs.
pub fn converge_request(
    request: &mut CodexResponsesRequest,
    key: &[u8; 32],
    account: &str,
    client: &str,
) {
    let scope = FingerprintScope {
        key,
        account,
        client,
    };
    let header = |name: &str| {
        request
            .passthrough_headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let raw_metadata = request
        .turn_metadata
        .clone()
        .or_else(|| header("x-codex-turn-metadata"));
    let embedded = request
        .turn_metadata
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Map<String, Value>>(raw).ok())
        .or_else(|| {
            raw_metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
        })
        .or_else(|| {
            request
                .client_metadata()?
                .get("x-codex-turn-metadata")?
                .as_str()
                .and_then(|raw| serde_json::from_str::<Map<String, Value>>(raw).ok())
        });
    let evidence = |names: &[&str]| -> Option<String> {
        names
            .iter()
            .find_map(|name| {
                request
                    .client_metadata()?
                    .get(*name)?
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
            })
            .or_else(|| {
                names.iter().find_map(|name| {
                    embedded
                        .as_ref()?
                        .get(*name)?
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                })
            })
    };
    let session = request
        .client_session_id
        .clone()
        .or_else(|| header("session-id"))
        .or_else(|| evidence(&["session_id", "session-id"]));
    let thread = request
        .client_thread_id
        .clone()
        .or_else(|| header("thread-id"))
        .or_else(|| evidence(&["thread_id", "thread-id"]))
        .or_else(|| session.clone());
    let turn = request
        .client_turn_id
        .clone()
        .or_else(|| evidence(&["turn_id"]));
    let parent = request
        .parent_thread_id
        .clone()
        .or_else(|| header("x-codex-parent-thread-id"))
        .or_else(|| evidence(&["parent_thread_id"]));
    let window = request
        .codex_window_id
        .clone()
        .or_else(|| header("x-codex-window-id"))
        .or_else(|| evidence(&["window_id", "x-codex-window-id"]));
    let projected_session = session.as_deref().map(|v| scope.id("conversation", v));
    let projected_thread = thread.as_deref().map(|v| scope.id("conversation", v));
    let projected_turn = turn.as_deref().map(|v| scope.id("turn", v));
    let projected_parent = parent.as_deref().map(|v| scope.id("conversation", v));
    let projected_window = window.as_deref().map(|v| scope.window(v));
    let projected_request = projected_thread.clone().or_else(|| {
        request
            .client_request_id
            .clone()
            .or_else(|| header("x-client-request-id"))
            .map(|v| scope.id("conversation", &v))
    });
    let identities: &[(&[&str], &Option<String>)] = &[
        (&["session_id", "session-id"], &projected_session),
        (&["thread_id", "thread-id"], &projected_thread),
        (&["x-client-request-id"], &projected_request),
        (&["turn_id"], &projected_turn),
        (
            &[
                "parent_thread_id",
                "parentThreadId",
                "x-codex-parent-thread-id",
            ],
            &projected_parent,
        ),
        (
            &["window_id", "x-codex-window-id", "codexWindowId"],
            &projected_window,
        ),
    ];

    if let Some(cache) = request.prompt_cache_key().map(str::to_owned) {
        let projected = if session.as_deref() == Some(cache.as_str()) {
            projected_session.clone().expect("session evidence")
        } else {
            scope.cache_key(&cache)
        };
        request
            .body_mut()
            .insert("prompt_cache_key".to_owned(), Value::String(projected));
    }
    scope.project_map(request.body_mut());
    align_carriers(request.body_mut(), identities);
    if let Some(metadata) = request
        .body_mut()
        .get_mut("client_metadata")
        .and_then(Value::as_object_mut)
    {
        scope.project_map(metadata);
        align_carriers(metadata, identities);
    }
    request.turn_metadata = raw_metadata.as_deref().and_then(|raw| {
        let Ok(mut metadata) = serde_json::from_str::<Map<String, Value>>(raw) else {
            return Some(raw.to_owned());
        };
        scope.project_map(&mut metadata);
        align_leaves(&mut metadata, identities);
        metadata.remove("tool_namespaces_info"); // Header-only compatibility projection.
        encode_turn_metadata(&metadata)
    });
    request.client_session_id = projected_session;
    request.client_thread_id = projected_thread;
    request.client_request_id = projected_request;
    request.client_turn_id = projected_turn;
    request.parent_thread_id = projected_parent;
    request.codex_window_id = projected_window;
    request.client_conversation_id = request
        .client_conversation_id
        .as_deref()
        .map(|v| scope.id("conversation", v));
    for name in [
        "session-id",
        "thread-id",
        "x-client-request-id",
        "x-codex-window-id",
        "x-codex-parent-thread-id",
        "x-codex-turn-metadata",
    ] {
        request.passthrough_headers.remove(name);
    }
    if request.client_session_id.is_some() {
        for name in ["session_id", "conversation_id"] {
            request.passthrough_headers.remove(name);
        }
    }
    request.fingerprint_convergence = true;
}

// Align only declared protocol carriers; never walk input/tools/arbitrary nested objects.
fn align_carriers(metadata: &mut Map<String, Value>, identities: &[(&[&str], &Option<String>)]) {
    align_leaves(metadata, identities);
    for name in ["turnMetadata", "turn_metadata", "x-codex-turn-metadata"] {
        if let Some(raw) = metadata.get(name).and_then(Value::as_str)
            && let Ok(mut embedded) = serde_json::from_str::<Map<String, Value>>(raw)
        {
            align_leaves(&mut embedded, identities);
            if let Some(encoded) = encode_turn_metadata(&embedded) {
                metadata.insert(name.to_owned(), Value::String(encoded));
            }
        }
    }
}

fn align_leaves(metadata: &mut Map<String, Value>, identities: &[(&[&str], &Option<String>)]) {
    for &(names, value) in identities {
        for name in names {
            if metadata.get(*name).is_some_and(Value::is_string)
                && let Some(value) = value
            {
                metadata.insert((*name).to_owned(), Value::String(value.clone()));
            }
        }
    }
}

struct FingerprintScope<'a> {
    key: &'a [u8; 32],
    account: &'a str,
    client: &'a str,
}

impl FingerprintScope<'_> {
    fn id(&self, kind: &str, raw: &str) -> String {
        let parsed = Uuid::parse_str(raw).ok();
        let canonical = parsed.map_or_else(|| raw.to_owned(), |id| id.to_string());
        let hash = hmac_sha256(
            self.key,
            &[
                b"codex-wire/v1\0",
                self.account.as_bytes(),
                b"\0",
                self.client.as_bytes(),
                b"\0",
                kind.as_bytes(),
                b"\0",
                canonical.as_bytes(),
            ],
        );
        let mut bytes: [u8; 16] = hash[..16].try_into().expect("SHA256 prefix");
        if let Some(original) = parsed.filter(|id| id.get_version_num() == 7) {
            bytes[..6].copy_from_slice(&original.as_bytes()[..6]);
            bytes[6] = (bytes[6] & 0x0f) | 0x70;
        } else {
            bytes[6] = (bytes[6] & 0x0f) | 0x40;
        }
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes).to_string()
    }

    fn window(&self, raw: &str) -> String {
        if let Some((thread, ordinal)) = raw.rsplit_once(':')
            && Uuid::parse_str(thread).is_ok()
            && !ordinal.is_empty()
            && ordinal.len() <= 19
            && ordinal.bytes().all(|b| b.is_ascii_digit())
        {
            return format!("{}:{ordinal}", self.id("conversation", thread));
        }
        self.id("window", raw)
    }

    fn cache_key(&self, raw: &str) -> String {
        if let Some((prefix, id)) = raw.split_once(':')
            && !prefix.is_empty()
            && prefix.len() <= 64
            && prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            && Uuid::parse_str(id).is_ok()
        {
            return format!("{prefix}:{}", self.id("prompt-cache", id));
        }
        self.id("prompt-cache", raw)
    }

    fn project_map(&self, metadata: &mut Map<String, Value>) {
        for (name, value) in metadata {
            let Some(raw) = value.as_str() else {
                continue;
            };
            let projected = match name.as_str() {
                "session_id"
                | "session-id"
                | "thread_id"
                | "thread-id"
                | "conversation_id"
                | "parent_thread_id"
                | "x-codex-parent-thread-id"
                | "forked_from_thread_id"
                | "x-client-request-id" => self.id("conversation", raw),
                "turn_id" | "root_turn_id" | "parent_turn_id" => self.id("turn", raw),
                "window_id" | "x-codex-window-id" => self.window(raw),
                "context_window_id" => self.id("context-window", raw),
                "turnMetadata" | "turn_metadata" | "x-codex-turn-metadata" => {
                    let Ok(mut embedded) = serde_json::from_str::<Map<String, Value>>(raw) else {
                        continue;
                    };
                    // Only known identity leaves: no recursive walk through arbitrary user content.
                    for key in [
                        "session_id",
                        "thread_id",
                        "parent_thread_id",
                        "forked_from_thread_id",
                        "turn_id",
                        "root_turn_id",
                        "parent_turn_id",
                        "window_id",
                        "context_window_id",
                    ] {
                        if let Some(Value::String(raw)) = embedded.get(key) {
                            let next = match key {
                                "turn_id" | "root_turn_id" | "parent_turn_id" => {
                                    self.id("turn", raw)
                                }
                                "window_id" => self.window(raw),
                                "context_window_id" => self.id("context-window", raw),
                                _ => self.id("conversation", raw),
                            };
                            embedded.insert(key.to_owned(), Value::String(next));
                        }
                    }
                    let Some(encoded) = encode_turn_metadata(&embedded) else {
                        continue;
                    };
                    encoded
                }
                _ => continue,
            };
            *value = Value::String(projected);
        }
    }
}
