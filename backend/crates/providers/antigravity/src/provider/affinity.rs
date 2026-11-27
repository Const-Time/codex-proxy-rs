use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::{
    account::ProviderAccountId,
    operation::ProtocolPayload,
    policy::ClientApiKeyId,
    provider_ports::{ProviderSessionAffinityKey, ProviderSessionAffinityPort},
    routing::{ProviderKind, UpstreamModelId},
};
use gateway_protocol::{chat_completions::CHAT_PROTOCOL, genai::GENAI_PROTOCOL};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const AFFINITY_TTL: Duration = Duration::from_hours(24);
const STORE_TIMEOUT: Duration = Duration::from_millis(100);

/// Derive the account affinity key without exposing client session data to the store.
pub fn session_key(
    payload: &ProtocolPayload,
    client: &ClientApiKeyId,
    model: &UpstreamModelId,
) -> Option<ProviderSessionAffinityKey> {
    let session = header(payload.context(), "x-http-session-id")
        .or_else(|| header(payload.context(), "x-session-id"))
        .or_else(|| {
            payload
                .context()
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| match payload.protocol() {
            GENAI_PROTOCOL => payload
                .body()
                .get("sessionId")
                .or_else(|| payload.body().get("session_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })?;
    let session = session.trim();
    if !matches!(payload.protocol(), CHAT_PROTOCOL | GENAI_PROTOCOL)
        || session.is_empty()
        || session.len() > 1024
        || session.chars().any(char::is_control)
    {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(b"antigravity-session-affinity-v1\0");
    for part in [client.as_str(), model.as_str(), payload.protocol(), session] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest: String = hash
        .finalize()
        .into_iter()
        .flat_map(|byte| [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 0xf)]])
        .map(char::from)
        .collect();
    ProviderSessionAffinityKey::try_new(digest).ok()
}

/// Convert an isolated affinity key into the Cloud Code numeric session format.
pub fn upstream_session_id(key: &ProviderSessionAffinityKey) -> String {
    // Cloud Code uses a signed decimal FNV-1a session ID.
    let hash = key
        .expose_to_store()
        .bytes()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    (hash as i64).to_string()
}

/// Replace client-controlled session identifiers with the key-scoped upstream ID.
pub fn apply_upstream_session_id(
    body: &mut Map<String, Value>,
    key: Option<&ProviderSessionAffinityKey>,
) {
    body.remove("sessionId");
    body.remove("session_id");
    if let Some(key) = key {
        body.insert("sessionId".into(), upstream_session_id(key).into());
    }
}

fn header(context: &Map<String, Value>, name: &str) -> Option<String> {
    context
        .get("headers")
        .and_then(|headers| headers.get(name))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            context
                .get("opaque_request_headers")?
                .as_array()?
                .iter()
                .find_map(|entry| {
                    let pair = entry.as_array()?;
                    if !pair.first()?.as_str()?.eq_ignore_ascii_case(name) {
                        return None;
                    }
                    String::from_utf8(STANDARD.decode(pair.get(1)?.as_str()?).ok()?).ok()
                })
        })
}

/// A loaded affinity binding with compare-and-bind updates for an eligible replacement.
pub struct SessionAffinity {
    key: ProviderSessionAffinityKey,
    bound: Option<ProviderAccountId>,
}

impl SessionAffinity {
    pub async fn load(
        store: &dyn ProviderSessionAffinityPort,
        provider: &ProviderKind,
        key: ProviderSessionAffinityKey,
    ) -> Option<Self> {
        match tokio::time::timeout(STORE_TIMEOUT, store.load(provider, &key)).await {
            Ok(Ok(bound)) => Some(Self { key, bound }),
            _ => {
                tracing::warn!("Antigravity session affinity could not be loaded");
                None
            }
        }
    }

    pub fn bound(&self) -> Option<&ProviderAccountId> {
        self.bound.as_ref()
    }

    /// Returns the concurrent winner when it differs from the prospective account.
    pub async fn claim(
        &mut self,
        store: &dyn ProviderSessionAffinityPort,
        provider: &ProviderKind,
        selected: &ProviderAccountId,
    ) -> Option<ProviderAccountId> {
        match tokio::time::timeout(
            STORE_TIMEOUT,
            store.claim_or_load(provider, &self.key, selected, AFFINITY_TTL),
        )
        .await
        {
            Ok(Ok(bound)) => {
                let winner = (&bound != selected).then(|| bound.clone());
                self.bound = Some(bound);
                winner
            }
            _ => {
                tracing::warn!("Antigravity session affinity could not be claimed");
                None
            }
        }
    }

    pub async fn record_success(
        &self,
        store: &dyn ProviderSessionAffinityPort,
        provider: &ProviderKind,
        selected: &ProviderAccountId,
    ) {
        let Some(expected) = &self.bound else {
            return;
        };
        if !matches!(
            tokio::time::timeout(
                STORE_TIMEOUT,
                store.compare_and_bind(provider, &self.key, expected, selected, AFFINITY_TTL),
            )
            .await,
            Ok(Ok(_))
        ) {
            tracing::warn!("Antigravity session affinity could not be updated");
        }
    }
}
