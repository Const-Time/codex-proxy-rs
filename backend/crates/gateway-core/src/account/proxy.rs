//! Explicit account egress. Credentials never appear in Debug or ordinary admin projections.

use std::{fmt, sync::Arc};
use url::Url;

#[derive(Clone)]
pub struct OutboundProxy(
    Url,
    Option<Arc<RequestLocation>>,
    Option<Arc<ProxyRequestContext>>,
);

impl PartialEq for OutboundProxy {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}
impl Eq for OutboundProxy {}
impl std::hash::Hash for OutboundProxy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0, state);
        std::hash::Hash::hash(&self.1, state);
    }
}

/// Snapshot of the selected proxy policy and its last probe, never credentials.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyRequestContext {
    pub proxy_id: String,
    pub mode: String,
    pub detected_ip: Option<String>,
    pub detected_at: Option<String>,
    pub location: Option<RequestLocation>,
}

/// Effective request metadata for this egress, resolved by the proxy store.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLocation {
    pub country: String,
    pub region: String,
    pub city: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid outbound proxy; expected http, https, socks5 or socks5h URL with a host and port")]
pub struct InvalidOutboundProxy;

impl OutboundProxy {
    pub fn parse(value: &str) -> Result<Self, InvalidOutboundProxy> {
        if value.len() > 4096 || value.chars().any(char::is_control) {
            return Err(InvalidOutboundProxy);
        }
        let url = Url::parse(value).map_err(|_| InvalidOutboundProxy)?;
        if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h")
            || url.host_str().is_none()
            || url.port_or_known_default().is_none_or(|port| port == 0)
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(InvalidOutboundProxy);
        }
        Ok(Self(url, None, None))
    }

    #[must_use]
    pub fn expose_url(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn with_location(mut self, location: Option<RequestLocation>) -> Self {
        self.1 = location.map(Arc::new);
        self.2 = None;
        self
    }

    #[must_use]
    pub fn location(&self) -> Option<&RequestLocation> {
        self.1.as_deref()
    }

    #[must_use]
    pub fn with_request_context(mut self, context: Option<ProxyRequestContext>) -> Self {
        self.1 = context
            .as_ref()
            .and_then(|value| value.location.clone())
            .map(Arc::new);
        self.2 = context.map(Arc::new);
        self
    }

    #[must_use]
    pub fn request_context(&self) -> Option<&ProxyRequestContext> {
        self.2.as_deref()
    }

    #[must_use]
    pub fn endpoint(&self) -> String {
        let mut url = self.0.clone();
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.to_string()
    }
}

impl fmt::Debug for OutboundProxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OutboundProxy(<redacted>)")
    }
}
