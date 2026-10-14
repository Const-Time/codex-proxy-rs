//! One-time login grants retain their administrator only in the native pending-flow store.
use super::{PluginHttpRequest, PluginInvocationContext};
use crate::model::MutationContext;
use gateway_core::engine::middleware::MiddlewareHeader;
use std::sync::Arc;

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCallbackTicket {
    pub state: String,
    pub expires_at_ms: i64,
}

pub struct PluginCallbackDescriptor {
    pub incarnation: String,
}

/// Host composition builds a service origin only after the stored actor is verified.
pub type PluginCallbackInvocation =
    Arc<dyn Fn(&MutationContext) -> Option<PluginInvocationContext> + Send + Sync>;
pub struct PluginCallbackRequest {
    pub request_id: String,
    pub query: String,
    pub headers: Vec<MiddlewareHeader>,
    pub invocation: PluginCallbackInvocation,
}

pub struct PluginCallbackCall {
    pub incarnation: String,
    pub request: PluginHttpRequest,
    pub headers: Vec<MiddlewareHeader>,
}
pub struct PluginCallbackResponse {
    pub response: super::PluginHttpResponse,
    pub headers: Vec<MiddlewareHeader>,
}
