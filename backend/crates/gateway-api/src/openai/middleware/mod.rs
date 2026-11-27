//! Request middleware keeps Core's admission, atomic delivery and cleanup owners.
mod attempt;
mod body;
pub(in crate::openai) mod delivery;
mod headers;
mod messages_sse;
mod native;
pub(super) mod raw;
pub(super) mod request;
mod root;
pub(super) mod websocket;

pub use attempt::raw_json_attempt_middleware;
pub use delivery::deliver_middleware_response;
use gateway_core::engine::{
    execution::ExecutionSession,
    middleware::{MiddlewareError, MiddlewareResponse},
};
pub use native::PluginModelExecution;

/// Build an uncommitted response for a middleware terminal. Dropping the response
/// delegates cancellation and settlement to the existing PendingExecution owner.
pub async fn execution_middleware_response(
    session: Box<dyn ExecutionSession>,
    transport: gateway_core::engine::execution::ClientTransport,
) -> Result<MiddlewareResponse, MiddlewareError> {
    body::ExecutionBody::response(session, WireFormat::responses(transport)).await
}

pub(in crate::openai) async fn generation_response(
    session: Box<dyn ExecutionSession>,
    transport: gateway_core::engine::execution::ClientTransport,
    protocol: &'static str,
    chat: Option<crate::openai::chat_completions::ChatProjection>,
) -> Result<MiddlewareResponse, MiddlewareError> {
    let format = WireFormat::generation(transport, protocol);
    body::ExecutionBody::generation(session, format, chat).await
}

/// Bind the Responses wire adapter to a frozen, root-scoped publication snapshot.
/// Core remains the sole owner of provider execution and original receipts.
pub fn responses_attempt_middleware(
    registry: std::sync::Arc<gateway_admin::plugins::middleware::PluginMiddlewareRegistry>,
    mut context: gateway_core::engine::middleware::MiddlewareContext,
) -> std::sync::Arc<dyn gateway_core::engine::attempt_middleware::AttemptMiddleware> {
    context.mount = gateway_core::engine::middleware::MiddlewareMount::Attempt;
    let format = WireFormat::responses(context.transport);
    std::sync::Arc::new(attempt::ResponsesAttempt {
        registry,
        context,
        format,
    })
}

#[derive(Clone, Copy)]
pub(super) enum WireFormat {
    Responses {
        streaming: bool,
    },
    Generation {
        streaming: bool,
        protocol: &'static str,
    },
    RawJson,
    WebSocket,
}
impl WireFormat {
    fn protocol(self) -> &'static str {
        match self {
            Self::Generation { protocol, .. } => protocol,
            _ => "openai",
        }
    }
    fn generation(
        transport: gateway_core::engine::execution::ClientTransport,
        protocol: &'static str,
    ) -> Self {
        if protocol == "openai" {
            return Self::responses(transport);
        }
        Self::Generation {
            streaming: transport == gateway_core::engine::execution::ClientTransport::HttpSse,
            protocol,
        }
    }

    fn responses(transport: gateway_core::engine::execution::ClientTransport) -> Self {
        use gateway_core::engine::execution::ClientTransport;
        match transport {
            ClientTransport::WebSocket => Self::WebSocket,
            ClientTransport::HttpSse => Self::Responses { streaming: true },
            ClientTransport::HttpJson | ClientTransport::InternalProbe => {
                Self::Responses { streaming: false }
            }
        }
    }
    fn streaming(self) -> bool {
        matches!(
            self,
            Self::Responses { streaming: true }
                | Self::Generation {
                    streaming: true,
                    ..
                }
                | Self::WebSocket
        )
    }
    fn websocket(self) -> bool {
        matches!(self, Self::WebSocket)
    }
    fn raw_json(self) -> bool {
        matches!(self, Self::RawJson)
    }
}
