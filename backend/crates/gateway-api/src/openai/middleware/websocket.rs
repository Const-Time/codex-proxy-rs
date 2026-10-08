//! Prepared WebSocket attempts use the existing Core session and WS forwarder.
use crate::openai::{responses::DecodedResponsesRequest, service::OpenAiService};
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    engine::execution::{AuthenticatedClient, ClientTransport, StartedExecution},
    error::{GatewayError, GatewayErrorKind},
};

pub(in crate::openai) async fn start(
    service: &OpenAiService,
    registry: PluginMiddlewareRegistry,
    client: AuthenticatedClient,
    decoded: DecodedResponsesRequest,
) -> Result<(StartedExecution, gateway_core::lifecycle::CancellationToken), GatewayError> {
    let prepared = service.prepare_execution(client)?;
    let context = super::root::context(
        &prepared,
        decoded.operation(),
        "/v1/responses",
        ClientTransport::WebSocket,
        Some(decoded.metadata().requested_model().into()),
    );
    let root = super::root::bind(prepared, registry, context).map_err(|_| {
        GatewayError::new(
            GatewayErrorKind::Internal,
            "plugin middleware is unavailable",
        )
    })?;
    // Never silently bypass a required layer while its WS delivery is not connected.
    if !root.plan.is_empty() {
        return Err(GatewayError::new(
            GatewayErrorKind::Unsupported,
            "WebSocket request middleware is not available yet",
        ));
    }
    let started = service
        .start_prepared_response(
            root.prepared,
            decoded,
            ClientTransport::WebSocket,
            "/v1/responses",
        )
        .await?;
    Ok((started, root.revocation))
}
