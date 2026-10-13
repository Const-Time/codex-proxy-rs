//! Native model callbacks use the same request/attempt protocol bridges as ingress.
mod session;

use futures::future::BoxFuture;
use gateway_admin::{plugins::PluginManager, ports::plugins::PluginService};
use gateway_core::{
    engine::{continuation::PreviousResponseId, execution::*, middleware::*},
    error::{GatewayError, GatewayErrorKind},
    lifecycle::CancellationToken,
    operation::{Operation, OperationKind},
    policy::ClientApiKeyId,
    routing::{ProviderCatalogUnavailable, ProviderKind, PublicModelDescriptor, PublicModelId},
};
use std::sync::{Arc, Weak};

/// Composition at the protocol owner. The manager reference is weak because it
/// owns the runtime which calls this service; no second execution owner is created.
pub struct PluginModelExecution {
    execution: Arc<dyn ExecutionService>,
    plugins: Weak<PluginManager>,
}
impl PluginModelExecution {
    fn bind_model_catalog(
        &self,
        client: AuthenticatedClient,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        let registry = self
            .plugins
            .upgrade()
            .and_then(|p| p.middleware_registry())
            .ok_or(ClientAuthenticationError::SnapshotUnavailable)?;
        client.with_model_catalog(
            registry
                .model_catalog()
                .map_err(|_| ClientAuthenticationError::SnapshotUnavailable)?,
        )
    }
    pub fn new(execution: Arc<dyn ExecutionService>, plugins: Weak<PluginManager>) -> Self {
        Self { execution, plugins }
    }
    fn begin(
        &self,
        prepared: PreparedRootExecution,
        operation: Operation,
        metadata: ExecutionRequestMetadata,
        model: Option<PublicModelId>,
        provider: Option<ProviderKind>,
    ) -> BoxFuture<'static, Result<StartedExecution, GatewayError>> {
        let execution = self.execution.clone();
        let registry = self.plugins.upgrade().and_then(|p| p.middleware_registry());
        Box::pin(async move {
            let registry = registry.ok_or_else(|| {
                GatewayError::new(
                    GatewayErrorKind::Internal,
                    "plugin middleware publication is unavailable",
                )
            })?;
            let context = super::root::context(
                &prepared,
                &operation,
                &metadata.endpoint,
                metadata.transport,
                model.as_ref().map(|m| m.as_str().into()),
            );
            let root =
                super::root::bind(prepared, registry, context).map_err(session::gateway_error)?;
            let super::root::RootPlan { prepared, plan, .. } = root;
            if plan.is_empty() {
                return start(execution, prepared, operation, metadata, model, provider).await;
            }
            let request_id = prepared.request_id().clone();
            let created_at = prepared.started_at();
            let stop = prepared.cancellation();
            let stream = metadata.stream;
            let format = if operation.kind() == OperationKind::Generate {
                super::WireFormat::responses(metadata.transport)
            } else {
                super::WireFormat::RawJson
            };
            let request = match &operation {
                Operation::Generate(generate) => {
                    let payload = generate.protocol_payload();
                    let headers = super::headers::from_context(payload.context())
                        .map_err(session::gateway_error)?;
                    MiddlewareRequest::new(
                        payload.protocol(),
                        crate::openai::responses::request::forwarded_request_headers(&headers)
                            .map(|(n, v)| {
                                MiddlewareHeader::new(
                                    n.as_str(),
                                    bytes::Bytes::copy_from_slice(v.as_bytes()),
                                )
                            })
                            .collect(),
                        bytes::Bytes::from(
                            serde_json::to_vec(payload.body()).map_err(|_| session::invalid())?,
                        ),
                    )
                }
                _ => super::raw::request(&operation, &axum::http::HeaderMap::new())
                    .map_err(session::gateway_error)?,
            }
            .with_settings(prepared.settings().clone())
            .with_execution_context(prepared.nested_context());
            let terminal =
                gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
                    Box::pin(async move {
                        let prepared = prepared.with_settings(
                            request
                                .settings()
                                .ok_or(MiddlewareError::InvalidState)?
                                .clone(),
                        )?;
                        let (operation, metadata, model) =
                            rewrite(operation, metadata, model, &request)?;
                        let started =
                            start(execution, prepared, operation, metadata, model, provider)
                                .await?;
                        super::body::ExecutionBody::response(started.session, format).await
                    })
                });
            let response = plan
                .run(request, terminal)
                .await
                .map_err(session::gateway_error)?;
            Ok(StartedExecution {
                request_id,
                created_at,
                stream,
                session: Box::new(session::NativeSession::new(response, format, stop)?),
            })
        })
    }
}
async fn start(
    execution: Arc<dyn ExecutionService>,
    prepared: PreparedRootExecution,
    operation: Operation,
    metadata: ExecutionRequestMetadata,
    model: Option<PublicModelId>,
    provider: Option<ProviderKind>,
) -> Result<StartedExecution, GatewayError> {
    crate::openai::execution::start(async move {
        if let Some(provider) = provider {
            execution
                .start_prepared_provider_endpoint(prepared, provider, operation, metadata)
                .await
        } else {
            execution
                .start_prepared(
                    prepared,
                    PreparedExecutionRequest {
                        public_model: model.ok_or_else(session::invalid)?,
                        operation,
                        metadata,
                    },
                )
                .await
        }
    })
    .await
}
fn rewrite(
    original: Operation,
    mut metadata: ExecutionRequestMetadata,
    model: Option<PublicModelId>,
    request: &MiddlewareRequest,
) -> Result<(Operation, ExecutionRequestMetadata, Option<PublicModelId>), MiddlewareError> {
    if original.kind() != OperationKind::Generate {
        return Ok((super::raw::rewrite(&original, request)?, metadata, model));
    }
    if request.protocol() != "openai" {
        return Err(MiddlewareError::InvalidState);
    }
    let headers = super::headers::filtered_headers(request.headers())?;
    let decoded = crate::openai::responses::decode_request_with_headers(request.body(), &headers)
        .map_err(|_| MiddlewareError::InvalidState)?
        .with_middleware_capabilities(request)?;
    let Operation::Generate(original) = &original else {
        return Err(MiddlewareError::InvalidState);
    };
    if decoded.metadata().stream()
        != original
            .protocol_payload()
            .body()
            .get("stream")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        return Err(MiddlewareError::InvalidState);
    }
    let (operation, decoded) = decoded.into_parts();
    let model = PublicModelId::from_client_wire(decoded.requested_model().to_owned())
        .map_err(|_| MiddlewareError::InvalidState)?;
    metadata.previous_response_id = decoded
        .continuation()
        .previous_response_id()
        .map(|id| PreviousResponseId::new(id.to_owned()));
    Ok((operation, metadata, Some(model)))
}
impl ExecutionService for PluginModelExecution {
    fn authenticate(&self, secret: &str) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        self.bind_model_catalog(self.execution.authenticate(secret)?)
    }
    fn client_access(
        &self,
        client: &AuthenticatedClient,
    ) -> Option<gateway_core::runtime::ClientAccessGuard> {
        self.execution.client_access(client)
    }
    fn authenticate_bound_key(
        &self,
        key: &ClientApiKeyId,
        stop: CancellationToken,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        self.bind_model_catalog(self.execution.authenticate_bound_key(key, stop)?)
    }
    fn public_models(&self, client: &AuthenticatedClient) -> Vec<PublicModelId> {
        self.execution.public_models(client)
    }
    fn client_model_catalog<'a>(
        &'a self,
        client: &'a AuthenticatedClient,
        protocol: &'a str,
        version: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PublicModelDescriptor>, ProviderCatalogUnavailable>> {
        self.execution
            .client_model_catalog(client, protocol, version)
    }
    fn contains_public_model(&self, client: &AuthenticatedClient, model: &PublicModelId) -> bool {
        self.execution.contains_public_model(client, model)
    }
    fn prepare_execution(
        &self,
        client: AuthenticatedClient,
    ) -> Result<PreparedRootExecution, GatewayError> {
        self.execution.prepare_execution(client)
    }
    fn start_prepared(
        &self,
        prepared: PreparedRootExecution,
        request: PreparedExecutionRequest,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.begin(
            prepared,
            request.operation,
            request.metadata,
            Some(request.public_model),
            None,
        )
    }
    fn start_prepared_provider_endpoint(
        &self,
        prepared: PreparedRootExecution,
        provider: ProviderKind,
        operation: Operation,
        metadata: ExecutionRequestMetadata,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.begin(prepared, operation, metadata, None, Some(provider))
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.execution.start(request)
    }
    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        self.execution.start_provider_endpoint(request)
    }
}
