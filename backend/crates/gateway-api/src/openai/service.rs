//! OpenAI wire adapter 到 Core 执行用例的唯一映射。

use std::net::IpAddr;
use std::sync::Arc;

use gateway_core::engine::continuation::PreviousResponseId;
use gateway_core::engine::execution::{
    AuthenticatedClient, ClientAuthenticationError, ClientTransport, ExecutionRequestMetadata,
    ExecutionService, StartExecution, StartProviderExecution, StartedExecution,
};
use gateway_core::error::{GatewayError, GatewayErrorKind};
use gateway_core::lifecycle::{ConnectionDraining, ConnectionGuard, ConnectionLifecycle};
use gateway_core::routing::{ProviderCatalogUnavailable, PublicModelDescriptor, PublicModelId};
use uuid::Uuid;

use super::auth::ClientApiKeyAuthError;
use super::responses::{ContinuationIntent, DecodedResponsesRequest};

/// OpenAI HTTP/WS adapter 共享的 Core 与连接生命周期能力。
#[derive(Clone)]
pub(crate) struct OpenAiService {
    administration: Option<gateway_admin::AdminServices>,
    execution: Arc<dyn ExecutionService>,
    lifecycle: Arc<dyn ConnectionLifecycle>,
}

impl OpenAiService {
    #[must_use]
    pub(crate) const fn new(
        execution: Arc<dyn ExecutionService>,
        lifecycle: Arc<dyn ConnectionLifecycle>,
    ) -> Self {
        Self {
            administration: None,
            execution,
            lifecycle,
        }
    }

    pub(crate) fn with_administration(
        mut self,
        administration: gateway_admin::AdminServices,
    ) -> Self {
        self.administration = Some(administration);
        self
    }
    pub(crate) async fn authenticate_ingress(
        &self,
        authorization: &str,
        builtin: Option<&str>,
    ) -> Result<AuthenticatedClient, ClientApiKeyAuthError> {
        use gateway_admin::model::plugins::PluginAuthenticationDecision;
        if let Some(plugins) = self.administration.as_ref().and_then(|a| a.plugins().ok()) {
            let decision = plugins
                .authenticate(authorization.to_owned())
                .await
                .map_err(|_| ClientApiKeyAuthError::RuntimeUnavailable)?;
            return match decision {
                PluginAuthenticationDecision::Authenticated { key, access } => self
                    .execution
                    .authenticate_bound_key(&key, access.cancellation())
                    .map_err(map_authentication_error),
                PluginAuthenticationDecision::Builtin { access } => self
                    .execution
                    .authenticate_revocable(
                        builtin.ok_or(ClientApiKeyAuthError::InvalidKeyFormat)?,
                        access.cancellation(),
                    )
                    .map_err(map_authentication_error),
                PluginAuthenticationDecision::Rejected => Err(ClientApiKeyAuthError::InvalidKey),
            };
        }
        self.authenticate(builtin.ok_or(ClientApiKeyAuthError::InvalidKeyFormat)?)
    }
    pub(crate) fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientApiKeyAuthError> {
        self.execution
            .authenticate(plaintext)
            .map_err(map_authentication_error)
    }

    pub(crate) fn client_access(
        &self,
        client: &AuthenticatedClient,
    ) -> Option<gateway_core::runtime::ClientAccessGuard> {
        self.execution.client_access(client)
    }
    pub(crate) fn public_models(&self, client: &AuthenticatedClient) -> Vec<String> {
        self.execution
            .public_models(client)
            .into_iter()
            .map(|model| model.as_str().to_owned())
            .collect()
    }

    pub(crate) async fn client_model_catalog(
        &self,
        client: &AuthenticatedClient,
        client_version: &str,
    ) -> Result<Vec<PublicModelDescriptor>, ProviderCatalogUnavailable> {
        self.execution
            .client_model_catalog(client, "codex", client_version)
            .await
    }

    pub(crate) fn contains_public_model(
        &self,
        client: &AuthenticatedClient,
        model: &PublicModelId,
    ) -> bool {
        self.execution.contains_public_model(client, model)
    }

    pub(crate) async fn start_response(
        &self,
        client: AuthenticatedClient,
        request: DecodedResponsesRequest,
        transport: ClientTransport,
        endpoint: &'static str,
    ) -> Result<StartedExecution, GatewayError> {
        let (operation, metadata) = request.into_parts();
        let public_model = PublicModelId::from_client_wire(metadata.requested_model().to_owned())
            .map_err(|_| {
            GatewayError::new(
                GatewayErrorKind::ModelNotFound,
                "requested model was not found",
            )
        })?;
        let previous_response_id = match metadata.continuation() {
            ContinuationIntent::None => None,
            ContinuationIntent::PreviousResponseId(value) => {
                Some(PreviousResponseId::new(value.clone()))
            }
        };
        let execution = Arc::clone(&self.execution);
        super::execution::start(async move {
            execution
                .start(StartExecution {
                    client,
                    public_model,
                    operation,
                    metadata: ExecutionRequestMetadata {
                        protocol: "openai".to_owned(),
                        endpoint: endpoint.to_owned(),
                        transport,
                        stream: metadata.stream(),
                        client_ip: metadata.client_ip(),
                        user_agent: metadata.user_agent().map(str::to_owned),
                        previous_response_id,
                    },
                })
                .await
        })
        .await
    }

    pub(crate) async fn start_provider_endpoint(
        &self,
        client: AuthenticatedClient,
        operation: gateway_core::operation::Operation,
        client_ip: Option<IpAddr>,
        user_agent: Option<String>,
        endpoint: &'static str,
    ) -> Result<StartedExecution, GatewayError> {
        let provider = gateway_core::routing::ProviderKind::new("openai").map_err(|_| {
            GatewayError::new(
                GatewayErrorKind::Internal,
                "OpenAI provider identifier is invalid",
            )
        })?;
        let execution = Arc::clone(&self.execution);
        super::execution::start(async move {
            execution
                .start_provider_endpoint(StartProviderExecution {
                    client,
                    provider,
                    operation,
                    metadata: ExecutionRequestMetadata {
                        protocol: "openai".to_owned(),
                        endpoint: endpoint.to_owned(),
                        transport: ClientTransport::HttpJson,
                        stream: false,
                        client_ip,
                        user_agent,
                        previous_response_id: None,
                    },
                })
                .await
        })
        .await
    }

    pub(crate) fn try_register_connection(
        &self,
    ) -> Result<Box<dyn ConnectionGuard>, ConnectionDraining> {
        self.lifecycle.try_register()
    }

    #[must_use]
    pub(crate) fn lifecycle(&self) -> Arc<dyn ConnectionLifecycle> {
        Arc::clone(&self.lifecycle)
    }

    #[must_use]
    pub(crate) fn next_request_id(&self) -> String {
        format!("req_{}", Uuid::now_v7().simple())
    }
}

const fn map_authentication_error(error: ClientAuthenticationError) -> ClientApiKeyAuthError {
    match error {
        ClientAuthenticationError::InvalidKey => ClientApiKeyAuthError::InvalidKey,
        ClientAuthenticationError::SnapshotUnavailable => ClientApiKeyAuthError::RuntimeUnavailable,
    }
}
