use std::sync::Arc;

use async_trait::async_trait;
use gateway_admin::{
    model::{
        observability::{CalculatedBillingBreakdown, DashboardWireProfile, ProviderBillingInput},
        provider_credentials::{
            AuthorizationStarted, CompleteAuthorization, PendingAuthorizationMutation,
            PrepareCredentialImport, PrepareCredentialRefresh, PrepareCredentialRotation,
            PreparedAuthorizationCommit, PreparedCredentialImport, PreparedCredentialRotation,
            ProviderExport, ProviderExportCredentialInput, ProviderModels, ProviderQuota,
            ProviderQuotaRequest,
        },
        settings::ProviderRequestProfile,
    },
    ports::provider::{ProviderAdmin, ProviderAdminError, ProviderAdminErrorKind as Kind},
};
use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    operation::{GenerateRequest, Operation, ProtocolPayload},
    routing::{ProviderKind, UpstreamModelId},
};
use gateway_protocol::genai::GENAI_PROTOCOL;
use serde_json::{Value, json};

use crate::credential::{
    AuthorizationService, CredentialService, catalog::CatalogService, oauth::error,
    quota::QuotaService,
};
use crate::profile::ProfileService;

pub(crate) struct AntigravityAdmin {
    pub(crate) kind: ProviderKind,
    pub(crate) credentials: Arc<CredentialService>,
    pub(crate) authorization: AuthorizationService,
    pub(crate) catalog: Arc<CatalogService>,
    pub(crate) quota: Arc<QuotaService>,
    pub(crate) profiles: Arc<ProfileService>,
}

#[async_trait]
impl ProviderAdmin for AntigravityAdmin {
    fn pricing_catalog(&self) -> gateway_admin::model::pricing::ProviderPricingCatalog {
        crate::billing::defaults().clone()
    }

    fn provider_kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn plan_type_display(&self, plan_type: &str) -> String {
        match plan_type {
            "free-tier" => "Free",
            "g1-pro-tier" => "Pro",
            "g1-ultra-tier" => "Ultra",
            other => other,
        }
        .to_owned()
    }

    async fn account_unavailable(&self, _: &ProviderAccountId) {
        self.catalog.invalidate();
    }

    async fn account_facts_changed(&self, _: &[ProviderAccountId]) {
        self.catalog.invalidate();
    }

    fn connection_test_operation(
        &self,
        _: &UpstreamModelId,
        input_text: &str,
    ) -> Result<Operation, ProviderAdminError> {
        let Value::Object(body) = json!({
            "contents": [{"role": "user", "parts": [{"text": input_text}]}],
        }) else {
            unreachable!();
        };
        let payload = ProtocolPayload::json_object(GENAI_PROTOCOL, body)
            .map_err(|_| error(Kind::Internal))?;
        Ok(Operation::Generate(GenerateRequest::from_protocol_payload(
            payload,
        )))
    }

    fn dashboard_wire_profile(&self) -> Option<DashboardWireProfile> {
        None
    }

    async fn load_dashboard_wire_profile(&self) -> Option<DashboardWireProfile> {
        match self.profiles.dashboard().await {
            Ok(profile) => Some(profile),
            Err(error) => {
                tracing::warn!(kind = ?error.kind(), "could not load Antigravity dashboard profile");
                None
            }
        }
    }

    fn client_profile_options(&self) -> Result<OpaqueProviderData, ProviderAdminError> {
        Ok(crate::profile::options())
    }

    async fn client_profile(
        &self,
        configuration: Option<&OpaqueProviderData>,
    ) -> Result<ProviderRequestProfile, ProviderAdminError> {
        self.profiles.view(configuration).await
    }

    fn calculated_billing(
        &self,
        _: &ProviderBillingInput,
    ) -> Result<Option<CalculatedBillingBreakdown>, ProviderAdminError> {
        Ok(None)
    }

    async fn start_authorization(
        &self,
        pending: PendingAuthorizationMutation,
    ) -> Result<AuthorizationStarted, ProviderAdminError> {
        self.authorization.start(pending).await
    }

    async fn complete_authorization(
        &self,
        command: CompleteAuthorization,
    ) -> Result<PreparedAuthorizationCommit, ProviderAdminError> {
        self.authorization.complete(command).await
    }

    async fn prepare_refresh(
        &self,
        command: PrepareCredentialRefresh,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        let id = ProviderAccountId::new(command.account.id).map_err(|_| error(Kind::Invalid))?;
        let current = self.credentials.current(&id).await?;
        if command.account.provider_kind != self.kind
            || current.account.revision().get() != command.account.credential_revision.get()
        {
            return Err(error(Kind::Conflict));
        }
        self.credentials.refresh_current(&current).await
    }

    async fn prepare_import(
        &self,
        command: PrepareCredentialImport,
    ) -> Result<PreparedCredentialImport, ProviderAdminError> {
        self.credentials.prepare_import(command).await
    }

    async fn prepare_rotation(
        &self,
        command: PrepareCredentialRotation,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        self.credentials.prepare_rotation(command).await
    }

    async fn prepare_file_reauthorization(
        &self,
        command: PrepareCredentialRotation,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        self.credentials.prepare_rotation(command).await
    }

    async fn models(
        &self,
        id: &ProviderAccountId,
        refresh: bool,
    ) -> Result<ProviderModels, ProviderAdminError> {
        self.catalog.account_models(id, refresh).await
    }

    async fn quota(
        &self,
        request: ProviderQuotaRequest,
    ) -> Result<ProviderQuota, ProviderAdminError> {
        if request.refresh {
            self.quota.refresh(&request.account_id).await?;
        }
        self.quota.read(&request.account_id).await
    }

    async fn export_credentials(
        &self,
        credentials: Vec<ProviderExportCredentialInput>,
    ) -> Result<ProviderExport, ProviderAdminError> {
        self.credentials.export_credentials(credentials)
    }
}
