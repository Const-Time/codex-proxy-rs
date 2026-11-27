//! Provider-owned credential mutations using the shared account and snapshot stores.

use std::sync::Arc;

use async_trait::async_trait;
use gateway_core::runtime::SnapshotControl;

use crate::{
    model::{
        AdminError,
        provider_credentials::{
            AuthorizationStarted, CompleteAuthorization, CredentialDeletion,
            CredentialDeletionResult, CredentialImportCommit, CredentialImportResult,
            CredentialListQuery, CredentialMutationResult, CredentialPage, ImportCredentials,
            PrepareCredentialImport, ProviderQuotaRequest, StartAuthorization,
        },
    },
    ports::{provider::ProviderAdmin, store::AccountStore},
};

use super::{
    commit_authorization, delete_credentials, map_provider_error, map_store_error,
    pending_authorization, publish_committed, validate_authorization_commit,
    validate_prepared_import,
};

/// Management operations shared by providers that use the standard credential contracts.
#[async_trait]
pub trait ProviderCredentialsService: Send + Sync {
    async fn list(&self, query: CredentialListQuery) -> Result<CredentialPage, AdminError>;
    async fn import_document(
        &self,
        command: ImportCredentials,
    ) -> Result<CredentialImportResult, AdminError>;
    async fn start_authorization(
        &self,
        command: StartAuthorization,
    ) -> Result<AuthorizationStarted, AdminError>;
    async fn complete_authorization(
        &self,
        command: CompleteAuthorization,
    ) -> Result<CredentialMutationResult, AdminError>;
    async fn delete(
        &self,
        command: CredentialDeletion,
    ) -> Result<CredentialDeletionResult, AdminError>;
}

pub(crate) struct CredentialResources {
    pub credential: &'static str,
    pub import: &'static str,
    pub authorization: &'static str,
}

pub(crate) struct DefaultProviderCredentialsService {
    provider: Arc<dyn ProviderAdmin>,
    accounts: Arc<dyn AccountStore>,
    proxies: Arc<dyn crate::ports::proxy::ProxyStore>,
    snapshot: Arc<dyn SnapshotControl>,
    resources: CredentialResources,
}

impl DefaultProviderCredentialsService {
    #[must_use]
    pub(crate) fn new(
        provider: Arc<dyn ProviderAdmin>,
        accounts: Arc<dyn AccountStore>,
        proxies: Arc<dyn crate::ports::proxy::ProxyStore>,
        snapshot: Arc<dyn SnapshotControl>,
        resources: CredentialResources,
    ) -> Self {
        Self {
            provider,
            accounts,
            proxies,
            snapshot,
            resources,
        }
    }
}

#[async_trait]
impl ProviderCredentialsService for DefaultProviderCredentialsService {
    async fn list(&self, query: CredentialListQuery) -> Result<CredentialPage, AdminError> {
        self.accounts
            .list_credentials(self.provider.provider_kind(), query)
            .await
            .map_err(|error| map_store_error(error, self.resources.credential))
    }

    async fn import_document(
        &self,
        command: ImportCredentials,
    ) -> Result<CredentialImportResult, AdminError> {
        if command.account_id.is_some() {
            let result = super::reauthorize_account_file(
                self.accounts.as_ref(),
                self.provider.as_ref(),
                command,
            )
            .await?;
            self.provider
                .account_facts_changed(&result.credential_ids())
                .await;
            publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
            return Ok(result);
        }
        let context = command.context;
        let proxy_reservation = super::import_proxy_binding(
            self.proxies.as_ref(),
            command.outbound_proxy_id.as_deref(),
        )
        .await?;
        let outbound_proxy = proxy_reservation
            .as_ref()
            .map(|reservation| reservation.binding.clone());
        let prepared = self
            .provider
            .prepare_import(PrepareCredentialImport {
                default_outbound_proxy: outbound_proxy
                    .as_ref()
                    .map(|binding| binding.proxy.clone()),
                document: command.document,
            })
            .await
            .map_err(|error| map_provider_error(error, self.resources.import))?;
        validate_prepared_import(
            self.provider.provider_kind(),
            &prepared,
            self.resources.import,
        )?;
        let result = self
            .accounts
            .commit_credential_import(
                CredentialImportCommit {
                    outbound_proxy,
                    prepared,
                    settings: command.settings,
                },
                &context,
            )
            .await
            .map_err(|error| map_store_error(error, self.resources.import))?;
        drop(proxy_reservation);
        self.provider
            .account_facts_changed(&result.credential_ids())
            .await;
        publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
        // 导入已经提交，额度属于可重建观察事实；单个账号查询失败由周期任务重试。
        for account_id in &result.credential_ids() {
            let _ = self
                .provider
                .quota(ProviderQuotaRequest {
                    account_id: account_id.clone(),
                    refresh: true,
                    rolling_usage: None,
                })
                .await;
        }
        Ok(result)
    }

    async fn start_authorization(
        &self,
        command: StartAuthorization,
    ) -> Result<AuthorizationStarted, AdminError> {
        let pending = pending_authorization(
            self.accounts.as_ref(),
            self.proxies.as_ref(),
            self.provider.provider_kind(),
            &command,
            self.resources.credential,
        )
        .await?;
        self.provider
            .start_authorization(pending)
            .await
            .map_err(|error| map_provider_error(error, self.resources.authorization))
    }

    async fn complete_authorization(
        &self,
        mut command: CompleteAuthorization,
    ) -> Result<CredentialMutationResult, AdminError> {
        let context = command.context.clone();
        let settings = command.settings.take();
        let prepared = self
            .provider
            .complete_authorization(command)
            .await
            .map_err(|error| map_provider_error(error, self.resources.authorization))?;
        let prepared = validate_authorization_commit(
            self.provider.provider_kind(),
            &context,
            prepared,
            self.resources.authorization,
        )
        .await?;
        let result = commit_authorization(
            self.accounts.as_ref(),
            prepared,
            settings,
            &context,
            self.resources.authorization,
        )
        .await?;
        self.provider
            .account_facts_changed(std::slice::from_ref(&result.account_id))
            .await;
        publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
        Ok(result)
    }

    async fn delete(
        &self,
        command: CredentialDeletion,
    ) -> Result<CredentialDeletionResult, AdminError> {
        let result = delete_credentials(
            self.accounts.as_ref(),
            self.provider.as_ref(),
            command,
            self.resources.credential,
        )
        .await?;
        publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
        Ok(result)
    }
}
