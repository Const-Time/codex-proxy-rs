//! Provider validates SDK facts; native account transactions own persistence and audit.
use super::{commit_credential_rotation, map_provider_error, map_store_error, publish_committed};
use crate::{
    model::{AdminError, MutationActor, plugins::*, provider_credentials::*},
    ports::{
        plugins::PluginCredentialService, provider::ProviderAdminRegistry, store::AccountStore,
    },
};
use async_trait::async_trait;
use gateway_core::{
    engine::nested::ExtensionCallScope, lifecycle::CancellationToken, runtime::SnapshotControl,
};
use std::sync::Arc;

pub(crate) struct DefaultPluginCredentialService {
    accounts: Arc<dyn AccountStore>,
    providers: ProviderAdminRegistry,
    snapshot: Arc<dyn SnapshotControl>,
}
impl DefaultPluginCredentialService {
    pub(crate) fn new(
        accounts: Arc<dyn AccountStore>,
        providers: ProviderAdminRegistry,
        snapshot: Arc<dyn SnapshotControl>,
    ) -> Self {
        Self {
            accounts,
            providers,
            snapshot,
        }
    }
}
#[async_trait]
impl PluginCredentialService for DefaultPluginCredentialService {
    async fn save(
        &self,
        scope: PluginMutationScope,
        command: PluginCredentialSave,
        cleanup: ExtensionCallScope,
        cancellation: CancellationToken,
    ) -> Result<PluginCredentialSaved, AdminError> {
        let valid_actor = matches!(&scope.actor.actor, MutationActor::Plugin { instance_id, revision }
            if instance_id == &scope.instance_id && revision.get() == scope.revision);
        if !valid_actor
            || scope.actor.request_id.is_empty()
            || cleanup.is_root()
            || cancellation.is_cancelled()
        {
            return Err(AdminError::conflict("插件凭据提交作用域已失效"));
        }
        match &command {
            PluginCredentialSave::Create { facts, .. }
            | PluginCredentialSave::Replace { facts, .. } => facts.validate()?,
        }
        let accounts = self.accounts.clone();
        let providers = self.providers.clone();
        let snapshot = self.snapshot.clone();
        // Native verification can rotate a token. Once begun, preserve its prepared
        // material through the original transaction and publication instead of
        // cancelling that responsibility when a plugin or CLI wait disappears.
        tokio::spawn(async move {
            let _cleanup = cleanup;
            if cancellation.is_cancelled() {
                return Err(AdminError::conflict("插件凭据提交已取消"));
            }
            let (provider, account_id, credential_revision, config_revision) = match command {
                PluginCredentialSave::Create { provider, facts } => {
                    let provider = providers
                        .require(&provider)
                        .map_err(|e| map_provider_error(e, "plugin credential provider"))?;
                    let prepared = provider
                        .prepare_plugin_import(facts)
                        .await
                        .map_err(|e| map_provider_error(e, "plugin credential preparation"))?;
                    if &prepared.provider_kind != provider.provider_kind() {
                        return Err(AdminError::conflict("Provider 凭据准备结果不一致"));
                    }
                    let result = accounts
                        .commit_credential_import(
                            CredentialImportCommit {
                                prepared: PreparedCredentialImport {
                                    provider_kind: prepared.provider_kind.clone(),
                                    credentials: vec![prepared],
                                },
                                settings: None,
                                outbound_proxy: None,
                            },
                            &scope.actor,
                        )
                        .await
                        .map_err(|e| map_store_error(e, "plugin credential commit"))?;
                    let [account_id]: [_; 1] = result
                        .credential_ids
                        .try_into()
                        .map_err(|_| AdminError::internal("账号提交已完成，但结果数量未确认"))?;
                    // Import may upsert an existing identity; never assume revision 1.
                    let revision = accounts
                        .credential_details(provider.provider_kind(), &account_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|details| details.credential.credential_revision);
                    (provider, account_id, revision, result.config_revision)
                }
                PluginCredentialSave::Replace {
                    account_id,
                    credential_revision,
                    facts,
                } => {
                    let account = accounts
                        .load_account(account_id.as_str(), Default::default())
                        .await
                        .map_err(|e| map_store_error(e, "plugin credential target"))?
                        .ok_or_else(|| AdminError::not_found("账号不存在"))?
                        .account;
                    if account.credential_revision != credential_revision {
                        return Err(AdminError::conflict("账号凭据版本已变化"));
                    }
                    let provider = providers
                        .require(&account.provider_kind)
                        .map_err(|e| map_provider_error(e, "plugin credential provider"))?;
                    let prepared = provider
                        .prepare_plugin_rotation(account.clone(), facts)
                        .await
                        .map_err(|e| map_provider_error(e, "plugin credential preparation"))?;
                    super::validate_prepared_rotation(
                        &account,
                        &prepared,
                        "plugin credential rotation",
                    )?;
                    if prepared.facts().expected_credential_revision != credential_revision
                        || prepared.facts().replacement_identity.is_some()
                    {
                        return Err(AdminError::conflict("插件凭据替换不能重绑账号身份或版本"));
                    }
                    let result = commit_credential_rotation(
                        accounts.as_ref(),
                        prepared,
                        &scope.actor,
                        "plugin credential rotation",
                    )
                    .await?;
                    (
                        provider,
                        result.account_id,
                        result.credential_revision,
                        result.config_revision,
                    )
                }
            };
            provider
                .account_facts_changed(std::slice::from_ref(&account_id))
                .await;
            publish_committed(snapshot.as_ref(), config_revision).await?;
            Ok(PluginCredentialSaved {
                account_id,
                credential_revision,
            })
        })
        .await
        .map_err(|_| AdminError::internal("账号提交任务结束状态未确认"))?
    }
}
