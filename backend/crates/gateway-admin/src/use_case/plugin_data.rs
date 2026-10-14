//! Plugin fact reads remain passive; explicit refresh uses the original native account pipeline.
use crate::{
    model::{
        AdminError, PageSize,
        plugins::*,
        provider_credentials::{ProviderQuota, ProviderQuotaRequest},
    },
    ports::{
        plugins::PluginDataService,
        provider::ProviderAdminRegistry,
        store::{AccountStore, AuthStore, ClientKeyStore},
    },
    use_case::{map_provider_error, map_store_error},
};
use async_trait::async_trait;
use gateway_core::{account::ProviderAccountId, policy::ClientApiKeyId};
use std::sync::Arc;

pub(crate) struct DefaultPluginDataService {
    accounts: Arc<dyn AccountStore>,
    keys: Arc<dyn ClientKeyStore>,
    providers: ProviderAdminRegistry,
    auth: Arc<dyn AuthStore>,
    native: Arc<dyn super::accounts::AccountsService>,
}
impl DefaultPluginDataService {
    pub(crate) fn new(
        accounts: Arc<dyn AccountStore>,
        keys: Arc<dyn ClientKeyStore>,
        providers: ProviderAdminRegistry,
        auth: Arc<dyn AuthStore>,
        native: Arc<dyn super::accounts::AccountsService>,
    ) -> Self {
        Self {
            accounts,
            keys,
            providers,
            auth,
            native,
        }
    }
}
#[async_trait]
impl PluginDataService for DefaultPluginDataService {
    async fn keys(&self, query: PluginKeyQuery) -> Result<PluginKeyPage, AdminError> {
        if PageSize::new(query.limit).is_err()
            || query
                .cursor
                .as_deref()
                .is_some_and(|id| ClientApiKeyId::new(id).is_err())
        {
            return Err(AdminError::invalid("Key 查询参数无效"));
        }
        self.keys
            .plugin_keys(query)
            .await
            .map_err(|e| map_store_error(e, "plugin key listing"))
    }
    async fn key(&self, id: &str) -> Result<PluginKeyFacts, AdminError> {
        let id = ClientApiKeyId::new(id).map_err(|_| AdminError::invalid("Key ID 无效"))?;
        self.keys
            .plugin_key_facts(&id)
            .await
            .map_err(|e| map_store_error(e, "plugin key facts"))?
            .ok_or_else(|| AdminError::not_found("Key 不存在"))
    }
    async fn accounts(&self, query: PluginAccountQuery) -> Result<PluginAccountPage, AdminError> {
        if !(1..=100).contains(&query.limit)
            || query
                .provider
                .as_deref()
                .is_some_and(|p| !matches!(p, "openai" | "xai"))
            || query
                .cursor
                .as_deref()
                .is_some_and(|id| ProviderAccountId::new(id).is_err())
        {
            return Err(AdminError::invalid("账号查询参数无效"));
        }
        self.accounts
            .plugin_account_facts(query)
            .await
            .map_err(|e| map_store_error(e, "plugin account facts"))
    }
    async fn quota(&self, id: &str) -> Result<ProviderQuota, AdminError> {
        let id = ProviderAccountId::new(id).map_err(|_| AdminError::invalid("账号 ID 无效"))?;
        let account = self
            .accounts
            .load_account(id.as_str(), Default::default())
            .await
            .map_err(|e| map_store_error(e, "plugin quota account"))?
            .ok_or_else(|| AdminError::not_found("账号不存在"))?;
        let provider = self
            .providers
            .require(&account.account.provider_kind)
            .map_err(|e| map_provider_error(e, "plugin quota provider"))?;
        provider
            .quota(ProviderQuotaRequest {
                account_id: id,
                refresh: false,
                rolling_usage: None,
            })
            .await
            .map_err(|e| map_provider_error(e, "plugin quota"))
    }
    async fn refresh_quota(
        &self,
        scope: PluginMutationScope,
        initiator: crate::model::MutationContext,
        account: ProviderAccountId,
        cleanup: gateway_core::engine::nested::ExtensionCallScope,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<ProviderQuota, AdminError> {
        if cleanup.is_root() || cancellation.is_cancelled() {
            return Err(AdminError::conflict("插件额度刷新作用域已失效"));
        }
        super::plugin_account_authority::authorize(self.auth.as_ref(), &scope, &initiator).await?;
        let accounts = self.accounts.clone();
        let auth = self.auth.clone();
        let native = self.native.clone();
        tokio::spawn(async move {
            let _cleanup = cleanup;
            verify_account(accounts.as_ref(), &scope, &account).await?;
            super::plugin_account_authority::authorize(auth.as_ref(), &scope, &initiator).await?;
            if cancellation.is_cancelled() {
                return Err(AdminError::conflict("插件额度刷新已取消"));
            }
            _cleanup.observe_upstream_effect();
            // Provider refresh, subscription observations and local attribution
            // keep their original native owners even when the RPC waiter leaves.
            let result = native.quota(&account, true).await?.quota;
            super::plugin_account_authority::authorize(auth.as_ref(), &scope, &initiator).await?;
            verify_account(accounts.as_ref(), &scope, &account).await?;
            Ok(result)
        })
        .await
        .map_err(|_| AdminError::internal("额度刷新结果未确认，请重新查询额度"))?
    }
}

async fn verify_account(
    accounts: &dyn AccountStore,
    scope: &PluginMutationScope,
    account: &ProviderAccountId,
) -> Result<(), AdminError> {
    let page = accounts
        .plugin_credential_accounts(
            scope.clone(),
            PluginCredentialQuery {
                provider: None,
                cursor: None,
                account_id: Some(account.clone()),
                limit: PageSize::new(1)
                    .map_err(|_| AdminError::internal("额度刷新查询范围无效"))?,
            },
        )
        .await
        .map_err(|e| map_store_error(e, "plugin quota authority"))?;
    if page.accounts.len() != 1 || page.accounts[0].id != account.as_str() {
        return Err(AdminError::not_found("账号不存在"));
    }
    Ok(())
}
