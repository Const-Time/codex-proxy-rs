//! Read-only plugin projections. Provider owns quota decoding; reads never trigger refresh or billing side effects.
use crate::{
    model::{
        AdminError, PageSize,
        plugins::*,
        provider_credentials::{ProviderQuota, ProviderQuotaRequest},
    },
    ports::{
        plugins::PluginDataService,
        provider::ProviderAdminRegistry,
        store::{AccountStore, ClientKeyStore},
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
}
impl DefaultPluginDataService {
    pub(crate) fn new(
        accounts: Arc<dyn AccountStore>,
        keys: Arc<dyn ClientKeyStore>,
        providers: ProviderAdminRegistry,
    ) -> Self {
        Self {
            accounts,
            keys,
            providers,
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
}
