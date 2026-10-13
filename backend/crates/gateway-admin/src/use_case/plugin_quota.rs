//! Quota callbacks use explicit shared scopes and the normal store ledger.
use super::{map_store_error, publish_committed};
use crate::{
    model::{AdminError, MutationActor, plugins::*, users::SubscriptionTarget},
    ports::{
        plugins::PluginQuotaService,
        store::{AccountGroupStore, AuthStore},
    },
};
use async_trait::async_trait;
use gateway_core::{metering::Decimal, routing::AccountGroupId, runtime::SnapshotControl};
use std::sync::Arc;

pub(crate) struct DefaultPluginQuotaService {
    auth: Arc<dyn AuthStore>,
    groups: Arc<dyn AccountGroupStore>,
    snapshot: Arc<dyn SnapshotControl>,
}
impl DefaultPluginQuotaService {
    pub(crate) fn new(
        auth: Arc<dyn AuthStore>,
        groups: Arc<dyn AccountGroupStore>,
        snapshot: Arc<dyn SnapshotControl>,
    ) -> Self {
        Self {
            auth,
            groups,
            snapshot,
        }
    }
}
fn validate_scope(scope: &PluginMutationScope) -> Result<(), AdminError> {
    if !matches!(
        scope.actor.actor,
        MutationActor::AdminSession { .. } | MutationActor::AdminApiKey
    ) {
        return Err(AdminError::invalid("额度操作需要管理员身份"));
    }
    Ok(())
}
fn target(target: &SubscriptionTarget) -> Result<(), AdminError> {
    if target.user_id.is_empty()
        || target.user_id.len() > 256
        || target.user_id.trim() != target.user_id
        || target.user_id.chars().any(char::is_control)
        || AccountGroupId::new(&target.group_id).is_err()
    {
        return Err(AdminError::invalid("用户分组范围无效"));
    }
    Ok(())
}
#[async_trait]
impl PluginQuotaService for DefaultPluginQuotaService {
    async fn get(
        &self,
        scope: PluginMutationScope,
        query: SubscriptionTarget,
    ) -> Result<PluginSharedBudget, AdminError> {
        validate_scope(&scope)?;
        target(&query)?;
        self.auth
            .plugin_shared_budget(scope, query)
            .await
            .map_err(|e| map_store_error(e, "plugin quota"))
    }
    async fn update_group_limits(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupBudgetUpdate,
    ) -> Result<PluginResourceMutation<PluginGroupBudget>, AdminError> {
        validate_scope(&scope)?;
        if AccountGroupId::new(&command.group_id).is_err()
            || command.expected_revision == 0
            || (command.daily_limit_usd.is_none() && command.weekly_limit_usd.is_none())
        {
            return Err(AdminError::invalid("分组额度更新参数无效"));
        }
        for value in [&command.daily_limit_usd, &command.weekly_limit_usd]
            .into_iter()
            .flatten()
        {
            value
                .parse::<Decimal>()
                .map_err(|_| AdminError::invalid("额度格式无效"))?;
        }
        let groups = self.groups.clone();
        let snapshot = self.snapshot.clone();
        tokio::spawn(async move {
            let result = groups
                .update_plugin_group_budget(scope, command)
                .await
                .map_err(|e| map_store_error(e, "plugin quota"))?;
            publish_committed(snapshot.as_ref(), result.revision).await?;
            Ok(result)
        })
        .await
        .map_err(|_| AdminError::internal("额度操作中断，请查询当前版本"))?
    }
    async fn reset(
        &self,
        scope: PluginMutationScope,
        mut command: PluginBudgetReset,
    ) -> Result<PluginResourceMutation<u64>, AdminError> {
        validate_scope(&scope)?;
        let id = uuid::Uuid::parse_str(&command.event_id)
            .map_err(|_| AdminError::invalid("重置事件需要 UUIDv4"))?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || command.targets.is_empty()
            || command.targets.len() > 500
        {
            return Err(AdminError::invalid("重置事件或订阅范围无效"));
        }
        for entry in &command.targets {
            target(entry)?;
        }
        command.targets.sort();
        command.targets.dedup();
        command.event_id = id.to_string();
        let auth = self.auth.clone();
        let snapshot = self.snapshot.clone();
        tokio::spawn(async move {
            let result = auth
                .reset_plugin_shared_budget(scope, command)
                .await
                .map_err(|e| map_store_error(e, "plugin quota"))?;
            publish_committed(snapshot.as_ref(), result.revision).await?;
            Ok(result)
        })
        .await
        .map_err(|_| AdminError::internal("重置操作中断，请使用相同事件 ID 重试"))?
    }
}
