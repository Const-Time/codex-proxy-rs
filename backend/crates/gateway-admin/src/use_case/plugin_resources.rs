//! Plugin resources use normal group configuration and snapshot publication.
use super::{map_store_error, publish_committed};
use crate::{
    model::{
        AdminError,
        account_groups::{AccountGroupColor, NewAccountGroup},
        plugins::*,
    },
    ports::{plugins::PluginResourceService, store::AccountGroupStore},
};
use async_trait::async_trait;
use gateway_core::{account::ProviderAccountId, routing::AccountGroupId, runtime::SnapshotControl};
use std::{collections::BTreeSet, sync::Arc};

pub(crate) struct DefaultPluginResourceService {
    groups: Arc<dyn AccountGroupStore>,
    snapshot: Arc<dyn SnapshotControl>,
}
impl DefaultPluginResourceService {
    pub(crate) fn new(
        groups: Arc<dyn AccountGroupStore>,
        snapshot: Arc<dyn SnapshotControl>,
    ) -> Self {
        Self { groups, snapshot }
    }
}
fn resource_key(key: &str) -> Result<(), AdminError> {
    if key.is_empty() || key.len() > 128 || key.trim() != key || key.chars().any(char::is_control) {
        return Err(AdminError::invalid("插件资源标识无效"));
    }
    Ok(())
}
#[async_trait]
impl PluginResourceService for DefaultPluginResourceService {
    async fn ensure_group(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupEnsure,
    ) -> Result<PluginManagedResource, AdminError> {
        resource_key(&command.resource_key)?;
        let color = AccountGroupColor::parse(&command.color)
            .ok_or_else(|| AdminError::invalid("分组颜色无效"))?;
        let group = NewAccountGroup {
            id: AccountGroupId::new(format!("grp_{}", uuid::Uuid::now_v7().simple()))
                .map_err(|_| AdminError::internal("分组 ID 无效"))?,
            name: command.name,
            description: command.description,
            color,
            budget: Default::default(),
            model_multipliers: Default::default(),
        };
        let groups = self.groups.clone();
        let snapshot = self.snapshot.clone();
        // Finish commit and snapshot publication even if the requesting plugin disconnects.
        tokio::spawn(async move {
            let result = groups
                .ensure_plugin_group(scope, command.resource_key, group)
                .await
                .map_err(|e| map_store_error(e, "plugin group"))?;
            publish_committed(snapshot.as_ref(), result.revision).await?;
            Ok(result.value)
        })
        .await
        .map_err(|_| AdminError::internal("插件资源操作中断，请查询后重试"))?
    }
    async fn change_members(
        &self,
        scope: PluginMutationScope,
        mut command: PluginGroupMembers,
    ) -> Result<PluginMembersChanged, AdminError> {
        resource_key(&command.resource_key)?;
        if command.add.len() + command.remove.len() > 1000 {
            return Err(AdminError::invalid("一次最多修改 1000 个账号"));
        }
        let added: BTreeSet<_> = command.add.into_iter().collect();
        let removed: BTreeSet<_> = command.remove.into_iter().collect();
        if !added.is_disjoint(&removed)
            || added
                .iter()
                .chain(removed.iter())
                .any(|id| ProviderAccountId::new(id).is_err())
        {
            return Err(AdminError::invalid("分组成员参数无效"));
        }
        command.add = added.into_iter().collect();
        command.remove = removed.into_iter().collect();
        let groups = self.groups.clone();
        let snapshot = self.snapshot.clone();
        tokio::spawn(async move {
            let result = groups
                .change_plugin_group_members(scope, command)
                .await
                .map_err(|e| map_store_error(e, "plugin group members"))?;
            publish_committed(snapshot.as_ref(), result.revision).await?;
            Ok(result.value)
        })
        .await
        .map_err(|_| AdminError::internal("插件资源操作中断，请查询后重试"))?
    }
}
