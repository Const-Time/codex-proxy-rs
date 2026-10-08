//! Plugin access to the existing shared subscription ledger, never a Key-local ledger.
use crate::users_adapter::require_administrator;
use crate::{
    admin_adapter::UserAuthStore,
    admin_revision, admin_store_error, mutation_audit,
    postgres::{
        append_admin_audit_event_in_transaction, bump_config_revision_in_transaction,
        lock_plugin_resource, plugin_current_revision,
    },
};
use gateway_admin::{
    model::{plugins::*, users::SubscriptionTarget},
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use sqlx::Row;
fn error(kind: AdminStoreErrorKind) -> AdminStoreError {
    AdminStoreError::new(kind, "plugin quota", "shared quota operation failed")
}
fn db_error(_: sqlx::Error) -> AdminStoreError {
    error(AdminStoreErrorKind::Unavailable)
}
impl UserAuthStore {
    pub(crate) async fn plugin_budget_record(
        &self,
        scope: PluginMutationScope,
        target: SubscriptionTarget,
    ) -> AdminStoreResult<PluginSharedBudget> {
        let mut tx = self.security.pool.begin().await.map_err(db_error)?;
        lock_plugin_resource(&mut tx, &scope).await?;
        sqlx::query("select config_revision from runtime_settings where id=1 for share")
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        require_administrator(&mut tx, &scope.actor).await?;
        let revision = plugin_current_revision(&mut tx).await?;
        let row = sqlx::query("select u.enabled and g.enabled and ((u.role='admin' and not u.group_grants_configured) or ug.user_id is not null) as enabled,
            g.daily_limit_usd::text as group_daily, g.weekly_limit_usd::text as group_weekly,
            coalesce(ug.quota_multiplier,1)::text as multiplier,
            user_group_quota_limit(g.daily_limit_usd,u.id,g.id)::text as daily_limit,
            user_group_quota_limit(g.weekly_limit_usd,u.id,g.id)::text as weekly_limit,
            coalesce(w.daily_used_usd,0)::text as daily_used, coalesce(w.weekly_used_usd,0)::text as weekly_used,
            w.daily_end,w.weekly_end,w.last_reset_at
            from users u cross join account_groups g
            left join user_account_groups ug on ug.user_id=u.id and ug.account_group_id=g.id
            left join user_group_budget_windows w on w.user_id=u.id and w.account_group_id=g.id
            where u.id=$1 and g.id=$2 and u.deleted_at is null")
            .bind(&target.user_id).bind(&target.group_id).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(|| error(AdminStoreErrorKind::NotFound))?;
        let result = PluginSharedBudget {
            revision,
            target,
            enabled: row.get("enabled"),
            group_daily_limit_usd: row.get("group_daily"),
            group_weekly_limit_usd: row.get("group_weekly"),
            quota_multiplier: row.get("multiplier"),
            daily_limit_usd: row.get("daily_limit"),
            weekly_limit_usd: row.get("weekly_limit"),
            daily_used_usd: row.get("daily_used"),
            weekly_used_usd: row.get("weekly_used"),
            daily_resets_at: row.get("daily_end"),
            weekly_resets_at: row.get("weekly_end"),
            last_reset_at: row.get("last_reset_at"),
        };
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }
    pub(crate) async fn reset_plugin_budget(
        &self,
        scope: PluginMutationScope,
        mut command: PluginBudgetReset,
    ) -> AdminStoreResult<PluginResourceMutation<u64>> {
        let uuid = uuid::Uuid::parse_str(&command.event_id)
            .map_err(|_| error(AdminStoreErrorKind::Invalid))?;
        if uuid.get_version_num() != 4
            || uuid.get_variant() != uuid::Variant::RFC4122
            || command.targets.is_empty()
            || command.targets.len() > 500
        {
            return Err(error(AdminStoreErrorKind::Invalid));
        }
        command.targets.sort();
        command.targets.dedup();
        let event = format!("plugin:{}:reset:{}", scope.instance_id, uuid);
        let mut tx = self.security.pool.begin().await.map_err(db_error)?;
        lock_plugin_resource(&mut tx, &scope).await?;
        // Follow normal configuration -> user -> group -> budget-window lock order.
        // Serialize with user/grant changes without bumping a revision on an event replay.
        sqlx::query("select config_revision from runtime_settings where id=1 for update")
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        require_administrator(&mut tx, &scope.actor).await?;
        let targets = serde_json::to_value(&command.targets)
            .map_err(|_| error(AdminStoreErrorKind::Invalid))?;
        if let Some(row) =
            sqlx::query("select targets,affected_count from subscription_reset_events where id=$1")
                .bind(&event)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_error)?
        {
            if row.get::<Option<serde_json::Value>, _>("targets").as_ref() != Some(&targets) {
                return Err(error(AdminStoreErrorKind::Conflict));
            }
            let result = PluginResourceMutation {
                revision: plugin_current_revision(&mut tx).await?,
                value: row.get::<i64, _>("affected_count").unsigned_abs(),
            };
            tx.commit().await.map_err(db_error)?;
            return Ok(result);
        }
        let count: i64 = sqlx::query_scalar("with scopes as (select user_id,account_group_id from user_account_groups union select k.owner_user_id,kg.account_group_id from client_api_keys k join client_api_key_groups kg on kg.client_api_key_id=k.id)
            select count(*) from jsonb_array_elements($1::jsonb) t join scopes s on s.user_id=t->>'userId' and s.account_group_id=t->>'groupId' join users u on u.id=s.user_id and u.deleted_at is null join account_groups g on g.id=s.account_group_id")
            .bind(&targets).fetch_one(&mut *tx).await.map_err(db_error)?;
        if count as usize != command.targets.len() {
            return Err(error(AdminStoreErrorKind::NotFound));
        }
        let revision = bump_config_revision_in_transaction(&mut tx)
            .await
            .map_err(|e| admin_store_error("plugin quota", e))?;
        let count: i64 = sqlx::query_scalar(
            "select reset_user_subscriptions($1,null,null,'plugin',clock_timestamp(),$2)",
        )
        .bind(&event)
        .bind(&targets)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        append_admin_audit_event_in_transaction(
            &mut tx,
            mutation_audit(
                &scope.actor,
                "plugin.quota.reset",
                "subscription_reset",
                &event,
                vec!["daily_used_usd".into(), "weekly_used_usd".into()],
            ),
            revision,
        )
        .await
        .map_err(|e| admin_store_error("plugin quota", e))?;
        tx.commit().await.map_err(db_error)?;
        Ok(PluginResourceMutation {
            revision: admin_revision(revision)?,
            value: count.unsigned_abs(),
        })
    }
}
