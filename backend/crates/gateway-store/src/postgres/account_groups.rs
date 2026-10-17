//! PostgreSQL owner for provider-neutral account groups and memberships.

use std::{collections::BTreeMap, str::FromStr as _};

use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_admin::{
    model::{
        MutationContext,
        account_groups::{
            AccountGroupAccountSummary, AccountGroupCapacity, AccountGroupColor,
            AccountGroupListQuery, AccountGroupMemberFact, AccountGroupMutation, AccountGroupPage,
            AccountGroupRecord, AccountGroupUsage, DeleteAccountGroup, NewAccountGroup,
            SetAccountGroupEnabled, UpdateAccountGroup,
        },
        observability::DecimalAmount,
    },
    ports::store::{AccountGroupStore, AdminStoreError, AdminStoreResult},
};
use gateway_core::{account::AccountStatusFacts, routing::AccountGroupId};
use sqlx::{PgPool, Postgres, QueryBuilder, Row as _, Transaction};

use crate::{
    ConflictKind, StoreError, StoreResult, admin_revision, admin_store_error, mutation_audit,
    postgres_unavailable,
};

use super::plugins::{lock_plugin_resource, plugin_current_revision};
use gateway_admin::model::plugins::*;

use super::{
    PgRuntimeSettingsRepository, RuntimeSettingsRepository, account_summary_from_row,
    append_admin_audit_event_in_transaction, bump_config_revision_in_transaction,
    completed_usage_fact_predicate,
};

const ENTITY: &str = "account group";

/// Account group store with transactional revision and audit ownership.
#[derive(Clone)]
pub struct PgAccountGroupRepository {
    pool: PgPool,
}

impl PgAccountGroupRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn current_revision(&self) -> AdminStoreResult<gateway_admin::model::Revision> {
        RuntimeSettingsRepository::load_runtime_settings(&PgRuntimeSettingsRepository::new(
            self.pool.clone(),
        ))
        .await
        .map_err(|error| admin_store_error(ENTITY, error))
        .and_then(|settings| admin_revision(settings.config_revision))
    }

    async fn required_record(&self, id: &AccountGroupId) -> AdminStoreResult<AccountGroupRecord> {
        let mut record = load_record(&self.pool, id.as_str())
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?
            .ok_or_else(|| not_found(id.as_str()))?;
        let costs = group_costs(&self.pool, &[id.as_str().to_owned()])
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        if let Some(usage) = costs.get(id.as_str()) {
            record.usage = usage.clone();
        }
        Ok(record)
    }

    async fn mutate<F>(
        &self,
        audit: super::AdminAuditEvent,
        mutation: F,
    ) -> AdminStoreResult<gateway_admin::model::Revision>
    where
        F: for<'a> FnOnce(&'a mut Transaction<'_, Postgres>) -> BoxFuture<'a, StoreResult<()>>,
    {
        let mut transaction =
            self.pool.begin().await.map_err(|_| {
                admin_store_error(ENTITY, unavailable("begin account group mutation"))
            })?;
        let result = async {
            let revision = bump_config_revision_in_transaction(&mut transaction).await?;
            mutation(&mut transaction).await?;
            append_admin_audit_event_in_transaction(&mut transaction, audit, revision).await?;
            Ok(revision)
        }
        .await;
        match result {
            Ok(revision) => {
                transaction.commit().await.map_err(|_| {
                    admin_store_error(ENTITY, unavailable("commit account group mutation"))
                })?;
                admin_revision(revision)
            }
            Err(error) => {
                transaction.rollback().await.map_err(|_| {
                    admin_store_error(ENTITY, unavailable("rollback account group mutation"))
                })?;
                Err(admin_store_error(ENTITY, error))
            }
        }
    }
}

#[async_trait]
impl AccountGroupStore for PgAccountGroupRepository {
    async fn account_group_catalog(
        &self,
        query: AccountGroupListQuery,
    ) -> AdminStoreResult<Vec<gateway_admin::model::account_groups::AccountGroupRef>> {
        validate_page_query(&query)?;
        let mut statement = QueryBuilder::<Postgres>::new(
            "select g.id,g.name,g.color,g.enabled from account_groups g where true",
        );
        push_group_filter(&mut statement, &query);
        statement
            .push(" order by g.created_at desc,g.id desc limit ")
            .push_bind(i64::from(query.page_size.get()));
        statement
            .push(" offset ")
            .push_bind(i64::from(query.page - 1) * i64::from(query.page_size.get()));
        let rows = statement
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| admin_store_error(ENTITY, unavailable("load group catalog")))?;
        rows.iter()
            .map(|row| {
                Ok(gateway_admin::model::account_groups::AccountGroupRef {
                    id: AccountGroupId::new(row.get::<String, _>("id"))
                        .map_err(|_| invalid_admin("invalid group id"))?,
                    name: row.get("name"),
                    color: AccountGroupColor::parse(&row.get::<String, _>("color"))
                        .ok_or_else(|| invalid_admin("invalid group color"))?,
                    enabled: row.get("enabled"),
                })
            })
            .collect()
    }
    async fn update_plugin_group_budget(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupBudgetUpdate,
    ) -> AdminStoreResult<PluginResourceMutation<PluginGroupBudget>> {
        use gateway_admin::ports::store::AdminStoreErrorKind;
        let fail = |kind| AdminStoreError::new(kind, "plugin quota", "group budget update failed");
        if AccountGroupId::new(&command.group_id).is_err()
            || command.expected_revision == 0
            || (command.daily_limit_usd.is_none() && command.weekly_limit_usd.is_none())
        {
            return Err(fail(AdminStoreErrorKind::Invalid));
        }
        let amount = |value: &Option<String>| {
            value
                .as_ref()
                .map(|s| {
                    s.parse::<gateway_core::metering::Decimal>()
                        .map(|v| v.canonical())
                        .map_err(|_| fail(AdminStoreErrorKind::Invalid))
                })
                .transpose()
        };
        let daily = amount(&command.daily_limit_usd)?;
        let weekly = amount(&command.weekly_limit_usd)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| fail(AdminStoreErrorKind::Unavailable))?;
        lock_plugin_resource(&mut tx, &scope).await?;
        let current: i64 = sqlx::query_scalar(
            "select config_revision from runtime_settings where id=1 for update",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| fail(AdminStoreErrorKind::Unavailable))?;
        crate::users_adapter::require_administrator(&mut tx, &scope.actor).await?;
        if u64::try_from(current).ok() != Some(command.expected_revision) {
            return Err(fail(AdminStoreErrorKind::Conflict));
        }
        let row = sqlx::query("update account_groups set daily_limit_usd=coalesce($2::text::numeric,daily_limit_usd),weekly_limit_usd=coalesce($3::text::numeric,weekly_limit_usd),updated_at=now() where id=$1 returning daily_limit_usd::text,weekly_limit_usd::text")
            .bind(&command.group_id).bind(daily).bind(weekly).fetch_optional(&mut *tx).await.map_err(|e| fail(if e.as_database_error().is_some_and(|e| e.is_check_violation()) {AdminStoreErrorKind::Invalid} else {AdminStoreErrorKind::Unavailable}))?.ok_or_else(|| fail(AdminStoreErrorKind::NotFound))?;
        let revision = bump_config_revision_in_transaction(&mut tx)
            .await
            .map_err(|e| admin_store_error("plugin quota", e))?;
        let mut fields = vec![];
        if command.daily_limit_usd.is_some() {
            fields.push("daily_limit_usd".into());
        }
        if command.weekly_limit_usd.is_some() {
            fields.push("weekly_limit_usd".into());
        }
        append_admin_audit_event_in_transaction(
            &mut tx,
            mutation_audit(
                &scope.actor,
                "plugin.quota.update_group_limits",
                "account_group",
                &command.group_id,
                fields,
            ),
            revision,
        )
        .await
        .map_err(|e| admin_store_error("plugin quota", e))?;
        tx.commit()
            .await
            .map_err(|_| fail(AdminStoreErrorKind::Unavailable))?;
        Ok(PluginResourceMutation {
            revision: admin_revision(revision)?,
            value: PluginGroupBudget {
                group_id: command.group_id,
                daily_limit_usd: row.get("daily_limit_usd"),
                weekly_limit_usd: row.get("weekly_limit_usd"),
            },
        })
    }

    async fn ensure_plugin_group(
        &self,
        scope: PluginMutationScope,
        resource_key: String,
        command: NewAccountGroup,
    ) -> AdminStoreResult<PluginResourceMutation<PluginManagedResource>> {
        validate_group_fields(&command.name, command.description.as_deref())?;
        let mut tx = self.pool.begin().await.map_err(|_| plugin_unavailable())?;
        lock_plugin_resource(&mut tx, &scope).await?;
        let existing=sqlx::query("select p.group_id,g.name,g.enabled from plugin_managed_groups p left join account_groups g on g.id=p.group_id where p.instance_id=$1 and p.resource_key=$2")
            .bind(&scope.instance_id).bind(&resource_key).fetch_optional(&mut *tx).await.map_err(|_|plugin_unavailable())?;
        if let Some(row) = existing {
            let id: Option<String> = row.try_get("group_id").map_err(|_| plugin_unavailable())?;
            let id = id.ok_or_else(|| not_found("deleted plugin group"))?;
            let revision = plugin_current_revision(&mut tx).await?;
            let value = PluginManagedResource {
                id,
                name: row.try_get("name").map_err(|_| plugin_unavailable())?,
                enabled: row.try_get("enabled").map_err(|_| plugin_unavailable())?,
            };
            tx.commit().await.map_err(|_| plugin_unavailable())?;
            return Ok(PluginResourceMutation { revision, value });
        }
        let count: i64 =
            sqlx::query_scalar("select count(*) from plugin_managed_groups where instance_id=$1")
                .bind(&scope.instance_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| plugin_unavailable())?;
        if count >= 256 {
            return Err(invalid_admin("plugin group capacity reached"));
        }
        let revision = bump_config_revision_in_transaction(&mut tx)
            .await
            .map_err(|e| admin_store_error(ENTITY, e))?;
        insert_group(&mut tx, &command)
            .await
            .map_err(|e| admin_store_error(ENTITY, e))?;
        sqlx::query(
            "insert into plugin_managed_groups(instance_id,resource_key,group_id) values($1,$2,$3)",
        )
        .bind(&scope.instance_id)
        .bind(resource_key)
        .bind(command.id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|_| plugin_unavailable())?;
        append_admin_audit_event_in_transaction(
            &mut tx,
            mutation_audit(
                &scope.actor,
                "plugin.group.ensure",
                "account_group",
                command.id.as_str(),
                vec!["plugin_resource".into()],
            ),
            revision,
        )
        .await
        .map_err(|e| admin_store_error(ENTITY, e))?;
        tx.commit().await.map_err(|_| plugin_unavailable())?;
        Ok(PluginResourceMutation {
            revision: admin_revision(revision)?,
            value: PluginManagedResource {
                id: command.id.as_str().into(),
                name: command.name,
                enabled: true,
            },
        })
    }
    async fn change_plugin_group_members(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupMembers,
    ) -> AdminStoreResult<PluginResourceMutation<PluginMembersChanged>> {
        let mut tx = self.pool.begin().await.map_err(|_| plugin_unavailable())?;
        lock_plugin_resource(&mut tx, &scope).await?;
        let id: Option<Option<String>> = sqlx::query_scalar(
            "select group_id from plugin_managed_groups where instance_id=$1 and resource_key=$2",
        )
        .bind(&scope.instance_id)
        .bind(&command.resource_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| plugin_unavailable())?;
        let id = id.flatten().ok_or_else(|| not_found("plugin group"))?;
        // Lock configuration before membership updates, matching normal admin mutation order.
        let revision = bump_config_revision_in_transaction(&mut tx)
            .await
            .map_err(|e| admin_store_error(ENTITY, e))?;
        let exists: Option<String> =
            sqlx::query_scalar("select id from account_groups where id=$1 for update")
                .bind(&id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| plugin_unavailable())?;
        if exists.is_none() {
            return Err(not_found("plugin group"));
        }
        let present: Vec<String> = sqlx::query_scalar(
            "select id from provider_accounts where id=any($1::text[]) order by id for key share",
        )
        .bind(&command.add)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| plugin_unavailable())?;
        if command.add.iter().any(|id| !present.contains(id)) {
            return Err(not_found("plugin member account"));
        }
        let removed=sqlx::query("delete from account_group_accounts where account_group_id=$1 and provider_account_id=any($2::text[])").bind(&id).bind(&command.remove).execute(&mut *tx).await.map_err(|_|plugin_unavailable())?.rows_affected();
        let added=sqlx::query("insert into account_group_accounts(account_group_id,provider_account_id,created_at) select $1,id,now() from unnest($2::text[]) as id on conflict do nothing").bind(&id).bind(&command.add).execute(&mut *tx).await.map_err(|_|plugin_unavailable())?.rows_affected();
        if added + removed > 0 {
            sqlx::query("update account_groups set updated_at=now() where id=$1")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_err(|_| plugin_unavailable())?;
            append_admin_audit_event_in_transaction(
                &mut tx,
                mutation_audit(
                    &scope.actor,
                    "plugin.group.members",
                    "account_group",
                    &id,
                    vec!["accounts".into()],
                ),
                revision,
            )
            .await
            .map_err(|e| admin_store_error(ENTITY, e))?;
        } else {
            tx.rollback().await.map_err(|_| plugin_unavailable())?;
            return Ok(PluginResourceMutation {
                revision: self.current_revision().await?,
                value: PluginMembersChanged { added, removed },
            });
        }
        tx.commit().await.map_err(|_| plugin_unavailable())?;
        Ok(PluginResourceMutation {
            revision: admin_revision(revision)?,
            value: PluginMembersChanged { added, removed },
        })
    }

    async fn list_account_groups(
        &self,
        query: AccountGroupListQuery,
    ) -> AdminStoreResult<AccountGroupPage> {
        validate_page_query(&query)?;
        let total = count_groups(&self.pool, &query)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let offset = u64::from(query.page.saturating_sub(1)) * u64::from(query.page_size.get());
        let mut statement = group_select();
        push_group_filter(&mut statement, &query);
        statement.push(" order by g.created_at desc, g.id desc limit ");
        statement.push_bind(i64::from(query.page_size.get()));
        statement.push(" offset ");
        statement.push_bind(i64::try_from(offset).map_err(|_| invalid_admin("page is too large"))?);
        let rows = statement
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| admin_store_error(ENTITY, unavailable("list account groups")))?;
        let mut items = rows
            .iter()
            .map(group_record)
            .collect::<StoreResult<Vec<_>>>()
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let group_ids = items
            .iter()
            .map(|record| record.id.as_str().to_owned())
            .collect::<Vec<_>>();
        let costs = group_costs(&self.pool, &group_ids)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        for record in &mut items {
            if let Some(usage) = costs.get(record.id.as_str()) {
                record.usage = usage.clone();
            }
        }
        Ok(AccountGroupPage {
            config_revision: self.current_revision().await?,
            items,
            total,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn load_account_group_members(
        &self,
        group_ids: &[AccountGroupId],
    ) -> AdminStoreResult<Vec<AccountGroupMemberFact>> {
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }
        let group_ids = group_ids
            .iter()
            .map(|group_id| group_id.as_str().to_owned())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "select membership.account_group_id,
                    account.id, account.provider_kind, account.name, account.email,
                    account.upstream_user_id, account.upstream_account_id, account.plan_type,
                    account.authentication_kind, account.credential_revision, outbound_proxy_request_context(account.outbound_proxy_id) as request_context, account.outbound_proxy_url,
                    account.has_refresh_token, account.access_token_expires_at,
                    account.next_refresh_at, account.enabled, account.concurrency_limit,
                    account.weight, account.model_access_json, account.credential_state, account.quota_access_state,
                    account.quota_evidence, account.quota_access_observed_at,
                    account.quota_reset_at, account.last_error_reason,
                    account.last_error_message, account.credential_observed_at,
                    account.created_at, account.updated_at,
                    settings.max_concurrent_per_account
               from account_group_accounts membership
               join provider_accounts account on account.id = membership.provider_account_id
               cross join runtime_settings settings
              where settings.id = 1
                and membership.account_group_id = any($1::text[])
              order by membership.account_group_id, membership.provider_account_id",
        )
        .bind(group_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| admin_store_error(ENTITY, unavailable("load account group members")))?;
        rows.into_iter()
            .map(|row| {
                let group_id = AccountGroupId::new(
                    row.try_get::<String, _>("account_group_id")
                        .map_err(|_| invalid("invalid group ID"))?,
                )
                .map_err(|_| invalid("invalid group ID"))?;
                let default_slots = u64::try_from(
                    row.try_get::<i64, _>("max_concurrent_per_account")
                        .map_err(|_| invalid("invalid default account concurrency"))?,
                )
                .map_err(|_| invalid("invalid default account concurrency"))?;
                let account = account_summary_from_row(row)?;
                Ok(AccountGroupMemberFact {
                    group_id,
                    account_id: account.id,
                    status: AccountStatusFacts {
                        enabled: account.enabled,
                        credential_state: account.credential_state,
                        access_token_expires_at: account.access_token_expires_at.map(Into::into),
                        quota: account.quota,
                        rate_limited_until: None,
                        last_error_reason: account.last_error_reason,
                        last_error_message: account.last_error_message,
                    },
                    total_slots: account
                        .concurrency_limit
                        .map_or(default_slots, |limit| u64::from(limit.get())),
                })
            })
            .collect::<StoreResult<Vec<_>>>()
            .map_err(|error| admin_store_error(ENTITY, error))
    }

    async fn create_account_group(
        &self,
        command: NewAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        validate_group_fields(&command.name, command.description.as_deref())?;
        if !gateway_admin::model::account_groups::valid_model_multipliers(
            &command.model_multipliers,
        ) {
            return Err(admin_store_error(
                ENTITY,
                invalid("invalid model billing multipliers"),
            ));
        }
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            "create",
            "account_group",
            id.as_str(),
            vec![
                "name".to_owned(),
                "description".to_owned(),
                "color".to_owned(),
                "daily_limit_usd".to_owned(),
                "weekly_limit_usd".to_owned(),
            ],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    insert_group(transaction, &command).await?;
                    Ok(())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn update_account_group(
        &self,
        command: UpdateAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        validate_group_fields(&command.name, command.description.as_deref())?;
        if !gateway_admin::model::account_groups::valid_model_multipliers(
            &command.model_multipliers,
        ) {
            return Err(admin_store_error(
                ENTITY,
                invalid("invalid model billing multipliers"),
            ));
        }
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            "update",
            "account_group",
            id.as_str(),
            vec![
                "name".to_owned(),
                "description".to_owned(),
                "model_multipliers".to_owned(),
                "daily_limit_usd".to_owned(),
                "weekly_limit_usd".to_owned(),
            ],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let result = sqlx::query(
                        "update account_groups
                 set name = $2, description = $3, color = $4, updated_at = now(), daily_limit_usd = $5::text::numeric, weekly_limit_usd = $6::text::numeric, model_multipliers = $7
                 where id = $1",
                    )
                    .bind(command.id.as_str())
                    .bind(command.name)
                    .bind(command.description)
                    .bind(command.color.as_str())
                    .bind(command.budget.daily_usd.canonical())
                    .bind(command.budget.weekly_usd.canonical())
                    .bind(sqlx::types::Json(&command.model_multipliers))
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| map_group_write_error(error, command.id.as_str()))?;
                    require_one(result.rows_affected(), command.id.as_str())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn set_account_group_enabled(
        &self,
        command: SetAccountGroupEnabled,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            if command.enabled { "enable" } else { "disable" },
            "account_group",
            id.as_str(),
            vec!["enabled".to_owned()],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let result = sqlx::query(
                        "update account_groups set enabled = $2, updated_at = now() where id = $1",
                    )
                    .bind(command.id.as_str())
                    .bind(command.enabled)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|_| unavailable("set account group state"))?;
                    require_one(result.rows_affected(), command.id.as_str())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn delete_account_group(
        &self,
        command: DeleteAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        let id = command.id.clone();
        let audit = mutation_audit(context, "delete", "account_group", id.as_str(), Vec::new());
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let result = sqlx::query(
                        "delete from account_groups g
                         where g.id = $1
                           and not exists (
                             select 1 from client_api_key_groups k where k.account_group_id = g.id
                           )",
                    )
                    .bind(command.id.as_str())
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| {
                        // Concurrent references can appear after the statement snapshot.
                        // Keep the foreign-key guard and report a conflict, not an outage.
                        if error
                            .as_database_error()
                            .is_some_and(|error| error.is_foreign_key_violation())
                        {
                            conflict(command.id.as_str())
                        } else {
                            unavailable("delete account group")
                        }
                    })?;
                    if result.rows_affected() == 1 {
                        return Ok(());
                    }
                    let exists = sqlx::query_scalar::<_, bool>(
                        "select exists(select 1 from account_groups where id = $1)",
                    )
                    .bind(command.id.as_str())
                    .fetch_one(&mut **transaction)
                    .await
                    .map_err(|_| unavailable("check account group delete conflict"))?;
                    if exists {
                        Err(conflict(command.id.as_str()))
                    } else {
                        Err(not_found_store(command.id.as_str()))
                    }
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id,
            record: None,
        })
    }
}

fn group_select() -> QueryBuilder<Postgres> {
    QueryBuilder::new(
        "select g.id, g.name, g.model_multipliers, g.daily_limit_usd::text, g.weekly_limit_usd::text, g.description, g.color, g.enabled, g.created_at, g.updated_at,
                coalesce(members.member_count, 0)::bigint as member_count,
                coalesce(keys.client_key_count, 0)::bigint as client_key_count,
                authorized_users.user_count,
                coalesce(members.provider_counts, '{}'::jsonb) as provider_counts
         from account_groups g
         left join lateral (
           select coalesce(sum(counts.provider_count), 0)::bigint as member_count,
                  jsonb_object_agg(counts.provider_kind, counts.provider_count)
                    as provider_counts
           from (
             select a.provider_kind, count(*)::bigint as provider_count
             from account_group_accounts gm
             join provider_accounts a on a.id = gm.provider_account_id
             where gm.account_group_id = g.id
             group by a.provider_kind
           ) counts
         ) members on true
         left join lateral (
           select count(*)::bigint as client_key_count
           from client_api_key_groups kg
           where kg.account_group_id = g.id
         ) keys on true
         left join lateral (
           select count(*)::bigint as user_count
           from users u
           where u.deleted_at is null
             and ((u.role = 'admin' and not u.group_grants_configured)
               or exists (select 1 from user_account_groups ug
                          where ug.user_id = u.id and ug.account_group_id = g.id))
         ) authorized_users on true
         where true",
    )
}

fn push_group_filter(statement: &mut QueryBuilder<Postgres>, query: &AccountGroupListQuery) {
    if let Some(search) = &query.search {
        statement.push(" and lower(g.name) like ");
        statement.push_bind(format!("%{}%", search.to_lowercase()));
    }
    if let Some(enabled) = query.enabled {
        statement.push(" and g.enabled = ");
        statement.push_bind(enabled);
    }
}

async fn count_groups(pool: &PgPool, query: &AccountGroupListQuery) -> StoreResult<u64> {
    let mut statement =
        QueryBuilder::<Postgres>::new("select count(*)::bigint from account_groups g where true");
    if let Some(search) = &query.search {
        statement.push(" and lower(g.name) like ");
        statement.push_bind(format!("%{}%", search.to_lowercase()));
    }
    if let Some(enabled) = query.enabled {
        statement.push(" and g.enabled = ");
        statement.push_bind(enabled);
    }
    let count = statement
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await
        .map_err(|_| unavailable("count account groups"))?;
    u64::try_from(count).map_err(|_| invalid("negative account group count"))
}

async fn load_record(pool: &PgPool, id: &str) -> StoreResult<Option<AccountGroupRecord>> {
    let mut statement = group_select();
    statement.push(" and g.id = ");
    statement.push_bind(id.to_owned());
    statement
        .build()
        .fetch_optional(pool)
        .await
        .map_err(|_| unavailable("load account group"))?
        .as_ref()
        .map(group_record)
        .transpose()
}

fn group_record(row: &sqlx::postgres::PgRow) -> StoreResult<AccountGroupRecord> {
    let provider_counts: serde_json::Value = row
        .try_get("provider_counts")
        .map_err(|_| invalid("invalid provider counts"))?;
    let provider_counts = serde_json::from_value::<BTreeMap<String, u64>>(provider_counts)
        .map_err(|_| invalid("invalid provider counts"))?;
    Ok(AccountGroupRecord {
        model_multipliers: row
            .try_get::<sqlx::types::Json<
                BTreeMap<String, gateway_admin::model::account_groups::GroupModelMultiplier>,
            >, _>("model_multipliers")
            .map_err(|_| invalid("invalid model billing multipliers"))?
            .0,
        budget: gateway_core::engine::budget::ClientBudgetLimits {
            daily_usd: row
                .try_get::<String, _>("daily_limit_usd")
                .map_err(|_| invalid("invalid daily limit"))?
                .parse()
                .map_err(|_| invalid("invalid daily limit"))?,
            weekly_usd: row
                .try_get::<String, _>("weekly_limit_usd")
                .map_err(|_| invalid("invalid weekly limit"))?
                .parse()
                .map_err(|_| invalid("invalid weekly limit"))?,
        },
        id: AccountGroupId::new(
            row.try_get::<String, _>("id")
                .map_err(|_| invalid("invalid id"))?,
        )
        .map_err(|_| invalid("invalid id"))?,
        name: row.try_get("name").map_err(|_| invalid("invalid name"))?,
        description: row
            .try_get("description")
            .map_err(|_| invalid("invalid description"))?,
        color: AccountGroupColor::parse(
            row.try_get::<String, _>("color")
                .map_err(|_| invalid("invalid color"))?
                .as_str(),
        )
        .ok_or_else(|| invalid("invalid color"))?,
        enabled: row
            .try_get("enabled")
            .map_err(|_| invalid("invalid enabled"))?,
        member_count: count_value(row, "member_count")?,
        user_count: count_value(row, "user_count")?,
        provider_counts,
        client_key_count: count_value(row, "client_key_count")?,
        account_summary: AccountGroupAccountSummary {
            available: 0,
            limited: 0,
            total: 0,
        },
        capacity: AccountGroupCapacity {
            used_slots: None,
            total_slots: 0,
        },
        usage: AccountGroupUsage {
            today_usd: DecimalAmount::from_str("0").map_err(|_| invalid("invalid zero cost"))?,
            retained_total_usd: DecimalAmount::from_str("0")
                .map_err(|_| invalid("invalid zero cost"))?,
        },
        created_at: row
            .try_get("created_at")
            .map_err(|_| invalid("invalid created_at"))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|_| invalid("invalid updated_at"))?,
    })
}

async fn group_costs(
    pool: &PgPool,
    group_ids: &[String],
) -> StoreResult<BTreeMap<String, AccountGroupUsage>> {
    if group_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let completed_usage = completed_usage_fact_predicate("mr");
    let statement = format!(
        "with bounds as (
           select now() - make_interval(days => usage_retention_days::int) as first,
             date_bin(interval '1 hour',now(),timestamptz '1970-01-01')-interval '1 hour' as last,
             date_trunc('day',now() at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai' as today
           from runtime_settings where id=1
         ), covered as materialized (
           select h.bucket_start from usage_hourly_covered h cross join bounds b
           where h.bucket_start>=b.first and h.bucket_start+interval '1 hour'<=b.last
             and not exists(select 1 from usage_hourly_dirty d where d.bucket_start=h.bucket_start)
         ), raw_ranges as (
           select greatest(h,b.first) as first,least(h+interval '1 hour',b.last) as last
           from bounds b cross join lateral generate_series(
             date_bin(interval '1 hour',b.first,timestamptz '1970-01-01'),b.last,interval '1 hour') h
           where h<b.last and not exists(select 1 from covered c where c.bucket_start=h)
           union all select greatest(first,last),'infinity'::timestamptz from bounds
         ), facts as (
           select s.routing_group_refs,s.bucket_start as started_at,s.group_billing_usd as amount
           from usage_hourly_stats s join covered c using(bucket_start)
           where s.routing_group_refs && $1::text[]
           union all
           select mr.routing_group_refs,mr.started_at,mr.billed_cost_amount as amount
           from raw_ranges r cross join lateral (
             select routing_group_refs,started_at,billed_cost_amount from model_requests mr
             where mr.started_at>=r.first and mr.started_at<r.last
               and mr.routing_group_refs && $1::text[] and {completed_usage}
               and mr.cost_currency='USD' and mr.cost_amount is not null
           ) mr
         )
         select refs.group_id,
           coalesce(sum(f.amount) filter(where f.started_at>=b.today),0)::text as today_usd,
           coalesce(sum(f.amount),0)::text as retained_total_usd
         from facts f cross join bounds b cross join lateral (
           select distinct unnest(f.routing_group_refs) as group_id
         ) refs where refs.group_id=any($1::text[]) group by refs.group_id"
    );
    // 动态片段仅为共享的固定 usage-fact predicate；group IDs 仍使用 bind。
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| unavailable("begin account group costs"))?;
    sqlx::query("set local jit=off")
        .execute(&mut *tx)
        .await
        .map_err(|_| unavailable("plan account group costs"))?;
    let rows = sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(group_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| unavailable("load account group costs"))?;
    tx.commit()
        .await
        .map_err(|_| unavailable("complete account group costs"))?;
    rows.into_iter()
        .map(|row| {
            let group_id: String = row
                .try_get("group_id")
                .map_err(|_| invalid("invalid group ID"))?;
            let today = row
                .try_get::<String, _>("today_usd")
                .map_err(|_| invalid("invalid today cost"))?;
            let retained_total = row
                .try_get::<String, _>("retained_total_usd")
                .map_err(|_| invalid("invalid retained total cost"))?;
            Ok((
                group_id,
                AccountGroupUsage {
                    today_usd: DecimalAmount::from_str(&today)
                        .map_err(|_| invalid("invalid today cost"))?,
                    retained_total_usd: DecimalAmount::from_str(&retained_total)
                        .map_err(|_| invalid("invalid retained total cost"))?,
                },
            ))
        })
        .collect()
}

fn count_value(row: &sqlx::postgres::PgRow, field: &str) -> StoreResult<u64> {
    u64::try_from(
        row.try_get::<i64, _>(field)
            .map_err(|_| invalid("invalid count"))?,
    )
    .map_err(|_| invalid("negative count"))
}

fn validate_page_query(query: &AccountGroupListQuery) -> AdminStoreResult<()> {
    if query.page == 0 {
        return Err(invalid_admin("page must be positive"));
    }
    if query.search.as_deref().is_some_and(|search| {
        search.trim().is_empty() || search.len() > 256 || search.chars().any(char::is_control)
    }) {
        return Err(invalid_admin("invalid search"));
    }
    Ok(())
}

fn validate_group_fields(name: &str, description: Option<&str>) -> AdminStoreResult<()> {
    if name.trim() != name
        || name.is_empty()
        || name.chars().count() > 100
        || name.chars().any(char::is_control)
        || description
            .is_some_and(|value| value.len() > 4096 || value.chars().any(char::is_control))
    {
        return Err(invalid_admin("invalid account group fields"));
    }
    Ok(())
}

fn require_one(rows: u64, id: &str) -> StoreResult<()> {
    if rows == 1 {
        Ok(())
    } else {
        Err(not_found_store(id))
    }
}

fn map_group_write_error(error: sqlx::Error, id: &str) -> StoreError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        conflict(id)
    } else {
        unavailable("write account group")
    }
}

fn not_found(id: &str) -> AdminStoreError {
    admin_store_error(ENTITY, not_found_store(id))
}

fn not_found_store(id: &str) -> StoreError {
    StoreError::NotFound {
        entity: ENTITY,
        id: id.to_owned(),
    }
}

fn conflict(id: &str) -> StoreError {
    StoreError::Conflict {
        entity: ENTITY,
        id: id.to_owned(),
        kind: ConflictKind::InvalidTransition,
    }
}

fn invalid_admin(message: &str) -> AdminStoreError {
    admin_store_error(ENTITY, invalid(message))
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: ENTITY,
        message: message.to_owned(),
    }
}

fn unavailable(message: &'static str) -> StoreError {
    postgres_unavailable(message)
}

async fn insert_group(
    transaction: &mut Transaction<'_, Postgres>,
    command: &NewAccountGroup,
) -> StoreResult<()> {
    sqlx::query(
        "insert into account_groups
         (id, name, description, color, enabled, created_at, updated_at, daily_limit_usd, weekly_limit_usd, model_multipliers)
         values ($1, $2, $3, $4, true, now(), now(), $5::text::numeric, $6::text::numeric, $7)",
    )
    .bind(command.id.as_str())
    .bind(&command.name)
    .bind(&command.description)
    .bind(command.color.as_str())
    .bind(command.budget.daily_usd.canonical())
    .bind(command.budget.weekly_usd.canonical())
    .bind(sqlx::types::Json(&command.model_multipliers))
    .execute(&mut **transaction)
    .await
    .map_err(|error| map_group_write_error(error, command.id.as_str()))?;
    Ok(())
}

fn plugin_unavailable() -> AdminStoreError {
    admin_store_error(ENTITY, unavailable("plugin group operation"))
}
