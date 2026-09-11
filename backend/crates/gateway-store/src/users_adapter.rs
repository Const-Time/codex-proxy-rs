//! User persistence. Password changes and grants are committed with their audit event.

use gateway_admin::model::{
    MutationActor, MutationContext,
    users::{UpdateUser, UserRecord, UserRole},
};
use gateway_admin::ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

use crate::{admin_adapter::UserAuthStore, admin_store_error, mutation_audit};

const USER_SELECT: &str = "select u.id, u.username, u.role, u.enabled, u.auth_version, u.max_concurrency, u.requests_per_minute, u.created_at, u.updated_at,
    array(select account_group_id from user_account_groups where user_id = u.id order by account_group_id) as group_ids from users u";

fn decode(row: PgRow) -> UserRecord {
    UserRecord {
        id: row.get("id"),
        username: row.get("username"),
        role: if row.get::<String, _>("role") == "admin" {
            UserRole::Admin
        } else {
            UserRole::User
        },
        enabled: row.get("enabled"),
        auth_version: row.get("auth_version"),
        limits: gateway_core::policy::RateLimits {
            max_concurrency: row.get::<i64, _>("max_concurrency").unsigned_abs(),
            requests_per_minute: row.get::<i64, _>("requests_per_minute").unsigned_abs(),
        },
        group_ids: row.get("group_ids"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn invalid_limits() -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Invalid, "user", "invalid rate limits")
}

fn db_error(error: sqlx::Error) -> AdminStoreError {
    let kind = if error
        .as_database_error()
        .is_some_and(|e| e.is_unique_violation())
    {
        AdminStoreErrorKind::Conflict
    } else if error
        .as_database_error()
        .is_some_and(|e| e.is_foreign_key_violation())
    {
        AdminStoreErrorKind::Invalid
    } else {
        AdminStoreErrorKind::Unavailable
    };
    AdminStoreError::new(kind, "user", "user operation failed")
}

async fn require_administrator(
    tx: &mut Transaction<'_, Postgres>,
    context: &MutationContext,
) -> AdminStoreResult<()> {
    match &context.actor {
        MutationActor::AdminApiKey => Ok(()),
        MutationActor::AdminSession { admin_user_id } => {
            let allowed = sqlx::query_scalar::<_, bool>(
                "select enabled and role = 'admin' from users where id = $1 for share",
            )
            .bind(admin_user_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db_error)?
            .unwrap_or(false);
            if allowed {
                Ok(())
            } else {
                Err(AdminStoreError::new(
                    AdminStoreErrorKind::Invalid,
                    "user",
                    "administrator required",
                ))
            }
        }
        _ => Err(AdminStoreError::new(
            AdminStoreErrorKind::Invalid,
            "user",
            "administrator required",
        )),
    }
}

async fn grants(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    group_ids: &[String],
) -> AdminStoreResult<()> {
    sqlx::query("delete from user_account_groups where user_id = $1")
        .bind(id)
        .execute(&mut **tx)
        .await
        .map_err(db_error)?;
    for group in group_ids {
        sqlx::query("insert into user_account_groups(user_id, account_group_id) values ($1, $2)")
            .bind(id)
            .bind(group)
            .execute(&mut **tx)
            .await
            .map_err(db_error)?;
    }
    Ok(())
}

async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    context: &MutationContext,
    id: &str,
    action: &str,
    fields: &[&str],
) -> AdminStoreResult<()> {
    let revision = crate::postgres::bump_config_revision_in_transaction(tx)
        .await
        .map_err(|e| admin_store_error("user", e))?;
    crate::postgres::append_admin_audit_event_in_transaction(
        tx,
        mutation_audit(
            context,
            action,
            "user",
            id,
            fields.iter().map(|s| (*s).to_owned()).collect(),
        ),
        revision,
    )
    .await
    .map_err(|e| admin_store_error("user", e))
}

impl UserAuthStore {
    pub(crate) async fn available_groups(
        &self,
        id: &str,
    ) -> AdminStoreResult<Vec<gateway_admin::model::users::UserGroup>> {
        let rows = sqlx::query("select g.id, g.name, g.color, g.enabled, g.daily_limit_usd::text, g.weekly_limit_usd::text,
            (case when w.daily_end > now() then w.daily_used_usd else 0 end)::text as daily_used,
            (case when w.weekly_end > now() then w.weekly_used_usd else 0 end)::text as weekly_used,
            case when w.daily_end > now() then w.daily_end end as daily_end,
            case when w.weekly_end > now() then w.weekly_end end as weekly_end
            from users u cross join account_groups g
            left join user_group_budget_windows w on w.user_id = u.id and w.account_group_id = g.id
            where u.id = $1 and u.enabled and (u.role = 'admin' or exists(
                select 1 from user_account_groups ug where ug.user_id = u.id and ug.account_group_id = g.id))
            order by g.name, g.id")
            .bind(id).fetch_all(&self.security.pool).await.map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                let amount = |column| {
                    row.get::<String, _>(column).parse().map_err(|_| {
                        AdminStoreError::new(
                            AdminStoreErrorKind::Invalid,
                            "user groups",
                            "invalid amount",
                        )
                    })
                };
                Ok(gateway_admin::model::users::UserGroup {
                    id: row.get("id"),
                    name: row.get("name"),
                    color: row.get("color"),
                    enabled: row.get("enabled"),
                    budget: gateway_core::engine::budget::ClientBudgetStatus {
                        limits: gateway_core::engine::budget::ClientBudgetLimits {
                            daily_usd: amount("daily_limit_usd")?,
                            weekly_usd: amount("weekly_limit_usd")?,
                        },
                        daily_used_usd: amount("daily_used")?,
                        weekly_used_usd: amount("weekly_used")?,
                        daily_resets_at: row
                            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("daily_end")
                            .map(Into::into),
                        weekly_resets_at: row
                            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("weekly_end")
                            .map(Into::into),
                    },
                })
            })
            .collect()
    }

    pub(crate) async fn user_by(
        &self,
        value: &str,
        username: bool,
    ) -> AdminStoreResult<Option<UserRecord>> {
        let predicate = if username {
            "lower(u.username) = lower($1)"
        } else {
            "u.id = $1"
        };
        sqlx::QueryBuilder::<Postgres>::new(USER_SELECT)
            .push(" where ")
            .push(predicate)
            .build()
            .bind(value)
            .fetch_optional(&self.security.pool)
            .await
            .map(|r| r.map(decode))
            .map_err(db_error)
    }

    pub(crate) async fn users(&self) -> AdminStoreResult<Vec<UserRecord>> {
        sqlx::QueryBuilder::<Postgres>::new(USER_SELECT)
            .push(" order by u.created_at desc, u.id desc")
            .build()
            .fetch_all(&self.security.pool)
            .await
            .map(|r| r.into_iter().map(decode).collect())
            .map_err(db_error)
    }

    pub(crate) async fn insert_user(
        &self,
        id: &str,
        username: &str,
        password_hash: &str,
        group_ids: &[String],
        limits: gateway_core::policy::RateLimits,
        context: &MutationContext,
    ) -> AdminStoreResult<UserRecord> {
        let mut tx = self.security.pool.begin().await.map_err(db_error)?;
        audit(
            &mut tx,
            context,
            id,
            "user.create",
            &[
                "username",
                "group_ids",
                "max_concurrency",
                "requests_per_minute",
            ],
        )
        .await?;
        require_administrator(&mut tx, context).await?;
        sqlx::query("insert into users(id, username, password_hash, max_concurrency, requests_per_minute, created_at, updated_at) values ($1, $2, $3, $4, $5, now(), now())")
            .bind(id).bind(username).bind(password_hash).bind(i64::try_from(limits.max_concurrency).map_err(|_| invalid_limits())?).bind(i64::try_from(limits.requests_per_minute).map_err(|_| invalid_limits())?).execute(&mut *tx).await.map_err(db_error)?;
        grants(&mut tx, id, group_ids).await?;
        let record = sqlx::QueryBuilder::<Postgres>::new(USER_SELECT)
            .push(" where u.id = $1")
            .build()
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(decode(record))
    }

    pub(crate) async fn modify_user(
        &self,
        command: UpdateUser,
        context: &MutationContext,
    ) -> AdminStoreResult<(gateway_admin::model::Revision, UserRecord)> {
        let mut tx = self.security.pool.begin().await.map_err(db_error)?;
        // Serialize user administration before taking identity locks.
        let revision = crate::postgres::bump_config_revision_in_transaction(&mut tx)
            .await
            .map_err(|e| admin_store_error("user", e))?;
        require_administrator(&mut tx, context).await?;
        let row = sqlx::query("select role from users where id = $1 for update")
            .bind(&command.id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?
            .ok_or_else(|| {
                AdminStoreError::new(AdminStoreErrorKind::NotFound, "user", "user not found")
            })?;
        if row.get::<String, _>("role") == "admin" && !command.enabled {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Conflict,
                "user",
                "cannot disable administrator",
            ));
        }
        sqlx::query("update users set enabled = $2, max_concurrency = $3, requests_per_minute = $4, auth_version = auth_version + 1, updated_at = now() where id = $1")
            .bind(&command.id).bind(command.enabled).bind(i64::try_from(command.limits.max_concurrency).map_err(|_| invalid_limits())?).bind(i64::try_from(command.limits.requests_per_minute).map_err(|_| invalid_limits())?).execute(&mut *tx).await.map_err(db_error)?;
        grants(&mut tx, &command.id, &command.group_ids).await?;
        crate::postgres::append_admin_audit_event_in_transaction(
            &mut tx,
            mutation_audit(
                context,
                "user.update",
                "user",
                &command.id,
                vec![
                    "enabled".into(),
                    "group_ids".into(),
                    "max_concurrency".into(),
                    "requests_per_minute".into(),
                ],
            ),
            revision,
        )
        .await
        .map_err(|e| admin_store_error("user", e))?;
        let record = sqlx::QueryBuilder::<Postgres>::new(USER_SELECT)
            .push(" where u.id = $1")
            .build()
            .bind(&command.id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok((crate::admin_revision(revision)?, decode(record)))
    }

    pub(crate) async fn replace_password(
        &self,
        id: &str,
        expected_version: i64,
        password_hash: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<()> {
        if !matches!(&context.actor, MutationActor::AdminSession { admin_user_id } if admin_user_id == id)
        {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "user",
                "own session required",
            ));
        }
        let mut tx = self.security.pool.begin().await.map_err(db_error)?;
        // Same lock order as user administration.
        audit(&mut tx, context, id, "user.password.change", &["password"]).await?;
        let count = sqlx::query("update users set password_hash = $3, auth_version = auth_version + 1, updated_at = now() where id = $1 and auth_version = $2 and enabled")
            .bind(id).bind(expected_version).bind(password_hash).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
        if count != 1 {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Conflict,
                "user",
                "session changed; sign in again",
            ));
        }
        tx.commit().await.map_err(db_error)
    }
}
