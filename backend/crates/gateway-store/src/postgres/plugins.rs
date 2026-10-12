//! 插件制品、实例 CAS 与受 revision 保护的私有状态；不读写用户账本。
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationContext, plugins::*},
    ports::plugins::PluginStore,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;

pub struct PgPluginStore {
    pool: PgPool,
}
impl PgPluginStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
fn unavailable() -> AdminError {
    AdminError::unavailable("插件存储暂不可用")
}
fn conflict() -> AdminError {
    AdminError::conflict("插件或状态版本已变更，请重新读取")
}
fn signed(value: u64) -> Result<i64, AdminError> {
    i64::try_from(value).map_err(|_| AdminError::invalid("插件版本不合法"))
}

async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    actor: &MutationContext,
    id: &str,
    action: &str,
) -> Result<(), AdminError> {
    let event = crate::mutation_audit(actor, action, "plugin", id, vec!["plugins".into()]);
    let revision: i64 =
        sqlx::query_scalar("select config_revision from runtime_settings where id=1")
            .fetch_one(&mut **tx)
            .await
            .map_err(|_| unavailable())?;
    super::append_admin_audit_event_in_transaction(
        tx,
        event,
        crate::Revision::new(revision as u64).map_err(|_| unavailable())?,
    )
    .await
    .map_err(|_| unavailable())
}
async fn lock_scope(
    tx: &mut Transaction<'_, Postgres>,
    scope: &PluginStateScope,
) -> Result<(), AdminError> {
    let revision: Option<i64> = sqlx::query_scalar(
        "select revision from plugin_instances where id=$1 and enabled for update",
    )
    .bind(&scope.instance_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| unavailable())?;
    if revision != Some(signed(scope.revision)?) {
        return Err(conflict());
    }
    Ok(())
}
#[async_trait]
impl PluginStore for PgPluginStore {
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        let infos: Vec<serde_json::Value> =
            sqlx::query_scalar("select info from plugin_artifacts order by created_at desc")
                .fetch_all(&self.pool)
                .await
                .map_err(|_| unavailable())?;
        infos
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(|_| unavailable()))
            .collect()
    }
    async fn delete_artifact(
        &self,
        digest: &str,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query("select pg_advisory_xact_lock(727001)")
            .execute(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
        let used: bool = sqlx::query_scalar(
            "select exists(select 1 from plugin_instances where artifact_sha256=$1)",
        )
        .bind(digest)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
        if used {
            return Err(AdminError::conflict("此制品仍被插件实例使用"));
        }
        let result = sqlx::query("delete from plugin_artifacts where sha256=$1")
            .bind(digest)
            .execute(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
        if result.rows_affected() > 0 {
            audit(&mut tx, actor, digest, "plugin.artifact.delete").await?;
        }
        tx.commit().await.map_err(|_| unavailable())
    }
    async fn artifact_info(&self, digest: &str) -> Result<PluginArtifactInfo, AdminError> {
        let info: serde_json::Value =
            sqlx::query_scalar("select info from plugin_artifacts where sha256=$1")
                .bind(digest)
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| unavailable())?
                .ok_or_else(|| AdminError::not_found("插件制品不存在"))?;
        serde_json::from_value(info).map_err(|_| unavailable())
    }
    async fn artifact(&self, digest: &str) -> Result<PluginArtifact, AdminError> {
        let row = sqlx::query("select info, archive from plugin_artifacts where sha256=$1")
            .bind(digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| unavailable())?
            .ok_or_else(|| AdminError::not_found("插件制品不存在"))?;
        let archive: Vec<u8> = row.try_get("archive").map_err(|_| unavailable())?;
        Ok(PluginArtifact {
            info: serde_json::from_value(row.try_get("info").map_err(|_| unavailable())?)
                .map_err(|_| unavailable())?,
            archive: Arc::from(archive),
        })
    }
    async fn put_artifact(
        &self,
        artifact: PluginArtifact,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query("select pg_advisory_xact_lock(727001)")
            .execute(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from plugin_artifacts where sha256=$1)")
                .bind(&artifact.info.sha256)
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| unavailable())?;
        if exists {
            return Ok(());
        }
        let (count, size): (i64, i64) = sqlx::query_as(
            "select count(*), coalesce(sum(octet_length(archive)),0)::bigint from plugin_artifacts",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
        if count >= 128 || size.saturating_add(artifact.archive.len() as i64) > 512 * 1024 * 1024 {
            return Err(AdminError::invalid("插件制品存储达到上限"));
        }
        sqlx::query("insert into plugin_artifacts(sha256,info,archive) values($1,$2,$3)")
            .bind(&artifact.info.sha256)
            .bind(serde_json::to_value(&artifact.info).map_err(|_| unavailable())?)
            .bind(artifact.archive.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
        audit(&mut tx, actor, &artifact.info.sha256, "plugin.upload").await?;
        tx.commit().await.map_err(|_| unavailable())
    }
    async fn list(&self) -> Result<Vec<PluginRecord>, AdminError> {
        let rows = sqlx::query("select id,revision,name,artifact_sha256,enabled,configuration,secrets,frontend_authentication,middleware,model_router,account_scheduler from plugin_instances order by name,id").fetch_all(&self.pool).await.map_err(|_| unavailable())?;
        rows.into_iter()
            .map(|row| {
                Ok(PluginRecord {
                    account_scheduler: row
                        .try_get::<Option<serde_json::Value>, _>("account_scheduler")
                        .map_err(|_| unavailable())?
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|_| unavailable())?,
                    model_router: row
                        .try_get::<Option<serde_json::Value>, _>("model_router")
                        .map_err(|_| unavailable())?
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|_| unavailable())?,
                    middleware: row
                        .try_get::<Option<serde_json::Value>, _>("middleware")
                        .map_err(|_| unavailable())?
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|_| unavailable())?,
                    frontend_authentication: row
                        .try_get::<Option<serde_json::Value>, _>("frontend_authentication")
                        .map_err(|_| unavailable())?
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|_| unavailable())?,
                    id: row.try_get("id").map_err(|_| unavailable())?,
                    revision: row
                        .try_get::<i64, _>("revision")
                        .map_err(|_| unavailable())? as u64,
                    name: row.try_get("name").map_err(|_| unavailable())?,
                    artifact_sha256: row.try_get("artifact_sha256").map_err(|_| unavailable())?,
                    enabled: row.try_get("enabled").map_err(|_| unavailable())?,
                    configuration: row.try_get("configuration").map_err(|_| unavailable())?,
                    secrets: serde_json::from_value(
                        row.try_get("secrets").map_err(|_| unavailable())?,
                    )
                    .map_err(|_| unavailable())?,
                })
            })
            .collect()
    }
    async fn save(
        &self,
        record: &PluginRecord,
        expected: u64,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        if let Some(binding) = &record.account_scheduler {
            binding.validate()?;
        }
        if let Some(binding) = &record.model_router {
            binding.validate()?;
        }
        if let Some(binding) = &record.middleware {
            binding.validate()?;
        }
        if let Some(binding) = &record.frontend_authentication {
            binding.validate()?;
        }
        let query = if expected == 0 {
            "insert into plugin_instances(id,revision,name,artifact_sha256,enabled,configuration,secrets,frontend_authentication,middleware,model_router,account_scheduler) select $1,$2,$3,$4,$5,$6,$7,$9,$10,$11,$12 where $8=0 on conflict do nothing"
        } else {
            "update plugin_instances set revision=$2,name=$3,artifact_sha256=$4,enabled=$5,configuration=$6,secrets=$7,frontend_authentication=$9,middleware=$10,model_router=$11,account_scheduler=$12,updated_at=now() where id=$1 and revision=$8"
        };
        let result = sqlx::query(query)
            .bind(&record.id)
            .bind(signed(record.revision)?)
            .bind(&record.name)
            .bind(&record.artifact_sha256)
            .bind(record.enabled)
            .bind(&record.configuration)
            .bind(serde_json::to_value(&record.secrets).map_err(|_| unavailable())?)
            .bind(signed(expected)?)
            .bind(
                record
                    .frontend_authentication
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| unavailable())?,
            )
            .bind(
                record
                    .middleware
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| unavailable())?,
            )
            .bind(
                record
                    .model_router
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| unavailable())?,
            )
            .bind(
                record
                    .account_scheduler
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| unavailable())?,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                if e.as_database_error()
                    .is_some_and(|e| e.is_unique_violation())
                {
                    conflict()
                } else {
                    unavailable()
                }
            })?;
        if result.rows_affected() != 1 {
            return Err(conflict());
        }
        audit(
            &mut tx,
            actor,
            &record.id,
            if expected == 0 {
                "plugin.install"
            } else {
                "plugin.update"
            },
        )
        .await?;
        tx.commit().await.map_err(|_| unavailable())
    }
    async fn delete(
        &self,
        id: &str,
        expected: u64,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        let result =
            sqlx::query("delete from plugin_instances where id=$1 and revision=$2 and not enabled")
                .bind(id)
                .bind(signed(expected)?)
                .execute(&mut *tx)
                .await
                .map_err(|_| unavailable())?;
        if result.rows_affected() != 1 {
            return Err(AdminError::conflict("请先停用插件并刷新版本后卸载"));
        }
        audit(&mut tx, actor, id, "plugin.uninstall").await?;
        tx.commit().await.map_err(|_| unavailable())
    }
    async fn state_get(
        &self,
        scope: &PluginStateScope,
        key: &str,
    ) -> Result<Option<PluginStateRecord>, AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        lock_scope(&mut tx, scope).await?;
        let row = sqlx::query("select value,version,schema_version from plugin_private_state where instance_id=$1 and namespace=$2 and key=$3")
            .bind(&scope.instance_id).bind(&scope.namespace).bind(key).fetch_optional(&mut *tx).await.map_err(|_| unavailable())?;
        row.map(|row| {
            Ok(PluginStateRecord {
                value: row.try_get("value").map_err(|_| unavailable())?,
                version: row
                    .try_get::<i64, _>("version")
                    .map_err(|_| unavailable())? as u64,
                schema_version: row
                    .try_get::<i32, _>("schema_version")
                    .map_err(|_| unavailable())? as u32,
            })
        })
        .transpose()
    }
    async fn state_put(
        &self,
        scope: &PluginStateScope,
        key: &str,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
    ) -> Result<u64, AdminError> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        lock_scope(&mut tx, scope).await?;
        let current: Option<i64> = sqlx::query_scalar("select version from plugin_private_state where instance_id=$1 and namespace=$2 and key=$3")
            .bind(&scope.instance_id).bind(&scope.namespace).bind(key).fetch_optional(&mut *tx).await.map_err(|_| unavailable())?;
        if current != expected.map(signed).transpose()? {
            return Err(conflict());
        }
        let version: i64 = if let Some(value) = value {
            let (count, bytes): (i64,i64) = sqlx::query_as("select count(*),coalesce(sum(octet_length(value::text)),0)::bigint from plugin_private_state where instance_id=$1 and namespace=$2 and key<>$3")
                .bind(&scope.instance_id).bind(&scope.namespace).bind(key).fetch_one(&mut *tx).await.map_err(|_| unavailable())?;
            let size: i32 = sqlx::query_scalar("select octet_length($1::jsonb::text)")
                .bind(&value)
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| unavailable())?;
            if count >= i64::from(scope.maximum_records)
                || bytes.saturating_add(i64::from(size)) as u64 > scope.maximum_bytes
            {
                return Err(AdminError::invalid("插件私有状态达到配额"));
            }
            sqlx::query_scalar("insert into plugin_private_state(instance_id,namespace,key,schema_version,value) values($1,$2,$3,$4,$5) on conflict(instance_id,namespace,key) do update set value=excluded.value,schema_version=excluded.schema_version,version=nextval('plugin_state_version') returning version")
                .bind(&scope.instance_id).bind(&scope.namespace).bind(key).bind(i32::try_from(scope.schema_version).map_err(|_| unavailable())?).bind(value)
                .fetch_one(&mut *tx).await.map_err(|_| unavailable())?
        } else {
            if current.is_none() {
                return Err(conflict());
            }
            sqlx::query(
                "delete from plugin_private_state where instance_id=$1 and namespace=$2 and key=$3",
            )
            .bind(&scope.instance_id)
            .bind(&scope.namespace)
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
            current.unwrap_or_default()
        };
        tx.commit().await.map_err(|_| unavailable())?;
        Ok(version as u64)
    }
}

fn plugin_unavailable() -> gateway_admin::ports::store::AdminStoreError {
    gateway_admin::ports::store::AdminStoreError::new(
        gateway_admin::ports::store::AdminStoreErrorKind::Unavailable,
        "plugin resource",
        "storage unavailable",
    )
}
pub(crate) async fn plugin_current_revision(
    tx: &mut Transaction<'_, Postgres>,
) -> gateway_admin::ports::store::AdminStoreResult<gateway_admin::model::Revision> {
    let revision: i64 =
        sqlx::query_scalar("select config_revision from runtime_settings where id=1")
            .fetch_one(&mut **tx)
            .await
            .map_err(|_| plugin_unavailable())?;
    crate::admin_revision(
        crate::Revision::new(revision as u64)
            .map_err(|e| crate::admin_store_error("plugin resource", e))?,
    )
}
pub(crate) async fn lock_plugin_resource(
    tx: &mut Transaction<'_, Postgres>,
    scope: &PluginMutationScope,
) -> gateway_admin::ports::store::AdminStoreResult<()> {
    let revision: Option<i64> = sqlx::query_scalar(
        "select revision from plugin_instances where id=$1 and enabled for update",
    )
    .bind(&scope.instance_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| plugin_unavailable())?;
    if revision.and_then(|v| u64::try_from(v).ok()) != Some(scope.revision) {
        return Err(gateway_admin::ports::store::AdminStoreError::new(
            gateway_admin::ports::store::AdminStoreErrorKind::Conflict,
            "plugin resource",
            "stale plugin generation",
        ));
    }
    Ok(())
}
