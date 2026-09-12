//! Persistent, allowlisted control-plane security audit and safe usage catalog.
use crate::admin_adapter::UserAuthStore;
use gateway_admin::{
    model::operations::*,
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use sqlx::Row;

fn error(_: sqlx::Error) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "operation log",
        "operation log storage unavailable",
    )
}

impl UserAuthStore {
    pub(crate) async fn record_operation_stored(
        &self,
        mut event: OperationLog,
    ) -> AdminStoreResult<()> {
        if let Some(id) = &event.actor_user_id
            && let Some(row) = sqlx::query("select username, display_name from users where id = $1")
                .bind(id)
                .fetch_optional(&self.security.pool)
                .await
                .map_err(error)?
        {
            event.email = Some(row.get("username"));
            event.username = Some(row.get("display_name"));
        }
        sqlx::query("insert into operation_logs(id, occurred_at, payload) values($1,$2,$3) on conflict do nothing")
            .bind(&event.id).bind(event.occurred_at).bind(sqlx::types::Json(&event))
            .execute(&self.security.pool).await.map_err(error)?;
        Ok(())
    }

    pub(crate) async fn operation_logs_stored(
        &self,
        query: OperationLogQuery,
    ) -> AdminStoreResult<OperationLogPage> {
        let end = query.end_time.unwrap_or_else(chrono::Utc::now);
        let start = query.start_time.unwrap_or(end - chrono::Duration::days(7));
        let mut tx = self.security.pool.begin().await.map_err(error)?;
        sqlx::query("set transaction isolation level repeatable read, read only")
            .execute(&mut *tx)
            .await
            .map_err(error)?;
        sqlx::query("set local statement_timeout = '10s'")
            .execute(&mut *tx)
            .await
            .map_err(error)?;
        let filter = " from operation_logs l where occurred_at >= $1 and occurred_at < $2
            and ($3::text is null or strpos(lower(coalesce(payload->>'email','')), lower($3)) > 0)
            and ($4::text is null or strpos(lower(payload->>'path'), lower($4)) > 0)
            and ($5::text is null or payload->>'clientIp' = $5 or payload->>'forwardedIp' = $5)
            and ($6::text is null or payload->>'method' = $6)
            and ($7::text is null or payload->>'authMethod' = $7)
            and ($8::text is null or ($8 = 'success' and (payload->>'status')::int < 400) or ($8 = 'failure' and (payload->>'status')::int >= 400))";
        // SQL is assembled exclusively from the fixed fragments above; all filters are bound.
        let bind = |sql: String| {
            sqlx::query_scalar::<_, serde_json::Value>(sqlx::AssertSqlSafe(sql))
                .bind(start)
                .bind(end)
                .bind(query.email.clone())
                .bind(query.action.clone())
                .bind(query.ip.clone())
                .bind(query.method.clone())
                .bind(query.auth_method.clone())
                .bind(query.result.clone())
        };
        let count_sql = format!("select to_jsonb(count(*)){filter}");
        let total = bind(count_sql)
            .fetch_one(&mut *tx)
            .await
            .map_err(error)?
            .as_i64()
            .unwrap_or(0);
        let list_sql = format!(
            "select payload || jsonb_build_object('changes', coalesce((select jsonb_agg(jsonb_build_object('action', action, 'entityKind', entity_kind, 'entityRef', entity_ref, 'changedFields', changed_fields)) from admin_audit_events where admin_request_id = l.payload->>'requestId'), '[]'::jsonb)){filter} order by occurred_at desc, id desc limit 50 offset $9"
        );
        let items = bind(list_sql)
            .bind(i64::from(query.page.unwrap_or(1).saturating_sub(1)) * 50)
            .fetch_all(&mut *tx)
            .await
            .map_err(error)?;
        tx.commit().await.map_err(error)?;
        Ok(OperationLogPage { items, total })
    }

    pub(crate) async fn usage_key_options_stored(
        &self,
        owner: Option<&str>,
    ) -> AdminStoreResult<Vec<UsageKeyOption>> {
        let rows = sqlx::query("with refs as (
            select id, owner_user_id as user_id from client_api_keys where $1::text is null or owner_user_id = $1
            union select client_api_key_ref, user_id from model_requests where $1::text is null or user_id = $1)
            select r.id, r.user_id, coalesce(k.name, r.id) as name from refs r left join client_api_keys k on k.id = r.id
            where r.id is not null order by name, r.id")
            .bind(owner).fetch_all(&self.security.pool).await.map_err(error)?;
        Ok(rows
            .into_iter()
            .map(|row| UsageKeyOption {
                id: row.get("id"),
                name: row.get("name"),
                user_id: row.get("user_id"),
            })
            .collect())
    }
}
