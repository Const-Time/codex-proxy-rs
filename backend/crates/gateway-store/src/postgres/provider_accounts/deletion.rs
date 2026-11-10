//! Detach retained history in bounded transactions before the terminal delete.
use super::*;

pub(super) async fn detach_history(
    pool: &PgPool,
    scope: &ProviderAccountAdminScope,
    account_ids: &[String],
) -> StoreResult<()> {
    // Validate the complete selection before any preparatory writes. History
    // snapshots remain authoritative even if deletion is cancelled afterwards.
    let count: i64 = sqlx::query_scalar(
        "select count(*) from provider_accounts where id = any($1) and provider_kind = $2",
    )
    .bind(account_ids)
    .bind(&scope.provider_kind)
    .fetch_one(pool)
    .await
    .map_err(|_| postgres_unavailable("validate account deletion"))?;
    if usize::try_from(count).ok() != Some(account_ids.len()) {
        return Err(invalid(
            "all deleted accounts must exist and match Provider scope",
        ));
    }
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for statement in [
            "with batch as materialized (
                select id from model_requests
                where provider_account_id = any($1) and outcome <> 'running'
                order by id limit 1000 for update
             ) update model_requests r set provider_account_id = null
               from batch where r.id = batch.id",
            "with batch as materialized (
                select id from ops_events where provider_account_id = any($1)
                order by id limit 1000 for update
             ) update ops_events e set provider_account_id = null
               from batch where e.id = batch.id",
        ] {
            loop {
                let mut tx = pool
                    .begin()
                    .await
                    .map_err(|_| postgres_unavailable("begin account history detach"))?;
                sqlx::raw_sql("set local lock_timeout = '1s'; set local statement_timeout = '5s';")
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| postgres_unavailable("bound account history detach"))?;
                let changed = sqlx::query(statement)
                    .bind(account_ids)
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| postgres_unavailable("detach account history"))?
                    .rows_affected();
                tx.commit()
                    .await
                    .map_err(|_| postgres_unavailable("commit account history detach"))?;
                if changed < 1000 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| postgres_unavailable("account history detach deadline"))?
}
