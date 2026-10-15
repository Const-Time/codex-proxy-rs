//! 观测查询族：dashboard / usage / ops / accounts。

mod accounts;
mod dashboard;
mod ops;
mod usage;

pub(crate) use accounts::*;
pub(crate) use dashboard::*;
pub(crate) use ops::*;
pub(crate) use usage::*;

/// Exact totals and candidate rows must share a snapshot during concurrent writes.
async fn begin_page_snapshot(
    connection: &mut sqlx::PgConnection,
) -> crate::StoreResult<sqlx::Transaction<'_, sqlx::Postgres>> {
    use sqlx::Connection;
    let mut tx = connection
        .begin()
        .await
        .map_err(|_| crate::postgres_unavailable("begin history page snapshot"))?;
    sqlx::query("set transaction isolation level repeatable read, read only")
        .execute(&mut *tx)
        .await
        .map_err(|_| crate::postgres_unavailable("isolate history page snapshot"))?;
    sqlx::query("set local statement_timeout = '60s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| crate::postgres_unavailable("bound history page query"))?;
    Ok(tx)
}
