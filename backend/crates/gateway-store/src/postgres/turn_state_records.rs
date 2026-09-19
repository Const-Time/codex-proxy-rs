use gateway_admin::{
    model::turn_state::*,
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use sqlx::{PgPool, Postgres, QueryBuilder};

fn unavailable() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "state probe records",
        "探测记录存储不可用",
    )
}

fn filtered(query: &TurnStateRecordQuery) -> QueryBuilder<Postgres> {
    // Expired unfinished requests are unknown/interrupted, never assumed successes.
    let mut sql = QueryBuilder::new(
        "with records as (select *, case when finished_at is null and deadline_at < now() - interval '30 seconds'
         and facts->>'decision' = 'running'
         then facts || '{\"decision\":\"interrupted\",\"message\":\"进程中断或结果未落盘，完成情况未知\"}'::jsonb
         else facts end as details from turn_state_probe_records where started_at >= "
    );
    sql.push_bind(query.from)
        .push(" and started_at < ")
        .push_bind(query.to);
    if let Some(value) = &query.account_id {
        sql.push(" and account_id = ").push_bind(value);
    }
    if let Some(value) = &query.model {
        sql.push(" and model = ").push_bind(value);
    }
    if let Some(value) = &query.phase {
        sql.push(" and phase = ").push_bind(value);
    }
    if let Some(value) = &query.cycle_id {
        sql.push(" and cycle_id = ").push_bind(value);
    }
    sql.push("), filtered as (select * from records");
    if let Some(value) = &query.decision {
        sql.push(" where details->>'decision' = ").push_bind(value);
    }
    sql.push(") ");
    sql
}

pub(super) async fn list(
    pool: &PgPool,
    query: &TurnStateRecordQuery,
) -> AdminStoreResult<TurnStateRecordPage> {
    query.validate().map_err(|_| unavailable())?;
    let mut tx = pool.begin().await.map_err(|_| unavailable())?;
    sqlx::query("set transaction isolation level repeatable read, read only")
        .execute(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    let mut summary = filtered(query);
    summary.push(
        "select jsonb_build_object(
          'total', count(*),
          'completed', count(*) filter(where details->>'completed' = 'true'),
          'collections', count(*) filter(where phase='collect'),
          'collectionResults', count(*) filter(where phase='collect' and details->>'decision' in ('accepted','rejected','failed')),
          'accepted', count(*) filter(where phase='collect' and details->>'decision'='accepted'),
          'verifications', count(*) filter(where phase='verify'),
          'verificationResults', count(*) filter(where phase='verify' and details->>'decision' in ('verified','failed')),
          'verified', count(*) filter(where phase='verify' and details->>'decision'='verified'),
          'rejected', count(*) filter(where details->>'decision'='rejected'),
          'failed', count(*) filter(where details->>'decision' in ('failed','interrupted')),
          'unknownTokens', count(*) filter(where details->>'inputTokens' is null or details->>'outputTokens' is null),
          'knownTokens', coalesce(sum((details->>'inputTokens')::bigint + (details->>'outputTokens')::bigint),0),
          'averageLatencyMs', avg((details->>'latencyMs')::bigint)
        ) from filtered"
    );
    let stats: serde_json::Value = summary
        .build_query_scalar()
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    let mut rows = filtered(query);
    rows.push("select jsonb_build_object('id',id,'cycleId',cycle_id,'accountId',account_id,'accountName',account_name,
      'model',model,'phase',phase,'poolId',pool_id,'routeName',route_name,'startedAt',started_at,'finishedAt',finished_at,
      'facts',details) from filtered order by started_at desc, id desc limit ")
        .push_bind(i64::from(query.page_size)).push(" offset ")
        .push_bind(i64::from(query.page - 1) * i64::from(query.page_size));
    let values: Vec<serde_json::Value> = rows
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    let items = values
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| unavailable())?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(TurnStateRecordPage {
        items,
        stats: serde_json::from_value(stats).map_err(|_| unavailable())?,
    })
}
