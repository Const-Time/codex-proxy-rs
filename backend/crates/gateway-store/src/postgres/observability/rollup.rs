//! Incremental additive statistics. Percentiles deliberately remain exact raw queries.
use super::*;

const DIMENSIONS: &str = "user_id, client_api_key_ref, routing_group_refs, provider_account_ref,
    operation, provider_kind, requested_model_id, upstream_model_id, outcome,
    client_status_code, upstream_status_code, client_transport, upstream_transport";
const EPOCH: &str = "timestamptz '1970-01-01 00:00:00+00'";

#[derive(Clone)]
pub struct PgUsageRollupRepository {
    pool: PgPool,
}
impl PgUsageRollupRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// One bounded transaction per hour, never a full-table materialized-view refresh.
    /// Writers racing the dirty-row deletion reinsert it after this transaction commits.
    pub async fn refresh_batch(&self, now: DateTime<Utc>) -> StoreResult<usize> {
        let cutoff = DateTime::from_timestamp(now.timestamp().div_euclid(3600) * 3600 - 3600, 0)
            .ok_or_else(|| invalid("rollup cutoff outside supported timestamps"))?;
        let mut refreshed = 0;
        for _ in 0..12 {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|_| postgres_unavailable("begin usage rollup"))?;
            sqlx::query("set local statement_timeout = '5s'")
                .execute(&mut *tx)
                .await
                .map_err(|_| postgres_unavailable("bound usage rollup"))?;
            // The cursor lock serializes initial backfill and late corrections across workers.
            let Some(cursor): Option<DateTime<Utc>> = sqlx::query_scalar(
                "select next_hour from usage_hourly_cursor where singleton for update skip locked",
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| postgres_unavailable("lock usage rollup cursor"))?
            else {
                break;
            };
            let dirty: Option<DateTime<Utc>> = sqlx::query_scalar("select bucket_start from usage_hourly_dirty where bucket_start < $1 order by bucket_start limit 1 for update skip locked")
                .bind(cutoff).fetch_optional(&mut *tx).await.map_err(|_| postgres_unavailable("lock dirty usage hour"))?;
            let Some(hour) = dirty.or((cursor < cutoff).then_some(cursor)) else {
                break;
            };
            sqlx::query("delete from usage_hourly_dirty where bucket_start = $1")
                .bind(hour)
                .execute(&mut *tx)
                .await
                .map_err(|_| postgres_unavailable("consume dirty usage hour"))?;
            sqlx::query(
                "insert into usage_hourly_covered(bucket_start) values ($1) on conflict do nothing",
            )
            .bind(hour)
            .execute(&mut *tx)
            .await
            .map_err(|_| postgres_unavailable("cover usage hour"))?;
            sqlx::query("delete from usage_hourly_stats where bucket_start = $1")
                .bind(hour)
                .execute(&mut *tx)
                .await
                .map_err(|_| postgres_unavailable("replace usage hour"))?;
            let dimensions = DIMENSIONS
                .split(',')
                .map(|s| format!("mr.{}", s.trim()))
                .collect::<Vec<_>>()
                .join(", ");
            let mut query = QueryBuilder::<Postgres>::new(format!("insert into usage_hourly_stats
                (bucket_start, {DIMENSIONS}, request_count, success_count, input_tokens, output_tokens,
                 cached_tokens, cache_write_tokens, total_tokens, latency_sum, latency_count, billing_usd, usage_billing_usd)
                select "));
            query.push_bind(hour).push(format!(", {dimensions}, {} from model_requests mr
                left join user_group_charge_events charge on charge.request_id = mr.id where mr.started_at >= ", aggregates()));
            query
                .push_bind(hour)
                .push(" and mr.started_at < ")
                .push_bind(hour + TimeDelta::hours(1));
            query.push(format!(" group by {dimensions}"));
            query
                .build()
                .execute(&mut *tx)
                .await
                .map_err(|_| postgres_unavailable("aggregate usage hour"))?;
            if hour == cursor {
                sqlx::query("update usage_hourly_cursor set next_hour = $1 where singleton")
                    .bind(hour + TimeDelta::hours(1))
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| postgres_unavailable("advance usage rollup"))?;
            }
            tx.commit()
                .await
                .map_err(|_| postgres_unavailable("commit usage rollup"))?;
            refreshed += 1;
        }
        Ok(refreshed)
    }
}

fn aggregates() -> String {
    let fact = format!(
        "({}) and mr.recovered_at is null",
        completed_usage_fact_predicate("mr")
    );
    format!("count(*) filter (where mr.recovered_at is null)::bigint as request_count,
        count(*) filter (where mr.recovered_at is null and mr.outcome='succeeded')::bigint as success_count,
        coalesce(sum(mr.input_tokens) filter (where {fact}),0)::bigint as input_tokens,
        coalesce(sum(mr.output_tokens) filter (where {fact}),0)::bigint as output_tokens,
        coalesce(sum(mr.cached_tokens) filter (where {fact}),0)::bigint as cached_tokens,
        coalesce(sum(mr.cache_write_tokens) filter (where {fact}),0)::bigint as cache_write_tokens,
        coalesce(sum(mr.total_tokens) filter (where {fact}),0)::bigint as total_tokens,
        coalesce(sum(mr.latency_ms) filter (where {fact}),0)::bigint as latency_sum,
        count(mr.latency_ms) filter (where {fact})::bigint as latency_count,
        coalesce(sum(coalesce(charge.amount_usd, case when mr.cost_currency='USD' then mr.billed_cost_amount end)),0) as billing_usd,
        sum(mr.billed_cost_amount) filter (where {fact} and mr.cost_currency='USD') as usage_billing_usd")
}

pub(crate) async fn card_summary(
    pool: &PgPool,
    range: ObservabilityRange,
    filter: &UsageRecordFilter,
) -> StoreResult<Option<UsageCardSummary>> {
    // Request-specific/search/attempt filters need raw evidence. Short windows do not benefit.
    if range.end - range.start < TimeDelta::hours(2)
        || range.end - range.start > TimeDelta::days(366)
        || filter.request_id.is_some()
        || filter.attempt_index.is_some()
        || filter.response_id.is_some()
        || filter.upstream_request_id.is_some()
        || filter.search.is_some()
    {
        return Ok(None);
    }
    let covered: bool = sqlx::query_scalar("select exists(select 1 from usage_hourly_covered where bucket_start >= $1 and bucket_start + interval '1 hour' <= $2)")
        .bind(range.start).bind(range.end).fetch_one(pool).await.map_err(|_| postgres_unavailable("check usage rollup coverage"))?;
    if !covered {
        return Ok(None);
    }
    // Eligible whole hours and the raw remainder share one statement snapshot. Dirty/missing
    // hours always read raw facts, even while an asynchronous correction/backfill is pending.
    let mut query = QueryBuilder::<Postgres>::new("with bounds as (select ");
    query
        .push_bind(range.start)
        .push("::timestamptz as first, ")
        .push_bind(range.end)
        .push(
            "::timestamptz as last), covered as (
        select h.bucket_start from usage_hourly_covered h cross join bounds b
        where h.bucket_start >= b.first and h.bucket_start + interval '1 hour' <= b.last
        and h.bucket_start < ",
        );
    query.push(format!("date_bin(interval '1 hour', now(), {EPOCH}) - interval '1 hour'
        and not exists(select 1 from usage_hourly_dirty d where d.bucket_start=h.bucket_start)),
        raw_hours as (select greatest(h,b.first) as first, least(h+interval '1 hour',b.last) as last
        from bounds b cross join lateral generate_series(date_bin(interval '1 hour',b.first,{EPOCH}), b.last, interval '1 hour') h
        where h < b.last and not exists(select 1 from covered c where c.bucket_start=h)),
        rows as (select mr.request_count, mr.success_count, mr.input_tokens, mr.output_tokens, mr.cached_tokens,
            mr.cache_write_tokens, mr.total_tokens, mr.latency_sum, mr.latency_count, mr.billing_usd, mr.usage_billing_usd
        from usage_hourly_stats mr join covered c using(bucket_start) where true"));
    push_usage_filter(&mut query, filter, "mr");
    query.push(format!(" union all select facts.* from raw_hours r cross join lateral (
        select {} from model_requests mr left join user_group_charge_events charge on charge.request_id=mr.id
        where mr.started_at>=r.first and mr.started_at<r.last", aggregates()));
    push_usage_filter(&mut query, filter, "mr");
    query.push(") facts) select coalesce(sum(success_count),0)::bigint as success_count,
        coalesce(sum(input_tokens),0)::bigint as input_tokens, coalesce(sum(output_tokens),0)::bigint as output_tokens,
        coalesce(sum(cached_tokens),0)::bigint as cached_tokens, coalesce(sum(cache_write_tokens),0)::bigint as cache_write_tokens,
        coalesce(sum(total_tokens),0)::bigint as total_tokens, coalesce(sum(latency_sum),0)::bigint as latency_sum,
        coalesce(sum(latency_count),0)::bigint as latency_count, coalesce(sum(billing_usd),0)::text as amount from rows");
    // The planner estimates many missing hours before initial backfill. JIT compilation
    // can dominate an otherwise tiny hybrid read; disable it only for this transaction.
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| postgres_unavailable("begin hybrid usage cards"))?;
    sqlx::query("set local jit = off")
        .execute(&mut *tx)
        .await
        .map_err(|_| postgres_unavailable("plan hybrid usage cards"))?;
    let row = query
        .build()
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| postgres_unavailable("read hybrid usage cards"))?;
    tx.commit()
        .await
        .map_err(|_| postgres_unavailable("complete hybrid usage cards"))?;
    let count = unsigned(&row, "latency_count")?;
    Ok(Some(UsageCardSummary {
        total_cost_usd: DecimalAmount::from_str(&get::<String>(&row, "amount")?)?,
        success_count: unsigned(&row, "success_count")?,
        input_tokens: unsigned(&row, "input_tokens")?,
        output_tokens: unsigned(&row, "output_tokens")?,
        cached_tokens: unsigned(&row, "cached_tokens")?,
        cache_write_tokens: unsigned(&row, "cache_write_tokens")?,
        total_tokens: unsigned(&row, "total_tokens")?,
        average_latency_ms: unsigned(&row, "latency_sum")?.checked_div(count),
    }))
}
