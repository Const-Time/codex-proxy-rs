//! Group-configured budgets, shared by every key owned by the same user in a group.

use crate::{StoreResult, postgres_unavailable};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_core::{
    engine::budget::{
        ClientBudgetCharge, ClientBudgetError, ClientBudgetLimits, ClientBudgetPort,
        ClientBudgetScope, ClientBudgetStatus,
    },
    error::{GatewayError, GatewayErrorKind},
    metering::Decimal,
    policy::ClientApiKeyId,
    routing::AccountGroupId,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};

pub struct PgClientBudgetStore {
    pool: PgPool,
    retry: Mutex<BTreeMap<String, ClientBudgetCharge>>,
}

impl PgClientBudgetStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: Mutex::new(BTreeMap::new()),
        }
    }

    async fn admit_inner(
        &self,
        key_id: ClientApiKeyId,
        expected_group: Option<AccountGroupId>,
    ) -> Result<ClientBudgetScope, GatewayError> {
        let group = expected_group.ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "a single account group is required",
            )
        })?;
        // Recheck live grants, enabled state, and the routing snapshot at admission.
        let row = sqlx::query("select k.owner_user_id, user_group_quota_limit(g.daily_limit_usd, k.owner_user_id, g.id)::text as daily_limit_usd, user_group_quota_limit(g.weekly_limit_usd, k.owner_user_id, g.id)::text as weekly_limit_usd
            from client_api_keys k join users u on u.id = k.owner_user_id and u.enabled
            join client_api_key_groups kg on kg.client_api_key_id = k.id
            join account_groups g on g.id = kg.account_group_id and g.enabled
            where k.id = $1 and k.enabled and g.id = $2
            and (u.role = 'admin' or exists(select 1 from user_account_groups ug where ug.user_id = u.id and ug.account_group_id = g.id))")
            .bind(key_id.as_str()).bind(group.as_str()).fetch_optional(&self.pool).await.map_err(|_| unavailable())?
            .ok_or_else(|| GatewayError::new(GatewayErrorKind::PolicyDenied, "user, key or group access is disabled or changed"))?;
        let scope = ClientBudgetScope {
            user_id: row.get("owner_user_id"),
            group_id: group,
        };
        let retries = self
            .retry
            .lock()
            .map_err(|_| unavailable())?
            .values()
            .filter(|charge| charge.scope.as_ref() == Some(&scope))
            .cloned()
            .collect::<Vec<_>>();
        for charge in retries {
            self.settle(charge).await.map_err(|_| unavailable())?;
        }
        let limits = ClientBudgetLimits {
            daily_usd: row
                .get::<String, _>("daily_limit_usd")
                .parse()
                .map_err(|_| unavailable())?,
            weekly_usd: row
                .get::<String, _>("weekly_limit_usd")
                .parse()
                .map_err(|_| unavailable())?,
        };
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        let now = Utc::now();
        advance_windows(&mut tx, &scope, now)
            .await
            .map_err(|_| unavailable())?;
        let row = sqlx::query(
            "select daily_used_usd::text, weekly_used_usd::text, daily_end, weekly_end
            from user_group_budget_windows where user_id = $1 and account_group_id = $2",
        )
        .bind(&scope.user_id)
        .bind(scope.group_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
        let daily: Decimal = row
            .get::<String, _>("daily_used_usd")
            .parse()
            .map_err(|_| unavailable())?;
        let weekly: Decimal = row
            .get::<String, _>("weekly_used_usd")
            .parse()
            .map_err(|_| unavailable())?;
        let daily_exceeded = limits.daily_usd != Decimal::ZERO && daily >= limits.daily_usd;
        let weekly_exceeded = limits.weekly_usd != Decimal::ZERO && weekly >= limits.weekly_usd;
        if daily_exceeded || weekly_exceeded {
            let reset: DateTime<Utc> = row.get(if weekly_exceeded {
                "weekly_end"
            } else {
                "daily_end"
            });
            return Err(GatewayError::new(
                GatewayErrorKind::RateLimited,
                "user group budget is exhausted",
            )
            .with_client_code(if weekly_exceeded {
                "group_weekly_budget_exceeded"
            } else {
                "group_daily_budget_exceeded"
            })
            .with_retry_after((reset - now).to_std().unwrap_or(Duration::from_secs(1))));
        }
        tx.commit().await.map_err(|_| unavailable())?;
        Ok(scope)
    }

    async fn settle_inner(&self, charge: &ClientBudgetCharge) -> Result<(), ClientBudgetError> {
        let scope = charge.scope.as_ref().ok_or(ClientBudgetError)?;
        let mut tx = self.pool.begin().await.map_err(|_| ClientBudgetError)?;
        advance_windows(&mut tx, scope, Utc::now())
            .await
            .map_err(|_| ClientBudgetError)?;
        let billed_amount = sqlx::query_scalar::<_, String>("insert into user_group_charge_events(request_id, user_id, account_group_id, client_api_key_ref, amount_usd, completed_at)
            values ($1, $2, $3, $4, $5::text::numeric, $6) on conflict(request_id) do nothing returning amount_usd::text")
            .bind(charge.request_id.as_str()).bind(&scope.user_id).bind(scope.group_id.as_str()).bind(charge.key_id.as_str())
            .bind(charge.amount_usd.canonical()).bind(DateTime::<Utc>::from(charge.completed_at))
            .fetch_optional(&mut *tx).await.map_err(|_| ClientBudgetError)?;
        if let Some(billed_amount) = billed_amount {
            sqlx::query("update user_group_budget_windows set
                daily_used_usd = least(9999999999.9999999999, daily_used_usd + case when $4 >= daily_start and $4 < daily_end then $3::text::numeric else 0 end),
                weekly_used_usd = least(9999999999.9999999999, weekly_used_usd + case when $4 >= weekly_start and $4 < weekly_end then $3::text::numeric else 0 end)
                where user_id = $1 and account_group_id = $2")
                .bind(&scope.user_id).bind(scope.group_id.as_str()).bind(billed_amount)
                .bind(DateTime::<Utc>::from(charge.completed_at)).execute(&mut *tx).await.map_err(|_| ClientBudgetError)?;
        }
        tx.commit().await.map_err(|_| ClientBudgetError)
    }
}

impl ClientBudgetPort for PgClientBudgetStore {
    fn admit(
        &self,
        key_id: ClientApiKeyId,
        expected_group: Option<AccountGroupId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async move { self.admit_inner(key_id, expected_group).await })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            let result = self.settle_inner(&charge).await;
            let mut retry = self.retry.lock().map_err(|_| ClientBudgetError)?;
            if result.is_err() {
                retry.insert(charge.request_id.as_str().to_owned(), charge);
            } else {
                retry.remove(charge.request_id.as_str());
            }
            result
        })
    }
}

async fn advance_windows(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ClientBudgetScope,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("insert into user_group_budget_windows(user_id, account_group_id, daily_start, daily_end, weekly_start, weekly_end)
        select $1, $2, day, day + interval '24 hours', day, day + interval '168 hours'
        from (select date_trunc('day', $3::timestamptz at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai' as day) d
        on conflict(user_id, account_group_id) do update set
            daily_start = case when user_group_budget_windows.daily_end <= $3 then excluded.daily_start else user_group_budget_windows.daily_start end,
            daily_end = case when user_group_budget_windows.daily_end <= $3 then excluded.daily_end else user_group_budget_windows.daily_end end,
            daily_used_usd = case when user_group_budget_windows.daily_end <= $3 then 0 else user_group_budget_windows.daily_used_usd end,
            weekly_start = case when user_group_budget_windows.weekly_end <= $3 then excluded.weekly_start else user_group_budget_windows.weekly_start end,
            weekly_end = case when user_group_budget_windows.weekly_end <= $3 then excluded.weekly_end else user_group_budget_windows.weekly_end end,
            weekly_used_usd = case when user_group_budget_windows.weekly_end <= $3 then 0 else user_group_budget_windows.weekly_used_usd end")
        .bind(&scope.user_id).bind(scope.group_id.as_str()).bind(now).execute(&mut **tx).await?;
    Ok(())
}

pub(super) async fn load_client_key_budgets(
    pool: &PgPool,
    records: &mut [super::ClientApiKeyRecord],
) -> StoreResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let ids = records
        .iter()
        .map(|record| record.id.as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "select k.id, user_group_quota_limit(g.daily_limit_usd, k.owner_user_id, g.id)::text as daily_limit_usd, user_group_quota_limit(g.weekly_limit_usd, k.owner_user_id, g.id)::text as weekly_limit_usd,
        (case when w.daily_end > now() then w.daily_used_usd else 0 end)::text as daily_used,
        (case when w.weekly_end > now() then w.weekly_used_usd else 0 end)::text as weekly_used,
        case when w.daily_end > now() then w.daily_end end as daily_end,
        case when w.weekly_end > now() then w.weekly_end end as weekly_end
        from client_api_keys k join client_api_key_groups kg on kg.client_api_key_id = k.id
        join account_groups g on g.id = kg.account_group_id
        left join user_group_budget_windows w on w.user_id = k.owner_user_id and w.account_group_id = g.id
        where k.id = any($1)",
    )
    .bind(ids)
    .fetch_all(pool)
    .await
    .map_err(|_| postgres_unavailable("load client budgets"))?;
    let mut budgets = BTreeMap::new();
    for row in rows {
        let parse = |field| -> StoreResult<Decimal> {
            row.get::<String, _>(field)
                .parse()
                .map_err(|_| postgres_unavailable("decode client budget"))
        };
        budgets.insert(
            row.get::<String, _>("id"),
            ClientBudgetStatus {
                limits: ClientBudgetLimits {
                    daily_usd: parse("daily_limit_usd")?,
                    weekly_usd: parse("weekly_limit_usd")?,
                },
                daily_used_usd: parse("daily_used")?,
                weekly_used_usd: parse("weekly_used")?,
                daily_resets_at: row
                    .get::<Option<DateTime<Utc>>, _>("daily_end")
                    .map(Into::into),
                weekly_resets_at: row
                    .get::<Option<DateTime<Utc>>, _>("weekly_end")
                    .map(Into::into),
            },
        );
    }
    for record in records {
        record.budget = budgets
            .remove(&record.id)
            .ok_or_else(|| postgres_unavailable("load client budget policy"))?;
    }
    Ok(())
}

fn unavailable() -> GatewayError {
    GatewayError::new(
        GatewayErrorKind::ProviderInfrastructureUnavailable,
        "client budget service is temporarily unavailable",
    )
    .with_client_code("key_budget_unavailable")
}
