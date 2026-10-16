//! Group-configured budgets, shared by every key owned by the same user in a group.

use crate::{StoreResult, postgres_unavailable};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_core::{
    engine::budget::{
        ClientBudgetBilling, ClientBudgetCharge, ClientBudgetError, ClientBudgetLimits,
        ClientBudgetPort, ClientBudgetScope, ClientBudgetStatus,
    },
    error::{GatewayError, GatewayErrorKind},
    metering::Decimal,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, PublicModelId},
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
mod journal;

pub struct PgClientBudgetStore {
    pool: PgPool,
    retry: Mutex<BTreeMap<String, ClientBudgetCharge>>,
    journal: journal::Journal,
    journal_failed: AtomicBool,
}

impl PgClientBudgetStore {
    pub async fn open(pool: PgPool, directory: PathBuf) -> Result<Self, ClientBudgetError> {
        let journal = journal::Journal::new(directory);
        let pending = journal.pending().await?;
        Ok(Self {
            pool,
            retry: Mutex::new(
                pending
                    .into_iter()
                    .map(|charge| (charge.request_id.as_str().to_owned(), charge))
                    .collect(),
            ),
            journal,
            journal_failed: AtomicBool::new(false),
        })
    }

    pub async fn recover_pending(&self) -> Result<(), ClientBudgetError> {
        // Includes writes completed after the original settlement future was cancelled.
        let pending = self.journal.pending().await.inspect_err(|_| {
            self.journal_failed.store(true, Ordering::Release);
        })?;
        for charge in pending {
            self.settle(charge).await?;
        }
        Ok(())
    }

    async fn admit_inner(
        &self,
        key_id: ClientApiKeyId,
        expected_group: Option<AccountGroupId>,
        requested_model: Option<PublicModelId>,
    ) -> Result<ClientBudgetScope, GatewayError> {
        if self.journal_failed.load(Ordering::Acquire)
            || self.retry.lock().map_err(|_| unavailable())?.len() >= 4096
        {
            return Err(unavailable());
        }
        let group = expected_group.ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "a single account group is required",
            )
        })?;
        // Recheck live grants, enabled state, and the routing snapshot at admission.
        let row = sqlx::query("select k.owner_user_id, g.model_multipliers -> $3::text as billing_rule, user_group_quota_limit(g.daily_limit_usd, k.owner_user_id, g.id)::text as daily_limit_usd, user_group_quota_limit(g.weekly_limit_usd, k.owner_user_id, g.id)::text as weekly_limit_usd
            from client_api_keys k join users u on u.id = k.owner_user_id and u.enabled
            join client_api_key_groups kg on kg.client_api_key_id = k.id
            join account_groups g on g.id = kg.account_group_id and g.enabled
            where k.id = $1 and k.enabled and g.id = $2
            and ((u.role = 'admin' and not u.group_grants_configured) or exists(select 1 from user_account_groups ug where ug.user_id = u.id and ug.account_group_id = g.id))")
            .bind(key_id.as_str()).bind(group.as_str()).bind(requested_model.as_ref().map(PublicModelId::as_str)).fetch_optional(&self.pool).await.map_err(|_| unavailable())?
            .ok_or_else(|| GatewayError::new(GatewayErrorKind::PolicyDenied, "user, key or group access is disabled or changed"))?;
        let scope = ClientBudgetScope {
            user_id: row.get("owner_user_id"),
            group_id: group,
            billing: match row.get::<Option<serde_json::Value>, _>("billing_rule") {
                Some(value) => {
                    use gateway_admin::model::account_groups::GroupModelMultiplier;
                    let rule: GroupModelMultiplier =
                        serde_json::from_value(value).map_err(|_| unavailable())?;
                    let multiplier = rule.multiplier().parse().map_err(|_| unavailable())?;
                    let scope = match rule {
                        GroupModelMultiplier::All(_) => {
                            gateway_core::metering::MultiplierScope::All
                        }
                        GroupModelMultiplier::Scoped { scope, .. } => scope,
                    };
                    ClientBudgetBilling::new(multiplier, scope).map_err(|_| unavailable())?
                }
                None => ClientBudgetBilling::default(),
            },
        };
        let retries = self
            .retry
            .lock()
            .map_err(|_| unavailable())?
            .values()
            .filter(|charge| {
                charge.scope.as_ref().is_some_and(|prior| {
                    prior.user_id == scope.user_id && prior.group_id == scope.group_id
                })
            })
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
            tx.commit().await.map_err(|_| unavailable())?;
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
        // Explicit raw amount preserves the original ledger trigger's historical
        // contract. Billing uses the admission fact, never an observation query.
        let [input, output, cache_read, cache_write] =
            charge.components_usd.map(|amount| amount.canonical());
        let snapshot = serde_json::json!({"version":1,"total":charge.amount_usd.canonical(),"input":input,"output":output,"cacheRead":cache_read,"cacheWrite":cache_write});
        let billed_amount = sqlx::query_scalar::<_, String>("insert into user_group_charge_events(request_id, user_id, account_group_id, client_api_key_ref, amount_usd, completed_at, raw_amount_usd, billing_multiplier, billing_multiplier_scope)
            values ($1, $2, $3, $4, scoped_billed_cost($5::text::numeric,$7::text::numeric,$8,$9), $6, $5::text::numeric, $7::text::numeric, $8) on conflict(request_id) do nothing returning amount_usd::text")
            .bind(charge.request_id.as_str()).bind(&scope.user_id).bind(scope.group_id.as_str()).bind(charge.key_id.as_str())
            .bind(charge.amount_usd.canonical()).bind(DateTime::<Utc>::from(charge.completed_at))
            .bind(scope.billing.multiplier().canonical()).bind(sqlx::types::Json(scope.billing.scope())).bind(sqlx::types::Json(snapshot))
            .fetch_optional(&mut *tx).await.map_err(|_| ClientBudgetError)?;
        if let Some(billed_amount) = billed_amount {
            sqlx::query("update user_group_budget_windows set
                daily_used_usd = least(9999999999.9999999999, daily_used_usd + case when $4 >= daily_start and $4 < daily_end then $3::text::numeric else 0 end),
                weekly_used_usd = least(9999999999.9999999999, weekly_used_usd + case when $4 >= weekly_start and ($4 < weekly_end or exists(select 1 from subscription_group_cycles c where c.account_group_id = $2 and c.mode = 'upstream')) then $3::text::numeric else 0 end)
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
        requested_model: Option<PublicModelId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async move {
            self.admit_inner(key_id, expected_group, requested_model)
                .await
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            // Persist before touching the ledger, including during a complete PostgreSQL outage.
            let charge = match self.journal.persist(charge).await {
                Ok(charge) => charge,
                Err(error) => {
                    self.journal_failed.store(true, Ordering::Release);
                    return Err(error);
                }
            };
            self.retry
                .lock()
                .map_err(|_| ClientBudgetError)?
                .insert(charge.request_id.as_str().to_owned(), charge.clone());
            let result = self.settle_inner(&charge).await;
            if result.is_ok() {
                self.journal
                    .remove(charge.request_id.as_str().to_owned())
                    .await?;
            }
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

impl gateway_core::health::HealthProbe for PgClientBudgetStore {
    fn name(&self) -> &'static str {
        "client_budget_settlement"
    }
    fn check(&self) -> BoxFuture<'_, gateway_core::health::HealthState> {
        Box::pin(async {
            use gateway_core::health::HealthState;
            if self.journal_failed.load(Ordering::Acquire) {
                return HealthState::Unhealthy(
                    "budget journal write failed; admission stopped".into(),
                );
            }
            match self.retry.lock() {
                Ok(pending) if pending.is_empty() => HealthState::Healthy,
                Ok(pending) => HealthState::Degraded(format!(
                    "{} budget settlements await durable retry",
                    pending.len()
                )),
                Err(_) => HealthState::Unhealthy("budget settlement state unavailable".into()),
            }
        })
    }
}

async fn advance_windows(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ClientBudgetScope,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("select advance_subscription_windows($1, $2, $3)")
        .bind(&scope.user_id)
        .bind(scope.group_id.as_str())
        .bind(now)
        .execute(&mut **tx)
        .await?;
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
        coalesce(w.daily_used_usd, 0)::text as daily_used,
        coalesce(w.weekly_used_usd, 0)::text as weekly_used,
        w.daily_end as daily_end,
        w.weekly_end as weekly_end
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
