//! Reset subscriptions from neutral provider window facts, never from opaque provider JSON.
use chrono::{DateTime, Utc};
use gateway_admin::{
    model::provider_credentials::{ProviderQuota, QuotaLocalUsageAttribution},
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use sqlx::{PgPool, Postgres, Row, Transaction};

fn error(_: sqlx::Error) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "subscriptions",
        "quota synchronization failed",
    )
}
async fn lock(tx: &mut Transaction<'_, Postgres>, account_id: &str) -> AdminStoreResult<()> {
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("subscription:{account_id}"))
        .execute(&mut **tx)
        .await
        .map_err(error)?;
    Ok(())
}
async fn reset(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
    event_id: &str,
    reason: &str,
    at: DateTime<Utc>,
) -> AdminStoreResult<()> {
    sqlx::query("select reset_user_subscriptions($1, array(select account_group_id from account_group_accounts where provider_account_id = $2), $2, $3, $4)")
        .bind(event_id).bind(account_id).bind(reason).bind(at).execute(&mut **tx).await.map_err(error)?;
    Ok(())
}
pub(crate) async fn reset_account(
    pool: &PgPool,
    account_id: &str,
    event_id: &str,
) -> AdminStoreResult<()> {
    let mut tx = pool.begin().await.map_err(error)?;
    lock(&mut tx, account_id).await?;
    let event_id = format!("account:{account_id}:{event_id}");
    let existing: bool =
        sqlx::query_scalar("select exists(select 1 from subscription_reset_events where id = $1)")
            .bind(&event_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(error)?;
    if !existing {
        reset(
            &mut tx,
            account_id,
            &event_id,
            "upstream_manual",
            Utc::now(),
        )
        .await?;
        // A fresh upstream observation establishes the new baseline after explicit redemption.
        sqlx::query("delete from subscription_quota_observations where account_id = $1")
            .bind(account_id)
            .execute(&mut *tx)
            .await
            .map_err(error)?;
    }
    tx.commit().await.map_err(error)
}

pub(crate) async fn observe(
    pool: &PgPool,
    account_id: &str,
    quota: &ProviderQuota,
) -> AdminStoreResult<()> {
    let Some(observed_at) = quota.observed_at else {
        return Ok(());
    };
    if quota.windows.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await.map_err(error)?;
    lock(&mut tx, account_id).await?;
    let mut detected: Option<(&str, DateTime<Utc>)> = None;
    let manual_at: Option<DateTime<Utc>> = sqlx::query_scalar("select max(occurred_at) from subscription_reset_events where account_id = $1 and reason = 'upstream_manual'")
        .bind(account_id).fetch_one(&mut *tx).await.map_err(error)?;
    if manual_at.is_some_and(|at| observed_at <= at) {
        return Ok(());
    }
    for window in &quota.windows {
        if window.local_usage_attribution != QuotaLocalUsageAttribution::AccountWide {
            continue;
        }
        let used = window
            .used_percent
            .filter(|n| n.is_finite() && (0.0..=100.0).contains(n));
        if used.is_none() && window.reset_at.is_none() {
            continue;
        }
        let old = sqlx::query("select observed_at, reset_at, used_percent from subscription_quota_observations where account_id = $1 and window_key = $2")
            .bind(account_id).bind(&window.key).fetch_optional(&mut *tx).await.map_err(error)?;
        if let Some(old) = old {
            if old.get::<DateTime<Utc>, _>("observed_at") >= observed_at {
                continue;
            }
            let previous_reset: Option<DateTime<Utc>> = old.get("reset_at");
            let previous_used: Option<f64> = old.get("used_percent");
            let boundary_advanced = previous_reset
                .zip(window.reset_at)
                .is_some_and(|(old, new)| new > old + chrono::Duration::seconds(60));
            let natural = boundary_advanced && previous_reset.is_some_and(|at| at <= observed_at);
            let externally_reset = boundary_advanced;
            // A fixed-boundary reset can only be identified from an observed return to near-zero.
            let recovered = previous_used
                .zip(used)
                .is_some_and(|(before, after)| before >= 5.0 && after <= 1.0);
            if natural || externally_reset || recovered {
                let at = if natural {
                    window
                        .reset_at
                        .zip(window.window_seconds)
                        .and_then(|(end, seconds)| {
                            i64::try_from(seconds)
                                .ok()
                                .and_then(|s| end.checked_sub_signed(chrono::Duration::seconds(s)))
                        })
                        .filter(|at| {
                            *at <= observed_at && previous_reset.is_some_and(|old| *at >= old)
                        })
                        .or(previous_reset)
                        .unwrap_or(observed_at)
                } else {
                    observed_at
                };
                if detected.is_none_or(|(_, previous)| at > previous) {
                    detected = Some((
                        if natural || externally_reset {
                            "upstream_window"
                        } else {
                            "upstream_recovery"
                        },
                        at,
                    ));
                }
            }
        }
        sqlx::query("insert into subscription_quota_observations(account_id, window_key, observed_at, reset_at, used_percent)
            values($1, $2, $3, $4, $5) on conflict(account_id, window_key) do update set
            observed_at = excluded.observed_at, reset_at = excluded.reset_at, used_percent = excluded.used_percent")
            .bind(account_id).bind(&window.key).bind(observed_at).bind(window.reset_at).bind(used)
            .execute(&mut *tx).await.map_err(error)?;
    }
    if let Some((reason, at)) = detected {
        reset(
            &mut tx,
            account_id,
            &format!(
                "observation:{account_id}:{}",
                observed_at.timestamp_micros()
            ),
            reason,
            at,
        )
        .await?;
    }
    tx.commit().await.map_err(error)
}
