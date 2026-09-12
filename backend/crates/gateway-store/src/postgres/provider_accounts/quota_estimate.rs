//! Persistent, optimistic-CAS sampling for informational quota estimates.

use chrono::{DateTime, Duration, Utc};
use gateway_admin::model::provider_credentials::{
    ProviderQuota, QuotaIntervalEstimate, QuotaLocalUsageAttribution,
};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Clone, Serialize, Deserialize)]
struct Sample {
    reset_at: DateTime<Utc>,
    seconds: u64,
    start_at: DateTime<Utc>,
    start_percent: f64,
    observed_at: DateTime<Utc>,
    percent: f64,
    estimate: Option<QuotaIntervalEstimate>,
}

fn error(error: impl std::fmt::Display) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "quota estimate",
        error.to_string(),
    )
}

pub(super) async fn attach(
    store: &PgAdminAccountStore,
    pool: &PgPool,
    account_id: &str,
    quota: &mut ProviderQuota,
) -> AdminStoreResult<()> {
    let Some(observed_at) = quota.observed_at.filter(|at| *at <= Utc::now()) else {
        return Ok(());
    };
    for window in &mut quota.windows {
        window.estimated_quota = None;
        if window.local_usage_attribution != QuotaLocalUsageAttribution::AccountWide {
            continue;
        }
        let (Some(percent), Some(reset_at), Some(seconds)) =
            (window.used_percent, window.reset_at, window.window_seconds)
        else {
            continue;
        };
        let Some(duration) = i64::try_from(seconds).ok().and_then(Duration::try_seconds) else {
            continue;
        };
        let Some(start) = reset_at.checked_sub_signed(duration) else {
            continue;
        };
        if !percent.is_finite()
            || !(0.0..=100.0).contains(&percent)
            || seconds == 0
            || observed_at < start
            || observed_at >= reset_at
            || Utc::now() >= reset_at
        {
            continue;
        }
        let previous: Option<serde_json::Value> = sqlx::query_scalar(
            "select state from account_quota_estimate_samples where account_id=$1 and window_key=$2"
        ).bind(account_id).bind(&window.key).fetch_optional(pool).await.map_err(error)?;
        let previous = previous
            .map(serde_json::from_value::<Sample>)
            .transpose()
            .map_err(error)?;
        let fresh = Sample {
            reset_at,
            seconds,
            start_at: observed_at,
            start_percent: percent,
            observed_at,
            percent,
            estimate: None,
        };
        let mut sample = previous.clone().unwrap_or_else(|| fresh.clone());
        if let Some(old) = &previous {
            // A cached or out-of-order snapshot must not move the sampling endpoints.
            if observed_at <= old.observed_at {
                if reset_at == old.reset_at && seconds == old.seconds && percent == old.percent {
                    window.estimated_quota = old.estimate.clone();
                }
                continue;
            }
            if reset_at != old.reset_at || seconds != old.seconds || percent < old.percent {
                sample = fresh.clone();
            } else {
                sample.observed_at = observed_at;
                sample.percent = percent;
                // Accumulate the whole observed interval instead of averaging noisy point estimates.
                if percent > old.percent && percent - sample.start_percent >= 1.0 {
                    let usage = store
                        .usage_by_windows(&[AccountUsageWindowQuery {
                            account_id: account_id.to_owned(),
                            key: window.key.clone(),
                            range: TimeRange {
                                start: sample.start_at,
                                end: observed_at,
                            },
                        }])
                        .await?;
                    sample.estimate = usage.first().and_then(|usage| {
                        interval_estimate(
                            &usage.usage,
                            sample.start_percent,
                            percent,
                            sample.start_at,
                            observed_at,
                        )
                    });
                    // Missing/partial billing or an externally consumed interval is not usable.
                    if sample.estimate.is_none() {
                        sample = fresh.clone();
                    }
                }
            }
        }
        let state = serde_json::to_value(&sample).map_err(error)?;
        let changed = if let Some(old) = previous {
            sqlx::query(
                "update account_quota_estimate_samples set observed_at=$3, state=$4
                where account_id=$1 and window_key=$2 and observed_at=$5",
            )
            .bind(account_id)
            .bind(&window.key)
            .bind(observed_at)
            .bind(state)
            .bind(old.observed_at)
            .execute(pool)
            .await
            .map_err(error)?
            .rows_affected()
        } else {
            sqlx::query("insert into account_quota_estimate_samples(account_id,window_key,observed_at,state)
                values($1,$2,$3,$4) on conflict do nothing")
                .bind(account_id).bind(&window.key).bind(observed_at).bind(state)
                .execute(pool).await.map_err(error)?.rows_affected()
        };
        if changed == 1 {
            window.estimated_quota = sample.estimate;
        }
    }
    Ok(())
}

fn interval_estimate(
    usage: &AccountUsage,
    start_percent: f64,
    end_percent: f64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Option<QuotaIntervalEstimate> {
    let delta = end_percent - start_percent;
    if !delta.is_finite()
        || delta < 1.0
        || start >= end
        || usage.request_count == 0
        || usage.cost_coverage.unavailable_count > 0
        || usage
            .cost_coverage
            .provider_reported_count
            .saturating_add(usage.cost_coverage.calculated_count)
            == 0
    {
        return None;
    }
    let mut amount = 0.0;
    for cost in &usage.costs {
        if !cost.currency.eq_ignore_ascii_case("USD") {
            return None;
        }
        amount += cost.amount.as_str().parse::<f64>().ok()?;
    }
    let total = amount / (delta / 100.0);
    if !amount.is_finite() || amount <= 0.0 || !total.is_finite() || total <= 0.0 {
        return None;
    }
    Some(QuotaIntervalEstimate {
        used_usd: format!("{amount:.2}"),
        total_usd: format!("{total:.2}"),
        percent_delta: delta,
        sample_start: start,
        sample_end: end,
    })
}
