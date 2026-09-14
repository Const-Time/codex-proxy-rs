//! Persistent, optimistic-CAS sampling for informational quota estimates.

use chrono::{DateTime, Duration, Utc};
use gateway_admin::model::provider_credentials::{
    ProviderQuota, QuotaIntervalEstimate, QuotaLocalUsageAttribution,
};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Clone, Serialize, Deserialize)]
struct Sample {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    cycle_based: bool,
    reset_at: DateTime<Utc>,
    seconds: u64,
    start_at: DateTime<Utc>,
    start_percent: f64,
    observed_at: DateTime<Utc>,
    percent: f64,
    estimate: Option<QuotaIntervalEstimate>,
    #[serde(default)]
    last_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pending_reason: Option<String>,
    #[serde(default)]
    pending_end: Option<DateTime<Utc>>,
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
    let created_at: Option<DateTime<Utc>> =
        sqlx::query_scalar("select created_at from provider_accounts where id=$1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .map_err(error)?;
    for window in &mut quota.windows {
        window.estimated_quota = None;
        window.estimate_hint = Some("等待有效的上游额度快照".to_owned());
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
        let previous_state = previous.clone();
        let previous = previous
            .map(serde_json::from_value::<Sample>)
            .transpose()
            .map_err(error)?;
        // Registration before the window allows a local cycle estimate; it cannot
        // prove that no usage occurred outside this service or that logs were retained.
        let cycle_based = created_at.is_some_and(|created| created <= start);
        let fresh = Sample {
            version: 2,
            cycle_based,
            reset_at,
            seconds,
            start_at: if cycle_based { start } else { observed_at },
            start_percent: if cycle_based { 0.0 } else { percent },
            observed_at,
            percent,
            estimate: None,
            last_attempt_at: None,
            pending_reason: None,
            pending_end: None,
        };
        let mut sample = previous.clone().unwrap_or_else(|| fresh.clone());
        if let Some(old) = &previous {
            // A cached or out-of-order snapshot must not move the sampling endpoints.
            if observed_at < old.observed_at {
                if reset_at == old.reset_at && seconds == old.seconds && percent == old.percent {
                    window.estimated_quota = old.estimate.clone();
                    window.estimate_hint = Some(sampling_hint(old));
                }
                continue;
            }
            if observed_at == old.observed_at
                && (reset_at != old.reset_at || seconds != old.seconds || percent != old.percent)
            {
                continue;
            }
            if reset_at != old.reset_at || seconds != old.seconds || percent < old.percent {
                sample = fresh.clone();
                if reset_at == old.reset_at && seconds == old.seconds && percent < old.percent {
                    // A manual reset must not mix pre-reset costs into a cycle total.
                    sample.cycle_based = false;
                    sample.start_at = observed_at;
                    sample.start_percent = percent;
                }
            } else {
                sample.observed_at = observed_at;
                sample.percent = percent;
                if old.version < 2 {
                    sample.version = 2;
                    if cycle_based && old.start_percent <= percent {
                        sample = fresh.clone();
                        sample.estimate = old.estimate.clone();
                        sample.pending_end = Some(observed_at);
                    }
                }
            }
        }
        // Accumulate the whole observed interval instead of averaging noisy point estimates.
        // Retry incomplete intervals, including cached snapshots, after late usage settles.
        // A plateau with an existing estimate keeps its original paired endpoints.
        let retry_due = sample
            .last_attempt_at
            .is_none_or(|at| Utc::now() - at >= Duration::seconds(30));
        if (previous.as_ref().is_some_and(|old| percent > old.percent) || retry_due)
            && (if sample.cycle_based {
                percent > 0.0
            } else {
                percent - sample.start_percent >= 1.0 - 1e-9
            })
        {
            let estimate_end = if previous.as_ref().is_some_and(|old| percent == old.percent) {
                sample
                    .pending_end
                    .or_else(|| sample.estimate.as_ref().map(|estimate| estimate.sample_end))
                    .unwrap_or(observed_at)
            } else {
                observed_at
            };
            let usage = store
                .usage_by_windows(&[AccountUsageWindowQuery {
                    account_id: account_id.to_owned(),
                    key: window.key.clone(),
                    range: TimeRange {
                        start: sample.start_at,
                        end: estimate_end,
                    },
                }])
                .await?;
            sample.last_attempt_at = Some(Utc::now());
            let estimate = usage.first().and_then(|usage| {
                interval_estimate(
                    &usage.usage,
                    sample.start_percent,
                    percent,
                    sample.start_at,
                    estimate_end,
                    sample.cycle_based,
                )
            });
            if let Some(estimate) = estimate {
                if estimate.missing_cost_count > 0
                    && sample
                        .estimate
                        .as_ref()
                        .is_some_and(|old| old.missing_cost_count == 0)
                {
                    sample.pending_reason = Some(format!(
                        "保留上次估算：新采样有 {} 笔费用缺失",
                        estimate.missing_cost_count
                    ));
                    sample.pending_end = Some(estimate_end);
                } else {
                    sample.estimate = Some(estimate);
                    sample.pending_reason = None;
                    sample.pending_end = None;
                }
            } else {
                // Keep the baseline so late completion can repair this same interval.
                sample.pending_reason = Some(if sample.estimate.is_some() {
                    "保留上次估算：新采样暂无可用费用".to_owned()
                } else {
                    "暂无有效费用；等待计费完整或补齐价格数据".to_owned()
                });
                sample.pending_end = Some(estimate_end);
            }
        }
        let state = serde_json::to_value(&sample).map_err(error)?;
        let changed = if let Some(old) = previous {
            sqlx::query(
                "update account_quota_estimate_samples set observed_at=$3, state=$4
                where account_id=$1 and window_key=$2 and observed_at=$5 and state=$6",
            )
            .bind(account_id)
            .bind(&window.key)
            .bind(observed_at)
            .bind(state)
            .bind(old.observed_at)
            .bind(previous_state)
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
            window.estimate_hint = Some(sampling_hint(&sample));
            window.estimated_quota = sample.estimate;
        }
    }
    Ok(())
}

fn sampling_hint(sample: &Sample) -> String {
    if let Some(reason) = &sample.pending_reason {
        return reason.clone();
    }
    if let Some(estimate) = &sample.estimate
        && estimate.missing_cost_count > 0
    {
        return format!(
            "记录不完整：{} / {} 笔费用缺失，按已知费用推算，估值可能偏低",
            estimate.missing_cost_count, estimate.request_count
        );
    }
    let delta = (sample.percent - sample.start_percent).max(0.0);
    if sample.cycle_based && sample.percent == 0.0 {
        return "当前已用比例为 0%，等待有效消费和非零比例".to_owned();
    }
    if delta < 1.0 - 1e-9 {
        format!(
            "起点 {:.1}% · 已上涨 {:.1} 个百分点，还需 {:.1}；后台自动采样",
            sample.start_percent,
            (delta * 10.0).floor() / 10.0,
            ((1.0 - delta) * 10.0).ceil() / 10.0
        )
    } else {
        "同一采样区间，按请求发生时的倍率计算".to_owned()
    }
}

fn interval_estimate(
    usage: &AccountUsage,
    start_percent: f64,
    end_percent: f64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cycle_based: bool,
) -> Option<QuotaIntervalEstimate> {
    let delta = end_percent - start_percent;
    if !delta.is_finite()
        || (if cycle_based {
            delta <= 0.0
        } else {
            delta < 1.0 - 1e-9
        })
        || start >= end
        || usage.request_count == 0
        || usage
            .cost_coverage
            .provider_reported_count
            .saturating_add(usage.cost_coverage.calculated_count)
            == 0
    {
        return None;
    }
    let mut amount = 0.0;
    let mut billed_amount = Some(0.0);
    for cost in &usage.costs {
        if !cost.currency.eq_ignore_ascii_case("USD") {
            return None;
        }
        amount += cost.amount.as_str().parse::<f64>().ok()?;
        billed_amount = billed_amount
            .zip(cost.billed_amount.as_ref())
            .and_then(|(sum, value)| value.as_str().parse::<f64>().ok().map(|value| sum + value));
    }
    let total = amount / (delta / 100.0);
    if !amount.is_finite() || amount <= 0.0 || !total.is_finite() || total <= 0.0 {
        return None;
    }
    Some(QuotaIntervalEstimate {
        cycle_based,
        missing_cost_count: usage.cost_coverage.unavailable_count,
        request_count: usage.request_count,
        billed_used_usd: billed_amount
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| format!("{value:.2}")),
        billed_total_usd: billed_amount
            .map(|value| value / (delta / 100.0))
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| format!("{value:.2}")),
        used_usd: format!("{amount:.2}"),
        total_usd: format!("{total:.2}"),
        percent_delta: delta,
        sample_start: start,
        sample_end: end,
    })
}
