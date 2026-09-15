//! Read-only forecast adaptation of upstream 5041d6ba / b98c2f01.
//! Provider documents stay with their owner; raw and billed costs share paired endpoints.

use super::*;
use crate::model::{
    provider_credentials::QuotaIntervalEstimate,
    quota_forecast_sampling::{QuotaForecastMethod, QuotaForecastPoint, select_forecast_sample},
};

impl DefaultAccountsService {
    pub(super) async fn attach_quota_estimates(
        &self,
        item: &AccountPageItem,
        quota: &mut ProviderQuota,
    ) -> Result<(), AdminError> {
        let provider = self
            .providers
            .require(&item.account.provider_kind)
            .map_err(|error| map_provider_error(error, "forecast provider"))?;
        let now = Utc::now();
        for window in &mut quota.windows {
            window.estimated_quota = None;
            window.estimate_hint = None;
            if window.local_usage_attribution != QuotaLocalUsageAttribution::AccountWide {
                continue;
            }
            window.estimate_hint = Some("等待本周期的有效额度快照".to_owned());
            let (Some(mut query), Some(observed), Some(percent)) = (
                quota_usage_window(&item.account.id, window),
                quota.observed_at,
                window.used_percent,
            ) else {
                continue;
            };
            let cycle_start = query.range.start;
            let reset_at = query.range.end;
            query.range.start = cycle_start.max(item.account.created_at);
            if query.range.start >= observed
                || observed > now
                || now >= reset_at
                || !percent.is_finite()
                || !(0.0..=100.0).contains(&percent)
            {
                continue;
            }
            query.range.end = observed;
            let history = self
                .accounts
                .load_quota_forecast_history(&query)
                .await
                .map_err(|error| map_store_error(error, "forecast paired usage"))?;
            let mut points = Vec::new();
            let mut interrupted = false;
            for point in history.points {
                let Some(fact) =
                    provider.quota_forecast_observation(&point.provider_observation, window)
                else {
                    continue;
                };
                let same_plan = quota
                    .plan_type
                    .as_deref()
                    .zip(fact.plan_type.as_deref())
                    .is_some_and(|(current, previous)| current.eq_ignore_ascii_case(previous));
                if !same_plan || (fact.reset_at - reset_at).abs() > Duration::seconds(2) {
                    points.clear();
                    interrupted = true;
                    continue;
                }
                points.push(QuotaForecastPoint {
                    observed_at: point.completed_at,
                    used_percent: fact.used_percent,
                    usage: point.usage,
                });
            }
            let sample = select_forecast_sample(
                window.key.clone(),
                query.range.start,
                QuotaForecastPoint {
                    observed_at: observed,
                    used_percent: percent,
                    usage: history.usage,
                },
                points,
                history.pending_request_count,
            );
            let cycle_based = sample.method == QuotaForecastMethod::Cumulative;
            if sample.discontinuous || (interrupted && cycle_based) {
                window.estimate_hint = Some("额度或套餐发生变化，正在重新积累配对样本".to_owned());
                continue;
            }
            if cycle_based && item.account.created_at > cycle_start {
                window.estimate_hint =
                    Some("账号在本周期中途接入，等待至少 5 个百分点的配对观测".to_owned());
                continue;
            }
            let delta = sample.sampled_percent;
            if !delta.is_finite()
                || delta < 5.0
                || delta > percent
                || sample.start_at >= sample.end_at
            {
                window.estimate_hint = Some(format!(
                    "有效采样进度 {:.1} 个百分点，至少需要 5 个百分点",
                    delta.max(0.0)
                ));
                continue;
            }
            let usage = &sample.usage;
            if usage.request_count == 0 {
                window.estimate_hint = Some("采样区间没有已完成的推理用量".to_owned());
                continue;
            }
            let amount = (usage.known_cost_count > 0 && usage.unavailable_cost_count == 0)
                .then_some(usage.usd);
            // A missing historical multiplier must not silently become ×1.
            let billed = (usage.missing_billed_count == 0).then_some(usage.billed_usd);
            let tokens = (usage.tokens > 0).then_some(usage.tokens);
            let factor = 100.0 / delta;
            let remaining_factor = (100.0 - percent) / delta;
            let estimated_tokens = tokens.and_then(|value| token_estimate(value, factor));
            let total_usd = amount.and_then(|value| money(value * factor));
            if estimated_tokens.is_none() && total_usd.is_none() {
                window.estimate_hint = Some("暂无可用的 Token 或费用数据，等待用量入账".to_owned());
                continue;
            }
            window.estimated_quota = Some(QuotaIntervalEstimate {
                cycle_based,
                missing_cost_count: usage.unavailable_cost_count,
                request_count: usage.request_count,
                used_usd: amount.and_then(money),
                total_usd,
                billed_used_usd: billed.and_then(money),
                billed_total_usd: billed.and_then(|value| money(value * factor)),
                estimated_tokens,
                remaining_tokens: tokens.and_then(|value| token_estimate(value, remaining_factor)),
                remaining_usd: amount.and_then(|value| money(value * remaining_factor)),
                remaining_billed_usd: billed.and_then(|value| money(value * remaining_factor)),
                block_count: sample.block_count,
                low_sample: delta < 10.0 || (!cycle_based && sample.block_count < 2),
                incomplete_tokens: usage.missing_token_count > 0,
                percent_delta: delta,
                sample_start: sample.start_at,
                sample_end: sample.end_at,
            });
            window.estimate_hint = if sample.pending_request_count > 0 {
                Some(format!(
                    "{} 笔请求尚未完成，后续刷新会重新核算",
                    sample.pending_request_count
                ))
            } else {
                None
            };
        }
        Ok(())
    }
}

fn money(value: f64) -> Option<String> {
    (value.is_finite() && value >= 0.0).then(|| format!("{value:.2}"))
}

fn token_estimate(value: u64, factor: f64) -> Option<u64> {
    let total = value as f64 * factor;
    (total.is_finite() && total >= 0.0 && total.round() < u64::MAX as f64)
        .then(|| total.round() as u64)
}
