use super::*;
use gateway_admin::model::provider_credentials::{
    ProviderQuota, ProviderQuotaWindow, QuotaLocalUsageAttribution,
};

fn quota(at: chrono::DateTime<Utc>, reset: chrono::DateTime<Utc>, percent: f64) -> ProviderQuota {
    ProviderQuota {
        plan_type: None,
        observed_at: Some(at),
        refresh_token_expires_at: None,
        limit_reached: false,
        provider_data: None,
        windows: vec![ProviderQuotaWindow {
            key: "weekly".into(),
            group: "longTerm".into(),
            label: "周额度".into(),
            limit_id: None,
            limit_name: None,
            role: None,
            local_usage_attribution: QuotaLocalUsageAttribution::AccountWide,
            estimated_quota: None,
            window_seconds: Some(604800),
            used_percent: Some(percent),
            reset_at: Some(reset),
            limit_reached: false,
            local_usage: None,
            provider_data: None,
        }],
    }
}

#[tokio::test]
async fn quota_estimates_use_persisted_interval_cost_not_lifetime_or_cached_snapshot_cost() {
    let Some(db) = TestDatabase::create("quota_estimate_interval").await else {
        return;
    };
    let id = "acct_estimate";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "estimate-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let base = now - TimeDelta::hours(3);
    let reset = now + TimeDelta::days(3);
    for (request_id, amount, time) in [
        ("req_before_baseline", "900", base - TimeDelta::minutes(10)),
        ("req_interval", "10", base + TimeDelta::minutes(30)),
        ("req_after_snapshot", "90", base + TimeDelta::minutes(90)),
    ] {
        seed_model_request(
            &db.pool,
            ModelRequestSeed {
                request_id,
                account_id: id,
                provider_kind: "openai",
                model: "test-model",
                total_tokens: 100,
                cost_amount: amount,
                started_at: time,
            },
        )
        .await
        .unwrap();
    }
    let mut first = quota(base, reset, 20.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    assert!(first.windows[0].estimated_quota.is_none());
    // A fresh store instance still uses the persisted baseline.
    let mut second = quota(base + TimeDelta::hours(1), reset, 22.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut second)
        .await
        .unwrap();
    let estimate = second.windows[0].estimated_quota.clone().unwrap();
    assert_eq!(estimate.used_usd, "10.00");
    assert_eq!(estimate.total_usd, "500.00");
    assert_eq!(estimate.percent_delta, 2.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut second)
        .await
        .unwrap();
    assert_eq!(second.windows[0].estimated_quota.as_ref(), Some(&estimate));
    // Unchanged percentages retain the previous completed interval.
    let mut plateau = quota(base + TimeDelta::hours(2), reset, 22.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut plateau)
        .await
        .unwrap();
    assert_eq!(plateau.windows[0].estimated_quota.as_ref(), Some(&estimate));
    // Out-of-order snapshots cannot rewind the persisted observation.
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    let mut third = quota(now - TimeDelta::minutes(10), reset, 24.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut third)
        .await
        .unwrap();
    let result = third.windows[0].estimated_quota.as_ref().unwrap();
    assert_eq!(result.used_usd, "100.00");
    assert_eq!(result.total_usd, "2500.00");
    assert_eq!(result.percent_delta, 4.0);
    // A manual reset within the same cycle starts a new baseline.
    let mut rollback = quota(now - TimeDelta::minutes(5), reset, 1.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut rollback)
        .await
        .unwrap();
    assert!(rollback.windows[0].estimated_quota.is_none());
    let mut next_cycle = quota(now, reset + TimeDelta::days(1), 0.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut next_cycle)
        .await
        .unwrap();
    assert!(next_cycle.windows[0].estimated_quota.is_none());
    let count: i64 = sqlx::query_scalar("select count(*) from account_quota_estimate_samples")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    db.close().await;
}

#[tokio::test]
async fn quota_estimates_wait_for_sufficient_delta_and_reject_partial_or_unattributed_usage() {
    let Some(db) = TestDatabase::create("quota_estimate_invalid").await else {
        return;
    };
    let id = "acct_estimate_invalid";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "estimate-invalid-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let base = now - TimeDelta::hours(3);
    let reset = now + TimeDelta::days(3);
    let mut first = quota(base, reset, 0.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    seed_model_request(
        &db.pool,
        ModelRequestSeed {
            request_id: "req_partial_interval",
            account_id: id,
            provider_kind: "openai",
            model: "test-model",
            total_tokens: 100,
            cost_amount: "10",
            started_at: base + TimeDelta::minutes(30),
        },
    )
    .await
    .unwrap();
    let mut small = quota(base + TimeDelta::hours(1), reset, 0.5);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut small)
        .await
        .unwrap();
    assert!(small.windows[0].estimated_quota.is_none());
    sqlx::query(
        "update model_requests set cost_source = 'unavailable', cost_amount = null, cost_currency = null
        where id = 'req_partial_interval'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let mut partial = quota(base + TimeDelta::hours(2), reset, 2.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut partial)
        .await
        .unwrap();
    assert!(partial.windows[0].estimated_quota.is_none());
    let mut external = quota(now - TimeDelta::minutes(1), reset, 4.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut external)
        .await
        .unwrap();
    assert!(external.windows[0].estimated_quota.is_none());
    let mut invalid = quota(now, reset, 6.0);
    invalid.windows[0].local_usage_attribution = QuotaLocalUsageAttribution::Unavailable;
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut invalid)
        .await
        .unwrap();
    assert!(invalid.windows[0].estimated_quota.is_none());
    let state: serde_json::Value =
        sqlx::query_scalar("select state from account_quota_estimate_samples")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(state["start_percent"], 4.0);
    db.close().await;
}
