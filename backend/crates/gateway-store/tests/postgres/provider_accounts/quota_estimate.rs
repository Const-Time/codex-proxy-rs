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
            estimate_hint: None,
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
    sqlx::query("update model_requests set billing_multiplier = case when id = 'req_interval' then 2 else 0.5 end where provider_account_ref = $1")
        .bind(id).execute(&db.pool).await.unwrap();
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
    assert_eq!(estimate.billed_used_usd.as_deref(), Some("20.00"));
    assert_eq!(estimate.billed_total_usd.as_deref(), Some("1000.00"));
    let windows = admin_account_store(&db.pool)
        .load_account_usage_by_windows(&[AccountUsageWindowQuery {
            account_id: id.to_owned(),
            key: "weekly".to_owned(),
            range: TimeRange {
                start: base,
                end: base + TimeDelta::hours(1),
            },
        }])
        .await
        .unwrap();
    let cost = &windows[0].usage.costs[0];
    assert_eq!(cost.amount.as_str(), "10");
    assert_eq!(cost.billed_amount.as_ref().unwrap().as_str(), "20");
    assert_eq!(windows[0].usage.models[0].costs[0], *cost);
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
    assert_eq!(result.billed_used_usd.as_deref(), Some("65.00"));
    assert_eq!(result.billed_total_usd.as_deref(), Some("1625.00"));
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
    assert_eq!(state["start_percent"], 0.0);
    db.close().await;
}

#[tokio::test]
async fn quota_estimates_retry_late_billing_without_losing_baseline_or_extending_plateau() {
    let Some(db) = TestDatabase::create("quota_estimate_retry").await else {
        return;
    };
    let id = "acct_estimate_retry";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "estimate-retry-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let base = now - TimeDelta::hours(3);
    let reset = now + TimeDelta::days(3);
    let mut first = quota(base, reset, 4.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    assert!(
        first.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .contains("起点 4.0%")
    );
    let endpoint = base + TimeDelta::hours(1);
    let mut second = quota(endpoint, reset, 5.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut second)
        .await
        .unwrap();
    assert!(second.windows[0].estimated_quota.is_none());
    assert!(
        second.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .contains("计费完整")
    );
    for (request_id, started_at, amount) in [
        ("req_late", base + TimeDelta::minutes(30), "10"),
        ("req_plateau", base + TimeDelta::minutes(90), "90"),
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
                started_at,
            },
        )
        .await
        .unwrap();
    }
    // Zero-rate groups still have a valid (zero) billed estimate.
    sqlx::query("update model_requests set billing_multiplier = 0 where provider_account_ref = $1")
        .bind(id)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update account_quota_estimate_samples set state = state - 'last_attempt_at'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut plateau = quota(base + TimeDelta::hours(2), reset, 5.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut plateau)
        .await
        .unwrap();
    let estimate = plateau.windows[0].estimated_quota.as_ref().unwrap();
    assert_eq!(estimate.total_usd, "1000.00");
    assert_eq!(estimate.billed_total_usd.as_deref(), Some("0.00"));
    assert_eq!(estimate.sample_start, base);
    assert_eq!(estimate.sample_end, endpoint);
    // Older releases' serialized estimates lack billed fields. Backfill the same interval.
    sqlx::query("update account_quota_estimate_samples set state = (state - 'last_attempt_at') #- '{estimate,billed_used_usd}' #- '{estimate,billed_total_usd}'").execute(&db.pool).await.unwrap();
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut plateau)
        .await
        .unwrap();
    let backfill = plateau.windows[0].estimated_quota.as_ref().unwrap();
    assert_eq!(backfill.total_usd, "1000.00");
    assert_eq!(backfill.billed_total_usd.as_deref(), Some("0.00"));
    assert_eq!(backfill.sample_end, endpoint);
    db.close().await;
}

#[tokio::test]
async fn quota_cycle_estimates_work_on_first_snapshot_and_upgrade_legacy_pending_samples() {
    let Some(db) = TestDatabase::create("quota_cycle_first").await else {
        return;
    };
    let id = "acct_cycle";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "cycle-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let reset = now + TimeDelta::days(3);
    let start = reset - TimeDelta::days(7);
    sqlx::query("update provider_accounts set created_at=$2 where id=$1")
        .bind(id)
        .bind(start - TimeDelta::days(1))
        .execute(&db.pool)
        .await
        .unwrap();
    for (request_id, amount, started_at) in [
        ("req_old_cycle", "900", start - TimeDelta::hours(1)),
        ("req_current_cycle", "70", start + TimeDelta::hours(1)),
        (
            "req_after_cycle_snapshot",
            "90",
            now - TimeDelta::minutes(5),
        ),
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
                started_at,
            },
        )
        .await
        .unwrap();
    }
    sqlx::query("update model_requests set billing_multiplier=2 where provider_account_ref=$1")
        .bind(id)
        .execute(&db.pool)
        .await
        .unwrap();
    let at = now - TimeDelta::minutes(10);
    let mut first = quota(at, reset, 35.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    let value = first.windows[0].estimated_quota.as_ref().unwrap();
    assert!(value.cycle_based);
    assert_eq!(value.total_usd, "200.00");
    assert_eq!(value.billed_total_usd.as_deref(), Some("400.00"));
    assert_eq!(value.sample_start, start);
    assert_eq!(value.request_count, 1);

    // Recreate v3.8.1's late baseline. The same cached percentage can now
    // recover a cycle estimate without waiting for another percentage rise.
    sqlx::query("update account_quota_estimate_samples set state=$2 where account_id=$1")
        .bind(id)
        .bind(json!({
            "reset_at":reset,"seconds":604800,"start_at":at-TimeDelta::hours(1),
            "start_percent":34.0,"observed_at":at,"percent":35.0,
            "estimate":{
                "used_usd":"1.00","total_usd":"100.00","percent_delta":1.0,
                "sample_start":start,"sample_end":start+TimeDelta::minutes(30)
            },
            "pending_reason":"等待采样区间计费完整","pending_end":at
        }))
        .execute(&db.pool)
        .await
        .unwrap();
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    assert_eq!(
        first.windows[0].estimated_quota.as_ref().unwrap().total_usd,
        "200.00"
    );
    // A later plateau must not pull post-snapshot consumption into the old ratio.
    sqlx::query("update account_quota_estimate_samples set state=state-'last_attempt_at'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut plateau = quota(now - TimeDelta::minutes(1), reset, 35.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut plateau)
        .await
        .unwrap();
    assert_eq!(
        plateau.windows[0]
            .estimated_quota
            .as_ref()
            .unwrap()
            .used_usd,
        "70.00"
    );
    // Reset inside the same cycle invalidates the cumulative pre-reset costs.
    let mut rollback = quota(now, reset, 1.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut rollback)
        .await
        .unwrap();
    assert!(rollback.windows[0].estimated_quota.is_none());
    db.close().await;
}

#[tokio::test]
async fn quota_partial_costs_are_disclosed_and_late_costs_repair_the_same_cycle() {
    let Some(db) = TestDatabase::create("quota_partial_cycle").await else {
        return;
    };
    let id = "acct_partial_cycle";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "partial-cycle-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let reset = now + TimeDelta::days(3);
    let start = reset - TimeDelta::days(7);
    sqlx::query("update provider_accounts set created_at=$2 where id=$1")
        .bind(id)
        .bind(start - TimeDelta::days(1))
        .execute(&db.pool)
        .await
        .unwrap();
    for request_id in ["req_known", "req_unknown"] {
        seed_model_request(
            &db.pool,
            ModelRequestSeed {
                request_id,
                account_id: id,
                provider_kind: "openai",
                model: "test-model",
                total_tokens: 100,
                cost_amount: "10",
                started_at: start + TimeDelta::hours(1),
            },
        )
        .await
        .unwrap();
    }
    sqlx::query("update model_requests set cost_source='unavailable', cost_amount=null, cost_currency=null where id='req_unknown'")
        .execute(&db.pool).await.unwrap();
    let mut first = quota(now - TimeDelta::minutes(10), reset, 10.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    let partial = first.windows[0].estimated_quota.as_ref().unwrap();
    assert_eq!(partial.total_usd, "100.00");
    assert_eq!(partial.missing_cost_count, 1);
    assert_eq!(partial.request_count, 2);
    assert!(
        first.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .contains("估值可能偏低")
    );
    sqlx::query("update model_requests set cost_source='calculated', cost_amount=10, cost_currency='USD' where id='req_unknown'")
        .execute(&db.pool).await.unwrap();
    sqlx::query("update account_quota_estimate_samples set state=state-'last_attempt_at'")
        .execute(&db.pool)
        .await
        .unwrap();
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut first)
        .await
        .unwrap();
    let complete = first.windows[0].estimated_quota.clone().unwrap();
    assert_eq!(complete.total_usd, "200.00");
    assert_eq!(complete.missing_cost_count, 0);
    sqlx::query("update model_requests set cost_source='unavailable', cost_amount=null, cost_currency=null where id='req_unknown'")
        .execute(&db.pool).await.unwrap();
    let mut next = quota(now - TimeDelta::minutes(5), reset, 11.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut next)
        .await
        .unwrap();
    let current = next.windows[0].estimated_quota.clone().unwrap();
    assert_eq!(current.total_usd, "90.91");
    assert_eq!(current.percent_delta, 11.0);
    assert_eq!(current.missing_cost_count, 1);
    assert!(
        next.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .contains("估值可能偏低")
    );
    // Only a completely unusable interval retains the last result.
    sqlx::query("update model_requests set cost_source='unavailable', cost_amount=null, cost_currency=null where id='req_known'")
        .execute(&db.pool).await.unwrap();
    let mut unavailable = quota(now - TimeDelta::minutes(2), reset, 12.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut unavailable)
        .await
        .unwrap();
    assert_eq!(
        unavailable.windows[0].estimated_quota.as_ref(),
        Some(&current)
    );
    assert!(
        unavailable.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .starts_with("保留上次估算")
    );
    // Expired cycles never show the retained result as current.
    let mut expired = quota(
        now - TimeDelta::minutes(1),
        now - TimeDelta::seconds(1),
        12.0,
    );
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut expired)
        .await
        .unwrap();
    assert!(expired.windows[0].estimated_quota.is_none());
    db.close().await;
}

#[tokio::test]
async fn quota_estimates_replace_frozen_one_point_result_even_with_missing_costs() {
    let Some(db) = TestDatabase::create("quota_unfreeze_partial").await else {
        return;
    };
    let id = "acct_frozen";
    PgProviderAccountRepository::new(db.pool.clone())
        .insert_provider_account(account(id, "frozen-user"))
        .await
        .unwrap();
    let now = Utc::now();
    let start = now - TimeDelta::hours(3);
    let first_end = start + TimeDelta::hours(1);
    let end = start + TimeDelta::hours(2);
    let reset = now + TimeDelta::days(3);
    // Persist the exact v3.8.2 state shape: current snapshot at 46%, but
    // the visible complete estimate still covers only the first 1% rise.
    sqlx::query("insert into account_quota_estimate_samples(account_id,window_key,observed_at,state) values($1,'weekly',$2,$3)")
        .bind(id).bind(end).bind(json!({
            "version":2,"cycle_based":false,"reset_at":reset,"seconds":604800,
            "start_at":start,"start_percent":30.0,"observed_at":end,"percent":46.0,
            "estimate":{
                "cycle_based":false,"missing_cost_count":0,"request_count":1,
                "used_usd":"0.56","total_usd":"56.37","billed_used_usd":"0.85",
                "billed_total_usd":"84.86","percent_delta":1.0,
                "sample_start":start,"sample_end":first_end
            },
            "pending_reason":"保留上次估算：新采样有 4 笔费用缺失",
            "pending_end":end
        })).execute(&db.pool).await.unwrap();
    for (request_id, amount, started_at) in [
        ("req_frozen_known", "679.12", start + TimeDelta::minutes(30)),
        ("req_frozen_after", "9000", end + TimeDelta::minutes(10)),
        ("req_frozen_missing1", "1", start + TimeDelta::minutes(40)),
        ("req_frozen_missing2", "1", start + TimeDelta::minutes(41)),
        ("req_frozen_missing3", "1", start + TimeDelta::minutes(42)),
        ("req_frozen_missing4", "1", start + TimeDelta::minutes(43)),
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
                started_at,
            },
        )
        .await
        .unwrap();
    }
    sqlx::query("update model_requests set billing_multiplier=1.5 where provider_account_ref=$1")
        .bind(id)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update model_requests set cost_source='unavailable',cost_amount=null,cost_currency=null where id like 'req_frozen_missing%'")
        .execute(&db.pool).await.unwrap();
    // A cached observation repairs the old state without another percentage rise.
    let mut snapshot = quota(end, reset, 46.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut snapshot)
        .await
        .unwrap();
    let result = snapshot.windows[0].estimated_quota.as_ref().unwrap();
    assert_eq!(result.percent_delta, 16.0);
    assert_eq!(result.used_usd, "679.12");
    assert_eq!(result.total_usd, "4244.50");
    assert_eq!(result.billed_total_usd.as_deref(), Some("6366.75"));
    assert_eq!(result.missing_cost_count, 4);
    assert_eq!(result.request_count, 5);
    assert_eq!(result.sample_end, end);
    assert!(
        snapshot.windows[0]
            .estimate_hint
            .as_ref()
            .unwrap()
            .contains("4 / 5 笔费用缺失")
    );
    // A later unchanged quota snapshot still excludes consumption after the
    // paired endpoint; the larger result is not obtained by moving boundaries.
    sqlx::query("update account_quota_estimate_samples set state=state-'last_attempt_at'")
        .execute(&db.pool)
        .await
        .unwrap();
    let expected = result.clone();
    let mut plateau = quota(now, reset, 46.0);
    admin_account_store(&db.pool)
        .attach_quota_estimates(id, &mut plateau)
        .await
        .unwrap();
    assert_eq!(plateau.windows[0].estimated_quota.as_ref(), Some(&expected));
    db.close().await;
}
