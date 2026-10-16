use super::*;
use gateway_store::postgres::PgUsageRollupRepository;

#[tokio::test]
async fn hourly_rollups_upgrade_backfills_existing_history_without_changing_facts() {
    let Some(db) = TestDatabase::create_at_version("hourly_upgrade", 42).await else {
        return;
    };
    let now = Utc::now();
    seed_observability_facts(&db.pool, now - TimeDelta::hours(5))
        .await
        .unwrap();
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    super::super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    assert!(worker.refresh_batch(now).await.unwrap() > 0);
    assert!(
        sqlx::query_scalar::<_, bool>("select exists(select 1 from usage_hourly_stats)")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    );
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    compare(
        &db.pool,
        admin_observability::TimeRange {
            start: now - TimeDelta::days(1),
            end: now,
        },
    )
    .await;
    db.close().await;
}

#[tokio::test]
#[ignore = "manual local PostgreSQL performance measurement; no production data"]
async fn hourly_rollups_measure_large_history() {
    let Some(db) = TestDatabase::create("hourly_perf").await else {
        return;
    };
    let now = Utc::now();
    let hour =
        chrono::DateTime::from_timestamp(now.timestamp().div_euclid(3600) * 3600, 0).unwrap();
    seed_observability_facts(&db.pool, hour - TimeDelta::hours(3))
        .await
        .unwrap();
    clone_request_facts(&db.pool,"req_observe_success",100_000,hour-TimeDelta::hours(2),
        "jsonb_build_object('id','perf_'||n,'started_at',$3-(n%168)*interval '1 hour','deadline_at',$3+interval '1 minute','completed_at',$3)").await;
    sqlx::query("analyze model_requests")
        .execute(&db.pool)
        .await
        .unwrap();
    let store = admin_observability_store(&db.pool);
    let range = admin_observability::TimeRange {
        start: hour - TimeDelta::days(8),
        end: now,
    };
    let start = std::time::Instant::now();
    let raw = store
        .usage_card_summary(range, Default::default())
        .await
        .unwrap();
    let raw_ms = start.elapsed().as_millis();
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    while worker.refresh_batch(now).await.unwrap() > 0 {}
    // Measure the steady-state planner as after autoanalyze, not its empty-table estimates.
    sqlx::raw_sql("analyze usage_hourly_stats; analyze usage_hourly_covered;")
        .execute(&db.pool)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let rolled = store
        .usage_card_summary(range, Default::default())
        .await
        .unwrap();
    let rolled_ms = start.elapsed().as_millis();
    assert_eq!(raw, rolled);
    let rows: i64 = sqlx::query_scalar("select count(*) from usage_hourly_stats")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    eprintln!(
        "HOURLY_BENCH: 100000 synthetic requests; rollup_rows={rows}; raw_ms={raw_ms}; hybrid_ms={rolled_ms}; exact_values=PASS"
    );
    db.close().await;
}

#[tokio::test]
async fn hourly_rollups_do_not_lose_a_concurrent_dirty_notification() {
    use sqlx::Connection;
    let Some(db) = TestDatabase::create("hourly_race").await else {
        return;
    };
    let now = Utc::now();
    seed_observability_facts(&db.pool, now - TimeDelta::hours(5))
        .await
        .unwrap();
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    worker.refresh_batch(now).await.unwrap();
    sqlx::query("update model_requests set cost_amount=2 where id='req_observe_success'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut blocker = db.pool.begin().await.unwrap();
    sqlx::query("select bucket_start from usage_hourly_stats for update")
        .fetch_all(&mut *blocker)
        .await
        .unwrap();
    let refresh = tokio::spawn(async move { worker.refresh_batch(now).await });
    // Wait until the real worker has deleted the dirty marker and is blocked on stats.
    let mut inspector = sqlx::PgConnection::connect_with(&db.pool.connect_options())
        .await
        .unwrap();
    let schema: String = sqlx::query_scalar("select current_schema()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    sqlx::query("select set_config('search_path',$1,false)")
        .bind(schema)
        .execute(&mut inspector)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4),async {
        loop {
            let waiting:bool=sqlx::query_scalar("select exists(select 1 from pg_stat_activity where wait_event_type='Lock' and query like 'delete from usage_hourly_stats%')").fetch_one(&mut inspector).await.unwrap();
            if waiting {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    let writer = tokio::spawn(async move {
        sqlx::query("update model_requests set cost_amount=3 where id='req_observe_success'")
            .execute(&mut inspector)
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(4),async {
        loop {
            let waiting:bool=sqlx::query_scalar("select exists(select 1 from pg_stat_activity where wait_event_type='Lock' and query like 'update model_requests set cost_amount=3%')").fetch_one(&mut *blocker).await.unwrap();
            if waiting {break;}
            sqlx::query("select pg_stat_clear_snapshot()").execute(&mut *blocker).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    blocker.commit().await.unwrap();
    refresh.await.unwrap().unwrap();
    writer.await.unwrap();
    // The correction may still be dirty or already rebuilt by the next batch iteration.
    // In either case the endpoint must see the new amount, not the snapshot before it.
    compare(
        &db.pool,
        admin_observability::TimeRange {
            start: now - TimeDelta::days(1),
            end: now,
        },
    )
    .await;
    PgUsageRollupRepository::new(db.pool.clone())
        .refresh_batch(now)
        .await
        .unwrap();
    compare(
        &db.pool,
        admin_observability::TimeRange {
            start: now - TimeDelta::days(1),
            end: now,
        },
    )
    .await;
    db.close().await;
}

async fn compare(pool: &PgPool, range: admin_observability::TimeRange) {
    let store = admin_observability_store(pool);
    for filter in [
        admin_observability::UsageFilter::default(),
        admin_observability::UsageFilter {
            owner_user_id: Some("alice".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            owner_user_id: Some("nobody".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            group_id: Some("grp_history".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            client_api_key_ref: Some("key_observe".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            provider_account_ref: Some("acct_observe".into()),
            model: Some("public-model".into()),
            client_transport: Some("http_sse".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            status_code: Some(200),
            transport: Some("http_sse".into()),
            ..Default::default()
        },
        admin_observability::UsageFilter {
            request_id: Some("req_observe_success".into()),
            ..Default::default()
        },
    ] {
        let full = store.usage_summary(range, filter.clone()).await.unwrap();
        let cards = store.usage_card_summary(range, filter).await.unwrap();
        assert_eq!(cards.total_cost_usd, full.total_cost_usd);
        assert_eq!(cards.success_count, full.requests.success_count);
        assert_eq!(cards.input_tokens, full.requests.input_tokens);
        assert_eq!(cards.output_tokens, full.requests.output_tokens);
        assert_eq!(cards.cached_tokens, full.requests.cached_tokens);
        assert_eq!(cards.cache_write_tokens, full.requests.cache_write_tokens);
        assert_eq!(cards.total_tokens, full.requests.total_tokens);
        assert_eq!(
            cards.average_latency_ms,
            full.requests
                .latency_sum_ms
                .checked_div(full.requests.latency_count)
        );
    }
}

#[tokio::test]
async fn hourly_rollups_match_raw_after_late_costs_settlement_and_recovery() {
    let Some(db) = TestDatabase::create("hourly_rollup").await else {
        return;
    };
    let now = Utc::now();
    let hour =
        chrono::DateTime::from_timestamp(now.timestamp().div_euclid(3600) * 3600, 0).unwrap();
    let old = hour - TimeDelta::hours(8);
    seed_observability_facts(&db.pool, old + TimeDelta::minutes(35))
        .await
        .unwrap();
    sqlx::raw_sql("insert into users(id,username,password_hash,created_at,updated_at) values ('alice','alice','hash',now(),now());
        update model_requests set user_id='alice';
        update model_requests set cost_source='unavailable',cost_amount=null,cost_currency=null where id='req_observe_success';").execute(&db.pool).await.unwrap();
    clone_request_facts(&db.pool, "req_observe_success", 3, now,
        "jsonb_build_object('id','recent_'||n,'started_at',$3 - interval '1 minute','deadline_at',$3 + interval '5 minutes','completed_at',$3)").await;
    let range = admin_observability::TimeRange {
        start: old,
        end: now + TimeDelta::seconds(1),
    };
    compare(&db.pool, range).await;
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    assert!(worker.refresh_batch(now).await.unwrap() > 0);
    assert!(
        sqlx::query_scalar::<_, bool>("select exists(select 1 from usage_hourly_stats)")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "select exists(select 1 from usage_hourly_covered where bucket_start >= $1)"
        )
        .bind(hour - TimeDelta::hours(1))
        .fetch_one(&db.pool)
        .await
        .unwrap()
    );
    compare(&db.pool, range).await;
    compare(
        &db.pool,
        admin_observability::TimeRange {
            start: old + TimeDelta::minutes(29),
            end: now,
        },
    )
    .await;

    // Cost completion changes an already covered hour: the dirty hour must read raw immediately.
    sqlx::query("update model_requests set cost_source='calculated', cost_currency='USD', cost_amount=7 where id='req_observe_success'").execute(&db.pool).await.unwrap();
    compare(&db.pool, range).await;
    worker.refresh_batch(now).await.unwrap();
    compare(&db.pool, range).await;
    // A late journal settlement can change the amount without a request update.
    sqlx::query("insert into user_group_charge_events(request_id,user_id,account_group_id,client_api_key_ref,amount_usd,raw_amount_usd,billing_multiplier,completed_at)
        values ('req_observe_success','alice','history-group','key_observe',11,11,1,$1)").bind(now).execute(&db.pool).await.unwrap();
    compare(&db.pool, range).await;
    worker.refresh_batch(now).await.unwrap();
    compare(&db.pool, range).await;
    sqlx::query(
        "update user_group_charge_events set amount_usd=13 where request_id='req_observe_success'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    compare(&db.pool, range).await;
    worker.refresh_batch(now).await.unwrap();
    sqlx::query("update model_requests set recovered_at=$1,recovery_request_id='req_observe_uncommitted',recovery_attempt_count=1,recovery_retry_delay_ms=0,recovery_total_latency_ms=900 where id='req_observe_success'").bind(now).execute(&db.pool).await.unwrap();
    compare(&db.pool, range).await;
    worker.refresh_batch(now).await.unwrap();
    compare(&db.pool, range).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from user_group_charge_events")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        1
    );
    db.close().await;
}

#[tokio::test]
async fn hourly_rollups_keep_exact_global_p95_and_handle_moved_or_deleted_facts() {
    let Some(db) = TestDatabase::create("hourly_p95").await else {
        return;
    };
    let now = Utc::now();
    let hour =
        chrono::DateTime::from_timestamp(now.timestamp().div_euclid(3600) * 3600, 0).unwrap();
    seed_observability_facts(&db.pool, hour - TimeDelta::hours(4))
        .await
        .unwrap();
    clone_request_facts(&db.pool,"req_observe_success",100, hour-TimeDelta::hours(3),
        "jsonb_build_object('id','fast_'||n,'started_at',$3,'deadline_at',$3+interval '1 minute','completed_at',$3+interval '1 second','latency_ms',10,'first_token_ms',1)").await;
    clone_request_facts(&db.pool,"req_observe_success",1,hour-TimeDelta::hours(2),
        "jsonb_build_object('id','slow_'||n,'started_at',$3,'deadline_at',$3+interval '1 minute','completed_at',$3+interval '1 second','latency_ms',10000)").await;
    let range = admin_observability::TimeRange {
        start: hour - TimeDelta::hours(6),
        end: now,
    };
    let store = admin_observability_store(&db.pool);
    let before = store
        .usage_summary(range, Default::default())
        .await
        .unwrap();
    let worker = PgUsageRollupRepository::new(db.pool.clone());
    worker.refresh_batch(now).await.unwrap();
    let after = store
        .usage_summary(range, Default::default())
        .await
        .unwrap();
    assert_eq!(
        before.requests.latency_percentiles,
        after.requests.latency_percentiles
    );
    assert_eq!(
        after.requests.latency_percentiles.p95_ms.unwrap().as_f64(),
        10.0
    );
    compare(&db.pool, range).await;
    sqlx::query("update model_requests set started_at=$1,completed_at=$1+interval '10 seconds',deadline_at=$1+interval '1 minute' where id='slow_1'")
        .bind(now - TimeDelta::minutes(1))
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("delete from model_requests where id='fast_1'")
        .execute(&db.pool)
        .await
        .unwrap();
    compare(&db.pool, range).await;
    worker.refresh_batch(now).await.unwrap();
    compare(&db.pool, range).await;
    db.close().await;
}
