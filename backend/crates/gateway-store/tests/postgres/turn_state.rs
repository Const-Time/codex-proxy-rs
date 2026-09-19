use super::TestDatabase;
use gateway_admin::{
    model::{MutationActor, MutationContext},
    ports::{store::AdminStoreErrorKind, turn_state::TurnStateStore},
};
use gateway_store::postgres::PgTurnStateStore;

#[tokio::test]
async fn system_probes_preserve_unknown_cost_real_usage_and_owner_isolation() {
    use chrono::{TimeDelta, Utc};
    use gateway_admin::model::turn_state::{TurnStateProbeResult, TurnStateProbeStart};
    use gateway_store::postgres::{
        ObservabilityPageSize, ObservabilityRange, ObservabilityRepository,
        PgProviderAccountRepository, ProviderAccountRepository, UsageRecordFilter,
        UsageRecordQuery,
    };
    let Some(database) = TestDatabase::create("turn_state_probe").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(super::provider_accounts::account(
            "acct_probe",
            "user_probe",
        ))
        .await
        .unwrap();
    let store = PgTurnStateStore::new(database.pool.clone());
    let now = Utc::now();
    store
        .begin_probe(&TurnStateProbeStart {
            id: "probe_fixture".to_owned(),
            account_id: "acct_probe".to_owned(),
            model: "gpt-5.4".to_owned(),
            phase: "verify".to_owned(),
            started_at: now,
            timeout_seconds: 30,
            cycle_id: "cycle_fixture".to_owned(),
            pool_id: None,
            route_name: "业务出口".to_owned(),
        })
        .await
        .unwrap();
    store
        .finish_probe(
            "probe_fixture",
            &TurnStateProbeResult {
                succeeded: true,
                status: Some(200),
                input_tokens: Some(3),
                output_tokens: Some(2),
                total_tokens: Some(5),
                decision: "verified".to_owned(),
                sent_state: Some("sent-candidate".to_owned()),
                returned_state: Some("returned-state".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Finalization is idempotent; a cancelled late write cannot corrupt completed facts.
    store
        .finish_probe("probe_fixture", &TurnStateProbeResult::default())
        .await
        .unwrap();
    let row: serde_json::Value = sqlx::query_scalar(
        "select to_jsonb(mr) from turn_state_probe_records mr where id='probe_fixture'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(row["facts"]["decision"], "verified");
    assert_eq!(row["facts"]["inputTokens"], 3);
    assert!(row["facts"]["cachedTokens"].is_null());
    assert!(!row.to_string().contains("sent-candidate"));
    assert!(!row.to_string().contains("returned-state"));
    let page = store
        .records(&gateway_admin::model::turn_state::TurnStateRecordQuery {
            from: now - TimeDelta::hours(1),
            to: now + TimeDelta::hours(1),
            page: 1,
            page_size: 10,
            account_id: None,
            model: None,
            phase: None,
            decision: None,
            cycle_id: None,
        })
        .await
        .unwrap();
    assert_eq!(page.stats.total, 1);
    assert_eq!(page.stats.verified, 1);
    assert_eq!(page.stats.known_tokens, 5);
    assert_eq!(page.items[0].facts.state_length, Some(14));
    let mut filter = gateway_admin::model::turn_state::TurnStateRecordQuery {
        from: now - TimeDelta::hours(1),
        to: now + TimeDelta::hours(1),
        page: 1,
        page_size: 1,
        account_id: None,
        model: None,
        phase: None,
        decision: None,
        cycle_id: None,
    };
    // A normally completed HTTP request can still be rejected by candidate rules.
    for (id, decision, completed) in [
        ("collect_rejected", "rejected", true),
        ("collect_failed", "failed", false),
    ] {
        store
            .begin_probe(&TurnStateProbeStart {
                id: id.to_owned(),
                cycle_id: "cycle_fixture".to_owned(),
                account_id: "acct_probe".to_owned(),
                model: "gpt-5.4".to_owned(),
                phase: "collect".to_owned(),
                started_at: now,
                timeout_seconds: 30,
                pool_id: Some("pool_a".to_owned()),
                route_name: "采集代理".to_owned(),
            })
            .await
            .unwrap();
        store
            .finish_probe(
                id,
                &TurnStateProbeResult {
                    succeeded: completed,
                    decision: decision.to_owned(),
                    status: Some(200),
                    latency_ms: 2000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let page = store.records(&filter).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.stats.total, 3, "stats cover all matching pages");
    assert_eq!(page.stats.completed, 2);
    assert_eq!(page.stats.rejected, 1);
    assert_eq!(page.stats.failed, 1);
    assert_eq!(page.stats.unknown_tokens, 2);
    assert_eq!(page.stats.collection_results, 2);
    assert_eq!(page.stats.verification_results, 1);
    filter.cycle_id = Some("unknown-cycle".to_owned());
    assert_eq!(store.records(&filter).await.unwrap().stats.total, 0);
    filter.cycle_id = Some("cycle_fixture".to_owned());
    assert_eq!(store.records(&filter).await.unwrap().stats.total, 3);
    filter.cycle_id = None;
    filter.decision = Some("rejected".to_owned());
    let page = store.records(&filter).await.unwrap();
    assert_eq!(page.stats.total, 1);
    assert_eq!(page.items[0].id, "collect_rejected");
    filter.account_id = Some("another_account".to_owned());
    assert_eq!(store.records(&filter).await.unwrap().stats.total, 0);
    filter.account_id = None;
    filter.decision = None;
    filter.phase = Some("verify".to_owned());
    filter.model = Some("gpt-5.4' OR true --".to_owned());
    assert_eq!(
        store.records(&filter).await.unwrap().stats.total,
        0,
        "filter values must be bound"
    );
    let repository = super::observability_repository(&database.pool);
    let query = |owner| UsageRecordQuery {
        range: ObservabilityRange::new(now - TimeDelta::hours(1), now + TimeDelta::hours(1))
            .unwrap(),
        filter: UsageRecordFilter {
            client_transport: Some("maintenance".to_owned()),
            owner_user_id: owner,
            ..Default::default()
        },
        current_page: 1,
        page_size: ObservabilityPageSize::new(10).unwrap(),
    };
    assert_eq!(
        repository
            .list_usage_records(query(None))
            .await
            .unwrap()
            .total,
        0
    );
    assert_eq!(
        repository
            .list_usage_records(query(Some("test-owner".to_owned())))
            .await
            .unwrap()
            .total,
        0
    );
    assert!(matches!(
        repository
            .usage_record_detail("probe_fixture", Some("test-owner"))
            .await,
        Err(gateway_store::StoreError::NotFound { .. })
    ));
    database.close().await;
}

#[tokio::test]
async fn migration_moves_only_system_probes_and_keeps_business_facts_unchanged() {
    let Some(db) = TestDatabase::create_at_version("state_log_migration", 23).await else {
        return;
    };
    sqlx::query(
        "insert into model_requests (id,client_api_key_ref,config_revision,protocol,operation,endpoint,
          client_transport,started_at,deadline_at,outcome,completed_at,routing_scope,request_kind,
          requested_model_id,input_tokens,provider_observation_json)
         values ('old_probe','system:turn-state',1,'openai','maintenance_probe','/internal/turn-state/collect',
          'maintenance',now(),now()+interval '30 seconds','succeeded',now(),'legacy_provider','state_probe',
          'gpt-5.4',7,'{\"turnStateReturned\":\"secret-state\"}')"
    ).execute(&db.pool).await.unwrap();
    sqlx::query(
        "insert into model_requests (id,client_api_key_ref,config_revision,protocol,operation,endpoint,
          client_transport,started_at,deadline_at,outcome,completed_at,routing_scope)
         values ('business','deleted_key',1,'openai','responses','/v1/responses',
          'http_sse',now(),now()+interval '30 seconds','failed',now(),'all')"
    ).execute(&db.pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(m) from model_requests m where id='business'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let after: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(m) from model_requests m where id='business'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    let old: serde_json::Value =
        sqlx::query_scalar("select facts from turn_state_probe_records where id='old_probe'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(old["decision"], "legacy");
    assert_eq!(old["inputTokens"], 7);
    assert_eq!(old["stateLength"], 12);
    assert!(!old.to_string().contains("secret-state"));
    let remaining: i64 =
        sqlx::query_scalar("select count(*) from model_requests where id='old_probe'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0);
    db.close().await;
}

#[tokio::test]
async fn unfinished_probe_records_are_interrupted_after_their_deadline_without_forging_usage() {
    let Some(db) = TestDatabase::create("state_log_interrupted").await else {
        return;
    };
    sqlx::query(
        "insert into turn_state_probe_records
        (id,cycle_id,account_id,account_name,model,phase,route_name,started_at,deadline_at,facts)
        values ('lost','cycle','a','A','gpt-5.4','collect','pool',now()-interval '10 minutes',
        now()-interval '9 minutes','{\"decision\":\"running\"}')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let now = chrono::Utc::now();
    let query = gateway_admin::model::turn_state::TurnStateRecordQuery {
        from: now - chrono::TimeDelta::hours(1),
        to: now,
        page: 1,
        page_size: 10,
        account_id: None,
        model: None,
        phase: None,
        decision: Some("interrupted".to_owned()),
        cycle_id: None,
    };
    let page = PgTurnStateStore::new(db.pool.clone())
        .records(&query)
        .await
        .unwrap();
    assert_eq!(page.stats.total, 1);
    assert_eq!(page.stats.unknown_tokens, 1);
    assert_eq!(page.stats.completed, 0);
    assert_eq!(
        page.stats.collection_results, 0,
        "interrupted requests are not known rejections"
    );
    assert_eq!(page.items[0].facts.decision, "interrupted");
    assert!(page.items[0].facts.latency_ms.is_none());
    db.close().await;
}

#[tokio::test]
async fn turn_state_store_uses_cas_and_transactional_redacted_audit() {
    let Some(database) = TestDatabase::create("turn_state_cas").await else {
        return;
    };
    let store = PgTurnStateStore::new(database.pool.clone());
    assert_eq!(store.load().await.unwrap(), (1, None));
    let context = MutationContext {
        actor: MutationActor::AdminApiKey,
        request_id: "req_state_audit".to_owned(),
    };
    let sealed = b"test opaque bytes not an actual credential".to_vec();
    assert_eq!(
        store.save(1, sealed.clone(), Some(&context)).await.unwrap(),
        2
    );
    assert_eq!(store.load().await.unwrap(), (2, Some(sealed.clone())));
    assert_eq!(
        store
            .save(1, vec![0], Some(&context))
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::StaleRevision
    );
    let audits: Vec<String> = sqlx::query_scalar(
        "select row_to_json(a)::text from admin_audit_events a where action='turn_state.update'",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(audits.len(), 1);
    assert!(audits[0].contains("req_state_audit"));
    assert!(!audits[0].contains("opaque"));
    assert_eq!(store.save(2, sealed.clone(), None).await.unwrap(), 3);
    // Audit insert and document write must roll back together.
    sqlx::query("alter table admin_audit_events add constraint test_reject_state_audit check (action <> 'turn_state.update') not valid")
        .execute(&database.pool).await.unwrap();
    assert!(store.save(3, vec![1], Some(&context)).await.is_err());
    assert_eq!(store.load().await.unwrap(), (3, Some(sealed)));
    database.close().await;
}
