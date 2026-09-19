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
    let row: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr where id='probe_fixture'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(row["outcome"], "succeeded");
    assert_eq!(row["client_transport"], "maintenance");
    assert_eq!(row["client_api_key_ref"], "system:turn-state");
    assert!(row["client_api_key_id"].is_null());
    assert!(row["user_id"].is_null());
    assert_eq!(row["input_tokens"], 3);
    assert!(row["cached_tokens"].is_null());
    assert_eq!(row["cost_source"], "unavailable");
    assert!(row["cost_amount"].is_null());
    assert_eq!(
        row["provider_observation_json"]["turnStateSent"],
        "sent-candidate"
    );
    assert_eq!(
        row["provider_observation_json"]["turnStateReturned"],
        "returned-state"
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
        1
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
