use super::admin_observability_store;
use chrono::{Duration, Utc};
use gateway_admin::{
    model::observability as admin_observability, ports::store::ObservabilityStore as _,
};
use gateway_core::engine::{
    ExecutionOutcome, ExecutionStore, ModelRequestFailureObservation,
    ModelRequestFinalization as CoreModelRequestFinalization, ModelRequestId,
    ModelRequestTimings as CoreModelRequestTimings,
};
use gateway_core::error::{GatewayError, GatewayErrorKind, ProviderConnectionObservation};
use gateway_core::metering::{CalculatedCost, CostEstimate, Usage};
use gateway_core::upstream::UpstreamSendState;
use gateway_store::postgres::{
    ModelRequestAttemptStart, ModelRequestRepository, NewModelRequest, ObservabilityPageSize,
    ObservabilityRange, ObservabilityRepository, PgExecutionStore, UsageRecordFilter,
    UsageRecordQuery,
};
use serde_json::json;

use super::{TestDatabase, observability_repository};

#[test]
fn postgres_execution_adapter_implements_core_port() {
    fn assert_port<T: ExecutionStore>() {}
    assert_port::<PgExecutionStore>();
}

#[test]
fn model_request_rejects_mismatched_client_key_live_id() {
    let started_at = Utc::now();
    let request = NewModelRequest {
        billing: None,
        parent_request_id: None,
        initiating_plugin_instance_id: None,
        user_id: None,

        admission_decision_ms: None,
        id: "request-1".to_owned(),
        client_api_key_id: Some("key-live".to_owned()),
        client_api_key_ref: "key-history".to_owned(),
        config_revision: 1,
        protocol: "openai".to_owned(),
        operation: "responses".to_owned(),
        endpoint: "/v1/responses".to_owned(),
        client_transport: "http_sse".to_owned(),
        requested_model_id: Some("coding".to_owned()),
        routing_scope: "all".to_owned(),
        routing_group_refs: Vec::new(),
        routing_group_names_snapshot: serde_json::json!([]),
        client_ip: None,
        user_agent: None,
        reasoning_effort: None,
        reasoning_preset: None,
        request_kind: None,
        subagent_kind: None,
        compact: false,
        continuation: Default::default(),
        image_generation_requested: false,
        started_at,
        deadline_at: started_at + Duration::seconds(30),
    };
    assert!(request.validate().is_err());
}

#[tokio::test]
async fn merged_model_less_first_attempt_should_match_sequential_semantics() {
    let Some(database) = TestDatabase::create("execution_merged_insert").await else {
        return;
    };
    let repository = PgExecutionStore::new(database.pool.clone());
    let started_at = Utc::now();
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, email, upstream_user_id,
           upstream_account_id, plan_type, authentication_kind,
           provider_credentials_json, credential_revision,
           has_refresh_token, access_token_expires_at, enabled, credential_state,
           credential_observed_at, created_at, updated_at
         ) values (
           'acct_merged', 'openai', 'merged', null, 'user-merged', null, null, 'oauth',
           '{}'::jsonb, 1, false, $1 + interval '1 day', true, 'ready', $1, $1, $1
         )",
    )
    .bind(started_at)
    .execute(&database.pool)
    .await
    .expect("seed provider account");
    let request = NewModelRequest {
        billing: Some(
            gateway_core::engine::budget::ClientBudgetBilling::new(
                "2.5".parse().unwrap(),
                serde_json::from_value(json!({"components":["cacheRead"]})).unwrap(),
            )
            .unwrap(),
        ),
        parent_request_id: Some("req_parent_not_yet_written".into()),
        initiating_plugin_instance_id: Some("plugin-fixture".into()),
        user_id: None,

        admission_decision_ms: None,
        id: "req_merged".to_owned(),
        client_api_key_id: None,
        client_api_key_ref: "key_merged".to_owned(),
        config_revision: 1,
        protocol: "openai".to_owned(),
        operation: "generate_image".to_owned(),
        endpoint: "/v1/images/generations".to_owned(),
        client_transport: "http_json".to_owned(),
        requested_model_id: None,
        routing_scope: "all".to_owned(),
        routing_group_refs: Vec::new(),
        routing_group_names_snapshot: serde_json::json!([]),
        client_ip: None,
        user_agent: None,
        reasoning_effort: None,
        reasoning_preset: None,
        request_kind: None,
        subagent_kind: None,
        compact: false,
        continuation: Default::default(),
        image_generation_requested: false,
        started_at,
        deadline_at: started_at + Duration::seconds(30),
    };
    let attempt = ModelRequestAttemptStart {
        account_selection_wait_ms: None,
        capacity_used_slots: None,
        capacity_total_slots: None,
        model_request_id: "req_merged".to_owned(),
        attempt_count: 1,
        provider_kind: "openai".to_owned(),
        provider_account_id: Some("acct_merged".to_owned()),
        provider_account_ref: Some("acct_merged".to_owned()),
        upstream_model_id: None,
        upstream_transport: "http_json".to_owned(),
        http_version: None,
    };

    let mut sequential = request.clone();
    sequential.id = "req_sequential".into();
    repository.insert_model_request(sequential).await.unwrap();
    repository
        .insert_model_request_with_first_attempt(request, attempt)
        .await
        .expect("merged insert");

    let (attempt_count, send_state, provider_kind, outcome, requested_model, upstream_model): (
        i32,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "select attempt_count, upstream_send_state, provider_kind, outcome,
                    requested_model_id, upstream_model_id
             from model_requests where id = 'req_merged'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load merged request");
    assert_eq!(
        (
            attempt_count,
            send_state.as_str(),
            provider_kind.as_str(),
            outcome.as_str(),
            requested_model,
            upstream_model,
        ),
        (1, "not_sent", "openai", "running", None, None)
    );

    let frozen: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "select billing_multiplier::text,billing_multiplier_scope from model_requests order by id",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(
        frozen,
        vec![("2.5000000000".into(), json!({"components":["cacheRead"]})); 2]
    );
    // 后续 attempt 沿用常规 CAS 递增路径；已持久化的 sent 水位不被重试重置。
    repository
        .mark_upstream_send_state(
            "req_merged",
            gateway_store::postgres::UpstreamSendState::Sent,
        )
        .await
        .expect("mark sent before retry");
    let second = repository
        .begin_model_request_attempt(ModelRequestAttemptStart {
            account_selection_wait_ms: None,
            capacity_used_slots: None,
            capacity_total_slots: None,
            model_request_id: "req_merged".to_owned(),
            attempt_count: 2,
            provider_kind: "openai".to_owned(),
            provider_account_id: Some("acct_merged".to_owned()),
            provider_account_ref: Some("acct_merged".to_owned()),
            upstream_model_id: None,
            upstream_transport: "http_json".to_owned(),
            http_version: None,
        })
        .await
        .expect("second attempt");
    assert_eq!(second, 2);
    let send_state: String = sqlx::query_scalar(
        "select upstream_send_state from model_requests where id = 'req_merged'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load send state after retry");
    assert_eq!(send_state, "sent");
    let ancestry: (Option<String>, Option<String>) = sqlx::query_as(
        "select parent_request_id, initiating_plugin_instance_id from model_requests where id = 'req_merged'",
    ).fetch_one(&database.pool).await.unwrap();
    assert_eq!(
        ancestry,
        (
            Some("req_parent_not_yet_written".into()),
            Some("plugin-fixture".into())
        )
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "select exists(select 1 from model_requests where id = 'req_parent_not_yet_written')"
        )
        .fetch_one(&database.pool)
        .await
        .unwrap()
    );
    database.close().await;
}

#[tokio::test]
async fn model_request_persists_group_routing_snapshot_without_live_group_foreign_keys() {
    let Some(database) = TestDatabase::create("execution_group_routing_history").await else {
        return;
    };
    let repository = PgExecutionStore::new(database.pool.clone());
    let started_at = Utc::now();
    repository
        .insert_model_request(NewModelRequest {
            billing: None,
            parent_request_id: Some("req_unwritten_parent".into()),
            initiating_plugin_instance_id: Some("plugin-nested".into()),
            user_id: None,

            admission_decision_ms: None,
            id: "req_group_history".to_owned(),
            client_api_key_id: None,
            client_api_key_ref: "key_group_history".to_owned(),
            config_revision: 7,
            routing_scope: "groups".to_owned(),
            routing_group_refs: vec![
                "grp_00000000000000000000000000000001".to_owned(),
                "grp_00000000000000000000000000000002".to_owned(),
            ],
            routing_group_names_snapshot: serde_json::json!(["Production Pool", "Overflow Pool"]),
            protocol: "openai".to_owned(),
            operation: "responses".to_owned(),
            endpoint: "/v1/responses".to_owned(),
            client_transport: "http_sse".to_owned(),
            requested_model_id: Some("coding".to_owned()),
            client_ip: None,
            user_agent: None,
            reasoning_effort: None,
            reasoning_preset: None,
            request_kind: None,
            subagent_kind: None,
            compact: false,
            continuation: Default::default(),
            image_generation_requested: false,
            started_at,
            deadline_at: started_at + Duration::seconds(30),
        })
        .await
        .expect("insert grouped request history");

    let stored: (String, Vec<String>, serde_json::Value) = sqlx::query_as(
        "select routing_scope, routing_group_refs, routing_group_names_snapshot
         from model_requests where id = 'req_group_history'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load grouped request history");
    assert_eq!(stored.0, "groups");
    assert_eq!(
        stored.1,
        [
            "grp_00000000000000000000000000000001",
            "grp_00000000000000000000000000000002",
        ]
    );
    assert_eq!(
        stored.2,
        serde_json::json!(["Production Pool", "Overflow Pool"])
    );
    let ancestry: (Option<String>, Option<String>) = sqlx::query_as(
        "select parent_request_id, initiating_plugin_instance_id from model_requests where id = 'req_group_history'",
    ).fetch_one(&database.pool).await.unwrap();
    assert_eq!(
        ancestry,
        (
            Some("req_unwritten_parent".into()),
            Some("plugin-nested".into())
        )
    );
    for bad in [
        "update model_requests set parent_request_id = null where id = 'req_group_history'",
        "update model_requests set initiating_plugin_instance_id = null where id = 'req_group_history'",
        "update model_requests set parent_request_id = id where id = 'req_group_history'",
        "update model_requests set initiating_plugin_instance_id = '' where id = 'req_group_history'",
        "update model_requests set parent_request_id = E'forged\\nparent' where id = 'req_group_history'",
    ] {
        assert!(sqlx::query(bad).execute(&database.pool).await.is_err());
    }
    database.close().await;
}

#[tokio::test]
async fn downstream_commit_should_atomically_record_http_status_once() {
    let Some(database) = TestDatabase::create("execution_commit_status").await else {
        return;
    };
    seed_running_request(&database.pool, "req_commit_status")
        .await
        .expect("seed model request");
    let repository = PgExecutionStore::new(database.pool.clone());

    let committed = ModelRequestRepository::mark_downstream_committed(
        &repository,
        "req_commit_status",
        Utc::now(),
        Some(200),
    )
    .await
    .expect("commit downstream");
    let overwritten = repository
        .record_client_status_code("req_commit_status", 500)
        .await
        .expect("reject status overwrite without store failure");
    let (committed_at, status): (Option<chrono::DateTime<Utc>>, Option<i32>) = sqlx::query_as(
        "select downstream_committed_at, client_status_code
         from model_requests where id = 'req_commit_status'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load committed request");

    assert!(committed);
    assert!(!overwritten);
    assert!(committed_at.is_some());
    assert_eq!(status, Some(200));
    database.close().await;
}

#[tokio::test]
async fn terminal_failure_should_accept_exactly_one_client_status_backfill() {
    let Some(database) = TestDatabase::create("execution_terminal_status").await else {
        return;
    };
    seed_running_request(&database.pool, "req_terminal_status")
        .await
        .expect("seed model request");
    sqlx::query(
        "update model_requests
         set outcome = 'failed', completed_at = now()
         where id = 'req_terminal_status'",
    )
    .execute(&database.pool)
    .await
    .expect("finalize model request");
    let repository = PgExecutionStore::new(database.pool.clone());

    let recorded = repository
        .record_client_status_code("req_terminal_status", 429)
        .await
        .expect("record terminal client status");
    let overwritten = repository
        .record_client_status_code("req_terminal_status", 500)
        .await
        .expect("reject terminal status overwrite without store failure");
    let status: Option<i32> = sqlx::query_scalar(
        "select client_status_code from model_requests where id = 'req_terminal_status'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load terminal client status");

    assert!(recorded);
    assert!(!overwritten);
    assert_eq!(status, Some(429));
    database.close().await;
}

#[tokio::test]
async fn core_adapter_should_persist_calculated_cost_exactly() {
    let Some(database) = TestDatabase::create("execution_calculated_cost").await else {
        return;
    };
    seed_running_request(&database.pool, "req_calculated_cost")
        .await
        .expect("seed model request");
    sqlx::query(
        "update model_requests
         set provider_kind = 'xai', provider_account_ref = 'acct_xai',
             upstream_model_id = 'grok-4.5', upstream_transport = 'http_sse', attempt_count = 1
         where id = 'req_calculated_cost'",
    )
    .execute(&database.pool)
    .await
    .expect("seed model request attempt");
    let repository = PgExecutionStore::new(database.pool.clone());

    let mut finalization = successful_core_finalization("req_calculated_cost");
    finalization.downstream_committed_at = Some(std::time::SystemTime::now());
    finalization.websocket_pool = Some("reuse".to_owned());
    finalization.service_tier = Some("priority".to_owned());
    finalization.upstream_response_model = Some("grok-4.6-build".to_owned());
    finalization.cost = CalculatedCost::from_usd_ticks(12_345)
        .expect("calculated cost")
        .into_estimate();
    ExecutionStore::finalize_model_request(&repository, finalization)
        .await
        .expect("persist calculated cost");
    let persisted: (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "select cost_source, cost_amount::text, cost_currency, upstream_transport, http_version,
                websocket_pool, service_tier
         from model_requests where id = 'req_calculated_cost'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load calculated cost");

    assert_eq!(
        persisted,
        (
            "calculated".to_owned(),
            "0.0000012345".to_owned(),
            "USD".to_owned(),
            "websocket".to_owned(),
            "HTTP/2".to_owned(),
            "reuse".to_owned(),
            Some("priority".to_owned()),
        )
    );
    let observation = observability_repository(&database.pool);
    let detail = observation
        .usage_record_detail("req_calculated_cost", None)
        .await
        .expect("request detail");
    assert_eq!(
        detail.request.upstream_model_id.as_deref(),
        Some("grok-4.5")
    );
    assert_eq!(
        detail.request.upstream_response_model.as_deref(),
        Some("grok-4.6-build")
    );
    let page = observation
        .list_usage_records(UsageRecordQuery {
            range: ObservabilityRange::new(
                Utc::now() - Duration::hours(1),
                Utc::now() + Duration::hours(1),
            )
            .unwrap(),
            filter: UsageRecordFilter::default(),
            current_page: 1,
            page_size: ObservabilityPageSize::new(10).unwrap(),
        })
        .await
        .expect("request list");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        page.items[0].upstream_response_model.as_deref(),
        Some("grok-4.6-build")
    );
    database.close().await;
}

#[tokio::test]
async fn billing_snapshot_survives_later_price_changes_and_usage_detail_reads() {
    use gateway_core::metering::{
        CalculatedCostAmounts, CalculatedCostBreakdown, CalculatedCostRates, CurrencyCode, Decimal,
        Money,
    };
    let Some(database) = TestDatabase::create("billing_snapshot").await else {
        return;
    };
    seed_running_request(&database.pool, "req_billing_snapshot")
        .await
        .unwrap();
    let money = |ticks| {
        Money::new(
            Decimal::from_scaled(ticks).unwrap(),
            CurrencyCode::new("USD").unwrap(),
        )
    };
    let billing = CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            money(1_000_000),
            money(2_000_000),
            money(3_000_000),
            money(0),
            money(6_000_000),
            money(6_000_000),
        ),
        CalculatedCostRates::new(
            money(10_000_000_000),
            money(20_000_000_000),
            money(5_000_000_000),
            money(0),
        ),
        Some("default".to_owned()),
        100,
    )
    .with_custom_multiplier(
        12500,
        gateway_core::metering::MultiplierScope::Components(
            [gateway_core::metering::CostComponent::CacheRead].into(),
        ),
    )
    .unwrap();
    let mut finalization = successful_core_finalization("req_billing_snapshot");
    finalization.cost = billing.calculated_cost().into_estimate();
    ExecutionStore::finalize_model_request(
        &PgExecutionStore::new(database.pool.clone()),
        finalization,
    )
    .await
    .unwrap();
    let observation = admin_observability_store(&database.pool);
    let original = observation
        .usage_record_detail("req_billing_snapshot", None)
        .await
        .unwrap();
    let Some(admin_observability::UsageBilling::Calculated(saved)) = &original.request.billing
    else {
        panic!("persisted billing breakdown must be restored without provider recalculation");
    };
    assert_eq!(saved.custom_multiplier_bps, 12500);
    assert_eq!(saved.total_amount.amount, "0.000675".parse().unwrap());
    assert_eq!(
        saved.custom_multiplier_scope,
        *billing.custom_multiplier_scope()
    );
    assert!(saved.standard_components.is_some());
    sqlx::query("update runtime_settings set pricing_overrides_json = $1 where id = 1")
        .bind(json!({"openai":{"gpt-5.4":{"multiplierBps":90000,"bands":{}}}}))
        .execute(&database.pool)
        .await
        .unwrap();
    let after = observation
        .usage_record_detail("req_billing_snapshot", None)
        .await
        .unwrap();
    assert_eq!(original.request.billing, after.request.billing);
    database.close().await;
}

#[tokio::test]
async fn core_adapter_should_persist_image_result_and_new_websocket_pool() {
    let Some(database) = TestDatabase::create("execution_image_usage").await else {
        return;
    };
    seed_running_request(&database.pool, "req_image_usage")
        .await
        .expect("seed image request");
    sqlx::query(
        "update model_requests
         set image_generation_requested = true, provider_kind = 'openai',
             provider_account_ref = 'acct_openai', upstream_model_id = 'gpt-image',
             upstream_transport = 'websocket', attempt_count = 1
         where id = 'req_image_usage'",
    )
    .execute(&database.pool)
    .await
    .expect("seed image attempt");
    let repository = PgExecutionStore::new(database.pool.clone());
    let mut usage = Usage::new();
    usage.image_input_tokens = Some(31);
    usage.image_output_tokens = Some(9);
    let mut finalization = successful_core_finalization("req_image_usage");
    finalization.usage = usage;
    finalization.image_generation_succeeded = Some(true);
    finalization.websocket_pool = Some("new".to_owned());

    ExecutionStore::finalize_model_request(&repository, finalization)
        .await
        .expect("persist image usage");
    let persisted: (bool, Option<i64>, Option<i64>, Option<bool>, Option<String>) = sqlx::query_as(
        "select image_generation_requested, image_input_tokens, image_output_tokens,
                    image_generation_succeeded, websocket_pool
             from model_requests where id = 'req_image_usage'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load image usage");

    assert_eq!(
        persisted,
        (true, Some(31), Some(9), Some(true), Some("new".to_owned()))
    );
    database.close().await;
}

#[tokio::test]
async fn expired_image_request_should_be_recovered_as_failed() {
    let Some(database) = TestDatabase::create("execution_expired_image").await else {
        return;
    };
    seed_running_request(&database.pool, "req_expired_image")
        .await
        .expect("seed image request");
    sqlx::query(
        "update model_requests
         set image_generation_requested = true,
             started_at = now() - interval '2 seconds',
             deadline_at = now() - interval '1 second'
         where id = 'req_expired_image'",
    )
    .execute(&database.pool)
    .await
    .expect("expire image request");
    let repository = PgExecutionStore::new(database.pool.clone());

    repository
        .recover_expired_model_requests(Utc::now())
        .await
        .expect("recover expired image request");
    let persisted: (String, Option<bool>) = sqlx::query_as(
        "select outcome, image_generation_succeeded
         from model_requests where id = 'req_expired_image'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load recovered image request");

    assert_eq!(persisted, ("incomplete".to_owned(), Some(false)));
    database.close().await;
}

fn successful_core_finalization(id: &str) -> CoreModelRequestFinalization {
    CoreModelRequestFinalization {
        diagnostic_trace_json: None,
        request_id: ModelRequestId::new(id).expect("request id"),
        outcome: ExecutionOutcome::Succeeded,
        send_state: UpstreamSendState::Sent,
        attempt_count: 1,
        downstream_committed_at: None,
        client_status_code: Some(200),
        client_response_id: None,
        upstream_status_code: Some(200),
        upstream_request_id: None,
        upstream_response_id: None,
        upstream_transport: Some("websocket".to_owned()),
        http_version: Some("HTTP/2".to_owned()),
        websocket_pool: None,
        upstream_response_model: None,
        service_tier: None,
        provider_metadata_json: None,
        error: None,
        provider_error_code: None,
        raw_upstream_error: None,
        failure_observation: Default::default(),
        retry_after_ms: None,
        usage: Usage::new(),
        image_generation_succeeded: None,
        cost: CostEstimate::unavailable(),
        timings: CoreModelRequestTimings::default(),
        completed_at: std::time::SystemTime::now(),
    }
}

#[tokio::test]
async fn core_adapter_persists_opaque_response_ids_as_bytes() {
    let Some(database) = TestDatabase::create("execution_opaque_response_id").await else {
        return;
    };
    seed_running_request(&database.pool, "req_opaque_response_id")
        .await
        .expect("seed model request");
    let store = PgExecutionStore::new(database.pool.clone());
    let response_id = format!("resp_{}\0opaque", "x".repeat(4_096));
    let mut finalization = successful_core_finalization("req_opaque_response_id");
    finalization.client_response_id = Some(response_id.clone());
    finalization.upstream_response_id = Some(response_id.clone());

    ExecutionStore::finalize_model_request(&store, finalization)
        .await
        .expect("persist opaque response IDs");

    let persisted: (Option<Vec<u8>>, Option<Vec<u8>>) = sqlx::query_as(
        "select client_response_id, upstream_response_id
         from model_requests where id = 'req_opaque_response_id'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load opaque response IDs");
    assert_eq!(persisted.0.as_deref(), Some(response_id.as_bytes()));
    assert_eq!(persisted.1.as_deref(), Some(response_id.as_bytes()));

    database.close().await;
}

#[tokio::test]
async fn core_adapter_persists_raw_upstream_error_verbatim() {
    let Some(database) = TestDatabase::create("execution_raw_upstream_error").await else {
        return;
    };
    seed_running_request(&database.pool, "req_raw_upstream_error")
        .await
        .expect("seed model request");
    let store = PgExecutionStore::new(database.pool.clone());
    let raw = r#"{"error":{"message":"raw upstream body","opaque":"\u0000"}}"#;
    let mut finalization = successful_core_finalization("req_raw_upstream_error");
    finalization.outcome = ExecutionOutcome::Failed;
    finalization.client_status_code = Some(502);
    finalization.upstream_status_code = Some(500);
    finalization.error = Some(GatewayError::new(
        GatewayErrorKind::UpstreamUnavailable,
        "upstream service is unavailable",
    ));
    finalization.raw_upstream_error = Some(raw.to_owned());

    ExecutionStore::finalize_model_request(&store, finalization)
        .await
        .expect("persist raw upstream error");

    let persisted: Option<String> = sqlx::query_scalar(
        "select raw_upstream_error from model_requests where id = 'req_raw_upstream_error'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load raw upstream error");
    assert_eq!(persisted.as_deref(), Some(raw));

    database.close().await;
}

#[tokio::test]
async fn core_adapter_should_persist_only_object_provider_observation() {
    let Some(database) = TestDatabase::create("execution_provider_observation").await else {
        return;
    };
    seed_running_request(&database.pool, "req_provider_observation")
        .await
        .expect("seed model request");
    let store = PgExecutionStore::new(database.pool.clone());
    let mut finalization = successful_core_finalization("req_provider_observation");
    finalization.downstream_committed_at = Some(std::time::SystemTime::now());
    finalization.provider_metadata_json = Some(json!({"effectiveModel":"gpt-test", "requestedServiceTier":"priority", "upstreamServiceTier":"default"}).to_string());

    ExecutionStore::finalize_model_request(&store, finalization)
        .await
        .expect("persist provider observation");
    let persisted: serde_json::Value = sqlx::query_scalar(
        "select provider_observation_json from model_requests where id = 'req_provider_observation'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load provider observation");
    assert_eq!(persisted["effectiveModel"], "gpt-test");
    let page = observability_repository(&database.pool)
        .list_usage_records(UsageRecordQuery {
            range: ObservabilityRange::new(
                Utc::now() - Duration::hours(1),
                Utc::now() + Duration::hours(1),
            )
            .unwrap(),
            filter: UsageRecordFilter::default(),
            current_page: 1,
            page_size: ObservabilityPageSize::new(10).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        page.items[0].requested_service_tier.as_deref(),
        Some("priority")
    );
    assert_eq!(
        page.items[0].upstream_service_tier.as_deref(),
        Some("default")
    );

    let detail = admin_observability_store(&database.pool)
        .usage_record_detail("req_provider_observation", None)
        .await
        .unwrap();
    assert!(
        detail
            .request
            .provider_metadata_json
            .as_deref()
            .unwrap()
            .contains("priority")
    );

    database.close().await;
}

#[tokio::test]
async fn core_adapter_should_reject_non_object_provider_observation() {
    let Some(database) = TestDatabase::create("execution_invalid_provider_observation").await
    else {
        return;
    };
    seed_running_request(&database.pool, "req_invalid_provider_observation")
        .await
        .expect("seed model request");
    let store = PgExecutionStore::new(database.pool.clone());
    let mut finalization = successful_core_finalization("req_invalid_provider_observation");
    finalization.provider_metadata_json = Some("[]".to_owned());

    assert!(
        ExecutionStore::finalize_model_request(&store, finalization)
            .await
            .is_err()
    );

    database.close().await;
}

#[tokio::test]
async fn core_adapter_persists_continuation_connection_failure_observation() {
    let Some(database) = TestDatabase::create("execution_continuation_observation").await else {
        return;
    };
    seed_running_request(&database.pool, "req_continuation_observation")
        .await
        .expect("seed model request");
    let store = PgExecutionStore::new(database.pool.clone());
    let mut finalization = successful_core_finalization("req_continuation_observation");
    finalization.outcome = ExecutionOutcome::Failed;
    finalization.client_status_code = Some(400);
    finalization.error = Some(GatewayError::new(
        GatewayErrorKind::InvalidRequest,
        "conversation continuation must be rebuilt",
    ));
    finalization.failure_observation = ModelRequestFailureObservation {
        continuation_unavailable_reason: Some("reused_connection_lost".to_owned()),
        upstream_connection: Some(ProviderConnectionObservation::new(
            "connection-observed",
            "tcp_reset",
            12_000,
            4_000,
        )),
    };

    ExecutionStore::finalize_model_request(&store, finalization)
        .await
        .expect("persist continuation failure observation");

    type PersistedContinuationObservation = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    );
    let persisted: PersistedContinuationObservation = sqlx::query_as(
        "select error_kind, continuation_unavailable_reason,
                upstream_connection_id, upstream_connection_exit_reason,
                upstream_connection_age_ms, upstream_connection_idle_ms
           from model_requests where id = 'req_continuation_observation'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load continuation failure observation");
    assert_eq!(
        persisted.0.as_deref(),
        Some("continuation_recovery_required")
    );
    assert_eq!(persisted.1.as_deref(), Some("reused_connection_lost"));
    assert_eq!(persisted.2.as_deref(), Some("connection-observed"));
    assert_eq!(persisted.3.as_deref(), Some("tcp_reset"));
    assert_eq!(persisted.4, Some(12_000));
    assert_eq!(persisted.5, Some(4_000));

    database.close().await;
}

#[tokio::test]
async fn successful_new_chain_should_mark_recent_continuation_failure_recovered() {
    let Some(database) = TestDatabase::create("execution_continuation_recovered").await else {
        return;
    };
    let affinity_hash = "a".repeat(64);
    let failure_completed_at = Utc::now() - Duration::seconds(8);
    seed_continuation_failure(
        &database.pool,
        "req_continuation_failed",
        &affinity_hash,
        failure_completed_at,
    )
    .await;
    seed_recovery_request(
        &database.pool,
        "req_continuation_recovery",
        &affinity_hash,
        failure_completed_at + Duration::seconds(4),
    )
    .await;

    let store = PgExecutionStore::new(database.pool.clone());
    ExecutionStore::finalize_model_request(
        &store,
        successful_core_finalization("req_continuation_recovery"),
    )
    .await
    .expect("finalize successful recovery request");

    type PersistedRecoveryObservation = (
        Option<String>,
        Option<chrono::DateTime<Utc>>,
        i32,
        Option<i64>,
        Option<i64>,
    );
    let recovered: PersistedRecoveryObservation = sqlx::query_as(
        "select recovery_request_id, recovered_at, recovery_attempt_count,
                recovery_retry_delay_ms, recovery_total_latency_ms
           from model_requests where id = 'req_continuation_failed'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load recovered continuation failure");

    assert_eq!(recovered.0.as_deref(), Some("req_continuation_recovery"));
    assert!(recovered.1.is_some());
    assert_eq!(recovered.2, 1);
    assert_eq!(recovered.3, Some(4_000));
    assert!(recovered.4.is_some_and(|latency| latency >= 4_000));
    database.close().await;
}

#[tokio::test]
async fn successful_http_downgrade_should_mark_pending_websocket_failures_recovered() {
    let Some(database) = TestDatabase::create("execution_transport_downgrade_recovered").await
    else {
        return;
    };
    let affinity_hash = "d".repeat(64);
    let first_failure_at = Utc::now() - Duration::seconds(8);
    let second_failure_at = first_failure_at + Duration::seconds(1);
    seed_websocket_transport_failure(
        &database.pool,
        "req_websocket_failed_first",
        &affinity_hash,
        first_failure_at,
    )
    .await;
    seed_websocket_transport_failure(
        &database.pool,
        "req_websocket_failed_second",
        &affinity_hash,
        second_failure_at,
    )
    .await;
    seed_websocket_transport_failure(
        &database.pool,
        "req_websocket_failed_after_delivery",
        &affinity_hash,
        second_failure_at,
    )
    .await;
    sqlx::query(
        "update model_requests
            set downstream_committed_at = completed_at
          where id = 'req_websocket_failed_after_delivery'",
    )
    .execute(&database.pool)
    .await
    .expect("mark control failure delivered downstream");

    let recovery_started_at = first_failure_at + Duration::seconds(4);
    seed_transport_recovery_request(
        &database.pool,
        "req_http_fallback_succeeded",
        &affinity_hash,
        recovery_started_at,
    )
    .await;
    let store = PgExecutionStore::new(database.pool.clone());
    let mut recovery = successful_core_finalization("req_http_fallback_succeeded");
    recovery.upstream_transport = Some("http_sse".to_owned());
    recovery.downstream_committed_at = Some(std::time::SystemTime::now());
    ExecutionStore::finalize_model_request(&store, recovery)
        .await
        .expect("finalize successful HTTP fallback");

    type PersistedRecovery = (
        String,
        Option<String>,
        Option<chrono::DateTime<Utc>>,
        i32,
        Option<i64>,
        Option<i64>,
    );
    let recovered: Vec<PersistedRecovery> = sqlx::query_as(
        "select id, recovery_request_id, recovered_at, recovery_attempt_count,
                recovery_retry_delay_ms, recovery_total_latency_ms
           from model_requests
          where id like 'req_websocket_failed_%'
          order by id",
    )
    .fetch_all(&database.pool)
    .await
    .expect("load WebSocket recovery observations");

    assert_eq!(recovered.len(), 3);
    let delivered = &recovered[0];
    assert_eq!(delivered.0, "req_websocket_failed_after_delivery");
    assert_eq!((&delivered.1, delivered.2, delivered.3), (&None, None, 0));
    for (row, expected_delay_ms) in recovered[1..].iter().zip([4_000, 3_000]) {
        assert_eq!(row.1.as_deref(), Some("req_http_fallback_succeeded"));
        assert!(row.2.is_some());
        assert_eq!(row.3, 1);
        assert_eq!(row.4, Some(expected_delay_ms));
        assert!(row.5.is_some_and(|latency| latency >= expected_delay_ms));
    }
    let repository = super::observability_repository(&database.pool);
    let failed_detail = repository
        .usage_record_detail("req_websocket_failed_first", None)
        .await
        .unwrap();
    assert_eq!(
        failed_detail.related_requests[0]["requestId"],
        "req_http_fallback_succeeded"
    );
    assert_eq!(
        failed_detail.related_requests[0]["relation"],
        "recovered_by"
    );
    let recovered_detail = repository
        .usage_record_detail("req_http_fallback_succeeded", None)
        .await
        .unwrap();
    assert_eq!(recovered_detail.related_requests.len(), 2);
    assert!(
        recovered_detail
            .related_requests
            .iter()
            .all(|related| related["relation"] == "recovers")
    );
    database.close().await;
}

#[tokio::test]
async fn websocket_success_or_late_http_success_should_not_hide_transport_failure() {
    let Some(database) = TestDatabase::create("execution_transport_downgrade_unresolved").await
    else {
        return;
    };
    let failure_at = Utc::now() - Duration::seconds(60);
    let store = PgExecutionStore::new(database.pool.clone());

    let websocket_affinity = "e".repeat(64);
    seed_websocket_transport_failure(
        &database.pool,
        "req_websocket_control_failure",
        &websocket_affinity,
        failure_at,
    )
    .await;
    seed_transport_recovery_request(
        &database.pool,
        "req_websocket_control_success",
        &websocket_affinity,
        failure_at + Duration::seconds(4),
    )
    .await;
    ExecutionStore::finalize_model_request(
        &store,
        successful_core_finalization("req_websocket_control_success"),
    )
    .await
    .expect("finalize successful WebSocket control request");

    let late_affinity = "f".repeat(64);
    seed_websocket_transport_failure(
        &database.pool,
        "req_websocket_late_failure",
        &late_affinity,
        failure_at,
    )
    .await;
    seed_transport_recovery_request(
        &database.pool,
        "req_http_late_success",
        &late_affinity,
        failure_at + Duration::seconds(31),
    )
    .await;
    let mut late_recovery = successful_core_finalization("req_http_late_success");
    late_recovery.upstream_transport = Some("http_sse".to_owned());
    ExecutionStore::finalize_model_request(&store, late_recovery)
        .await
        .expect("finalize late HTTP request");

    type UnresolvedTransportFailure = (String, Option<String>, Option<chrono::DateTime<Utc>>, i32);
    let unresolved: Vec<UnresolvedTransportFailure> = sqlx::query_as(
        "select id, recovery_request_id, recovered_at, recovery_attempt_count
               from model_requests
              where id in ('req_websocket_control_failure', 'req_websocket_late_failure')
              order by id",
    )
    .fetch_all(&database.pool)
    .await
    .expect("load unresolved transport failures");
    assert_eq!(unresolved.len(), 2);
    assert!(
        unresolved
            .iter()
            .all(|row| row.1.is_none() && row.2.is_none() && row.3 == 0)
    );
    database.close().await;
}

#[tokio::test]
async fn failed_or_late_new_chain_should_leave_continuation_failure_unresolved() {
    let Some(database) = TestDatabase::create("execution_continuation_unresolved").await else {
        return;
    };
    let affinity_hash = "c".repeat(64);
    let failure_completed_at = Utc::now() - Duration::seconds(60);
    seed_continuation_failure(
        &database.pool,
        "req_continuation_unresolved",
        &affinity_hash,
        failure_completed_at,
    )
    .await;
    seed_recovery_request(
        &database.pool,
        "req_continuation_retry_failed",
        &affinity_hash,
        failure_completed_at + Duration::seconds(3),
    )
    .await;

    let store = PgExecutionStore::new(database.pool.clone());
    let mut failed = successful_core_finalization("req_continuation_retry_failed");
    failed.outcome = ExecutionOutcome::Failed;
    failed.client_status_code = Some(502);
    failed.upstream_status_code = Some(500);
    failed.error = Some(GatewayError::new(
        GatewayErrorKind::UpstreamUnavailable,
        "recovery request failed",
    ));
    ExecutionStore::finalize_model_request(&store, failed)
        .await
        .expect("finalize failed recovery request");

    let after_failed: (Option<String>, Option<chrono::DateTime<Utc>>, i32) = sqlx::query_as(
        "select recovery_request_id, recovered_at, recovery_attempt_count
           from model_requests where id = 'req_continuation_unresolved'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load unresolved continuation failure");
    assert_eq!(after_failed, (None, None, 1));

    seed_recovery_request(
        &database.pool,
        "req_continuation_retry_late",
        &affinity_hash,
        failure_completed_at + Duration::seconds(31),
    )
    .await;
    ExecutionStore::finalize_model_request(
        &store,
        successful_core_finalization("req_continuation_retry_late"),
    )
    .await
    .expect("finalize late recovery request");

    let after_late: (Option<String>, Option<chrono::DateTime<Utc>>, i32) = sqlx::query_as(
        "select recovery_request_id, recovered_at, recovery_attempt_count
           from model_requests where id = 'req_continuation_unresolved'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load late unresolved continuation failure");
    assert_eq!(after_late, (None, None, 1));
    database.close().await;
}

async fn seed_continuation_failure(
    pool: &sqlx::PgPool,
    id: &str,
    affinity_hash: &str,
    completed_at: chrono::DateTime<Utc>,
) {
    seed_running_request(pool, id)
        .await
        .expect("seed continuation failure request");
    sqlx::query(
        "update model_requests
            set client_api_key_ref = 'key_continuation', client_transport = 'websocket',
                continuation_affinity_hash = $2,
                continuation_previous_response_id_hash = $3,
                continuation_requested = true,
                outcome = 'failed', error_kind = 'continuation_recovery_required',
                continuation_unavailable_reason = 'reused_connection_lost',
                started_at = $4 - interval '1 second', deadline_at = $4,
                completed_at = $4
          where id = $1",
    )
    .bind(id)
    .bind(affinity_hash)
    .bind("b".repeat(64))
    .bind(completed_at)
    .execute(pool)
    .await
    .expect("finalize seeded continuation failure");
}

async fn seed_recovery_request(
    pool: &sqlx::PgPool,
    id: &str,
    affinity_hash: &str,
    started_at: chrono::DateTime<Utc>,
) {
    seed_running_request(pool, id)
        .await
        .expect("seed continuation recovery request");
    sqlx::query(
        "update model_requests
            set client_api_key_ref = 'key_continuation', client_transport = 'websocket',
                continuation_affinity_hash = $2, continuation_requested = false,
                started_at = $3, deadline_at = $3 + interval '1 minute'
          where id = $1",
    )
    .bind(id)
    .bind(affinity_hash)
    .bind(started_at)
    .execute(pool)
    .await
    .expect("prepare seeded recovery request");
}

async fn seed_websocket_transport_failure(
    pool: &sqlx::PgPool,
    id: &str,
    affinity_hash: &str,
    completed_at: chrono::DateTime<Utc>,
) {
    seed_running_request(pool, id)
        .await
        .expect("seed WebSocket transport failure request");
    sqlx::query(
        "update model_requests
            set client_api_key_ref = 'key_transport_recovery',
                client_transport = 'http_sse',
                continuation_affinity_hash = $2,
                attempt_count = 1, upstream_send_state = 'ambiguous',
                upstream_transport = 'websocket', outcome = 'failed',
                error_kind = 'upstream_unavailable',
                provider_error_code = 'websocket_close_1000',
                started_at = $3 - interval '1 second', deadline_at = $3,
                completed_at = $3
          where id = $1",
    )
    .bind(id)
    .bind(affinity_hash)
    .bind(completed_at)
    .execute(pool)
    .await
    .expect("finalize seeded WebSocket transport failure");
}

async fn seed_transport_recovery_request(
    pool: &sqlx::PgPool,
    id: &str,
    affinity_hash: &str,
    started_at: chrono::DateTime<Utc>,
) {
    seed_running_request(pool, id)
        .await
        .expect("seed transport recovery request");
    sqlx::query(
        "update model_requests
            set client_api_key_ref = 'key_transport_recovery',
                client_transport = 'http_sse',
                continuation_affinity_hash = $2,
                started_at = $3, deadline_at = $3 + interval '1 minute'
          where id = $1",
    )
    .bind(id)
    .bind(affinity_hash)
    .bind(started_at)
    .execute(pool)
    .await
    .expect("prepare seeded transport recovery request");
}

async fn seed_running_request(pool: &sqlx::PgPool, id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, config_revision, protocol, operation, endpoint,
           client_transport, requested_model_id, provider_kind, provider_account_ref, cost_source,
           started_at, deadline_at,
           routing_scope, routing_group_refs, routing_group_names_snapshot
         ) values ($1, 'key_status', 1, 'openai_responses', 'generate', '/v1/responses',
           'http_json', 'status-model', 'openai', 'acct_status', 'unavailable', now(), now() + interval '1 minute',
           'all', '{}'::text[], '[]'::jsonb)",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn diagnostic_trace_is_finalized_atomically_and_available_for_failed_request_details() {
    let Some(database) = TestDatabase::create("diagnostic_trace").await else {
        return;
    };
    seed_running_request(&database.pool, "req_diagnostic_failed")
        .await
        .unwrap();
    let store = PgExecutionStore::new(database.pool.clone());
    let mut finalization = successful_core_finalization("req_diagnostic_failed");
    finalization.outcome = ExecutionOutcome::Failed;
    finalization.client_status_code = Some(502);
    let diagnostic = gateway_core::error::ProviderDiagnostic::new(
        "OpenAI WebSocket receive idle timeout after 300s",
    )
    .with_classification("receive", "receive_idle_timeout");
    let provider_error = gateway_core::error::ProviderError::new(
        gateway_core::error::ProviderErrorKind::Timeout,
        UpstreamSendState::Ambiguous,
    )
    .with_diagnostic(diagnostic);
    finalization.error = Some(GatewayError::from_provider(&provider_error));
    let trace = serde_json::json!({"schemaVersion": 1, "requestId": "req_diagnostic_failed", "events": [
        {"attemptIndex": 1, "stage": "attempt.failed", "data": {"diagnostic": {
            "stage": "receive", "code": "receive_idle_timeout", "message": "OpenAI WebSocket receive idle timeout after 300s"
        }}},
    ]});
    finalization.diagnostic_trace_json = Some(trace.to_string());
    ExecutionStore::finalize_model_request(&store, finalization)
        .await
        .unwrap();
    let persisted: (String, serde_json::Value, String) = sqlx::query_as(
        "select outcome, diagnostic_trace_json, error_message from model_requests where id = 'req_diagnostic_failed'"
    ).fetch_one(&database.pool).await.unwrap();
    assert_eq!(
        persisted,
        (
            "failed".to_owned(),
            trace.clone(),
            "OpenAI WebSocket receive idle timeout after 300s".to_owned()
        )
    );
    let repository = super::observability_repository(&database.pool);
    let detail = repository
        .usage_record_detail("req_diagnostic_failed", None)
        .await
        .unwrap();
    assert_eq!(detail.trace, Some(trace));
    assert!(detail.related_requests.is_empty());
    database.close().await;
}

#[tokio::test]
async fn entry_rejection_is_observed_without_creating_a_model_request() {
    let Some(database) = TestDatabase::create("entry_rejection").await else {
        return;
    };
    sqlx::query("insert into client_api_keys(id,name,key,owner_user_id,created_at,updated_at) values('key_entry_rejected','entry-test','sk_' || repeat('a',43),'test-owner',now(),now())").execute(&database.pool).await.unwrap();
    let store = PgExecutionStore::new(database.pool.clone());
    store
        .record_entry_rejection(gateway_core::engine::EntryRejection {
            metadata: gateway_core::engine::execution::ExecutionRequestMetadata {
                protocol: "openai".into(),
                endpoint: "/v1/responses".into(),
                transport: gateway_core::engine::execution::ClientTransport::HttpSse,
                stream: true,
                client_ip: Some("127.0.0.1".parse().unwrap()),
                user_agent: Some("fixture-agent".into()),
                previous_response_id: None,
            },
            requested_model: Some(gateway_core::routing::PublicModelId::new("test-model").unwrap()),
            routing_group_refs: vec!["test-group".into()],
            request_id: ModelRequestId::new("req_entry_rejected").unwrap(),
            client_key_id: gateway_core::policy::ClientApiKeyId::new("key_entry_rejected").unwrap(),
            error: GatewayError::new(GatewayErrorKind::RateLimited, "request exceeds user limits"),
            latency: std::time::Duration::from_millis(12),
        })
        .await
        .unwrap();
    let (request_id, component, kind, message): (Option<String>, String, String, String) =
        sqlx::query_as("select model_request_id, component, failure_kind, message from ops_events")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert!(request_id.is_none());
    assert_eq!(component, "request_entry");
    assert_eq!(kind, GatewayErrorKind::RateLimited.as_str());
    assert_eq!(message, "request exceeds user limits");
    let context: serde_json::Value = sqlx::query_scalar("select entry_context from ops_events")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(context["requestId"], "req_entry_rejected");
    assert_eq!(context["clientKeyId"], "key_entry_rejected");
    assert_eq!(context["requestedModel"], "test-model");
    assert_eq!(context["clientStatusCode"], 429);
    let count: i64 = sqlx::query_scalar("select count(*) from model_requests")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(context["userId"], "test-owner");
    // Deleting a key cannot erase attribution or leak this event to another user.
    sqlx::query("delete from client_api_keys where id='key_entry_rejected'")
        .execute(&database.pool)
        .await
        .unwrap();
    for (user, expected) in [("test-owner", 1), ("another-user", 0)] {
        let page = ObservabilityRepository::list_ops_errors(
            &observability_repository(&database.pool),
            gateway_store::postgres::OpsErrorQuery {
                range: ObservabilityRange::new(
                    Utc::now() - Duration::minutes(1),
                    Utc::now() + Duration::minutes(1),
                )
                .unwrap(),
                filter: gateway_store::postgres::OpsErrorFilter {
                    user_id: Some(user.into()),
                    client_api_key_ref: Some("key_entry_rejected".into()),
                    request_id: Some("req_entry_rejected".into()),
                    group_id: Some("test-group".into()),
                    model: Some("test-model".into()),
                    client_transport: Some("http_sse".into()),
                    status_code: Some(429),
                    ..Default::default()
                },
                current_page: 1,
                page_size: ObservabilityPageSize::new(10).unwrap(),
            },
        )
        .await
        .unwrap();
        assert_eq!(page.total, expected);
        assert_eq!(page.items.len() as u64, expected);
        if let Some(error) = page.items.first() {
            assert_eq!(error.requested_model_id.as_deref(), Some("test-model"));
            assert_eq!(error.client_ip.as_deref(), Some("127.0.0.1"));
            assert_eq!(error.client_status_code, Some(429));
            assert_eq!(error.upstream_status_code, None);
        }
    }
    database.close().await;
}

#[tokio::test]
async fn recovery_renewal_respects_deadlines_and_never_creates_or_revives_records() {
    let Some(database) = TestDatabase::create("execution_recovery_renewal").await else {
        return;
    };
    let store = PgExecutionStore::new(database.pool.clone());
    let ttl = std::time::Duration::from_secs(120);
    assert!(
        store
            .renew_model_request("req_absent", None, ttl)
            .await
            .unwrap()
    );
    let count: i64 = sqlx::query_scalar("select count(*) from model_requests")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    seed_running_request(&database.pool, "req_live")
        .await
        .unwrap();
    assert!(
        store
            .renew_model_request("req_live", None, ttl)
            .await
            .unwrap()
    );
    let first = recovery_expiry(&database.pool, "req_live").await;
    assert!(first > Utc::now() + Duration::seconds(110));
    assert!(
        store
            .renew_model_request("req_live", None, std::time::Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert_eq!(recovery_expiry(&database.pool, "req_live").await, first);
    let deadline = Utc::now() + Duration::seconds(30);
    assert!(
        store
            .renew_model_request("req_live", Some(deadline), ttl)
            .await
            .unwrap()
    );
    assert_eq!(
        recovery_expiry(&database.pool, "req_live")
            .await
            .timestamp_micros(),
        deadline.timestamp_micros()
    );
    assert!(
        !store
            .renew_model_request("req_live", Some(Utc::now() - Duration::seconds(1)), ttl)
            .await
            .unwrap()
    );
    for invalid in [
        std::time::Duration::ZERO,
        std::time::Duration::from_nanos(1),
        std::time::Duration::MAX,
    ] {
        assert!(
            store
                .renew_model_request("req_live", None, invalid)
                .await
                .is_err()
        );
    }
    ExecutionStore::finalize_model_request(&store, successful_core_finalization("req_live"))
        .await
        .unwrap();
    let finalized = recovery_expiry(&database.pool, "req_live").await;
    assert!(
        !store
            .renew_model_request("req_live", None, ttl)
            .await
            .unwrap()
    );
    assert_eq!(recovery_expiry(&database.pool, "req_live").await, finalized);
    seed_running_request(&database.pool, "req_expired")
        .await
        .unwrap();
    sqlx::query("update model_requests set started_at = now() - interval '2 seconds', deadline_at = now() - interval '1 second' where id = 'req_expired'").execute(&database.pool).await.unwrap();
    assert!(
        !store
            .renew_model_request("req_expired", None, ttl)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .recover_expired_model_requests(Utc::now())
            .await
            .unwrap()
            .requests,
        1
    );
    assert!(
        !store
            .renew_model_request("req_expired", None, ttl)
            .await
            .unwrap()
    );
    database.close().await;
}

#[tokio::test]
async fn recovery_guard_handles_delayed_creation_and_crash_expiry() {
    let Some(database) = TestDatabase::create("execution_recovery_guard").await else {
        return;
    };
    let store = PgExecutionStore::new(database.pool.clone());
    let request = ModelRequestId::new("req_delayed").unwrap();
    let guard = store.maintain_request(&request, None, std::time::Duration::from_millis(900));
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from model_requests")
            .fetch_one(&database.pool)
            .await
            .unwrap(),
        0
    );
    seed_running_request(&database.pool, request.as_str())
        .await
        .unwrap();
    sqlx::query(
        "update model_requests set deadline_at = now() + interval '600 milliseconds' where id = $1",
    )
    .bind(request.as_str())
    .execute(&database.pool)
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
    assert_eq!(
        store
            .recover_expired_model_requests(Utc::now())
            .await
            .unwrap()
            .requests,
        0
    );
    assert!(recovery_expiry(&database.pool, request.as_str()).await > Utc::now());
    drop(guard);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    assert_eq!(
        store
            .recover_expired_model_requests(Utc::now())
            .await
            .unwrap()
            .requests,
        1
    );
    database.close().await;
}

#[tokio::test]
async fn recovery_guard_stops_at_explicit_execution_deadline() {
    let Some(database) = TestDatabase::create("execution_recovery_deadline").await else {
        return;
    };
    let store = PgExecutionStore::new(database.pool.clone());
    seed_running_request(&database.pool, "req_bounded")
        .await
        .unwrap();
    let deadline = std::time::SystemTime::now() + std::time::Duration::from_millis(900);
    let guard = store.maintain_request(
        &ModelRequestId::new("req_bounded").unwrap(),
        Some(deadline),
        std::time::Duration::from_millis(600),
    );
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert_eq!(
        recovery_expiry(&database.pool, "req_bounded")
            .await
            .timestamp_micros(),
        chrono::DateTime::<Utc>::from(deadline).timestamp_micros()
    );
    assert_eq!(
        store
            .recover_expired_model_requests(Utc::now())
            .await
            .unwrap()
            .requests,
        1
    );
    drop(guard);
    database.close().await;
}

async fn recovery_expiry(pool: &sqlx::PgPool, request: &str) -> chrono::DateTime<Utc> {
    sqlx::query_scalar("select deadline_at from model_requests where id = $1")
        .bind(request)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn recovery_renewal_does_not_revive_a_lease_that_expires_while_waiting_for_its_row_lock() {
    let Some(database) = TestDatabase::create("execution_recovery_lock").await else {
        return;
    };
    let store = PgExecutionStore::new(database.pool.clone());
    seed_running_request(&database.pool, "req_locked")
        .await
        .unwrap();
    let mut transaction = database.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("select id from model_requests where id = 'req_locked' for update")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    let renewal = tokio::spawn(async move {
        store
            .renew_model_request("req_locked", None, std::time::Duration::from_secs(60))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            // The holder and renewal occupy both pool connections. Observe from
            // the holder, and match its PID rather than another test's lock.
            let blocked: bool = sqlx::query_scalar("select exists(select 1 from pg_stat_activity where $1 = any(pg_blocking_pids(pid)))")
                .bind(blocker_pid)
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
            if blocked { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("renewal is waiting on the row lock");
    // Arm expiry only after the renewal's SQL statement has started waiting.
    // A statement-start timestamp would then incorrectly revive this lease.
    let expiry: chrono::DateTime<Utc> = sqlx::query_scalar("update model_requests set deadline_at = clock_timestamp() + interval '1 second' where id = 'req_locked' returning deadline_at")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    transaction.commit().await.unwrap();
    assert!(!renewal.await.unwrap().unwrap());
    assert_eq!(recovery_expiry(&database.pool, "req_locked").await, expiry);
    database.close().await;
}

#[tokio::test]
async fn entry_migration_recovers_only_recorded_context_and_preserves_historical_billing() {
    let Some(db) = TestDatabase::create_at_version("entry_migration", 31).await else {
        return;
    };
    sqlx::query("insert into client_api_keys(id,name,key,owner_user_id,created_at,updated_at) values('legacy-key','legacy','sk_' || repeat('b',43),'test-owner',now(),now())").execute(&db.pool).await.unwrap();
    for (id,message) in [("valid",json!({"requestId":"req_old","clientKeyId":"legacy-key","message":"request exceeds user limits"}).to_string()),("malformed","{not-json".into())] {
        sqlx::query("insert into ops_events(id,component,operation,level,failure_kind,message,created_at) values($1,'request_entry','reject','warning','rate_limited',$2,now())").bind(id).bind(message).execute(&db.pool).await.unwrap();
    }
    seed_running_request(&db.pool, "req_old_bill")
        .await
        .unwrap();
    sqlx::query("update model_requests set cost_source='calculated',cost_amount=1.25,cost_currency='USD',billing_multiplier=2.5 where id='req_old_bill'").execute(&db.pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr where id='req_old_bill'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let mut after: serde_json::Value = sqlx::query_scalar("select to_jsonb(mr) - 'billing_multiplier_scope' from model_requests mr where id='req_old_bill'").fetch_one(&db.pool).await.unwrap();
    for field in ["parent_request_id", "initiating_plugin_instance_id"] {
        assert_eq!(
            after.as_object_mut().unwrap().remove(field),
            Some(serde_json::Value::Null)
        );
    }
    assert_eq!(before, after);
    let rows: Vec<(String, Option<serde_json::Value>, String, Option<String>)> = sqlx::query_as(
        "select id,entry_context,message,upstream_send_state from ops_events order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows[0].0, "malformed");
    assert!(rows[0].1.is_none());
    assert_eq!(rows[0].2, "{not-json");
    let context = rows[1].1.as_ref().unwrap();
    assert_eq!(context["requestId"], "req_old");
    assert_eq!(context["userId"], "test-owner");
    assert!(context.get("requestedModel").is_none());
    assert_eq!(rows[1].2, "request exceeds user limits");
    assert_eq!(rows[1].3.as_deref(), Some("not_sent"));
    db.close().await;
}
