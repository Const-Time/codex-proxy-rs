use super::TestDatabase;

async fn seed_request(pool: &sqlx::PgPool) {
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, config_revision, protocol, operation, endpoint,
           client_transport, started_at, deadline_at, outcome, completed_at, routing_scope
         ) values (
           'req_integrity', 'deleted_key', 1, 'openai', 'responses', '/v1/responses',
           'http_sse', now(), now() + interval '1 hour', 'failed', now(), 'all'
         )",
    )
    .execute(pool)
    .await
    .expect("seed request without optional facts");
}

fn assert_check_rejected(error: &sqlx::Error) {
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514")
    );
}

#[tokio::test]
async fn model_access_migration_preserves_existing_accounts_and_defaults_to_unrestricted() {
    let Some(db) = TestDatabase::create_at_version("model_access_upgrade", 19).await else {
        return;
    };
    sqlx::query(
        "insert into provider_accounts (id, provider_kind, name, authentication_kind,
           provider_credentials_json, has_refresh_token, credential_observed_at,
           created_at, updated_at, enabled, weight, concurrency_limit, outbound_proxy_url)
         values ('acct_upgrade', 'openai', 'upgrade fixture', 'oauth', '{}', false,
           now(), now(), now(), false, 17, 3, 'http://127.0.0.1:1080')",
    )
    .execute(&db.pool)
    .await
    .expect("seed version 19 account");
    let before: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(a) from provider_accounts a where id='acct_upgrade'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR
        .run(&db.pool)
        .await
        .expect("upgrade to 20");
    let after: serde_json::Value = sqlx::query_scalar(
        "select to_jsonb(a) - 'model_access_json' from provider_accounts a where id='acct_upgrade'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        before, after,
        "migration must not alter existing account facts"
    );
    let policy: serde_json::Value = sqlx::query_scalar(
        "select model_access_json from provider_accounts where id='acct_upgrade'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(policy, serde_json::json!({"mode":"all","models":[]}));
    for invalid in [
        serde_json::json!({"mode":"all","models":["x"]}),
        serde_json::json!({"mode":"allowlist","models":[]}),
        serde_json::json!({"mode":"denylist","models":[12]}),
        serde_json::json!({"mode":"other","models":["x"]}),
        serde_json::json!({"mode":"all"}),
    ] {
        let error = sqlx::query(
            "update provider_accounts set model_access_json=$1 where id='acct_upgrade'",
        )
        .bind(sqlx::types::Json(invalid))
        .execute(&db.pool)
        .await
        .expect_err("invalid policy must be rejected");
        assert_check_rejected(&error);
    }
    db.close().await;
}

#[tokio::test]
async fn request_fact_groups_reject_partial_writes_and_accept_complete_observations() {
    let Some(db) = TestDatabase::create("fact_groups").await else {
        return;
    };
    seed_request(&db.pool).await;
    for assignments in [
        "cost_source = 'calculated', cost_amount = 1",
        "capacity_used_slots = 1",
        "capacity_total_slots = 2",
        "upstream_connection_id = 'connection'",
        "upstream_connection_id = 'connection', upstream_connection_exit_reason = 'peer_closed', upstream_connection_age_ms = 10",
        "recovery_request_id = 'req_recovery', recovered_at = completed_at, recovery_attempt_count = 1",
        "recovery_request_id = 'req_recovery', recovered_at = completed_at, recovery_attempt_count = 1, recovery_retry_delay_ms = 0",
        "outcome = 'running', completed_at = null, recovered_at = now(), recovery_request_id = 'req_recovery', recovery_attempt_count = 1, recovery_retry_delay_ms = 0, recovery_total_latency_ms = 1",
    ] {
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "update model_requests set {assignments} where id = 'req_integrity'"
        )))
        .execute(&db.pool)
        .await
        .expect_err(assignments);
        assert_check_rejected(&error);
    }
    sqlx::query(
        "update model_requests set cost_source = 'calculated', cost_amount = 1,
           cost_currency = 'USD', capacity_used_slots = 1, capacity_total_slots = 2,
           upstream_connection_id = 'connection', upstream_connection_exit_reason = 'peer_closed',
           upstream_connection_age_ms = 10, upstream_connection_idle_ms = 2,
           recovery_request_id = 'req_recovery', recovered_at = completed_at,
           recovery_attempt_count = 1, recovery_retry_delay_ms = 0, recovery_total_latency_ms = 1
         where id = 'req_integrity'",
    )
    .execute(&db.pool)
    .await
    .expect("complete observations satisfy presence and value constraints");
    db.close().await;
}

#[tokio::test]
async fn account_snapshots_are_required_before_live_foreign_keys_can_be_cleared() {
    let Some(db) = TestDatabase::create("account_fact_refs").await else {
        return;
    };
    seed_request(&db.pool).await;
    sqlx::query(
        "insert into provider_accounts (id, provider_kind, name, authentication_kind,
           provider_credentials_json, has_refresh_token, credential_observed_at, created_at, updated_at)
         values ('acct_integrity', 'openai', 'test', 'oauth', '{}', false, now(), now(), now())",
    ).execute(&db.pool).await.expect("seed account");
    let error = sqlx::query("update model_requests set provider_account_id = 'acct_integrity'")
        .execute(&db.pool)
        .await
        .expect_err("request requires snapshot ref");
    assert_check_rejected(&error);
    let error = sqlx::query(
        "insert into ops_events (id, level, component, operation, provider_account_id,
           failure_kind, message, created_at)
         values ('ops_missing_ref', 'error', 'probe', 'probe', 'acct_integrity', 'timeout', 'test', now())",
    ).execute(&db.pool).await.expect_err("event requires snapshot ref");
    assert_check_rejected(&error);
    sqlx::query(
        "update model_requests set provider_account_id = 'acct_integrity', provider_account_ref = 'acct_integrity'",
    ).execute(&db.pool).await.expect("write complete account reference");
    sqlx::query("delete from provider_accounts where id = 'acct_integrity'")
        .execute(&db.pool)
        .await
        .expect("delete live account through narrowed FK index");
    let refs: (Option<String>, String) = sqlx::query_as(
        "select provider_account_id, provider_account_ref from model_requests where id = 'req_integrity'",
    ).fetch_one(&db.pool).await.expect("read retained snapshot");
    assert_eq!(refs, (None, "acct_integrity".to_owned()));
    db.close().await;
}

#[tokio::test]
async fn audit_requires_canonical_admin_identity_and_retains_it_after_admin_deletion() {
    let Some(db) = TestDatabase::create("audit_identity").await else {
        return;
    };
    sqlx::raw_sql(
        "insert into users (id, username, password_hash, created_at, updated_at)
           values ('admin_test', 'admin_test', 'test_hash', now(), now());
         insert into admin_audit_events (id, actor_kind, actor_admin_user_id, actor_ref,
           action, entity_kind, entity_ref, created_at)
           values ('audit_identity', 'admin_session', 'admin_test', 'admin:admin_test',
             'update', 'settings', '1', now());",
    )
    .execute(&db.pool)
    .await
    .expect("seed audit event with canonical admin identity");
    let error = sqlx::query("update admin_audit_events set actor_ref = 'admin_test'")
        .execute(&db.pool)
        .await
        .expect_err("new writes require the canonical actor identity");
    assert_check_rejected(&error);
    sqlx::query("delete from users where id = 'admin_test'")
        .execute(&db.pool)
        .await
        .expect("audit retains identity when live admin is deleted");
    let identity: (Option<String>, String) =
        sqlx::query_as("select actor_admin_user_id, actor_ref from admin_audit_events")
            .fetch_one(&db.pool)
            .await
            .expect("read historical identity");
    assert_eq!(identity, (None, "admin:admin_test".to_owned()));
    db.close().await;
}

#[tokio::test]
async fn backup_completion_cannot_precede_its_start() {
    let Some(db) = TestDatabase::create("backup_time_order").await else {
        return;
    };
    let error = sqlx::query(
        "insert into backup_records (id, trigger_kind, status, object_key, size_bytes, sha256,
           started_at, completed_at, created_at, updated_at)
         values ('backup_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'manual', 'completed', 'test.dump',
           1, repeat('a', 64), now() + interval '10 seconds', now() + interval '5 seconds',
           now(), now() + interval '20 seconds')",
    )
    .execute(&db.pool)
    .await
    .expect_err("completed backup must not end before it starts");
    assert_check_rejected(&error);
    db.close().await;
}

#[tokio::test]
async fn response_model_migration_backfills_only_explicit_valid_openai_reports() {
    use serde_json::{Value, json};
    let Some(db) = TestDatabase::create_at_version("response_model_upgrade", 20).await else {
        return;
    };
    let fixtures = [
        (
            "valid",
            "openai",
            json!({"upstreamReportedModel":" gpt-returned "}),
            Some("gpt-returned"),
        ),
        ("missing", "openai", json!({}), None),
        (
            "null",
            "openai",
            json!({"upstreamReportedModel":null}),
            None,
        ),
        (
            "number",
            "openai",
            json!({"upstreamReportedModel":123}),
            None,
        ),
        (
            "empty",
            "openai",
            json!({"upstreamReportedModel":"   "}),
            None,
        ),
        (
            "control",
            "openai",
            json!({"upstreamReportedModel":"gpt\nsecret"}),
            None,
        ),
        (
            "long",
            "openai",
            json!({"upstreamReportedModel":"x".repeat(257)}),
            None,
        ),
        (
            "other",
            "xai",
            json!({"upstreamReportedModel":"grok-returned"}),
            None,
        ),
    ];
    for (id, provider, metadata, _) in &fixtures {
        sqlx::query("insert into model_requests (id, client_api_key_ref, config_revision, protocol, operation, endpoint, client_transport, started_at, deadline_at, outcome, completed_at, routing_scope, provider_kind, requested_model_id, upstream_model_id, provider_observation_json, cost_source, cost_amount, cost_currency) values ($1, 'deleted_key', 1, 'openai', 'responses', '/v1/responses', 'http_sse', now(), now() + interval '1 hour', 'failed', now(), 'all', $2, 'client-model', 'sent-model', $3, 'calculated', 1.23, 'USD')")
            .bind(id).bind(provider).bind(sqlx::types::Json(metadata)).execute(&db.pool).await.unwrap();
    }
    let before: Vec<Value> =
        sqlx::query_scalar("select to_jsonb(mr) from model_requests mr order by id")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR
        .run(&db.pool)
        .await
        .expect("upgrade to 21");
    let after: Vec<Value> = sqlx::query_scalar(
        "select to_jsonb(mr) - 'upstream_response_model' from model_requests mr order by id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        before, after,
        "migration must preserve costs, models and other historical facts"
    );
    for (id, _, _, expected) in fixtures {
        let model: Option<String> =
            sqlx::query_scalar("select upstream_response_model from model_requests where id=$1")
                .bind(id)
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(model.as_deref(), expected, "{id}");
    }
    db.close().await;
}
