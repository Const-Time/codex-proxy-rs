use chrono::{DateTime, Duration, Utc};
use gateway_store::postgres::{
    ClientAdmissionRecentRequest, ClientAdmissionRecovery, ClientAdmissionRecoveryRepository,
    ClientAdmissionRunningRequest, PgClientAdmissionRecoveryRepository,
};
use sqlx::PgPool;

use super::TestDatabase;

#[tokio::test]
async fn recovery_loads_precise_window_and_running_request_facts() {
    let Some(database) = TestDatabase::create("admission_recovery").await else {
        return;
    };
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
        .expect("current time is representable at PostgreSQL precision");
    let window_started_at = now - Duration::seconds(60);
    seed_request(
        &database.pool,
        "old-running",
        now - Duration::seconds(120),
        now + Duration::seconds(30),
        "running",
    )
    .await;
    seed_request(
        &database.pool,
        "old-complete",
        now - Duration::seconds(90),
        now - Duration::seconds(30),
        "succeeded",
    )
    .await;
    seed_request(
        &database.pool,
        "recent-complete",
        now - Duration::seconds(20),
        now + Duration::seconds(10),
        "succeeded",
    )
    .await;
    seed_request(
        &database.pool,
        "recent-running",
        now - Duration::seconds(10),
        now + Duration::seconds(40),
        "running",
    )
    .await;

    let repository = PgClientAdmissionRecoveryRepository::new(database.pool.clone());
    let actual = repository
        .load_client_admission_recovery(window_started_at)
        .await
        .expect("load precise admission recovery facts");
    let expected = vec![ClientAdmissionRecovery {
        client_api_key_ref: "key-recovery".to_owned(),
        recent_requests: vec![
            ClientAdmissionRecentRequest {
                model_request_id: "recent-complete".to_owned(),
                started_at: now - Duration::seconds(20),
            },
            ClientAdmissionRecentRequest {
                model_request_id: "recent-running".to_owned(),
                started_at: now - Duration::seconds(10),
            },
        ],
        running_requests: vec![
            ClientAdmissionRunningRequest {
                model_request_id: "old-running".to_owned(),
                deadline_at: now + Duration::seconds(30),
            },
            ClientAdmissionRunningRequest {
                model_request_id: "recent-running".to_owned(),
                deadline_at: now + Duration::seconds(40),
            },
        ],
    }];
    assert_eq!(actual, expected);

    database.close().await;
}

async fn seed_request(
    pool: &PgPool,
    id: &str,
    started_at: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
    outcome: &str,
) {
    let completed_at = (outcome != "running").then_some(started_at + Duration::seconds(1));
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, config_revision, protocol, operation, endpoint,
           client_transport, requested_model_id, outcome,
           started_at, deadline_at, completed_at,
           routing_scope, routing_group_refs, routing_group_names_snapshot
         ) values (
           $1, 'key-recovery', 1, 'openai', 'responses', '/v1/responses',
           'http_sse', 'coding', $2, $3, $4, $5,
           'all', '{}'::text[], '[]'::jsonb
         )",
    )
    .bind(id)
    .bind(outcome)
    .bind(started_at)
    .bind(deadline_at)
    .bind(completed_at)
    .execute(pool)
    .await
    .expect("seed model request recovery fact");
}

#[tokio::test]
async fn recovery_restores_one_user_bucket_across_multiple_keys() {
    let Some(db) = TestDatabase::create("user_admission_recovery").await else {
        return;
    };
    let now = Utc::now();
    for id in ["user-key-a", "user-key-b"] {
        seed_request(
            &db.pool,
            id,
            now - Duration::seconds(5),
            now + Duration::seconds(55),
            "running",
        )
        .await;
        sqlx::query("update model_requests set client_api_key_ref = $1, user_id = 'test-owner' where id = $1").bind(id).execute(&db.pool).await.unwrap();
    }
    let facts = PgClientAdmissionRecoveryRepository::new(db.pool.clone())
        .load_client_admission_recovery(now - Duration::seconds(60))
        .await
        .unwrap();
    let user_scope = gateway_core::policy::ClientApiKeyId::user_admission("test-owner");
    let user = facts
        .iter()
        .find(|fact| fact.client_api_key_ref == user_scope.as_str())
        .unwrap();
    assert_eq!(facts.len(), 3);
    assert_eq!(user.running_requests.len(), 2);
    assert_eq!(user.recent_requests.len(), 2);
    db.close().await;
}
