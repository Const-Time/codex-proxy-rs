use super::TestDatabase;
use gateway_admin::{
    model::{MutationActor, MutationContext},
    ports::{store::AdminStoreErrorKind, turn_state::TurnStateStore},
};
use gateway_store::postgres::PgTurnStateStore;

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
