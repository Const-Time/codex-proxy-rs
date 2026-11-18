use super::*;
use gateway_core::account::AccountCatalogObservation;

#[tokio::test]
async fn model_catalogs_restore_grouped_documents_with_revision_plan_and_delete_fences() {
    let Some(db) = TestDatabase::create("model_catalog_fences").await else {
        return;
    };
    let repo = PgProviderAccountRepository::new(db.pool.clone());
    let provider = ProviderKind::new("openai").unwrap();
    for (id, user) in [
        ("acct_catalog_a", "catalog-a"),
        ("acct_catalog_b", "catalog-b"),
    ] {
        repo.insert_provider_account(account(id, user))
            .await
            .unwrap();
    }
    let mut observations = Vec::new();
    for id in ["acct_catalog_a", "acct_catalog_b"] {
        let id = ProviderAccountId::new(id).unwrap();
        let a = repo.get_account(&id).await.unwrap().unwrap();
        let observation = AccountCatalogObservation {
            account_id: id,
            revision: a.revision(),
            plan: a.plan_type().map(str::to_owned),
            etag: Some("\"catalog-v1\"".into()),
            observed_at: SystemTime::now(),
        };
        assert!(
            repo.save_account_catalog(&observation, b"validated-provider-document")
                .await
                .unwrap()
        );
        observations.push(observation);
    }
    let restored = repo.load_account_catalogs(&provider).await.unwrap();
    assert_eq!(restored.len(), 1, "identical payloads are transferred once");
    assert_eq!(restored[0].accounts.len(), 2);
    let mut stale = observations[0].clone();
    stale.observed_at -= Duration::from_secs(60);
    assert!(!repo.save_account_catalog(&stale, b"older").await.unwrap());
    sqlx::query("update provider_accounts set credential_revision=credential_revision+1 where id='acct_catalog_a'").execute(&db.pool).await.unwrap();
    assert!(
        !repo
            .save_account_catalog(&observations[0], b"late")
            .await
            .unwrap()
    );
    assert_eq!(
        repo.load_account_catalogs(&provider).await.unwrap()[0]
            .accounts
            .len(),
        1
    );
    sqlx::query("update provider_accounts set plan_type='changed-plan' where id='acct_catalog_b'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        repo.load_account_catalogs(&provider)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !repo
            .save_account_catalog(&observations[1], b"wrong-plan")
            .await
            .unwrap()
    );
    sqlx::query("delete from provider_accounts where id='acct_catalog_b'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        !repo
            .save_account_catalog(&observations[1], b"deleted")
            .await
            .unwrap()
    );
    let count: i64 = sqlx::query_scalar(
        "select count(*) from provider_account_model_catalogs where account_id='acct_catalog_b'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    db.close().await;
}
