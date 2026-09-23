use gateway_admin::model::{
    MutationActor, MutationContext,
    pricing::{PricingChange, UpdatePricing},
};
use gateway_core::metering::ModelPriceOverride;
use gateway_store::postgres::{PgRuntimeSnapshotRepository, RuntimeSnapshotRepository};
use serde_json::json;

use super::TestDatabase;

#[tokio::test]
async fn concurrent_price_edits_preserve_other_models_and_sync_preserves_manual_prices() {
    let Some(redis_url) = crate::support::test_env("CPR_TEST_REDIS_URL") else {
        return;
    };
    let Some(database) = TestDatabase::create("pricing").await else {
        return;
    };
    let mut database_url = url::Url::parse(
        &crate::support::test_env("CPR_TEST_DATABASE_URL").expect("test database URL"),
    )
    .unwrap();
    database_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={}", database.schema));
    let connection = |mut url: url::Url| {
        let password = url.password().expect("test password").to_owned();
        url.set_password(None).unwrap();
        json!({"url":url.as_str(), "password":password})
    };
    let mut config: gateway_store::StoreConfig = serde_json::from_value(json!({
        "database":connection(database_url),
        "redis":connection(url::Url::parse(&redis_url).unwrap()),
    }))
    .unwrap();
    let runtime = tempfile::tempdir().unwrap();
    config.resolve_and_validate(runtime.path()).unwrap();
    let bundle = gateway_store::initialize(config).await.unwrap();
    let settings = bundle.admin_ports().settings();
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "pricing-test".to_owned(),
    };
    let pricing: ModelPriceOverride = serde_json::from_value(json!({
        "multiplierBps":12500,
        "bands":{"standard":{"input":"2","output":"10","cacheRead":"0","cacheWrite":"1"}}
    }))
    .unwrap();
    let update = |model: &str, change| UpdatePricing {
        provider: "openai".to_owned(),
        models: vec![model.to_owned()],
        change,
    };
    let (first, second) = tokio::join!(
        settings.update_pricing(
            update("model-a", PricingChange::Replace(pricing.clone())),
            &context
        ),
        settings.update_pricing(
            update("model-b", PricingChange::Replace(pricing.clone())),
            &context
        ),
    );
    assert_ne!(first.unwrap(), second.unwrap());
    let stored = settings.load_pricing().await.unwrap();
    assert_eq!(stored.overrides["openai"].len(), 2);

    let mut source = stored.overrides.clone();
    for models in source.values_mut() {
        for price in models.values_mut() {
            price.multiplier_bps = 10000;
        }
    }
    source
        .get_mut("openai")
        .unwrap()
        .get_mut("model-a")
        .unwrap()
        .bands
        .get_mut("standard")
        .unwrap()
        .input = "9".to_owned().try_into().unwrap();
    settings
        .sync_pricing(
            gateway_admin::model::pricing::SyncPricing {
                preview: gateway_admin::model::pricing::PricingSyncPreview {
                    prices: source.clone(),
                    skipped: vec![],
                    source_hash: "a".repeat(64),
                },
                automatic: None,
            },
            &context,
        )
        .await
        .unwrap();
    let stored = settings.load_pricing().await.unwrap();
    assert_eq!(stored.overrides["openai"]["model-a"], pricing);
    assert!(stored.synced_at.is_some());
    for _ in 0..2 {
        settings
            .update_pricing(
                update("model-a", PricingChange::Multiplier(20000)),
                &context,
            )
            .await
            .unwrap();
    }
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone());
    let frozen = snapshot.load_runtime_snapshot().await.unwrap();
    assert_eq!(
        frozen.settings.pricing["openai"]["model-a"].multiplier_bps,
        20000
    );
    assert_eq!(
        frozen.settings.pricing["openai"]["model-a"].bands,
        pricing.bands
    );

    settings
        .update_pricing(update("model-a", PricingChange::Reset), &context)
        .await
        .unwrap();
    let restored = snapshot.load_runtime_snapshot().await.unwrap();
    assert_eq!(
        restored.settings.pricing["openai"]["model-a"],
        source["openai"]["model-a"]
    );
    assert_eq!(restored.settings.pricing["openai"]["model-b"], pricing);
    assert_eq!(
        frozen.settings.pricing["openai"]["model-a"].multiplier_bps,
        20000
    );
    let audits: i64 = sqlx::query_scalar("select count(*) from admin_audit_events where action in ('pricing.update', 'pricing.sync')")
        .fetch_one(&database.pool).await.unwrap();
    assert_eq!(audits, 6);

    use gateway_admin::model::pricing::{PricingSyncPolicy, PricingSyncPreview, SyncPricing};
    let sync = |prices, hash: char, automatic| SyncPricing {
        preview: PricingSyncPreview {
            prices,
            skipped: vec![],
            source_hash: hash.to_string().repeat(64),
        },
        automatic,
    };
    assert!(
        settings
            .sync_pricing(sync(source.clone(), 'a', None), &context)
            .await
            .unwrap()
            .is_none(),
        "unchanged source does not bump revision or append audit events"
    );
    let policy = PricingSyncPolicy::default();
    assert_eq!(settings.load_pricing().await.unwrap().sync_policy, policy);
    let mut incoming = source.clone();
    let models = incoming.get_mut("openai").unwrap();
    models.remove("model-b");
    models
        .get_mut("model-a")
        .unwrap()
        .bands
        .get_mut("standard")
        .unwrap()
        .input = "11".to_owned().try_into().unwrap();
    models.insert("model-c".to_owned(), models["model-a"].clone());
    settings
        .sync_pricing(sync(incoming.clone(), 'b', Some(policy)), &context)
        .await
        .unwrap();
    let after = settings.load_pricing().await.unwrap();
    assert_eq!(
        String::from(after.synced["openai"]["model-a"].bands["standard"].input),
        "9"
    );
    assert_eq!(
        String::from(after.synced["openai"]["model-c"].bands["standard"].input),
        "11"
    );
    assert!(
        after.synced["openai"].contains_key("model-b"),
        "deleted source entries retain their validated rates"
    );
    assert_eq!(after.overrides["openai"]["model-b"], pricing);

    settings
        .update_pricing_sync_policy(
            PricingSyncPolicy {
                enabled: false,
                update_existing: false,
            },
            &context,
        )
        .await
        .unwrap();
    assert!(
        settings
            .sync_pricing(sync(incoming.clone(), 'c', Some(policy)), &context)
            .await
            .unwrap()
            .is_none(),
        "disabling sync during a fetch prevents its pending commit"
    );
    let all = PricingSyncPolicy {
        enabled: true,
        update_existing: true,
    };
    settings
        .update_pricing_sync_policy(all, &context)
        .await
        .unwrap();
    assert!(
        settings
            .sync_pricing(sync(incoming.clone(), 'c', Some(policy)), &context)
            .await
            .unwrap()
            .is_none(),
        "changed policy invalidates the old cycle"
    );
    settings
        .sync_pricing(sync(incoming.clone(), 'c', Some(all)), &context)
        .await
        .unwrap();
    let after = settings.load_pricing().await.unwrap();
    assert_eq!(
        String::from(after.synced["openai"]["model-a"].bands["standard"].input),
        "11"
    );
    assert_eq!(after.overrides["openai"]["model-b"], pricing);
    assert_eq!(after.source_hash, Some("c".repeat(64)));
    assert!(
        settings
            .sync_pricing(sync(incoming, 'z', None), &context)
            .await
            .is_err()
    );
    assert_eq!(
        settings.load_pricing().await.unwrap(),
        after,
        "invalid sync cannot change any persisted price"
    );
    assert_eq!(
        frozen.settings.pricing["openai"]["model-a"].multiplier_bps,
        20000
    );
    drop(settings);
    drop(bundle);
    database.close().await;
}
