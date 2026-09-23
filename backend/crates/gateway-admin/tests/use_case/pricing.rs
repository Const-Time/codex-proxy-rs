use async_trait::async_trait;
use gateway_admin::{
    model::{
        AdminError, MutationActor, MutationContext, Revision,
        pricing::{
            PricingSyncPolicy, PricingSyncPreview, StoredPricing, SyncPricing, UpdatePricing,
        },
        settings::{AdminApiKey, AdminApiKeyMutation, ReplaceRuntimeSettings, RuntimeSettings},
    },
    ports::{
        pricing::PricingSource,
        store::{AdminStoreResult, SettingsStore},
    },
};
use gateway_core::{
    lifecycle::CancellationToken,
    task::{WorkerContribution, WorkerKind, WorkerRunnable},
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Source {
    calls: AtomicUsize,
    fail: AtomicBool,
}
#[async_trait]
impl PricingSource for Source {
    async fn fetch(&self) -> Result<PricingSyncPreview, AdminError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(AdminError::bad_gateway("source unavailable"));
        }
        let price = json!({"multiplierBps":10000,"bands":{"standard":{"input":"2","output":"10","cacheRead":"0.2","cacheWrite":"2.5"}}});
        Ok(PricingSyncPreview {
            prices: serde_json::from_value(json!({"openai":{"builtin":price,"new":price}}))
                .unwrap(),
            skipped: vec![],
            source_hash: "a".repeat(64),
        })
    }
}

#[derive(Default)]
struct Store {
    stored: Mutex<StoredPricing>,
    commands: Mutex<Vec<SyncPricing>>,
}
#[async_trait]
impl SettingsStore for Store {
    async fn load_pricing(&self) -> AdminStoreResult<StoredPricing> {
        Ok(self.stored.lock().unwrap().clone())
    }
    async fn sync_pricing(
        &self,
        command: SyncPricing,
        context: &MutationContext,
    ) -> AdminStoreResult<Option<Revision>> {
        assert_eq!(context.actor, MutationActor::System);
        self.commands.lock().unwrap().push(command);
        Ok(None)
    }
    async fn update_pricing_sync_policy(
        &self,
        _: PricingSyncPolicy,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }
    async fn update_pricing(
        &self,
        _: UpdatePricing,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }
    async fn load_runtime_settings(&self) -> AdminStoreResult<RuntimeSettings> {
        unreachable!()
    }
    async fn admin_api_key_exists(&self) -> AdminStoreResult<bool> {
        unreachable!()
    }
    async fn replace_runtime_settings(
        &self,
        _: ReplaceRuntimeSettings,
        _: &MutationContext,
    ) -> AdminStoreResult<RuntimeSettings> {
        unreachable!()
    }
    async fn replace_admin_api_key(
        &self,
        _: AdminApiKey,
        _: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        unreachable!()
    }
    async fn delete_admin_api_key(
        &self,
        _: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        unreachable!()
    }
}

#[tokio::test]
async fn auto_new_models_never_reprices_builtins_and_failure_has_no_write() {
    let store = Arc::new(Store::default());
    let source = Arc::new(Source::default());
    let mut provider = super::UnavailableProvider::new("openai");
    provider.prices = source.fetch().await.unwrap().prices["openai"].clone();
    provider.prices.remove("new");
    let services = super::AdminHarness::new()
        .settings(store.clone())
        .pricing_source(source.clone())
        .provider(Arc::new(provider))
        .build()
        .await;
    services.settings().refresh_pricing().await.unwrap();
    let command = store.commands.lock().unwrap()[0].clone();
    assert!(!command.preview.prices["openai"].contains_key("builtin"));
    assert!(command.preview.prices["openai"].contains_key("new"));
    assert_eq!(command.automatic, Some(PricingSyncPolicy::default()));
    source.fail.store(true, Ordering::SeqCst);
    assert!(services.settings().refresh_pricing().await.is_err());
    assert_eq!(store.commands.lock().unwrap().len(), 1);
    store.stored.lock().unwrap().sync_policy.enabled = false;
    let calls = source.calls.load(Ordering::SeqCst);
    services.settings().refresh_pricing().await.unwrap();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        calls,
        "disabled sync does not access the network"
    );
}

#[tokio::test(start_paused = true)]
async fn pricing_worker_retries_after_failure_every_ten_minutes_and_stops_on_cancellation() {
    let store = Arc::new(Store::default());
    let source = Arc::new(Source::default());
    source.fail.store(true, Ordering::SeqCst);
    let mut bundle = super::AdminHarness::new()
        .settings(store.clone())
        .pricing_source(source.clone())
        .build_bundle()
        .await;
    let task = bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|c| {
            if let WorkerContribution::Registration(r) = c
                && r.id.kind() == WorkerKind::PricingSync
                && let WorkerRunnable::Daemon { task, .. } = r.runnable
            {
                Some(task)
            } else {
                None
            }
        })
        .expect("pricing daemon registered");
    let cancellation = CancellationToken::new();
    let child = cancellation.clone();
    let worker = tokio::spawn(async move { task.run(child).await });
    tokio::task::yield_now().await;
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert!(store.commands.lock().unwrap().is_empty());
    source.fail.store(false, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(599)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.commands.lock().unwrap().len(), 1);
    cancellation.cancel();
    worker.await.unwrap().unwrap();
}
