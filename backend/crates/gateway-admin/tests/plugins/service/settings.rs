use async_trait::async_trait;
use gateway_admin::{
    model::{MutationActor, MutationContext, Revision, pricing::*, settings::*},
    plugins::service::{Origin, PluginServiceAuthority},
    ports::store::{AdminStoreResult, SettingsStore},
};
use gateway_core::{
    engine::middleware::service as core,
    lifecycle::{CancellationToken, Deadline},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Store {
    mutations: Mutex<Vec<MutationContext>>,
    waiting: AtomicBool,
    entered: tokio::sync::Notify,
    finish: tokio::sync::Notify,
    done: AtomicBool,
}
#[async_trait]
impl SettingsStore for Store {
    async fn load_pricing(&self) -> AdminStoreResult<StoredPricing> {
        Ok(Default::default())
    }
    async fn sync_pricing(
        &self,
        _: SyncPricing,
        _: &MutationContext,
    ) -> AdminStoreResult<Option<Revision>> {
        unreachable!()
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
    async fn replace_runtime_settings(
        &self,
        _: ReplaceRuntimeSettings,
        _: &MutationContext,
    ) -> AdminStoreResult<RuntimeSettings> {
        unreachable!()
    }
    async fn admin_api_key_exists(&self) -> AdminStoreResult<bool> {
        Ok(!self.mutations.lock().unwrap().is_empty())
    }
    async fn replace_admin_api_key(
        &self,
        _: AdminApiKey,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        self.mutations.lock().unwrap().push(context.clone());
        self.entered.notify_one();
        if self.waiting.load(Ordering::SeqCst) {
            self.finish.notified().await;
        }
        self.done.store(true, Ordering::SeqCst);
        Ok(AdminApiKeyMutation {
            config_revision: Revision::new(2).unwrap(),
            exists: true,
        })
    }
    async fn delete_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        self.mutations.lock().unwrap().push(context.clone());
        Ok(AdminApiKeyMutation {
            config_revision: Revision::new(3).unwrap(),
            exists: false,
        })
    }
}
async fn dispatcher(
    store: Arc<Store>,
    access: gateway_admin::model::plugins::PluginAccess,
    cancellation: CancellationToken,
) -> Arc<dyn core::Dispatcher> {
    let services = crate::use_case::AdminHarness::new()
        .settings(store)
        .build()
        .await;
    let origin = Origin {
        registry: super::super::http::registry().await,
        request_id: "host-request".into(),
        call_id: "host-call".into(),
        cancellation,
        deadline_at: Deadline::default(),
        call_scope: Default::default(),
        dispatcher: None,
    };
    services.service_settings().plugin_dispatcher(
        origin,
        PluginServiceAuthority::new(&super::record(), access).unwrap(),
    )
}
fn input() -> serde_json::Value {
    serde_json::json!({"actor":{"admin_session":{"admin_user_id":"forged-user"}},"request_id":"forged-request"})
}
#[tokio::test]
async fn plugin_service_mutation_uses_bound_generation_and_original_request_not_wire_actor() {
    let store = Arc::new(Store::default());
    let host = dispatcher(store.clone(), Default::default(), Default::default()).await;
    let value = host
        .call(
            "plugin_fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            input(),
        )
        .await
        .unwrap();
    assert!(value["key"].as_str().unwrap().starts_with("admin-"));
    host.call(
        "plugin_fixture".into(),
        "settings.delete_admin_api_key".into(),
        serde_json::json!({"actor":"system","request_id":"forged"}),
    )
    .await
    .unwrap();
    assert_eq!(
        *store.mutations.lock().unwrap(),
        vec![
            MutationContext {
                actor: MutationActor::Plugin {
                    instance_id: "plugin_fixture".into(),
                    revision: 7.try_into().unwrap()
                },
                request_id: "host-request".into()
            };
            2
        ]
    );
}
#[tokio::test]
async fn plugin_service_rejects_other_instance_cancelled_origin_and_revoked_generation_before_write()
 {
    let store = Arc::new(Store::default());
    let access = gateway_admin::model::plugins::PluginAccess::default();
    let cancellation = CancellationToken::new();
    let host = dispatcher(store.clone(), access.clone(), cancellation.clone()).await;
    assert_eq!(
        host.call(
            "other".into(),
            "settings.regenerate_admin_api_key".into(),
            input()
        )
        .await
        .unwrap_err()
        .kind,
        "forbidden"
    );
    cancellation.cancel();
    assert_eq!(
        host.call(
            "plugin_fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            input()
        )
        .await
        .unwrap_err()
        .kind,
        "unavailable"
    );
    access.revoke();
    assert_eq!(
        host.call(
            "plugin_fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            input()
        )
        .await
        .unwrap_err()
        .kind,
        "forbidden"
    );
    assert!(store.mutations.lock().unwrap().is_empty());
}
#[tokio::test]
async fn scoped_plugin_service_keeps_identity_and_links_original_cancellation() {
    let store = Arc::new(Store::default());
    let host = dispatcher(store.clone(), Default::default(), Default::default()).await;
    let cancellation = CancellationToken::new();
    let context = core::Context {
        operation: "fixture",
        request_id: "host-request".into(),
        call_id: "child".into(),
        parent_call_id: Some("host-call".into()),
        cancellation: cancellation.clone(),
        deadline_at: Deadline::default(),
        call_scope: Default::default(),
        dispatcher: None,
    };
    let scoped = host.scoped(&context);
    scoped
        .call(
            "plugin_fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            input(),
        )
        .await
        .unwrap();
    cancellation.cancel();
    assert!(
        scoped
            .call(
                "plugin_fixture".into(),
                "settings.regenerate_admin_api_key".into(),
                input()
            )
            .await
            .is_err()
    );
    assert_eq!(store.mutations.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn revocation_after_native_mutation_started_keeps_single_transaction_alive_and_reports_unknown()
 {
    let store = Arc::new(Store::default());
    store.waiting.store(true, Ordering::SeqCst);
    let access = gateway_admin::model::plugins::PluginAccess::default();
    let host = dispatcher(store.clone(), access.clone(), Default::default()).await;
    let task = tokio::spawn({
        let host = host.clone();
        async move {
            host.call(
                "plugin_fixture".into(),
                "settings.regenerate_admin_api_key".into(),
                input(),
            )
            .await
        }
    });
    store.entered.notified().await;
    access.revoke();
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind, "upstream_result_unknown");
    assert!(!store.done.load(Ordering::SeqCst));
    assert!(
        host.call(
            "plugin_fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            input()
        )
        .await
        .is_err()
    );
    store.finish.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !store.done.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.mutations.lock().unwrap().len(), 1);
}
