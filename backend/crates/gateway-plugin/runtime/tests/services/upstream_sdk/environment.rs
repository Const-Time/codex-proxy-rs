use crate::support::database::Database;
use async_trait::async_trait;
use gateway_admin::{
    AdminBundle,
    model::{AdminError, system::*},
    ports::{
        client_distribution::ClientDistributionResolver, pricing::PricingSource, proxy::ProxyProbe,
        system::*,
    },
};
use gateway_core::{
    account::OutboundProxy, engine::provider::ProviderRegistry, routing::ConfigRevision,
    runtime::SnapshotControl,
};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::{Arc, Mutex};

pub(crate) struct Environment {
    pub store: gateway_store::StoreBundle,
    pub core: gateway_core::CoreBundle,
    pub admin: AdminBundle,
    pub pool: PgPool,
    pub published: Arc<Mutex<Vec<ConfigRevision>>>,
    pub cache: tempfile::TempDir,
    database: Database,
}
struct ObservedSnapshot {
    inner: Arc<dyn SnapshotControl>,
    published: Arc<Mutex<Vec<ConfigRevision>>>,
}
impl SnapshotControl for ObservedSnapshot {
    fn publish_committed(&self, revision: ConfigRevision) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            self.inner.publish_committed(revision).await;
            self.published.lock().unwrap().push(revision);
        })
    }
}
impl Environment {
    pub async fn new() -> Option<Self> {
        let (database, database_url, redis_url) = Database::create().await?;
        let cache = tempfile::tempdir().unwrap();
        for name in [
            "CPR_DATABASE_URL",
            "CPR_REDIS_URL",
            "CPR_DATABASE_PASSWORD",
            "CPR_REDIS_PASSWORD",
        ] {
            assert!(
                std::env::var_os(name).is_none(),
                "SDK fixture must not use deployment overrides"
            );
        }
        fn connection(value: &str) -> serde_json::Value {
            let mut url = url::Url::parse(value).unwrap();
            let password = url.password().unwrap_or_default().to_owned();
            url.set_password(None).unwrap();
            json!({"url":url.as_str(), "password":password})
        }
        let mut config: gateway_store::StoreConfig = serde_json::from_value(
            json!({"database":connection(&database_url),"redis":connection(&redis_url)}),
        )
        .unwrap();
        config.resolve_and_validate(cache.path()).unwrap();
        let store = gateway_store::initialize(config).await.unwrap();
        let mut openai_config = provider_openai::OpenAiConfig::default();
        openai_config.resolve_and_validate(cache.path()).unwrap();
        let openai = provider_openai::initialize(openai_config, store.provider_ports())
            .await
            .unwrap();
        let xai_config = serde_json::from_value(json!({"wire_profile":{
            "client_identifier":"grok-shell", "client_version":"0.2.106", "client_mode":"headless", "target_os":"linux", "target_arch":"x86_64", "verified_at":"2026-07-21T00:00:00+08:00"
        }})).unwrap();
        let xai = provider_xai::initialize(xai_config, store.provider_ports())
            .await
            .unwrap();
        let core = gateway_core::initialize(
            store.core_ports(),
            ProviderRegistry::new([openai.core_provider(), xai.core_provider()]).unwrap(),
        )
        .await
        .unwrap();
        let published = Arc::new(Mutex::new(vec![]));
        let outside = Arc::new(NoExternalOperations);
        let admin = gateway_admin::initialize(
            gateway_admin::AdminConfig {
                session_ttl_minutes: 60,
                default_username: "sdk-fixture".into(),
                default_password: gateway_admin::InitialAdminPassword::new(
                    "sdk-business-fixture-password",
                ),
            },
            store.admin_ports(),
            gateway_admin::AdminRuntimePorts {
                providers: vec![openai.admin_provider(), xai.admin_provider()],
                snapshot: Arc::new(ObservedSnapshot {
                    inner: core.snapshot_control(),
                    published: published.clone(),
                }),
                probes: (core.account_probe(), outside.clone()),
                client_distribution: outside.clone(),
                system: outside.clone(),
                pricing_source: outside,
            },
        )
        .await
        .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        Some(Self {
            store,
            core,
            admin,
            pool,
            published,
            cache,
            database,
        })
    }
    pub async fn close(self) {
        let Self {
            pool,
            database,
            store,
            core,
            admin,
            cache,
            ..
        } = self;
        drop(admin);
        drop(core);
        drop(store);
        drop(cache);
        pool.close().await;
        database.close().await;
    }
}
// Real Admin/Core/Store services are composed above. These unused external
// operations must never make network requests or mutate the running app.
struct NoExternalOperations;
#[async_trait]
impl ProxyProbe for NoExternalOperations {
    async fn test(&self, _: &OutboundProxy) -> gateway_admin::model::proxies::ProxyTestResult {
        unreachable!()
    }
    async fn quality(
        &self,
        _: &OutboundProxy,
    ) -> gateway_admin::model::proxies::ProxyQualityReport {
        unreachable!()
    }
}
#[async_trait]
impl ClientDistributionResolver for NoExternalOperations {
    async fn resolve_codex_desktop_windows(
        &self,
        _: bool,
    ) -> gateway_admin::model::client_distribution::CodexDesktopWindowsDownloads {
        unreachable!()
    }
}
#[async_trait]
impl PricingSource for NoExternalOperations {
    async fn fetch(&self) -> Result<gateway_admin::model::pricing::PricingSyncPreview, AdminError> {
        unreachable!()
    }
}
#[async_trait]
impl SystemOperations for NoExternalOperations {
    async fn version(&self) -> Result<SystemVersion, SystemOperationError> {
        unreachable!()
    }
    async fn update_detail(&self, _: bool) -> Result<SystemUpdateDetail, SystemOperationError> {
        unreachable!()
    }
    fn update_events(&self) -> SystemUpdateEventStream {
        Box::pin(futures::stream::pending())
    }
    async fn perform_update(
        &self,
        _: Option<String>,
    ) -> Result<SystemOperationAccepted, SystemOperationError> {
        unreachable!()
    }
    async fn update_status(&self) -> Result<SystemUpdateStatus, SystemOperationError> {
        unreachable!()
    }
    async fn rollback(&self) -> Result<SystemOperationAccepted, SystemOperationError> {
        unreachable!()
    }
    async fn restart(&self) -> Result<SystemOperationAccepted, SystemOperationError> {
        unreachable!()
    }
}
