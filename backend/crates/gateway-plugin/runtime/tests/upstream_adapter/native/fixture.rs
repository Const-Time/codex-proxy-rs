use super::super::{binding::CandidateRuntime, execution::Server, *};
use crate::models::fixture::Core;
use chrono::Utc;
use gateway_admin::model::plugins::*;
use gateway_core::{
    account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy},
    engine::{
        continuation::PreviousResponseId,
        execution::*,
        provider::{Provider, ProviderRegistry},
    },
    error::GatewayError,
    metering::{ModelPriceOverride, PricingOverrides},
    operation::{GenerateRequest, Operation, OperationKind, ProtocolPayload},
    policy::{ClientApiKeyId, ClientPolicy, PlaintextClientApiKey, RateLimits},
    provider_ports::ProviderStorePorts,
    routing::*,
    runtime::RuntimeSnapshotHandle,
};
use provider_openai::credential::{
    CodexAccountProfile, CodexCredentialAdmin, CodexOAuthSecret, ImportCodexOAuthCredential,
};
use secrecy::SecretString;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::atomic::Ordering,
    time::Duration,
};

pub(super) struct Fixture {
    pub manager: PluginManager,
    pub installed: PluginView,
    pub core: Arc<Core>,
    pub execution: DefaultExecutionService,
    snapshot: RuntimeSnapshotHandle,
    pub ports: ProviderStorePorts,
    pub server: Server,
    pub account: ProviderAccountId,
    pub forbidden: ProviderAccountId,
    pub provider: &'static str,
    native_model: &'static str,
    pub continuation: Arc<gateway_store::redis::RedisNativeContinuationRepository>,
    _store: gateway_store::StoreBundle,
    _cache: tempfile::TempDir,
    database: Database,
}

// Real Provider stores and coordination, isolated from every other fixture/production database.
struct Database {
    admin: PgPool,
    name: String,
}
impl Database {
    async fn create() -> Option<(Self, String, String)> {
        fn env(name: &str) -> Option<String> {
            let value = std::env::var(name).ok();
            let ci = std::env::var("CI")
                .ok()
                .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
            assert!(
                value.is_some() || !ci,
                "{name} is required for native adapter integration tests in CI"
            );
            value
        }
        let database_url = env("CPR_TEST_DATABASE_URL")?;
        let redis_url = env("CPR_TEST_REDIS_URL")?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let name = format!("cpr_plugin_adapter_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("create database \"{name}\"")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = url::Url::parse(&database_url).unwrap();
        url.set_path(&format!("/{name}"));
        Some((Self { admin, name }, url.into(), redis_url))
    }
    async fn close(self) {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "drop database \"{}\" with (force)",
            self.name
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}

fn connection(value: &str) -> serde_json::Value {
    let mut url = url::Url::parse(value).unwrap();
    let password = url.password().unwrap_or_default().to_owned();
    url.set_password(None).unwrap();
    json!({"url":url.as_str(),"password":password})
}

fn snapshot(
    account: &ProviderAccountId,
    forbidden: &ProviderAccountId,
    revision: u64,
    enabled: bool,
    provider_name: &str,
    native_model: &str,
) -> RuntimeSnapshot {
    let provider = ProviderKind::new(provider_name).unwrap();
    let group = AccountGroupId::new("grp_00000000000000000000000000000001").unwrap();
    let other = AccountGroupId::new("grp_00000000000000000000000000000002").unwrap();
    let directory = Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([
        (
            account.clone(),
            RuntimeAccount::new(provider.clone(), BTreeSet::from([group.clone()])),
        ),
        (
            forbidden.clone(),
            RuntimeAccount::new(provider.clone(), BTreeSet::from([other])),
        ),
    ])));
    let scope = Arc::new(FrozenAccountScope::new(
        directory.clone(),
        ClientRoutingScope::restricted(
            vec![RoutingGroupSnapshot::new(group.clone(), "Allowed".into())],
            BTreeSet::from([group]),
            BTreeSet::from([provider.clone()]),
        )
        .unwrap(),
    ));
    // Distinct fast/standard rates and a local multiplier prove the original pricing owner is used.
    let price: ModelPriceOverride = serde_json::from_value(json!({"multiplierBps":15000,"bands":{
        "standard":{"input":"1","output":"2","cacheRead":"0","cacheWrite":"0"},
        "fast":{"input":"3","output":"4","cacheRead":"0","cacheWrite":"0"}
    }}))
    .unwrap();
    let prices: PricingOverrides = BTreeMap::from([(
        provider_name.into(),
        BTreeMap::from([(native_model.into(), price)]),
    )]);
    RuntimeSnapshot::new(
        ConfigRevision::new(revision).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(1).unwrap(),
            Duration::ZERO,
        ),
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider,
            UpstreamModelId::new(native_model).unwrap(),
            ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
                .with_upstream_feature_validation(),
        )],
        vec![
            ClientPolicy::new(
                ClientApiKeyId::new("key_alice").unwrap(),
                PlaintextClientApiKey::new("sk_native_fixture").unwrap(),
                scope.clone(),
                enabled,
                RateLimits::unlimited(),
            ),
            ClientPolicy::new(
                ClientApiKeyId::new("key_alice_other").unwrap(),
                PlaintextClientApiKey::new("sk_native_other").unwrap(),
                scope,
                true,
                RateLimits::unlimited(),
            ),
        ],
    )
    .unwrap()
    .with_account_directory(directory)
    .with_model_mappings(BTreeMap::from([(
        "public-model".into(),
        native_model.into(),
    )]))
    .with_pricing(Arc::new(prices))
}

impl Fixture {
    pub async fn new() -> Option<Self> {
        Self::for_provider("openai").await
    }
    pub async fn for_provider(provider: &'static str) -> Option<Self> {
        let native_model = match provider {
            "openai" => "gpt-5.4",
            "xai" => "grok-4.5",
            _ => panic!("unsupported native Provider fixture"),
        };
        let (database, database_url, redis_url) = Database::create().await?;
        let cache = tempfile::tempdir().unwrap();
        // Never let deployment environment variables redirect this test fixture.
        for name in [
            "CPR_DATABASE_URL",
            "CPR_REDIS_URL",
            "CPR_DATABASE_PASSWORD",
            "CPR_REDIS_PASSWORD",
        ] {
            assert!(
                std::env::var_os(name).is_none(),
                "native adapter fixture must not use deployment environment overrides"
            );
        }
        let mut config: gateway_store::StoreConfig = serde_json::from_value(json!({
            "database":connection(&database_url),"redis":connection(&redis_url)
        }))
        .unwrap();
        config.resolve_and_validate(cache.path()).unwrap();
        let store = gateway_store::initialize(config).await.unwrap();
        let ports = store.provider_ports();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let account = ProviderAccountId::new(format!("acct_allowed_{suffix}")).unwrap();
        let forbidden = ProviderAccountId::new(format!("acct_forbidden_{suffix}")).unwrap();
        for (id, access) in [
            (&account, "fixture-provider-secret"),
            (&forbidden, "fixture-forbidden-secret"),
        ] {
            if provider == "xai" {
                super::xai::seed(&ports, id, access).await;
                continue;
            }
            let now = Utc::now();
            let input = CodexCredentialAdmin
                .prepare_import(ImportCodexOAuthCredential {
                    account_id: id.to_string(),
                    name: id.to_string(),
                    enabled: true,
                    secret: CodexOAuthSecret {
                        access_token: SecretString::from(access),
                        refresh_token: None,
                        id_token: None,
                    },
                    verified_account: CodexAccountProfile {
                        oauth_subject: format!("subject-{id}"),
                        poid: None,
                        chatgpt_account_id: format!("chatgpt-{id}"),
                        chatgpt_user_id: format!("user-{id}"),
                        email: None,
                        plan_type: Some("pro".into()),
                        access_token_expires_at: Some(now + chrono::Duration::hours(1)),
                    },
                    next_refresh_at: None,
                })
                .unwrap();
            ports.accounts().create_account(input).await.unwrap();
        }
        let server = Server::new().await;
        let core_provider: Arc<dyn Provider> = if provider == "xai" {
            super::xai::provider(&ports)
        } else {
            let mut provider_config = provider_openai::OpenAiConfig::default();
            provider_config.api.base_url = server.url.clone();
            provider_config.resolve_and_validate(cache.path()).unwrap();
            provider_openai::initialize(provider_config, ports.clone())
                .await
                .unwrap()
                .core_provider()
        };
        let connection =
            redis::aio::ConnectionManager::new(redis::Client::open(redis_url).unwrap())
                .await
                .unwrap();
        let continuation = Arc::new(
            gateway_store::redis::RedisNativeContinuationRepository::new(
                connection,
                &format!("cpr-native-adapter-{suffix}"),
            )
            .unwrap(),
        );
        let core = Arc::new(Core::default());
        let snapshot = RuntimeSnapshotHandle::new(snapshot(
            &account,
            &forbidden,
            1,
            true,
            provider,
            native_model,
        ));
        let execution = DefaultExecutionService::new(
            snapshot.clone(),
            core.clone(),
            ProviderRegistry::new(vec![core_provider]).unwrap(),
            core.clone(),
            continuation.clone(),
            core.clone(),
        )
        .with_budget(core.clone());
        let plugin_store = Arc::new(MemoryStore::default());
        let engine = Arc::new(PluginEngine::new(
            cache.path().join("plugins"),
            "3.11.2".parse().unwrap(),
            plugin_store.clone(),
        ));
        let manager = PluginManager::new(plugin_store, Arc::new(CandidateRuntime(engine)));
        let artifact = manager
            .upload(upstream_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        command.configuration = json!({"adapter_url":server.url,"adapter_provider":provider});
        command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
            scope: PluginExecutionScope::All {},
        });
        let installed = manager.update(None, command, &actor()).await.unwrap();
        Some(Self {
            manager,
            installed,
            core,
            execution,
            snapshot,
            ports,
            server,
            account,
            forbidden,
            provider,
            native_model,
            continuation,
            _store: store,
            _cache: cache,
            database,
        })
    }
    pub async fn start(&self, behavior: &str) -> StartedExecution {
        self.start_with("sk_native_fixture", behavior, None)
            .await
            .unwrap()
    }
    pub async fn start_with(
        &self,
        key: &str,
        behavior: &str,
        previous: Option<&str>,
    ) -> Result<StartedExecution, GatewayError> {
        let client = self.execution.authenticate(key).unwrap();
        let prepared = self
            .execution
            .prepare_execution(client)
            .unwrap()
            .with_upstream_adapters(Arc::new(self.manager.middleware_registry().unwrap()));
        self.execution.start_prepared(prepared, PreparedExecutionRequest {
            public_model: PublicModelId::new("public-model").unwrap(),
            operation: Operation::Generate(GenerateRequest::from_protocol_payload(ProtocolPayload::json_object("openai",
                serde_json::from_value(json!({"model":"public-model","input":"fixture prompt","service_tier":"default", "behavior":behavior,
                    "fixture_account":self.account.as_str(),"fixture_credential_revision":1,"fixture_model":self.native_model,
                    "previous_response_id":previous,"expect_continuation":previous.is_some()})).unwrap()
            ).unwrap())),
            metadata: ExecutionRequestMetadata { protocol:"openai".into(), endpoint:"/v1/responses".into(), transport:ClientTransport::HttpJson, stream:false, client_ip:None, user_agent:None, previous_response_id:previous.map(PreviousResponseId::new) },
        }).await
    }
    pub async fn clean(&self, charges: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count = self.core.charges.lock().unwrap().len();
                if self.core.active.load(Ordering::SeqCst) == 0
                    && count == charges
                    && self.in_flight().await == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original Core settlement and admission release must finish");
    }
    pub fn revoke_key(&self) {
        self.snapshot.publish(snapshot(
            &self.account,
            &self.forbidden,
            2,
            false,
            self.provider,
            self.native_model,
        ));
        assert!(self.execution.authenticate("sk_native_fixture").is_err());
    }
    pub async fn in_flight(&self) -> u32 {
        let state = self
            .ports
            .leases()
            .load_state(
                &ClientApiKeyId::new("key_alice").unwrap(),
                &ProviderKind::new(self.provider).unwrap(),
                &[self.account.clone(), self.forbidden.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            state
                .signals()
                .get(&self.forbidden)
                .map_or(0, |s| s.in_flight),
            0
        );
        state
            .signals()
            .get(&self.account)
            .map_or(0, |s| s.in_flight)
    }
    pub async fn close(self) {
        self.manager.shutdown().await;
        let database = self.database;
        drop(self.execution);
        drop(self.ports);
        drop(self._store);
        drop(self.continuation);
        database.close().await;
    }
    pub async fn upgrade(&mut self) {
        let artifact = self
            .manager
            .upload(upstream_archive("1.0.1"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, self.installed.revision, true);
        command.configuration =
            json!({"adapter_url":self.server.url,"adapter_provider":self.provider});
        command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
            scope: PluginExecutionScope::All {},
        });
        self.installed = self
            .manager
            .update(Some(&self.installed.id), command, &actor())
            .await
            .unwrap();
    }
    pub async fn disable(&self) {
        let mut command = update(
            &self.installed.artifact.sha256,
            self.installed.revision,
            false,
        );
        command.configuration =
            json!({"adapter_url":self.server.url,"adapter_provider":self.provider});
        command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
            scope: PluginExecutionScope::All {},
        });
        self.manager
            .update(Some(&self.installed.id), command, &actor())
            .await
            .unwrap();
    }
}
