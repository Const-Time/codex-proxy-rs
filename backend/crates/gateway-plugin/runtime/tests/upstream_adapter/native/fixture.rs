use super::super::{binding::CandidateRuntime, execution::Server, *};
use crate::models::fixture::Core;
use crate::services::upstream_sdk::environment::Environment;
use chrono::Utc;
use gateway_admin::model::plugins::*;
use gateway_core::{
    account::{
        AccountSelectionPolicy, CredentialCasOutcome, CredentialCasUpdate, ProviderAccountId,
        ProviderAccountUpdate, RotationStrategy,
    },
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
    plugin_store: Arc<dyn PluginStore>,
    adapter_url: String,
    adapter_transport: &'static str,
    credential_revision: u64,
    pub env: Environment,
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
        Self::for_adapter(provider, None).await
    }
    pub async fn for_websocket(provider: &'static str, url: &str) -> Option<Self> {
        Self::for_adapter(provider, Some(url)).await
    }
    async fn for_adapter(provider: &'static str, websocket_url: Option<&str>) -> Option<Self> {
        let native_model = match provider {
            "openai" => "gpt-5.4",
            "xai" => "grok-4.5",
            _ => panic!("unsupported native Provider fixture"),
        };
        let env = Environment::new().await?;
        let ports = env.store.provider_ports();
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
            provider_config
                .resolve_and_validate(env.cache.path())
                .unwrap();
            provider_openai::initialize(provider_config, ports.clone())
                .await
                .unwrap()
                .core_provider()
        };
        let connection = redis::aio::ConnectionManager::new(
            redis::Client::open(env.redis_url.as_str()).unwrap(),
        )
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
        let plugin_store = env.store.plugin_store();
        let engine = Arc::new(
            PluginEngine::new(
                env.cache.path().join("plugins"),
                "3.11.2".parse().unwrap(),
                plugin_store.clone(),
            )
            .with_credentials(env.admin.services().plugin_credentials()),
        );
        let manager = PluginManager::new(plugin_store.clone(), Arc::new(CandidateRuntime(engine)));
        let artifact = manager
            .upload(upstream_archive("1.0.0"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, 0, true);
        let adapter_url = websocket_url.unwrap_or(&server.url).to_owned();
        let adapter_transport = if websocket_url.is_some() {
            "websocket"
        } else {
            "http_sse"
        };
        command.configuration = json!({"adapter_url":adapter_url,"adapter_transport":adapter_transport,"adapter_provider":provider});
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
            plugin_store,
            adapter_url,
            adapter_transport,
            credential_revision: 1,
            env,
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
        self.start_transport(key, behavior, previous, ClientTransport::HttpJson)
            .await
    }
    pub async fn start_transport(
        &self,
        key: &str,
        behavior: &str,
        previous: Option<&str>,
        transport: ClientTransport,
    ) -> Result<StartedExecution, GatewayError> {
        self.start_with_plan(key, behavior, previous, transport, None)
            .await
    }
    pub async fn start_with_adapter_plan(
        &self,
        behavior: &str,
        plan: Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapterPlan>,
    ) -> StartedExecution {
        self.start_with_plan(
            "sk_native_fixture",
            behavior,
            None,
            ClientTransport::HttpJson,
            Some(plan),
        )
        .await
        .unwrap()
    }
    async fn start_with_plan(
        &self,
        key: &str,
        behavior: &str,
        previous: Option<&str>,
        transport: ClientTransport,
        plan: Option<Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapterPlan>>,
    ) -> Result<StartedExecution, GatewayError> {
        let client = self.execution.authenticate(key).unwrap();
        let prepared = self
            .execution
            .prepare_execution(client)
            .unwrap()
            .with_upstream_adapters(
                plan.unwrap_or_else(|| Arc::new(self.manager.middleware_registry().unwrap())),
            );
        self.execution.start_prepared(prepared, PreparedExecutionRequest {
            public_model: PublicModelId::new("public-model").unwrap(),
            operation: Operation::Generate(GenerateRequest::from_protocol_payload(ProtocolPayload::json_object("openai",
                serde_json::from_value(json!({"model":"public-model","input":"fixture prompt","service_tier":"default", "behavior":behavior,
                    "fixture_account":self.account.as_str(),"fixture_credential_revision":self.credential_revision,"fixture_model":self.native_model,
                    "fixture_forbidden":self.forbidden.as_str(), "fixture_provider":self.provider,
                    "previous_response_id":previous,"expect_continuation":previous.is_some()})).unwrap()
            ).unwrap())),
            metadata: ExecutionRequestMetadata { protocol:"openai".into(), endpoint:"/v1/responses".into(), transport, stream:transport==ClientTransport::WebSocket, client_ip:None, user_agent:None, previous_response_id:previous.map(PreviousResponseId::new) },
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
        drop(self.execution);
        drop(self.ports);
        drop(self.continuation);
        drop(self.manager);
        drop(self.plugin_store);
        self.env.close().await;
    }
    pub async fn upgrade(&mut self) {
        let artifact = self
            .manager
            .upload(upstream_archive("1.0.1"), &actor())
            .await
            .unwrap();
        let mut command = update(&artifact.sha256, self.installed.revision, true);
        command.configuration = json!({"adapter_url":self.adapter_url,"adapter_transport":self.adapter_transport,"adapter_provider":self.provider});
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
        command.configuration = json!({"adapter_url":self.adapter_url,"adapter_transport":self.adapter_transport,"adapter_provider":self.provider});
        command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
            scope: PluginExecutionScope::All {},
        });
        self.manager
            .update(Some(&self.installed.id), command, &actor())
            .await
            .unwrap();
    }
    pub async fn restart(&mut self) {
        self.manager.shutdown().await;
        let engine = Arc::new(
            PluginEngine::new(
                self.env.cache.path().join("plugins"),
                "3.11.2".parse().unwrap(),
                self.plugin_store.clone(),
            )
            .with_credentials(self.env.admin.services().plugin_credentials()),
        );
        self.manager = PluginManager::new(
            self.plugin_store.clone(),
            Arc::new(CandidateRuntime(engine)),
        );
        self.manager.restore().await.unwrap();
    }
    pub async fn advance_credential(&mut self) {
        self.advance_credential_with(|_| {}).await;
    }
    pub async fn advance_credential_with(
        &mut self,
        mutate: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
    ) -> gateway_core::account::CredentialRevision {
        let account_store = self.ports.accounts();
        let current = account_store
            .load_current_credential(&self.account)
            .await
            .unwrap();
        let profile = ProviderAccountUpdate {
            account_id: self.account.clone(),
            name: current.account.name().into(),
            email: current.account.email().map(str::to_owned),
            plan_type: current.account.plan_type().map(str::to_owned),
        };
        let mut material = current.credential.into_inner();
        mutate(&mut material);
        let update = CredentialCasUpdate::new(
            self.account.clone(),
            current.account.revision(),
            profile,
            gateway_core::account::PlaintextCredential::new(material),
            current.account.has_refresh_token(),
            current.account.access_token_expires_at(),
            current.account.next_refresh_at(),
        )
        .unwrap()
        .preserving_profile();
        let CredentialCasOutcome::Updated(revision) = account_store
            .compare_and_swap_credential(update)
            .await
            .unwrap()
        else {
            panic!("isolated credential CAS must succeed")
        };
        self.credential_revision = revision.get();
        revision
    }
}
