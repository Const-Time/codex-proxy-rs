//! 网关唯一组合根：加载包级配置并连接各 Bundle。

use std::sync::Arc;

use gateway_core::engine::provider::{ProviderRegistry, RegistryError};
use gateway_host::{ConfigError, HostConfig, LoadableConfig};
use serde::Deserialize;

const CONFIG_SCHEMA_VERSION: u32 = 1;

/// 顶层配置只组合各包拥有的配置段，不解释任何业务字段。
/// 各启动配置忽略未知字段，允许升级时保留旧配置；已知字段仍按类型和业务约束校验。
#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    schema_version: u32,
    host: HostConfig,
    store: gateway_store::StoreConfig,
    admin: gateway_admin::AdminConfig,
    api: gateway_api::ApiConfig,
    openai: provider_openai::OpenAiConfig,
    xai: provider_xai::XaiConfig,
}

impl LoadableConfig for GatewayConfig {
    const EXTERNAL_SECTIONS: &'static [&'static str] = &["services"];

    fn resolve_and_validate(&mut self, source_dir: &std::path::Path) -> Result<(), ConfigError> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::InvalidField("schema_version"));
        }
        self.api
            .resolve_and_validate(source_dir)
            .map_err(|_| ConfigError::InvalidField("api"))?;
        self.host
            .resolve_and_validate(source_dir, &self.api.asset_directory)?;
        let runtime_data_dir = self.host.runtime_data_dir().to_path_buf();
        self.store
            .resolve_and_validate(&runtime_data_dir)
            .map_err(|_| ConfigError::InvalidField("store"))?;
        self.admin
            .resolve_and_validate(source_dir)
            .map_err(|_| ConfigError::InvalidField("admin"))?;
        self.openai
            .resolve_and_validate(&runtime_data_dir)
            .map_err(|_| ConfigError::InvalidField("openai"))?;
        self.xai
            .resolve_and_validate(source_dir)
            .map_err(|_| ConfigError::InvalidField("xai"))?;
        Ok(())
    }
}

/// 按冻结顺序初始化全部 Bundle，并把进程阻塞权交给 Host。
pub async fn run() -> Result<(), BootstrapError> {
    let config = gateway_host::load_config::<GatewayConfig>()?;
    let GatewayConfig {
        schema_version: _,
        host,
        store,
        admin,
        api,
        openai,
        xai,
    } = config;

    let plugin_cache = host.runtime_data_dir().join("plugins");
    let plugin_version = semver::Version::parse(host.system_update.version.trim_start_matches('v'))
        .map_err(|_| ConfigError::InvalidField("host.system_update.version"))?;
    let host = gateway_host::initialize(host).await?;
    host.report_startup_ready("Host");
    let mut store = gateway_store::initialize(store).await?;
    host.report_startup_ready("Store");
    let provider_ports = store.provider_ports();
    let proxy_probe = host.proxy_probe(
        provider_openai::build_reqwest_client_with_custom_ca,
        Arc::new(provider_openai::transport::websocket::CodexProxyWebSocketProbe::default()),
    );
    let mut openai = provider_openai::initialize_with_state_probe(
        openai,
        provider_ports.clone(),
        store.turn_state_store(),
        proxy_probe.clone(),
    )
    .await?;
    host.report_startup_ready("OpenAI Provider");
    let mut xai = provider_xai::initialize(xai, provider_ports).await?;
    host.report_startup_ready("xAI Provider");
    let providers = ProviderRegistry::new([openai.core_provider(), xai.core_provider()])?;
    let mut core = gateway_core::initialize(store.core_ports(), providers).await?;
    host.report_startup_ready("Core");
    let mut admin = gateway_admin::initialize(
        admin,
        store.admin_ports(),
        gateway_admin::AdminRuntimePorts {
            providers: vec![openai.admin_provider(), xai.admin_provider()],
            snapshot: core.snapshot_control(),
            probes: (core.account_probe(), proxy_probe),
            client_distribution: host.client_distribution_resolver(),
            system: host.system_operations(),
            pricing_source: Arc::new(gateway_host::pricing::LiteLlmPricing::default()),
        },
    )
    .await?;
    host.report_startup_ready("Admin");
    let plugins = Arc::new_cyclic(
        |plugins: &std::sync::Weak<gateway_admin::plugins::PluginManager>| {
            gateway_admin::plugins::PluginManager::new(
                store.plugin_store(),
                Arc::new(
                    gateway_plugin_runtime::PluginEngine::new(
                        plugin_cache,
                        plugin_version,
                        store.plugin_store(),
                    )
                    .with_data(admin.services().plugin_data())
                    .with_resources(admin.services().plugin_resources())
                    .with_quota(admin.services().plugin_quota())
                    .with_models(
                        Arc::new(gateway_api::openai::middleware::PluginModelExecution::new(
                            core.execution_service(),
                            plugins.clone(),
                        )),
                        admin.services().client_keys(),
                    ),
                ),
            )
        },
    );
    plugins.restore().await?;
    host.report_startup_ready("Plugins");

    let mut probes = store.health_probes();
    probes.extend(core.health_probes());
    probes.push(host.logging_health_probe());
    let api = gateway_api::initialize(
        api,
        core.execution_service(),
        admin
            .services()
            .with_turn_state(openai.turn_state_service())
            .with_plugins(plugins.clone()),
        probes,
        host.worker_health(),
        host.connection_lifecycle(),
    )?;
    host.report_startup_ready("API");

    let mut plan = store.take_worker_contributions();
    plan.extend(core.take_worker_contributions());
    plan.extend(openai.take_worker_contributions());
    plan.extend(xai.take_worker_contributions());
    plan.extend(admin.take_worker_contributions());
    host.start_workers(plan, store.worker_leader_lease())?;
    host.report_startup_ready("Workers");
    let result = host.serve(api.router()).await;
    plugins.shutdown().await;
    result?;
    Ok(())
}

/// 组合根只保留包级错误分类，不展开内部实现或敏感配置。
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Config(#[from] gateway_host::ConfigError),
    #[error(transparent)]
    Host(#[from] gateway_host::HostError),
    #[error(transparent)]
    Store(#[from] gateway_store::StoreError),
    #[error(transparent)]
    OpenAi(#[from] provider_openai::OpenAiInitializeError),
    #[error(transparent)]
    Xai(#[from] provider_xai::XaiInitializeError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Core(#[from] gateway_core::CoreError),
    #[error(transparent)]
    Admin(#[from] gateway_admin::model::AdminError),
    #[error(transparent)]
    Api(#[from] gateway_api::ApiError),
}
