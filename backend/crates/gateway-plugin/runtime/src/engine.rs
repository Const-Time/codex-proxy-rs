//! 本地安装能力适配；保留上游线协议，只开放已实现并可验证的扩展。
use crate::{PackageLimits, PreparedPackage, RpcLimits, RpcSession, ValidatedPackage};
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, plugins::*},
    ports::plugins::{PluginProcess, PluginRuntime, PluginStore},
};
use gateway_host::process::ProcessSupervisor;
use gateway_plugin_sdk::{
    Capability, Handshake, Manifest, Stage,
    call::{management::*, registration::Registration},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Composition supplies the API port after initialization. Weak ownership prevents
/// the router, plugin manager and process callbacks from retaining each other.
#[derive(Default)]
pub struct HttpDispatcher(
    std::sync::Mutex<
        Option<std::sync::Weak<dyn gateway_core::engine::middleware::http::Dispatcher>>,
    >,
);
impl HttpDispatcher {
    pub fn attach(&self, dispatcher: &Arc<dyn gateway_core::engine::middleware::http::Dispatcher>) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(dispatcher));
    }
    pub(crate) fn get(
        &self,
    ) -> Option<Arc<dyn gateway_core::engine::middleware::http::Dispatcher>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
    }
}

pub struct PluginEngine {
    credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    services: Option<gateway_core::engine::middleware::service::Source>,
    outbound: crate::outbound::SharedClient,
    http: Option<Arc<HttpDispatcher>>,
    models: Option<Arc<crate::models::ModelServices>>,
    quota: Option<Arc<dyn gateway_admin::ports::plugins::PluginQuotaService>>,
    resources: Option<Arc<dyn gateway_admin::ports::plugins::PluginResourceService>>,
    cache: PathBuf,
    version: semver::Version,
    store: Arc<dyn PluginStore>,
    processes: ProcessSupervisor,
    validators: Arc<tokio::sync::Semaphore>,
    logs: crate::callbacks::LogRegistry,
    data: Option<Arc<dyn gateway_admin::ports::plugins::PluginDataService>>,
}
impl PluginEngine {
    pub fn new(cache: PathBuf, version: semver::Version, store: Arc<dyn PluginStore>) -> Self {
        Self {
            credentials: None,
            services: None,
            outbound: Arc::default(),
            http: None,
            models: None,
            quota: None,
            resources: None,
            cache,
            version,
            store,
            processes: ProcessSupervisor::default(),
            validators: Arc::new(tokio::sync::Semaphore::new(2)),
            logs: crate::callbacks::LogRegistry::default(),
            data: None,
        }
    }
    pub fn with_services(
        mut self,
        services: gateway_core::engine::middleware::service::Source,
    ) -> Self {
        self.services = Some(services);
        self
    }
    pub fn with_http(mut self, http: Arc<HttpDispatcher>) -> Self {
        self.http = Some(http);
        self
    }
    pub fn with_models(
        mut self,
        execution: Arc<dyn gateway_core::engine::execution::ExecutionService>,
        keys: Arc<dyn gateway_admin::ClientKeyService>,
    ) -> Self {
        self.models = Some(crate::models::ModelServices::new(execution, keys));
        self
    }
    pub fn with_quota(
        mut self,
        service: Arc<dyn gateway_admin::ports::plugins::PluginQuotaService>,
    ) -> Self {
        self.quota = Some(service);
        self
    }
    pub fn with_resources(
        mut self,
        resources: Arc<dyn gateway_admin::ports::plugins::PluginResourceService>,
    ) -> Self {
        self.resources = Some(resources);
        self
    }
    pub fn with_data(
        mut self,
        data: Arc<dyn gateway_admin::ports::plugins::PluginDataService>,
    ) -> Self {
        self.data = Some(data);
        self
    }
    pub fn with_credentials(
        mut self,
        credentials: Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>,
    ) -> Self {
        self.credentials = Some(credentials);
        self
    }
    async fn package(
        &self,
        archive: Arc<[u8]>,
        digest: Option<String>,
    ) -> Result<Arc<ValidatedPackage>, AdminError> {
        let slot = self
            .validators
            .clone()
            .try_acquire_owned()
            .map_err(|_| AdminError::unavailable("插件校验繁忙，请稍后重试"))?;
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            ValidatedPackage::read(archive, digest.as_deref(), PackageLimits::default())
                .map(Arc::new)
                .map_err(|_| AdminError::invalid("插件包路径、摘要、清单或大小校验失败"))
        })
        .await
        .map_err(|_| AdminError::internal("插件校验任务失败"))?
    }
}
fn configuration(record: &PluginRecord, manifest: &Manifest) -> Result<Value, AdminError> {
    let mut config = record
        .configuration
        .as_object()
        .cloned()
        .ok_or_else(|| AdminError::invalid("插件配置必须是 JSON 对象"))?;
    if serde_json::to_vec(&record.configuration)
        .map_err(|_| AdminError::invalid("配置格式错误"))?
        .len()
        > 64 * 1024
        || record.secrets.values().map(String::len).sum::<usize>() > 64 * 1024
    {
        return Err(AdminError::invalid("插件配置过大"));
    }
    for field in &manifest.secret_fields {
        if config.contains_key(field) {
            return Err(AdminError::invalid("敏感字段必须通过独立的密钥配置提交"));
        }
    }
    for (field, secret) in &record.secrets {
        if !manifest.secret_fields.contains(field) {
            return Err(AdminError::invalid("存在插件未声明的敏感配置字段"));
        }
        config.insert(field.clone(), Value::String(secret.clone()));
    }
    let value = Value::Object(config);
    let schema = schema(&manifest.configuration_schema)?;
    for error in schema.iter_errors(&value) {
        if !record.enabled
            && matches!(
                error.kind(),
                jsonschema::error::ValidationErrorKind::Required { .. }
            )
        {
            continue;
        }
        return Err(AdminError::invalid(
            "插件配置不符合清单要求，请检查必填项与字段类型",
        ));
    }
    Ok(value)
}
pub(crate) fn schema(value: &Value) -> Result<jsonschema::Validator, AdminError> {
    jsonschema::options()
        .offline()
        .with_pattern_options(jsonschema::PatternOptions::fancy_regex().backtrack_limit(20_000))
        .build(value)
        .map_err(|_| AdminError::invalid("插件 schema 无效或引用外部资源"))
}
fn unsupported(manifest: &Manifest) -> Vec<String> {
    manifest
        .contributes
        .iter()
        .filter(|(cap, decl)| {
            !matches!(
                cap,
                Capability::Management
                    | Capability::FrontendAuthentication
                    | Capability::ModelCatalog
                    | Capability::ModelRouter
                    | Capability::Scheduler
                    | Capability::RetryPolicy
                    | Capability::Maintenance
                    | Capability::Observer
            ) || decl.version != 1
        })
        .map(|(cap, _)| cap.identifier().into())
        .collect()
}
#[async_trait]
impl PluginRuntime for PluginEngine {
    async fn prepare_state(
        &self,
        record: &PluginRecord,
        artifact: &PluginArtifact,
        process: Option<&Arc<dyn PluginProcess>>,
    ) -> Result<Option<PluginStateCommit>, AdminError> {
        let manifest: Manifest = serde_json::from_value(artifact.info.manifest.clone())
            .map_err(|_| AdminError::invalid("invalid plugin manifest"))?;
        tokio::time::timeout(
            Duration::from_secs(30),
            crate::state::prepare(record, &manifest, self.store.as_ref(), process),
        )
        .await
        .map_err(|_| AdminError::unavailable("plugin state migration deadline reached"))?
    }

    async fn shutdown(&self) {
        self.logs.drain().await;
    }
    fn observer_plan(
        &self,
        targets: Vec<gateway_admin::ports::plugins::PluginObserverTarget>,
        services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<Option<Arc<dyn gateway_core::engine::observation::RequestObserverPlan>>, AdminError>
    {
        crate::observation::Plan::compile(targets, services, cancellation)
    }
    fn contributes_model_catalog(&self, artifact: &PluginArtifact) -> Result<bool, AdminError> {
        let manifest: Manifest = serde_json::from_value(artifact.info.manifest.clone())
            .map_err(|_| AdminError::invalid("插件清单无效"))?;
        Ok(manifest.contributes.contains_key(&Capability::ModelCatalog))
    }
    fn describe(&self, mut info: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        let manifest: Manifest = serde_json::from_value(info.manifest.clone())
            .map_err(|_| AdminError::invalid("插件清单无效"))?;
        info.unsupported_capabilities = unsupported(&manifest);
        info.version_warning =
            (!manifest.engines.codex_proxy_rs.matches(&self.version)).then(|| {
                format!(
                    "插件声明宿主版本 {}，当前为 {}，兼容性未经保证",
                    manifest.engines.codex_proxy_rs, self.version
                )
            });
        Ok(info)
    }
    async fn inspect(&self, archive: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        let package = self.package(archive, None).await?;
        let manifest = package.manifest();
        manifest
            .package_for_platform(std::env::consts::OS, std::env::consts::ARCH)
            .map_err(|_| AdminError::invalid("插件包平台与当前宿主不匹配"))?;
        schema(&manifest.configuration_schema)?;
        for state in &manifest.state {
            schema(&state.schema)?;
        }
        Ok(PluginArtifact {
            info: self.describe(PluginArtifactInfo {
                sha256: package.digest().into(),
                plugin_id: manifest
                    .plugin_id()
                    .map_err(|_| AdminError::invalid("插件身份无效"))?,
                version: manifest.version.to_string(),
                display_name: manifest.display_name.clone(),
                unsupported_capabilities: vec![],
                version_warning: None,
                manifest: serde_json::to_value(manifest)
                    .map_err(|_| AdminError::internal("插件清单编码失败"))?,
            })?,
            archive: package.archive(),
        })
    }
    async fn validate(
        &self,
        record: &PluginRecord,
        artifact: &PluginArtifact,
        previous: Option<&PluginArtifact>,
    ) -> Result<(), AdminError> {
        let manifest: Manifest = serde_json::from_value(artifact.info.manifest.clone())
            .map_err(|_| AdminError::invalid("插件清单无效"))?;
        configuration(record, &manifest)?;
        if let Some(binding) = &record.observer {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::Observer)
                .is_none_or(|d| d.version != 1 || !d.stages.contains(&Stage::Observation))
            {
                return Err(AdminError::invalid("此制品未提供观察能力"));
            }
        }
        if let Some(binding) = &record.upstream_adapter {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::UpstreamAdapter)
                .is_none_or(|d| !matches!(d.version, 1 | 2) || !d.stages.contains(&Stage::Upstream))
            {
                return Err(AdminError::invalid("此制品未提供上游适配器能力"));
            }
        }
        if let Some(binding) = &record.retry_policy {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::RetryPolicy)
                .is_none_or(|d| d.version != 1 || !d.stages.contains(&Stage::Retry))
            {
                return Err(AdminError::invalid("此制品未提供重试策略能力"));
            }
        }
        if let Some(binding) = &record.account_scheduler {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::Scheduler)
                .is_none_or(|d| d.version != 1 || !d.stages.contains(&Stage::Scheduling))
            {
                return Err(AdminError::invalid("此制品未提供账号调度能力"));
            }
        }
        if let Some(binding) = &record.model_router {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::ModelRouter)
                .is_none_or(|d| d.version != 1 || !d.stages.contains(&Stage::Routing))
            {
                return Err(AdminError::invalid("此制品未提供模型路由能力"));
            }
        }
        if let Some(binding) = &record.middleware {
            binding.validate()?;
            let declaration = manifest
                .contributes
                .get(&Capability::Middleware)
                .ok_or_else(|| AdminError::invalid("此制品未提供中间件能力"))?;
            if declaration.version != 4
                || binding.mounts.iter().any(|mount| {
                    !declaration.stages.contains(&match mount {
                        PluginMiddlewareMount::Http => Stage::Http,
                        PluginMiddlewareMount::WebSocket => Stage::WebSocket,
                        PluginMiddlewareMount::Service => Stage::Service,
                        PluginMiddlewareMount::Request => Stage::Request,
                        PluginMiddlewareMount::Attempt => Stage::Attempt,
                    })
                })
            {
                return Err(AdminError::invalid("中间件版本或挂载位置与制品声明不符"));
            }
        }
        if let Some(binding) = &record.frontend_authentication {
            binding.validate()?;
            if manifest
                .contributes
                .get(&Capability::FrontendAuthentication)
                .is_none_or(|v| v.version != 1)
            {
                return Err(AdminError::invalid("此制品未提供入口认证能力"));
            }
        }
        if record.enabled && !unsupported(&manifest).is_empty() {
            return Err(AdminError::invalid(format!(
                "此宿主尚未开放插件能力：{}",
                unsupported(&manifest).join(", ")
            )));
        }
        if manifest.state.len() > 16
            || manifest.state.iter().any(|s| {
                s.schema_version > i32::MAX as u32
                    || s.maximum_records > 4096
                    || s.maximum_bytes > 4 * 1024 * 1024
                    || s.maximum_value_bytes > 256 * 1024
            })
        {
            return Err(AdminError::invalid("插件私有状态声明超过宿主限制"));
        }
        if let Some(previous) = previous
            && previous.info.plugin_id != artifact.info.plugin_id
        {
            return Err(AdminError::invalid("实例只能切换同一插件的版本"));
        }
        Ok(())
    }
    async fn prepare(
        &self,
        record: &PluginRecord,
        artifact: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        let package = self
            .package(artifact.archive, Some(record.artifact_sha256.clone()))
            .await?;
        let manifest = package.manifest().clone();
        let config = configuration(record, &manifest)?;
        let cache = self.cache.clone();
        let prepared = tokio::task::spawn_blocking(move || package.prepare(&cache).map(Arc::new))
            .await
            .map_err(|_| AdminError::internal("插件准备任务失败"))?
            .map_err(|_| AdminError::invalid("插件平台不兼容或制品不可执行"))?;
        let active = Arc::new(AtomicBool::new(false));
        let access = PluginAccess::default();
        let failed_access = access.clone();
        let ended_access = access.clone();
        let outbound = crate::outbound::Registry::new(self.outbound.clone(), access.cancellation());
        let services = crate::services::Registry::new(
            gateway_core::engine::middleware::service::Caller::new(
                record.id.clone(),
                std::num::NonZeroU64::new(record.revision)
                    .ok_or_else(|| AdminError::invalid("插件版本无效"))?,
                access.cancellation(),
            )
            .map_err(|_| AdminError::invalid("插件服务身份无效"))?,
            self.services.clone(),
        );
        let models = self.models.clone().map(|services| {
            crate::models::Models::new(
                services,
                access.cancellation(),
                self.http.clone(),
                self.data.clone(),
            )
        });
        let logs = self.logs.register(&manifest)?;
        let callbacks = Arc::new(
            crate::callbacks::Callbacks::new(
                self.store.clone(),
                record,
                &manifest,
                active.clone(),
                self.data.clone(),
                outbound.clone(),
                logs.clone(),
            )?
            .with_resources(self.resources.clone())
            .with_credentials(self.credentials.clone())
            .with_quota(self.quota.clone())
            .with_models(models.clone()),
        );
        let calls = callbacks.calls.clone();
        let middleware = crate::middleware::MiddlewareCallbacks::new(callbacks)
            .with_models(models.clone())
            .with_outbound(outbound.clone())
            .with_services(services.clone());
        let upstream_callbacks = crate::upstream_adapter::callbacks::Callbacks::new(
            middleware.clone(),
            self.outbound.clone(),
            models.clone(),
            outbound.clone(),
            services.clone(),
            self.credentials.clone(),
            calls.clone(),
        );
        let session = Arc::new(
            RpcSession::start_supervised(
                prepared.clone(),
                Handshake {
                    protocol_version: gateway_plugin_sdk::PROTOCOL_VERSION,
                    artifact_sha256: record.artifact_sha256.clone(),
                    plugin_id: manifest
                        .plugin_id()
                        .map_err(|_| AdminError::invalid("插件身份无效"))?,
                    instance_id: record.id.clone(),
                    generation: record.revision,
                    incarnation: uuid::Uuid::new_v4().to_string(),
                    configuration: config,
                    contributes: manifest.contributes.clone(),
                },
                RpcLimits::default(),
                &self.processes,
                services.wrap(outbound.wrap(upstream_callbacks.clone())),
                crate::rpc::RpcSessionLifecycle::new(
                    move |_| failed_access.revoke(),
                    move || ended_access.revoke(),
                ),
            )
            .await
            .map_err(|_| AdminError::unavailable("插件进程启动或协议握手失败"))?,
        );
        let reply = session
            .call(
                "plugin.register",
                session.context(Stage::Registration, Duration::from_secs(5)),
                json!({}),
                vec![],
            )
            .await
            .map_err(|_| AdminError::invalid("插件注册失败"))?;
        let registration: Registration = session
            .decode_response(Stage::Registration, reply.result)
            .map_err(|_| AdminError::invalid("插件注册结果无效"))?;
        if !reply.payload.is_empty() || registration.contributes != manifest.contributes {
            return Err(AdminError::invalid("插件注册结果与清单不符"));
        }
        let upstream_adapters = crate::upstream_adapter::register(&session, &manifest).await?;
        let upstream_adapters = upstream_adapters
            .into_iter()
            .map(|registered| {
                crate::upstream_adapter::PluginUpstreamAdapter::new(
                    registered,
                    manifest.contributes[&Capability::UpstreamAdapter]
                        .id
                        .clone(),
                    session.clone(),
                    upstream_callbacks.clone(),
                    access.cancellation(),
                    active.clone(),
                )
            })
            .collect();
        let model_aliases = crate::catalog::register(&session, &record.id).await?;
        let commands = crate::command_line::register(&session).await?;
        let authentication = session
            .capability_version(Capability::FrontendAuthentication)
            .is_some();
        if authentication {
            crate::authentication::register(&session).await?;
        }
        let mut management = ManagementRegistration {
            routes: vec![],
            resources: vec![],
            pages: vec![],
            callbacks: vec![],
        };
        if session.capability_version(Capability::Management).is_some() {
            let reply = session
                .call(
                    "management.register",
                    session.context(Stage::Registration, Duration::from_secs(5)),
                    json!({}),
                    vec![],
                )
                .await
                .map_err(|_| AdminError::invalid("插件管理注册失败"))?;
            if reply.result != json!({}) || reply.payload.len() > 64 * 1024 {
                return Err(AdminError::invalid("管理注册信封无效"));
            }
            management = serde_json::from_slice(&reply.payload)
                .map_err(|_| AdminError::invalid("管理声明无效"))?;
            crate::management_validation::registration(&management)?;
            let resource_bytes: usize = management
                .resources
                .iter()
                .filter_map(|r| prepared.package().resource(&r.path))
                .map(|b| b.len())
                .sum();
            if resource_bytes > 8 * 1024 * 1024 {
                return Err(AdminError::invalid("插件管理资源总大小超过 8 MiB"));
            }
            for resource in &management.resources {
                if prepared
                    .package()
                    .resource(&resource.path)
                    .is_none_or(|bytes| bytes.len() > 1024 * 1024)
                    || manifest
                        .resources
                        .get(&resource.path)
                        .is_none_or(|t| !crate::management_validation::content_type(t))
                {
                    return Err(AdminError::invalid("插件资源未声明、过大或类型无效"));
                }
            }
            for page in &management.pages {
                if manifest
                    .resources
                    .get(&page.entry)
                    .is_none_or(|t| t != "text/html" && t != "text/html; charset=utf-8")
                {
                    return Err(AdminError::invalid("插件页面入口必须是 HTML"));
                }
            }
        }
        Ok(Arc::new(LivePlugin {
            logs,
            credentials: self.credentials.clone(),
            commands,
            services,
            upstream_adapters,
            model_aliases,
            outbound,
            models,
            middleware,
            authentication,
            calls,
            session,
            access,
            package: prepared,
            active,
            management,
        }))
    }
}
struct LivePlugin {
    logs: Arc<crate::callbacks::PluginLog>,
    credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    commands: Vec<CommandDescriptor>,
    services: Arc<crate::services::Registry>,
    outbound: Arc<crate::outbound::Registry>,
    upstream_adapters: Vec<Arc<crate::upstream_adapter::PluginUpstreamAdapter>>,
    model_aliases: Vec<gateway_core::routing::ContributedModelAlias>,
    models: Option<Arc<crate::models::Models>>,
    middleware: Arc<crate::middleware::MiddlewareCallbacks>,
    authentication: bool,
    calls: Arc<crate::resources::InvocationCalls>,
    access: PluginAccess,
    session: Arc<RpcSession>,
    package: Arc<PreparedPackage>,
    active: Arc<AtomicBool>,
    management: ManagementRegistration,
}
#[async_trait]
impl PluginProcess for LivePlugin {
    async fn migrate_state(
        &self,
        batch: PluginStateMigrationBatch,
    ) -> Result<Vec<PluginStateMigrationChange>, AdminError> {
        if self.active.load(Ordering::Acquire)
            || self.session.diagnostic() != crate::rpc::RpcSessionDiagnostic::Ready
            || self.access.is_revoked()
        {
            return Err(AdminError::conflict("plugin state candidate ended"));
        }
        crate::state::invoke(&self.session, batch).await
    }

    fn command_help(&self, name: Option<&str>) -> Result<Option<String>, AdminError> {
        crate::command_line::help(&self.commands, self.services.caller().instance_id(), name)
    }
    async fn command(
        &self,
        request: PluginCommandRequest,
        factory: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
    ) -> Result<PluginCommandOutput, PluginCommandError> {
        if !self.active.load(Ordering::Acquire)
            || self.status() != "ready"
            || self.access.is_revoked()
        {
            return Err(AdminError::conflict("插件命令代次不可用").into());
        }
        crate::command_line::Executor {
            credentials: self.credentials.as_ref(),
            session: &self.session,
            commands: &self.commands,
            services: &self.services,
            outbound: &self.outbound,
            models: self.models.clone(),
            calls: &self.calls,
        }
        .invoke(request, factory)
        .await
    }
    fn observer_identity(&self) -> Option<gateway_admin::ports::plugins::PluginObserverIdentity> {
        if !self.active.load(Ordering::Acquire)
            || self.status() != "ready"
            || self.access.is_revoked()
            || self.session.capability_version(Capability::Observer) != Some(1)
            || !self
                .session
                .capability_supports_stage(Capability::Observer, Stage::Observation)
        {
            return None;
        }
        Some(gateway_admin::ports::plugins::PluginObserverIdentity {
            plugin_id: self.package.package().manifest().plugin_id().ok()?,
            instance_id: self.services.caller().instance_id().into(),
        })
    }
    async fn observe(
        &self,
        observation: gateway_admin::ports::plugins::PluginObservation,
        services: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<(), AdminError> {
        if self.observer_identity().is_none() {
            return Err(AdminError::unavailable("插件观察代次不可用"));
        }
        crate::observation::invoke(
            &self.session,
            &self.services,
            &self.outbound,
            &self.calls,
            observation,
            services,
            cancellation,
        )
        .await
    }
    fn supports_maintenance(&self) -> bool {
        self.session.capability_version(Capability::Maintenance) == Some(1)
    }
    async fn reconcile(
        &self,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<(), AdminError> {
        use gateway_admin::model::{MutationActor, MutationContext};
        use gateway_core::lifecycle::CancellationToken;
        if !self.supports_maintenance()
            || self.status() != "ready"
            || !self.active.load(Ordering::Acquire)
        {
            return Err(AdminError::unavailable("插件维护代次不可用"));
        }
        let cancellation = CancellationToken::linked([cancellation, self.access.cancellation()]);
        let mut context = self.session.context(
            Stage::Maintenance,
            self.session
                .maximum_call_timeout()
                .min(Duration::from_secs(30)),
        );
        context.request_id = Some(context.resource_scope_id.clone());
        let mut origin = self
            .services
            .root_origin(&context)
            .map_err(|_| AdminError::unavailable("维护服务发布不可用"))?;
        origin.context.cancellation = cancellation.clone();
        let effects = origin.context.call_scope.clone();
        let _services = self
            .services
            .bind_origin(&context, origin)
            .map_err(|_| AdminError::unavailable("维护服务作用域不可用"))?;
        self.outbound
            .bind(&context, cancellation.clone(), Some(effects.clone()))
            .map_err(|_| AdminError::unavailable("维护网络作用域不可用"))?;
        let _outbound = self.outbound.guard(&context);
        let caller = self.services.caller();
        let _caller = self
            .calls
            .enter(
                context.clone(),
                MutationContext {
                    actor: MutationActor::Plugin {
                        instance_id: caller.instance_id().into(),
                        revision: caller.revision(),
                    },
                    request_id: context.resource_scope_id.clone(),
                },
                None,
                cancellation.clone(),
                Some(effects),
            )
            .map_err(|_| AdminError::unavailable("维护调用容量已耗尽"))?;
        let reply = tokio::select! { biased;
            () = cancellation.cancelled() => return Err(AdminError::unavailable("插件维护已取消")),
            result = self.session.call("plugin.reconcile", context, json!({}), vec![]) =>
                result.map_err(|_| AdminError::bad_gateway("插件维护失败，结果可能已生效"))?,
        };
        if reply.result != json!({}) || !reply.payload.is_empty() {
            self.session.invalid_response(Stage::Maintenance);
            return Err(AdminError::bad_gateway("插件维护响应无效"));
        }
        Ok(())
    }

    fn upstream_adapter(
        &self,
        id: &str,
    ) -> Option<Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapter>> {
        self.upstream_adapters
            .iter()
            .find(|adapter| adapter.id() == id)
            .map(|adapter| {
                adapter.clone() as Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapter>
            })
    }

    fn upstream_adapters(&self) -> Vec<PluginUpstreamAdapterDeclaration> {
        self.upstream_adapters
            .iter()
            .map(|adapter| adapter.facts())
            .collect()
    }

    fn model_aliases(&self) -> Vec<gateway_core::routing::ContributedModelAlias> {
        self.model_aliases.clone()
    }
    async fn service_middleware(
        &self,
        mut context: gateway_core::engine::middleware::service::Context,
        input: serde_json::Value,
        next: gateway_core::engine::middleware::service::Next,
    ) -> Result<serde_json::Value, gateway_core::engine::middleware::service::Error> {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(
                gateway_core::engine::middleware::service::Error::unavailable("服务中间件不可用"),
            );
        }
        context.cancellation = gateway_core::lifecycle::CancellationToken::linked([
            context.cancellation,
            self.access.cancellation(),
        ]);
        self.middleware
            .invoke_service(self.session.clone(), context, input, next)
            .await
    }
    async fn websocket_middleware(
        &self,
        mut context: gateway_core::engine::middleware::websocket::Context,
        message: gateway_core::engine::middleware::websocket::Message,
        next: gateway_core::engine::middleware::websocket::Next,
    ) -> Result<
        Option<gateway_core::engine::middleware::websocket::Message>,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::middleware::MiddlewareError::Fault);
        }
        context.cancellation = gateway_core::lifecycle::CancellationToken::linked([
            context.cancellation,
            self.access.cancellation(),
        ]);
        self.middleware
            .invoke_websocket(self.session.clone(), context, message, next)
            .await
    }
    async fn http_middleware(
        &self,
        mut context: gateway_core::engine::middleware::http::Context,
        request: gateway_core::engine::middleware::http::Request,
        next: gateway_core::engine::middleware::http::Next,
    ) -> Result<
        gateway_core::engine::middleware::http::Response,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::middleware::MiddlewareError::Fault);
        }
        context.cancellation = gateway_core::lifecycle::CancellationToken::linked([
            context.cancellation,
            self.access.cancellation(),
        ]);
        self.middleware
            .invoke_http(self.session.clone(), context, request, next)
            .await
    }
    async fn retry_decision(
        &self,
        input: gateway_core::engine::retry_policy::RetryInput,
        timeout: Duration,
    ) -> Result<
        gateway_core::engine::retry_policy::RetryDecision,
        gateway_core::engine::retry_policy::RetryPolicyFault,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::retry_policy::RetryPolicyFault);
        }
        crate::retry_policy::invoke(
            &self.session,
            self.outbound.clone(),
            self.services.clone(),
            input,
            timeout,
        )
        .await
    }
    async fn schedule_account(
        &self,
        input: gateway_core::engine::account_scheduling::AccountScheduleInput,
    ) -> Result<
        gateway_core::engine::account_scheduling::AccountScheduleDecision,
        gateway_core::engine::account_scheduling::AccountSchedulingFault,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::account_scheduling::AccountSchedulingFault);
        }
        crate::account_scheduling::invoke(
            &self.session,
            self.models.clone(),
            self.outbound.clone(),
            self.services.clone(),
            input,
        )
        .await
    }
    async fn route_model(
        &self,
        input: gateway_core::engine::model_routing::ModelRouteInput,
    ) -> Result<
        gateway_core::engine::model_routing::ModelRouteDecision,
        gateway_core::engine::model_routing::ModelRoutingFault,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::model_routing::ModelRoutingFault);
        }
        crate::model_routing::invoke(
            &self.session,
            self.models.clone(),
            self.outbound.clone(),
            self.services.clone(),
            input,
        )
        .await
    }
    async fn middleware(
        &self,
        mut context: gateway_core::engine::middleware::MiddlewareContext,
        request: gateway_core::engine::middleware::MiddlewareRequest,
        next: gateway_core::engine::middleware::MiddlewareNext,
    ) -> Result<
        gateway_core::engine::middleware::MiddlewareResponse,
        gateway_core::engine::middleware::MiddlewareError,
    > {
        if self.status() != "ready" || self.access.is_revoked() {
            return Err(gateway_core::engine::middleware::MiddlewareError::Fault);
        }
        context.cancellation = gateway_core::lifecycle::CancellationToken::linked([
            context.cancellation,
            self.access.cancellation(),
        ]);
        self.middleware
            .invoke(self.session.clone(), context, request, next)
            .await
    }

    async fn authenticate(
        &self,
        authorization: String,
        origin: Option<gateway_core::engine::middleware::service::Origin>,
    ) -> Result<PluginAuthenticationResult, AdminError> {
        if !self.authentication || self.status() != "ready" || self.access.is_revoked() {
            return Err(AdminError::unavailable("认证器不可用"));
        }
        let result = crate::authentication::authenticate(
            &self.session,
            self.services.clone(),
            authorization,
            origin,
        )
        .await?;
        if self.access.is_revoked() || self.status() != "ready" {
            return Err(AdminError::unavailable("认证器已失效"));
        }
        Ok(result)
    }

    fn access(&self) -> PluginAccess {
        self.access.clone()
    }
    fn status(&self) -> &'static str {
        if self.active.load(Ordering::Acquire)
            && self.session.diagnostic() == crate::rpc::RpcSessionDiagnostic::Ready
        {
            "ready"
        } else {
            "failed"
        }
    }
    fn pages(&self) -> Vec<PluginPage> {
        if self.status() != "ready" {
            return vec![];
        }
        self.management
            .pages
            .iter()
            .map(|p| PluginPage {
                id: p.id.clone(),
                title: p.title.clone(),
                entry: p.entry.clone(),
            })
            .collect()
    }
    fn resources(&self) -> Vec<PluginResource> {
        if self.status() != "ready" {
            return vec![];
        }
        self.management
            .resources
            .iter()
            .map(|r| PluginResource {
                path: r.path.clone(),
                content_type: self.package.package().manifest().resources[&r.path].clone(),
                public: r.public,
            })
            .collect()
    }
    fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }
    fn revoke(&self) {
        self.access.revoke();
        self.calls.revoke();
        self.active.store(false, Ordering::Release);
        self.session.quiesce();
    }
    async fn stop(&self) {
        self.revoke();
        self.session.shutdown(Duration::from_secs(5)).await;
        self.calls.drain().await;
        self.logs.drain().await;
    }
    async fn resource(&self, path: &str) -> Result<PluginHttpResponse, AdminError> {
        if self.status() != "ready" {
            return Err(AdminError::conflict("插件页面已失效"));
        }
        if !self.management.resources.iter().any(|r| r.path == path) {
            return Err(AdminError::not_found("插件资源不存在"));
        }
        let body = self
            .package
            .package()
            .resource(path)
            .ok_or_else(|| AdminError::not_found("插件资源不存在"))?
            .to_vec();
        let content_type = self
            .package
            .package()
            .manifest()
            .resources
            .get(path)
            .cloned()
            .ok_or_else(|| AdminError::not_found("插件资源未声明"))?;
        Ok(PluginHttpResponse {
            status: 200,
            content_type,
            body,
        })
    }
    fn management_callback(&self, path: &str) -> Option<PluginCallbackDescriptor> {
        (self.status() == "ready"
            && !self.access.is_revoked()
            && self
                .management
                .callbacks
                .iter()
                .any(|callback| callback.path == path))
        .then(|| PluginCallbackDescriptor {
            incarnation: self.session.incarnation().into(),
        })
    }
    async fn callback(
        &self,
        call: PluginCallbackCall,
    ) -> Result<PluginCallbackResponse, AdminError> {
        if self.management_callback(&call.request.path).is_none() {
            return Err(AdminError::conflict("插件登录回调已失效"));
        }
        crate::management::invoke(
            &self.session,
            &self.calls,
            &self.access,
            &self.management,
            call,
        )
        .await
    }
    async fn handle(&self, request: PluginHttpRequest) -> Result<PluginHttpResponse, AdminError> {
        if self.status() != "ready" {
            return Err(AdminError::conflict("插件已失效"));
        }
        let route = self
            .management
            .routes
            .iter()
            .find(|r| r.method == request.method && r.path == request.path)
            .ok_or_else(|| AdminError::not_found("插件路由未注册"))?;
        if request.body.len() > 1024 * 1024 || request.query.len() > 8192 {
            return Err(AdminError::invalid("插件请求过大"));
        }
        if request
            .content_type
            .as_ref()
            .is_some_and(|t| !route.request_content_types.contains(t))
            || (!request.body.is_empty() && request.content_type.is_none())
        {
            return Err(AdminError::invalid("插件路由未声明此请求类型"));
        }
        let params = serde_json::to_value(ManagementRequest {
            method: request.method,
            path: request.path,
            query: request.query,
            content_type: request.content_type,
            headers: vec![],
        })
        .map_err(|_| AdminError::invalid("插件请求无效"))?;
        let mut context = self.session.context(
            Stage::Management,
            self.session
                .maximum_call_timeout()
                .min(Duration::from_secs(30)),
        );
        context.request_id = Some(request.actor.request_id.clone());
        let _caller = self
            .calls
            .enter(
                context.clone(),
                request.actor,
                request.invocation,
                self.access.cancellation(),
                None,
            )
            .map_err(|_| AdminError::unavailable("管理调用容量已耗尽"))?;
        let reply = self
            .session
            .call("management.handle", context, params, request.body)
            .await
            .map_err(|_| AdminError::bad_gateway("插件调用失败，结果可能已生效，请查询后再重试"))?;
        let response: ManagementResponse = self
            .session
            .decode_response(Stage::Management, reply.result)
            .map_err(|_| AdminError::bad_gateway("插件响应无效"))?;
        if !(200..=599).contains(&response.status)
            || !route
                .response_content_types
                .contains(&response.content_type)
            || !response.headers.is_empty()
            || reply.payload.len() > 1024 * 1024
        {
            self.session.invalid_response(Stage::Management);
            return Err(AdminError::bad_gateway(
                "插件响应状态、类型、头部或大小不受支持",
            ));
        }
        Ok(PluginHttpResponse {
            status: response.status,
            content_type: response.content_type,
            body: reply.payload,
        })
    }
}
