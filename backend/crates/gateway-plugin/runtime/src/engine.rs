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

pub struct PluginEngine {
    cache: PathBuf,
    version: semver::Version,
    store: Arc<dyn PluginStore>,
    processes: ProcessSupervisor,
    validators: Arc<tokio::sync::Semaphore>,
}
impl PluginEngine {
    pub fn new(cache: PathBuf, version: semver::Version, store: Arc<dyn PluginStore>) -> Self {
        Self {
            cache,
            version,
            store,
            processes: ProcessSupervisor::default(),
            validators: Arc::new(tokio::sync::Semaphore::new(2)),
        }
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
        .filter(|(cap, decl)| **cap != Capability::Management || decl.version != 1)
        .map(|(cap, _)| cap.identifier().into())
        .collect()
}
#[async_trait]
impl PluginRuntime for PluginEngine {
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
        if record.enabled && !unsupported(&manifest).is_empty() {
            return Err(AdminError::invalid(format!(
                "此宿主尚未开放插件能力：{}",
                unsupported(&manifest).join(", ")
            )));
        }
        if manifest.state.len() > 16
            || manifest.state.iter().any(|s| {
                s.maximum_records > 4096
                    || s.maximum_bytes > 4 * 1024 * 1024
                    || s.maximum_value_bytes > 256 * 1024
            })
        {
            return Err(AdminError::invalid("插件私有状态声明超过宿主限制"));
        }
        if let Some(previous) = previous {
            if previous.info.plugin_id != artifact.info.plugin_id {
                return Err(AdminError::invalid("实例只能切换同一插件的版本"));
            }
            let old: Manifest = serde_json::from_value(previous.info.manifest.clone())
                .map_err(|_| AdminError::invalid("旧插件清单无效"))?;
            if old.state != manifest.state {
                return Err(AdminError::invalid(
                    "此版本变更了私有状态声明，需要状态迁移能力，当前不支持原地切换",
                ));
            }
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
        let callbacks = Arc::new(crate::callbacks::Callbacks::new(
            self.store.clone(),
            record,
            &manifest,
            active.clone(),
        )?);
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
                callbacks,
                crate::rpc::RpcSessionLifecycle::new(|_| {}, || {}),
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
            if !management.callbacks.is_empty() || management.resources.iter().any(|r| r.public) {
                return Err(AdminError::invalid(
                    "首版插件入口仅允许管理员访问，不支持公开资源或登录回调",
                ));
            }
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
            session,
            package: prepared,
            active,
            management,
        }))
    }
}
struct LivePlugin {
    session: Arc<RpcSession>,
    package: Arc<PreparedPackage>,
    active: Arc<AtomicBool>,
    management: ManagementRegistration,
}
#[async_trait]
impl PluginProcess for LivePlugin {
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
            })
            .collect()
    }
    fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }
    fn revoke(&self) {
        self.active.store(false, Ordering::Release);
        self.session.quiesce();
    }
    async fn stop(&self) {
        self.active.store(false, Ordering::Release);
        self.session.shutdown(Duration::from_secs(5)).await;
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
        let reply = self
            .session
            .call(
                "management.handle",
                self.session.context(
                    Stage::Management,
                    self.session
                        .maximum_call_timeout()
                        .min(Duration::from_secs(30)),
                ),
                params,
                request.body,
            )
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
