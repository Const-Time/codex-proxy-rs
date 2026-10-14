//! Frozen registration facts for a managed adapter; execution keeps Provider ownership.
pub(crate) mod callbacks;
mod credentials;
mod event;
mod execution;
mod failure;
mod network;
mod save;
mod sessions;
mod target;
use crate::RpcSession;
use gateway_admin::model::{AdminError, plugins::PluginUpstreamAdapterDeclaration};
use gateway_plugin_sdk::{
    Capability, ContributionDeclaration, Manifest, Stage, call::upstream_adapter::*,
};
use std::{collections::BTreeSet, time::Duration};

pub struct RegisteredAdapter {
    declaration: UpstreamAdapterDeclaration,
    target: std::sync::Arc<target::UpstreamTarget>,
}
impl RegisteredAdapter {
    pub fn compile(
        declaration: UpstreamAdapterDeclaration,
        contribution: &ContributionDeclaration,
    ) -> Result<Self, AdminError> {
        if declaration.id.is_empty()
            || declaration.id.len() > 64
            || !declaration
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            || declaration.authentication_kinds.is_empty()
            || declaration.authentication_kinds.len() > 8
            || declaration.authentication_kinds.iter().any(|kind| {
                kind.is_empty()
                    || kind.len() > 64
                    || !kind
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            })
            || !contribution.input_formats.contains(&declaration.protocol)
            || !contribution.output_formats.contains(&declaration.protocol)
            || declaration.models.len() > 256
        {
            return Err(AdminError::invalid("上游适配器身份、认证或协议范围无效"));
        }
        let mut models = BTreeSet::new();
        for model in &declaration.models {
            gateway_core::routing::PublicModelId::new(model.clone())
                .map_err(|_| AdminError::invalid("上游适配器模型无效"))?;
            if !models.insert(model) {
                return Err(AdminError::invalid("上游适配器模型重复"));
            }
        }
        let target = target::UpstreamTarget::compile(&declaration)?;
        Ok(Self {
            declaration,
            target: std::sync::Arc::new(target),
        })
    }
    pub fn declaration(&self) -> &UpstreamAdapterDeclaration {
        &self.declaration
    }
    pub fn resolve_target(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> Result<(url::Url, UpstreamPathPurpose), gateway_plugin_sdk::PluginFault> {
        self.target.resolve(path, query)
    }
    pub(crate) fn facts(&self) -> PluginUpstreamAdapterDeclaration {
        PluginUpstreamAdapterDeclaration {
            id: self.declaration.id.clone(),
            provider: self.declaration.provider.as_str().into(),
            protocol: self.declaration.protocol.clone(),
            transport: self.declaration.transport.as_str().into(),
            models: self.declaration.models.clone(),
            authentication_kinds: self.declaration.authentication_kinds.clone(),
        }
    }
}

pub(crate) async fn register(
    session: &RpcSession,
    manifest: &Manifest,
) -> Result<Vec<RegisteredAdapter>, AdminError> {
    let Some(contribution) = manifest.contributes.get(&Capability::UpstreamAdapter) else {
        return Ok(vec![]);
    };
    if !matches!(contribution.version, 1 | 2) || !contribution.stages.contains(&Stage::Upstream) {
        return Err(AdminError::invalid("上游适配器版本或阶段不支持"));
    }
    let reply = session
        .call(
            REGISTER_METHOD,
            session.context(Stage::Registration, Duration::from_secs(5)),
            serde_json::json!({}),
            vec![],
        )
        .await
        .map_err(|_| AdminError::invalid("上游适配器注册失败"))?;
    if reply.result != serde_json::json!({}) || reply.payload.len() > 64 * 1024 {
        return Err(AdminError::invalid("上游适配器注册信封无效或过大"));
    }
    let registration: UpstreamAdapterRegistration = serde_json::from_slice(&reply.payload)
        .map_err(|_| AdminError::invalid("上游适配器声明无效"))?;
    if registration.adapters.is_empty() || registration.adapters.len() > 16 {
        return Err(AdminError::invalid("单个插件须声明 1 至 16 个上游适配器"));
    }
    let mut ids = BTreeSet::new();
    registration
        .adapters
        .into_iter()
        .map(|declaration| {
            if !ids.insert(declaration.id.clone()) {
                return Err(AdminError::invalid("上游适配器 ID 重复"));
            }
            RegisteredAdapter::compile(declaration, contribution)
        })
        .collect()
}

use gateway_core::{
    engine::{
        provider::EventStream,
        upstream_adapter::{UpstreamAdapter, UpstreamAdapterInvocation},
    },
    lifecycle::CancellationToken,
};
use std::sync::Arc;
pub(crate) struct PluginUpstreamAdapter {
    registered: RegisteredAdapter,
    instance_id: String,
    contribution_id: String,
    session: Arc<RpcSession>,
    callbacks: Arc<callbacks::Callbacks>,
    connections: Arc<sessions::ConnectionPool>,
    cancellation: CancellationToken,
    active: Arc<std::sync::atomic::AtomicBool>,
}
impl PluginUpstreamAdapter {
    pub(crate) fn new(
        registered: RegisteredAdapter,
        contribution_id: String,
        session: Arc<RpcSession>,
        callbacks: Arc<callbacks::Callbacks>,
        cancellation: CancellationToken,
        active: Arc<std::sync::atomic::AtomicBool>,
    ) -> Arc<Self> {
        let instance_id = session
            .context(Stage::Upstream, Duration::from_secs(1))
            .instance_id;
        Arc::new(Self {
            registered,
            instance_id,
            contribution_id,
            session,
            callbacks,
            connections: sessions::ConnectionPool::new(cancellation.clone()),
            cancellation,
            active,
        })
    }
    pub(crate) fn facts(&self) -> PluginUpstreamAdapterDeclaration {
        self.registered.facts()
    }
    pub(crate) fn id(&self) -> &str {
        &self.registered.declaration.id
    }
}
impl std::fmt::Debug for PluginUpstreamAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginUpstreamAdapter")
            .field("instance_id", &self.instance_id)
            .field("adapter_id", &self.id())
            .finish_non_exhaustive()
    }
}
impl UpstreamAdapter for PluginUpstreamAdapter {
    fn transport(&self) -> &str {
        self.registered.declaration.transport.as_str()
    }
    fn execute(self: Arc<Self>, invocation: UpstreamAdapterInvocation) -> EventStream {
        execution::execute(self, invocation)
    }
}
