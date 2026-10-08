//! 插件实例是唯一配置权威；候选进程完成准备后才提交并替换旧代。
use crate::{
    model::{AdminError, MutationContext, plugins::*},
    ports::plugins::*,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::Mutex;

type ActivePlugins = BTreeMap<String, (u64, Arc<dyn PluginProcess>)>;

#[derive(Clone)]
struct AuthenticationEntry {
    id: String,
    binding: PluginAuthenticationBinding,
    process: Option<Arc<dyn PluginProcess>>,
}
#[derive(Default)]
struct AuthenticationPublication {
    entry: Option<AuthenticationEntry>,
    blocked_id: Option<String>,
    epoch: PluginAccess,
}
pub struct PluginManager {
    authentication: RwLock<AuthenticationPublication>,
    store: Arc<dyn PluginStore>,
    runtime: Arc<dyn PluginRuntime>,
    changes: Mutex<()>,
    active: RwLock<ActivePlugins>,
}
impl PluginManager {
    pub fn new(store: Arc<dyn PluginStore>, runtime: Arc<dyn PluginRuntime>) -> Self {
        Self {
            authentication: RwLock::default(),
            store,
            runtime,
            changes: Mutex::new(()),
            active: RwLock::new(BTreeMap::new()),
        }
    }
    /// An invalid plugin must not prevent the gateway or other instances from starting.
    pub async fn restore(&self) -> Result<(), AdminError> {
        let _guard = self.changes.lock().await;
        for record in self.store.list().await? {
            if !record.enabled {
                continue;
            }
            let restored = async {
                let artifact = self.store.artifact(&record.artifact_sha256).await?;
                self.runtime.validate(&record, &artifact, None).await?;
                self.runtime.prepare(&record, artifact).await
            }
            .await;
            if record.frontend_authentication.is_some() {
                self.publish_authentication(&record, restored.as_ref().ok().cloned());
            }
            if let Ok(process) = restored {
                process.activate();
                self.active
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(record.id, (record.revision, process));
            } else {
                tracing::warn!(instance_id = record.id, "插件恢复失败，实例保持不可用");
            }
        }
        Ok(())
    }
    pub async fn shutdown(&self) {
        let _guard = self.changes.lock().await;
        self.authentication
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .epoch
            .revoke();
        let active = std::mem::take(
            &mut *self
                .active
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        futures::future::join_all(active.into_values().map(|(_, p)| async move {
            p.revoke();
            p.stop().await;
        }))
        .await;
    }
    fn publish_authentication(
        &self,
        record: &PluginRecord,
        process: Option<Arc<dyn PluginProcess>>,
    ) {
        let mut state = self
            .authentication
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.entry.as_ref().is_some_and(|e| e.id == record.id)
            || state.blocked_id.as_deref() == Some(&record.id)
            || (record.enabled && record.frontend_authentication.is_some())
        {
            state.epoch.revoke();
            state.epoch = PluginAccess::default();
            state.blocked_id = None;
            state.entry = record
                .frontend_authentication
                .clone()
                .filter(|_| record.enabled)
                .map(|binding| AuthenticationEntry {
                    id: record.id.clone(),
                    binding,
                    process,
                });
        }
    }
    fn process(&self, id: &str, revision: u64) -> Result<Arc<dyn PluginProcess>, AdminError> {
        self.active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .filter(|(current, p)| *current == revision && p.status() == "ready")
            .map(|(_, p)| p.clone())
            .ok_or_else(|| AdminError::conflict("插件版本已失效、已停用或进程不可用，请刷新"))
    }
    async fn view(&self, record: PluginRecord) -> Result<PluginView, AdminError> {
        let artifact = self
            .runtime
            .describe(self.store.artifact_info(&record.artifact_sha256).await?)?;
        let (status, pages, resources) = {
            let active = self
                .active
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            active
                .get(&record.id)
                .filter(|(rev, _)| *rev == record.revision)
                .map_or(
                    (
                        if record.enabled { "failed" } else { "disabled" },
                        vec![],
                        vec![],
                    ),
                    |(_, p)| (p.status(), p.pages(), p.resources()),
                )
        };
        Ok(PluginView {
            frontend_authentication: record.frontend_authentication,
            id: record.id,
            revision: record.revision,
            name: record.name,
            artifact,
            enabled: record.enabled,
            status: status.into(),
            configuration: record.configuration,
            configured_secrets: record.secrets.into_keys().collect(),
            pages,
            resources,
        })
    }
}
#[async_trait]
impl PluginService for PluginManager {
    async fn authenticate(
        &self,
        authorization: String,
    ) -> Result<PluginAuthenticationDecision, AdminError> {
        let (entry, epoch) = {
            let state = self
                .authentication
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.blocked_id.is_some() || state.epoch.is_revoked() {
                return Err(AdminError::unavailable("入口认证暂不可用"));
            }
            (state.entry.clone(), state.epoch.clone())
        };
        let Some(entry) = entry else {
            return Ok(PluginAuthenticationDecision::Builtin { access: epoch });
        };
        let process = entry
            .process
            .ok_or_else(|| AdminError::unavailable("认证器暂不可用"))?;
        let access = PluginAccess::linked([epoch, process.access()]);
        if access.is_revoked() {
            return Err(AdminError::unavailable("认证器已失效"));
        }
        let result = process.authenticate(authorization).await?;
        if access.is_revoked() {
            return Err(AdminError::unavailable("认证器已失效"));
        }
        Ok(match result {
            PluginAuthenticationResult::Authenticated(principal) => {
                match entry.binding.identities.get(&principal) {
                    Some(key) => PluginAuthenticationDecision::Authenticated {
                        key: gateway_core::policy::ClientApiKeyId::new(key)
                            .map_err(|_| AdminError::unavailable("身份映射无效"))?,
                        access,
                    },
                    None => PluginAuthenticationDecision::Rejected,
                }
            }
            PluginAuthenticationResult::NotMatched
                if entry.binding.failure_policy == PluginAuthenticationPolicy::Delegate =>
            {
                PluginAuthenticationDecision::Builtin { access }
            }
            _ => PluginAuthenticationDecision::Rejected,
        })
    }

    fn access(&self, id: &str, revision: u64) -> Result<PluginAccess, AdminError> {
        let process = self.process(id, revision)?;
        let access = process.access();
        if access.is_revoked() {
            return Err(AdminError::conflict("插件版本已失效"));
        }
        Ok(access)
    }
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        self.store
            .artifacts()
            .await?
            .into_iter()
            .map(|info| self.runtime.describe(info))
            .collect()
    }
    async fn delete_artifact(
        &self,
        digest: &str,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let _guard = self.changes.lock().await;
        self.store.delete_artifact(digest, actor).await
    }
    async fn upload(
        &self,
        archive: Arc<[u8]>,
        actor: &MutationContext,
    ) -> Result<PluginArtifactInfo, AdminError> {
        let artifact = self.runtime.inspect(archive).await?;
        let info = artifact.info.clone();
        self.store.put_artifact(artifact, actor).await?;
        Ok(info)
    }
    async fn list(&self) -> Result<Vec<PluginView>, AdminError> {
        let mut result = Vec::new();
        for record in self.store.list().await? {
            result.push(self.view(record).await?);
        }
        Ok(result)
    }
    async fn update(
        &self,
        id: Option<&str>,
        update: PluginUpdate,
        actor: &MutationContext,
    ) -> Result<PluginView, AdminError> {
        let _guard = self.changes.lock().await;
        if update.name.trim().is_empty()
            || update.name.len() > 120
            || update.name.chars().any(char::is_control)
        {
            return Err(AdminError::invalid("插件实例名称不合法"));
        }
        if update.enabled && !update.trusted_process {
            return Err(AdminError::invalid("启用前必须接受可信进程运行方式"));
        }
        let records = self.store.list().await?;
        let previous = id.and_then(|id| records.iter().find(|r| r.id == id));
        if id.is_some() && previous.is_none() {
            return Err(AdminError::not_found("插件实例不存在"));
        }
        if previous.map_or(0, |r| r.revision) != update.revision {
            return Err(AdminError::conflict("插件设置已变更，请刷新"));
        }
        if id.is_none() && records.len() >= 64 {
            return Err(AdminError::invalid("最多安装 64 个插件实例"));
        }
        let artifact = self.store.artifact(&update.artifact_sha256).await?;
        let old_artifact = if let Some(previous) = previous {
            Some(self.store.artifact(&previous.artifact_sha256).await?)
        } else {
            None
        };
        let mut secrets = previous.map(|r| r.secrets.clone()).unwrap_or_default();
        for (key, value) in update.secrets {
            if let Some(value) = value {
                secrets.insert(key, value);
            } else {
                secrets.remove(&key);
            }
        }
        if let Some(binding) = &update.frontend_authentication {
            binding.validate()?;
        }
        if update.enabled
            && update.frontend_authentication.is_some()
            && records.iter().any(|r| {
                Some(r.id.as_str()) != id && r.enabled && r.frontend_authentication.is_some()
            })
        {
            return Err(AdminError::conflict("只能启用一个入口认证器"));
        }
        let record = PluginRecord {
            frontend_authentication: update.frontend_authentication,
            id: id
                .map(str::to_owned)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            revision: update
                .revision
                .checked_add(1)
                .ok_or_else(|| AdminError::invalid("插件版本溢出"))?,
            name: update.name.trim().into(),
            artifact_sha256: update.artifact_sha256,
            enabled: update.enabled,
            configuration: update.configuration,
            secrets,
        };
        self.runtime
            .validate(&record, &artifact, old_artifact.as_ref())
            .await?;
        let candidate = if record.enabled {
            Some(self.runtime.prepare(&record, artifact).await?)
        } else {
            None
        };
        if let Err(error) = self.store.save(&record, update.revision, actor).await {
            let persisted = self.store.list().await;
            let committed = persisted
                .as_ref()
                .is_ok_and(|rows| rows.iter().any(|r| r == &record));
            if !committed {
                let unchanged = persisted
                    .as_ref()
                    .is_ok_and(|rows| rows.iter().find(|r| r.id == record.id) == previous);
                if !unchanged {
                    if record.frontend_authentication.is_some()
                        || previous.is_some_and(|r| r.frontend_authentication.is_some())
                    {
                        let mut state = self
                            .authentication
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.epoch.revoke();
                        state.blocked_id = Some(record.id.clone());
                    }
                    // Lost commit acknowledgement or an external change: no stale generation may remain active.
                    let old = self
                        .active
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&record.id);
                    if let Some((_, old)) = old {
                        old.revoke();
                        tokio::spawn(async move {
                            old.stop().await;
                        });
                    }
                }
                if let Some(candidate) = candidate {
                    candidate.stop().await;
                }
                return Err(if unchanged {
                    error
                } else {
                    AdminError::upstream_result_unknown(
                        "插件配置提交结果未能确认，实例已停止；请刷新设置后重新启用",
                    )
                });
            }
        }
        // No await between persistent CAS and publication; cancelled HTTP requests cannot leave a prepared-only revision.
        let auth_candidate = candidate.clone();
        let old = {
            let mut active = self
                .active
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let old = active.remove(&record.id);
            if let Some(ref old) = old {
                old.1.revoke();
            }
            if let Some(candidate) = candidate {
                candidate.activate();
                active.insert(record.id.clone(), (record.revision, candidate));
            }
            self.publish_authentication(&record, auth_candidate);
            old
        };
        if let Some((_, old)) = old {
            tokio::spawn(async move {
                old.stop().await;
            });
        }
        self.view(record).await
    }
    async fn delete(
        &self,
        id: &str,
        revision: u64,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        let _guard = self.changes.lock().await;
        let result = self.store.delete(id, revision, actor).await;
        if let Err(error) = result {
            let records = self.store.list().await?;
            if records.iter().any(|r| r.id == id) {
                return Err(error);
            }
        }
        let old = self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        if let Some((_, old)) = old {
            old.revoke();
            tokio::spawn(async move {
                old.stop().await;
            });
        }
        Ok(())
    }
    async fn resource(
        &self,
        id: &str,
        revision: u64,
        path: &str,
    ) -> Result<PluginHttpResponse, AdminError> {
        self.process(id, revision)?.resource(path).await
    }
    async fn handle(
        &self,
        id: &str,
        revision: u64,
        request: PluginHttpRequest,
    ) -> Result<PluginHttpResponse, AdminError> {
        self.process(id, revision)?.handle(request).await
    }
}
