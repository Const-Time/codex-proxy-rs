//! 页面配置、短期 state 生命周期与 Provider 运行时读取。不会改变账号或业务出口。
mod crypto;
mod egress;
mod identity;
mod network;
mod probe;
mod scheduling;
mod traffic;
mod worker;
pub(crate) use worker::StateRefreshTask;

use crate::transport::protocol::responses::CodexResponsesRequest;
use crate::{credential::CodexCredentialRepository, transport::profile::CodexWireProfileState};
use async_trait::async_trait;
use chrono::Utc;
use crypto::StateCipher;
use gateway_admin::{
    model::{AdminError, MutationContext, turn_state::*},
    ports::turn_state::{TurnStateService, TurnStateStore},
};
use gateway_core::account::{ProviderAccount, ProviderAccountStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::sync::Mutex;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Pool {
    id: String,
    name: String,
    enabled: bool,
    mode: String,
    endpoint: String,
    bearer: String,
    json_pointer: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Current {
    token: String,
    fingerprint: String,
    issued_at: i64,
    expires_at: i64,
    account_revision: u64,
    #[serde(default)]
    binding: Option<[u8; 32]>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Target {
    input: TurnStateTargetInput,
    current: Option<Current>,
    #[serde(default)]
    alternatives: Vec<Current>,
    next_probe_at: i64,
    last_message: String,
    history: Vec<TurnStateObservation>,
    #[serde(default)]
    automatic: bool,
    #[serde(default)]
    last_traffic_at: Option<i64>,
    #[serde(default)]
    wait_reason: String,
    #[serde(default)]
    cooldown_until: i64,
    #[serde(default)]
    refresh_requested: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Document {
    generation: u64,
    policy: TurnStatePolicy,
    pools: Vec<Pool>,
    targets: Vec<Target>,
    #[serde(default)]
    accounts: Vec<TurnStateAccountPolicy>,
    #[serde(default)]
    budgets: Vec<scheduling::Budget>,
    #[serde(default)]
    cursor: Option<(String, String)>,
}

impl Default for Document {
    fn default() -> Self {
        Self {
            generation: 1,
            policy: TurnStatePolicy::default(),
            pools: vec![],
            targets: vec![],
            accounts: vec![],
            budgets: vec![],
            cursor: None,
        }
    }
}

struct Runtime {
    storage_revision: u64,
    document: Document,
    running: HashSet<(String, String)>,
    owners: HashMap<String, StateOwner>,
    dirty: bool,
}

struct StateOwner {
    account_id: String,
    binding: [u8; 32],
    expires_at: i64,
}

pub(crate) struct StateManager {
    egress_probe: Option<Arc<dyn gateway_admin::ports::proxy::ProxyProbe>>,
    egress_cache: Mutex<HashMap<String, TurnStateEgress>>,
    runtime: Mutex<Runtime>,
    store: Arc<dyn TurnStateStore>,
    cipher: StateCipher,
    repository: CodexCredentialRepository,
    leases: Arc<dyn gateway_core::provider_ports::ProviderLeasePort>,
    runtime_policy: Arc<dyn gateway_core::provider_ports::ProviderRuntimePolicyPort>,
    profile: CodexWireProfileState,
    base_url: String,
    test_slot: tokio::sync::Semaphore,
    account_policies: RwLock<HashMap<String, TurnStateAccountPolicy>>,
    fingerprint_key: [u8; 32],
    changes: tokio::sync::watch::Sender<u64>,
    traffic: std::sync::Mutex<HashMap<(String, String), traffic::Traffic>>,
}

pub(super) fn fingerprint(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))[..16].to_owned()
}

impl StateManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load(
        store: Arc<dyn TurnStateStore>,
        accounts: Arc<dyn ProviderAccountStore>,
        leases: Arc<dyn gateway_core::provider_ports::ProviderLeasePort>,
        runtime_policy: Arc<dyn gateway_core::provider_ports::ProviderRuntimePolicyPort>,
        profile: CodexWireProfileState,
        base_url: String,
        key: [u8; 32],
        egress_probe: Option<Arc<dyn gateway_admin::ports::proxy::ProxyProbe>>,
    ) -> Result<Arc<Self>, AdminError> {
        let cipher = StateCipher::new(&key)?;
        let (storage_revision, sealed) = store
            .load()
            .await
            .map_err(|_| AdminError::unavailable("State 存储不可用"))?;
        let document: Document = match sealed {
            None => Document::default(),
            Some(bytes) => serde_json::from_slice(&cipher.open(bytes)?)
                .map_err(|_| AdminError::unavailable("State 存储格式不合法"))?,
        };
        document.policy.validate()?;
        let account_policies = RwLock::new(
            document
                .accounts
                .iter()
                .map(|p| (p.account_id.clone(), p.clone()))
                .collect(),
        );
        let (changes, _) = tokio::sync::watch::channel(document.generation);
        Ok(Arc::new(Self {
            egress_probe,
            egress_cache: Mutex::new(HashMap::new()),
            runtime: Mutex::new(Runtime {
                storage_revision,
                document,
                running: HashSet::new(),
                owners: HashMap::new(),
                dirty: false,
            }),
            store,
            cipher,
            repository: CodexCredentialRepository::new(accounts),
            leases,
            runtime_policy,
            profile,
            base_url,
            test_slot: tokio::sync::Semaphore::new(1),
            account_policies,
            changes,
            traffic: std::sync::Mutex::new(HashMap::new()),
            fingerprint_key: crate::transport::session::hmac_sha256(
                &key,
                &[b"fingerprint-projection/v1"],
            ),
        }))
    }

    async fn commit(
        &self,
        runtime: &mut Runtime,
        mut document: Document,
        context: Option<&MutationContext>,
    ) -> Result<(), AdminError> {
        let traffic = self.traffic_snapshot();
        Self::apply_traffic(&mut document, &traffic);
        let data =
            serde_json::to_vec(&document).map_err(|_| AdminError::internal("State 序列化失败"))?;
        let sealed = self.cipher.seal(data)?;
        let revision = match self
            .store
            .save(runtime.storage_revision, sealed, context)
            .await
        {
            Ok(revision) => revision,
            Err(error)
                if error.kind()
                    == gateway_admin::ports::store::AdminStoreErrorKind::StaleRevision =>
            {
                self.synchronize(runtime).await?;
                return Err(AdminError::conflict("其他实例已修改 State，请重新加载"));
            }
            Err(_) => return Err(AdminError::unavailable("State 保存失败，未应用更改")),
        };
        self.publish_account_policies(&document);
        self.changes.send_if_modified(|generation| {
            if *generation == document.generation {
                false
            } else {
                *generation = document.generation;
                true
            }
        });
        runtime.storage_revision = revision;
        runtime.document = document;
        runtime.dirty = false;
        // An event arriving during the store write remains pending for the next CAS.
        self.traffic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, value| traffic.get(key) != Some(value));
        Ok(())
    }

    fn publish_account_policies(&self, document: &Document) {
        *self
            .account_policies
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = document
            .accounts
            .iter()
            .map(|p| (p.account_id.clone(), p.clone()))
            .collect();
    }

    pub(crate) fn account_policy(&self, account_id: &str) -> TurnStateAccountPolicy {
        self.account_policies
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(account_id)
            .cloned()
            .unwrap_or_else(|| TurnStateAccountPolicy {
                account_id: account_id.to_owned(),
                ..Default::default()
            })
    }

    pub(crate) fn converge(
        &self,
        request: &mut CodexResponsesRequest,
        account_id: &str,
        client_id: &str,
    ) {
        if self.account_policy(account_id).fingerprint_convergence {
            crate::transport::fingerprint::converge_request(
                request,
                &self.fingerprint_key,
                account_id,
                client_id,
            );
        }
    }

    /// 跨实例 CAS 冲突后及每次 worker 周期重新读取；全局关闭时也必须同步开关。
    async fn synchronize(&self, runtime: &mut Runtime) -> Result<(), AdminError> {
        let (revision, sealed) = self
            .store
            .load()
            .await
            .map_err(|_| AdminError::unavailable("State 存储不可用"))?;
        if revision == runtime.storage_revision {
            self.flush_traffic(runtime);
            return Ok(());
        }
        let document: Document = match sealed {
            Some(bytes) => serde_json::from_slice(&self.cipher.open(bytes)?)
                .map_err(|_| AdminError::unavailable("State 存储格式不合法"))?,
            None => Document::default(),
        };
        document.policy.validate()?;
        self.publish_account_policies(&document);
        self.changes.send_if_modified(|generation| {
            if *generation == document.generation {
                false
            } else {
                *generation = document.generation;
                true
            }
        });
        runtime.document = document;
        runtime.storage_revision = revision;
        runtime.dirty = false;
        // Replay only traffic, never stale switches, revoked candidates or budgets.
        // Pending activity is acknowledged only by a successful CAS commit.
        self.flush_traffic(runtime);
        Ok(())
    }

    /// 仅供新业务 attempt 读取；正常续接由调用方优先保留。截止时间在读取时再次检查。
    pub(crate) async fn select(
        &self,
        account: &ProviderAccount,
        model: &str,
        binding: &[u8; 32],
    ) -> Option<String> {
        let runtime =
            tokio::time::timeout(std::time::Duration::from_millis(20), self.runtime.lock())
                .await
                .ok()?;
        let doc = &runtime.document;
        if !doc.policy.enabled || !self.account_policy(account.id().as_str()).takeover {
            return None;
        }
        let target = doc.targets.iter().find(|t| {
            t.input.account_id == account.id().as_str() && t.input.model == model && t.input.enabled
        })?;
        if target.automatic
            && !self
                .account_policy(account.id().as_str())
                .maintenance
                .auto_models
        {
            return None;
        }
        if !doc
            .pools
            .iter()
            .any(|p| p.id == target.input.pool_id && p.enabled)
        {
            return None;
        }
        target
            .current
            .iter()
            .chain(&target.alternatives)
            .find(|state| {
                state.expires_at > Utc::now().timestamp() && state.belongs_to(account, binding)
            })
            .map(|state| state.token.clone())
    }

    pub(crate) async fn observe(&self, account: &str, model: &str, direction: &str, token: &str) {
        let Ok(mut runtime) = self.runtime.try_lock() else {
            return;
        };
        if !runtime.document.policy.enabled {
            return;
        }
        let Some(target) = runtime
            .document
            .targets
            .iter_mut()
            .find(|t| t.input.account_id == account && t.input.model == model)
        else {
            return;
        };
        let fp = fingerprint(token);
        if target
            .history
            .last()
            .is_some_and(|h| h.direction == direction && h.fingerprint == fp)
        {
            return;
        }
        record(target, direction, token, "业务观测；不自动提升为可用候选");
        runtime.dirty = true;
        // 业务热路径只更新有界诊断环；随下次策略/刷新提交持久化，不等待数据库。
    }

    /// 只处理本次成功完成响应的实际返回值；注入后的重新签发不判坏，也不循环养池。
    pub(crate) async fn observe_response(
        &self,
        account: &ProviderAccount,
        model: &str,
        token: &str,
        collect: bool,
        generation: Option<u64>,
        binding: Option<[u8; 32]>,
    ) {
        if token.is_empty() || token.len() > 4096 {
            return;
        }
        let Some(binding) = binding else { return };
        let identity_current = if collect {
            self.current_identity(account)
                .await
                .is_some_and(|(current, current_binding)| {
                    current_binding == binding
                        && current.enabled()
                        && current.model_access().allows(model)
                })
        } else {
            false
        };
        let Ok(mut runtime) = self.runtime.try_lock() else {
            return;
        };
        let now = Utc::now().timestamp();
        runtime.owners.retain(|_, owner| owner.expires_at > now);
        if runtime.owners.len() >= 4096
            && let Some(oldest) = runtime
                .owners
                .iter()
                .min_by_key(|(_, o)| o.expires_at)
                .map(|(k, _)| k.clone())
        {
            runtime.owners.remove(&oldest);
        }
        runtime.owners.insert(
            hex::encode(Sha256::digest(token.as_bytes())),
            StateOwner {
                account_id: account.id().as_str().to_owned(),
                binding,
                expires_at: now + 3600,
            },
        );
        let policy = runtime.document.policy.clone();
        let may_collect = collect
            && identity_current
            && generation == Some(runtime.document.generation)
            && policy.enabled
            && self.account_policy(account.id().as_str()).takeover;
        if may_collect && FernetShape::parse(token).is_some() {
            runtime.dirty |=
                Self::discover(&mut runtime.document, account.id().as_str(), model, now);
        }
        let Some(target) = runtime
            .document
            .targets
            .iter_mut()
            .find(|t| t.input.account_id == account.id().as_str() && t.input.model == model)
        else {
            return;
        };
        record(
            target,
            "returned",
            token,
            "上游实际返回；长度变化不是失效证据",
        );
        if may_collect
            && target.input.enabled
            && let Some(shape) = FernetShape::parse(token)
            && let Some(expires_at) = shape.candidate_expiry(&policy, now)
        {
            publish_candidate(
                target,
                Current {
                    token: token.to_owned(),
                    fingerprint: fingerprint(token),
                    issued_at: shape.issued_at,
                    expires_at,
                    account_revision: account.revision().get(),
                    binding: Some(binding),
                },
                now,
            );
            if let Some(current) = &target.current {
                target.next_probe_at = current.expires_at - policy.refresh_before_seconds;
                target.last_message = "已收集成功业务响应；等待到期前刷新".to_owned();
            }
        }
        runtime.dirty = true;
    }

    pub(crate) async fn guard_request(
        &self,
        account: &ProviderAccount,
        request: &mut CodexResponsesRequest,
        binding: [u8; 32],
    ) {
        let runtime = self.runtime.lock().await;
        request.turn_state_generation = Some(runtime.document.generation);
        request.turn_state_binding = Some(binding);
        let now = Utc::now().timestamp();
        let wrong_owner = |token: &str| {
            if token.len() > 4096 {
                return false;
            }
            let fingerprint = hex::encode(Sha256::digest(token.as_bytes()));
            runtime.owners.get(&fingerprint).is_some_and(|owner| {
                owner.expires_at > now
                    && (owner.account_id != account.id().as_str() || owner.binding != binding)
            }) || runtime.document.targets.iter().any(|target| {
                target
                    .current
                    .iter()
                    .chain(&target.alternatives)
                    .any(|state| {
                        state.token == token
                            && state.expires_at > now
                            && (target.input.account_id != account.id().as_str()
                                || !state.belongs_to(account, &binding))
                    })
            })
        };
        let reject = request.turn_state.as_deref().is_some_and(wrong_owner)
            || request
                .passthrough_headers
                .get_all("x-codex-turn-state")
                .iter()
                .filter_map(|v| v.to_str().ok())
                .any(wrong_owner)
            || request
                .client_metadata()
                .and_then(|m| m.get("x-codex-turn-state"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(wrong_owner);
        if reject {
            request.turn_state = None;
            request.passthrough_headers.remove("x-codex-turn-state");
            for key in ["turnState", "turn_state", "x-codex-turn-state"] {
                request.body_mut().remove(key);
                if let Some(metadata) = request
                    .body_mut()
                    .get_mut("client_metadata")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    metadata.remove(key);
                }
            }
        }
    }

    fn project(runtime: &Runtime) -> TurnStateView {
        let doc = &runtime.document;
        let now = Utc::now().timestamp();
        TurnStateView {
            revision: doc.generation,
            policy: doc.policy.clone(),
            accounts: doc.accounts.clone(),
            pools: doc
                .pools
                .iter()
                .map(|p| TurnStatePoolView {
                    id: p.id.clone(),
                    name: p.name.clone(),
                    enabled: p.enabled,
                    mode: p.mode.clone(),
                    endpoint: network::redacted_endpoint(&p.endpoint),
                    has_secret: true,
                    json_pointer: p.json_pointer.clone(),
                })
                .collect(),
            targets: doc
                .targets
                .iter()
                .map(|t| {
                    let maintenance = scheduling::maintenance(doc, &t.input.account_id);
                    let (hourly_used, budget_resets_at) =
                        scheduling::budget(doc, &t.input.account_id, now);
                    let active = t
                        .current
                        .iter()
                        .chain(&t.alternatives)
                        .find(|s| s.expires_at > now)
                        .or(t.current.as_ref());
                    TurnStateTargetView {
                        target: t.input.clone(),
                        status: if !doc.policy.enabled
                            || !t.input.enabled
                            || (t.automatic && !maintenance.auto_models)
                            || !doc
                                .accounts
                                .iter()
                                .any(|p| p.account_id == t.input.account_id && p.takeover)
                            || !doc
                                .pools
                                .iter()
                                .any(|p| p.id == t.input.pool_id && p.enabled)
                        {
                            "paused"
                        } else if runtime
                            .running
                            .contains(&(t.input.account_id.clone(), t.input.model.clone()))
                        {
                            "probing"
                        } else if active.is_some_and(|s| s.expires_at > now) {
                            "ready"
                        } else if t.current.is_some() {
                            "expired"
                        } else {
                            "empty"
                        }
                        .to_owned(),
                        fingerprint: active.map(|s| s.fingerprint.clone()),
                        issued_at: active.map(|s| s.issued_at),
                        expires_at: active.map(|s| s.expires_at),
                        next_probe_at: t.next_probe_at,
                        last_message: t.last_message.clone(),
                        history: t.history.clone(),
                        candidate_count: t
                            .current
                            .iter()
                            .chain(&t.alternatives)
                            .filter(|s| s.expires_at > now)
                            .count(),
                        takeover: doc
                            .accounts
                            .iter()
                            .any(|p| p.account_id == t.input.account_id && p.takeover),
                        wait_reason: {
                            let gate = scheduling::gate(doc, t, now);
                            if gate == "queued" && !t.wait_reason.is_empty() {
                                t.wait_reason.clone()
                            } else {
                                gate.to_owned()
                            }
                        },
                        hourly_used,
                        hourly_limit: maintenance.max_probes_per_hour,
                        budget_resets_at,
                        last_traffic_at: t.last_traffic_at,
                        automatic: t.automatic,
                    }
                })
                .collect(),
        }
    }
}

fn publish_candidate(target: &mut Target, current: Current, now: i64) {
    if target
        .current
        .as_ref()
        .is_some_and(|old| old.token == current.token)
    {
        return; // 重复观测不能给同一个 token 延长 TTL。
    }
    target.alternatives.retain(|old| {
        old.expires_at > now
            && old.binding == current.binding
            && (current.binding.is_some() || old.account_revision == current.account_revision)
            && old.token != current.token
    });
    if let Some(previous) = target.current.take()
        && previous.expires_at > now
        && previous.binding == current.binding
        && (current.binding.is_some() || previous.account_revision == current.account_revision)
    {
        target.alternatives.insert(0, previous);
    }
    target.alternatives.truncate(2);
    target.current = Some(current);
}

fn record(target: &mut Target, direction: &str, token: &str, message: &str) {
    target.history.push(TurnStateObservation {
        at: Utc::now().timestamp(),
        direction: direction.to_owned(),
        fingerprint: if token.is_empty() {
            String::new()
        } else {
            fingerprint(token)
        },
        shape: FernetShape::parse(token),
        message: message.to_owned(),
    });
    if target.history.len() > 20 {
        target.history.remove(0);
    }
}

#[async_trait]
impl TurnStateService for StateManager {
    async fn records(
        &self,
        query: TurnStateRecordQuery,
    ) -> Result<TurnStateRecordPage, AdminError> {
        query.validate()?;
        self.store
            .records(&query)
            .await
            .map_err(|_| AdminError::unavailable("探测记录读取失败"))
    }
    async fn view(&self) -> Result<TurnStateView, AdminError> {
        let mut runtime = self.runtime.lock().await;
        self.synchronize(&mut runtime).await?;
        Ok(Self::project(&runtime))
    }

    async fn configure_account(
        &self,
        account_id: &str,
        input: TurnStateAccountUpdate,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        let accounts = self
            .repository
            .list_for_provider()
            .await
            .map_err(|_| AdminError::unavailable("账号目录不可用"))?;
        if !accounts.iter().any(|a| a.id().as_str() == account_id) {
            return Err(AdminError::not_found("OpenAI 账号不存在"));
        }
        let mut runtime = self.runtime.lock().await;
        self.synchronize(&mut runtime).await?;
        if input.revision != runtime.document.generation {
            return Err(AdminError::conflict("设置已改变，请重新加载"));
        }
        let mut document = runtime.document.clone();
        if document.accounts.len() >= 4096
            && !document.accounts.iter().any(|p| p.account_id == account_id)
        {
            return Err(AdminError::invalid("账号策略数量已达上限"));
        }
        let old = document
            .accounts
            .iter()
            .find(|p| p.account_id == account_id);
        let maintenance = input
            .maintenance
            .unwrap_or_else(|| old.map(|p| p.maintenance.clone()).unwrap_or_default());
        maintenance.validate()?;
        if maintenance
            .pool_id
            .as_ref()
            .is_some_and(|id| !document.pools.iter().any(|p| &p.id == id))
        {
            return Err(AdminError::invalid("自动模型代理池不存在"));
        }
        let changed = old.is_none_or(|p| {
            p.takeover != input.takeover
                || p.fingerprint_convergence != input.fingerprint_convergence
        });
        let fingerprint_changed =
            old.is_some_and(|p| p.fingerprint_convergence) != input.fingerprint_convergence;
        document.accounts.retain(|p| p.account_id != account_id);
        document.accounts.push(TurnStateAccountPolicy {
            account_id: account_id.to_owned(),
            fingerprint_convergence: input.fingerprint_convergence,
            takeover: input.takeover,
            maintenance: maintenance.clone(),
        });
        for target in &mut document.targets {
            if target.input.account_id == account_id {
                if fingerprint_changed || !input.takeover {
                    target.current = None;
                    target.alternatives.clear();
                }
                if target.automatic
                    && let Some(pool) = &maintenance.pool_id
                    && &target.input.pool_id != pool
                {
                    target.input.pool_id = pool.clone();
                    target.current = None;
                    target.alternatives.clear();
                }
                if changed {
                    target.next_probe_at = Utc::now().timestamp();
                }
                target.last_message = "账号策略已更新；预算和上游冷却保持不变".to_owned();
            }
        }
        document.generation += 1;
        self.commit(&mut runtime, document, Some(context)).await?;
        Ok(Self::project(&runtime))
    }

    async fn configure(
        &self,
        input: TurnStateSettingsInput,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        input.policy.validate()?;
        if input.pools.len() > 16 || input.targets.len() > 128 {
            return Err(AdminError::invalid("最多 16 个代理池和 128 个账号模型"));
        }
        let accounts = self
            .repository
            .list_for_provider()
            .await
            .map_err(|_| AdminError::unavailable("账号目录不可用"))?;
        let mut runtime = self.runtime.lock().await;
        self.synchronize(&mut runtime).await?;
        if input.revision != runtime.document.generation {
            return Err(AdminError::conflict("设置已改变，请重新加载"));
        }
        let mut doc = runtime.document.clone();
        let mut pool_ids = HashSet::new();
        let mut pools = Vec::new();
        for p in input.pools {
            if p.id.is_empty()
                || p.id.len() > 64
                || !p.id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                || !pool_ids.insert(p.id.clone())
                || p.name.trim().is_empty()
                || p.name.len() > 128
            {
                return Err(AdminError::invalid("代理池名称或 ID 不合法／重复"));
            }
            let old = doc.pools.iter().find(|old| old.id == p.id);
            let endpoint = p
                .endpoint
                .or_else(|| old.map(|p| p.endpoint.clone()))
                .ok_or_else(|| AdminError::invalid("请填写代理入口"))?;
            let bearer = p
                .bearer
                .or_else(|| old.map(|p| p.bearer.clone()))
                .unwrap_or_default();
            let pool = Pool {
                id: p.id,
                name: p.name.trim().to_owned(),
                enabled: p.enabled,
                mode: p.mode,
                endpoint,
                bearer,
                json_pointer: p.json_pointer,
            };
            network::validate_pool(&pool)?;
            pools.push(pool);
        }
        let mut targets = Vec::new();
        if doc.accounts.iter().any(|a| {
            a.maintenance
                .pool_id
                .as_ref()
                .is_some_and(|id| !pool_ids.contains(id))
        }) {
            return Err(AdminError::invalid("请先解除账号自动模型对代理池的引用"));
        }
        let mut target_ids = HashSet::new();
        for mut t in input.targets {
            t.model = t.model.trim().to_owned();
            if t.model.is_empty()
                || t.model.len() > 128
                || t.model
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace() || c == '*')
                || !target_ids.insert((t.account_id.clone(), t.model.clone()))
                || !pool_ids.contains(&t.pool_id)
                || !accounts.iter().any(|a| a.id().as_str() == t.account_id)
            {
                return Err(AdminError::invalid(
                    "请选择 OpenAI 账号、精确模型和已配置代理池，不能重复",
                ));
            }
            let old = doc
                .targets
                .iter()
                .find(|o| o.input.account_id == t.account_id && o.input.model == t.model);
            // Scheduling-only edits preserve tickets and their absolute expiry.
            let preserve = old.is_some_and(|old| old.input.pool_id == t.pool_id)
                && doc.policy.header_length == input.policy.header_length
                && doc.policy.cipher_blocks == input.policy.cipher_blocks
                && doc.policy.ttl_seconds == input.policy.ttl_seconds
                && doc
                    .pools
                    .iter()
                    .find(|p| p.id == t.pool_id)
                    .zip(pools.iter().find(|p| p.id == t.pool_id))
                    .is_some_and(|(old, new)| {
                        old.mode == new.mode
                            && old.endpoint == new.endpoint
                            && old.bearer == new.bearer
                            && old.json_pointer == new.json_pointer
                    });
            targets.push(Target {
                input: t,
                current: old.filter(|_| preserve).and_then(|t| t.current.clone()),
                alternatives: old
                    .filter(|_| preserve)
                    .map(|t| t.alternatives.clone())
                    .unwrap_or_default(),
                next_probe_at: old.map_or(Utc::now().timestamp(), |t| t.next_probe_at),
                last_message: "设置已保存；预算、上游冷却和绝对到期时间保持不变".to_owned(),
                history: old.map(|t| t.history.clone()).unwrap_or_default(),
                automatic: old.is_some_and(|t| t.automatic),
                last_traffic_at: old.and_then(|t| t.last_traffic_at),
                wait_reason: "queued".to_owned(),
                cooldown_until: old.map_or(0, |t| t.cooldown_until),
                refresh_requested: old.is_some_and(|t| t.refresh_requested),
            });
        }
        doc.policy = input.policy;
        doc.pools = pools;
        doc.targets = targets;
        doc.generation += 1;
        self.commit(&mut runtime, doc, Some(context)).await?;
        Ok(Self::project(&runtime))
    }

    async fn action(
        &self,
        account_id: &str,
        model: &str,
        action: &str,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        let mut runtime = self.runtime.lock().await;
        self.synchronize(&mut runtime).await?;
        if action == "refresh"
            && runtime
                .running
                .contains(&(account_id.to_owned(), model.to_owned()))
        {
            return Err(AdminError::conflict("该账号模型正在探测"));
        }
        let mut doc = runtime.document.clone();
        let target = doc
            .targets
            .iter_mut()
            .find(|t| t.input.account_id == account_id && t.input.model == model)
            .ok_or_else(|| AdminError::not_found("维护项不存在"))?;
        match action {
            "refresh" => {
                if !doc.policy.enabled
                    || !target.input.enabled
                    || !self.account_policy(account_id).takeover
                    || !doc
                        .pools
                        .iter()
                        .any(|p| p.id == target.input.pool_id && p.enabled)
                {
                    return Err(AdminError::invalid(
                        "请先开启全局维护、账号接管、维护项和代理池",
                    ));
                }
                target.next_probe_at = Utc::now().timestamp();
                target.refresh_requested = true;
                target.wait_reason = "queued".to_owned();
                target.last_message = "已登记刷新；执行前仍需检查小时预算、并发和冷却".to_owned();
            }
            "pause" => target.input.enabled = false,
            "resume" => {
                target.input.enabled = true;
                target.next_probe_at = Utc::now().timestamp();
            }
            "revoke" => {
                target.current = None;
                target.alternatives.clear();
                target.input.enabled = false;
                target.last_message = "已撤销并暂停".to_owned();
            }
            _ => return Err(AdminError::invalid("不支持的操作")),
        }
        doc.generation += 1;
        self.commit(&mut runtime, doc, Some(context)).await?;
        Ok(Self::project(&runtime))
    }

    async fn test_pool(&self, id: &str) -> Result<TurnStatePoolTest, AdminError> {
        let _permit = self
            .test_slot
            .try_acquire()
            .map_err(|_| AdminError::conflict("代理测试正在运行"))?;
        let pool = self
            .runtime
            .lock()
            .await
            .document
            .pools
            .iter()
            .find(|p| p.id == id)
            .cloned()
            .ok_or_else(|| AdminError::not_found("请先保存代理池"))?;
        network::test(&pool, self).await
    }
}
