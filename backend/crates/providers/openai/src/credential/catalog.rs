//! Codex 账号 entitlement 的进程内快照，以及账号级 Redis 模型目录 cache。

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use gateway_core::account::{
    CredentialRevision, CredentialState, OpaqueProviderData, ProviderAccount, ProviderAccountId,
};
use gateway_core::engine::provider::ProviderCatalogGeneration;
use gateway_core::provider_ports::{
    ProviderCatalogCacheKey, ProviderCatalogCachePort, ProviderCatalogScope,
};
use gateway_core::routing::{
    FrozenAccountScope, ProviderKind, ProviderModelDescriptor, UpstreamModelId,
};
use secrecy::ExposeSecret;
use thiserror::Error;
use tokio::sync::{Notify, OnceCell};
use tokio::time::Instant;
use uuid::Uuid;

use super::repository::{CodexCredentialRepository, CredentialRepositoryError};
use crate::transport::profile::{CodexWireProfile, CodexWireProfileState};
use crate::transport::{CodexBackendClient, CodexCatalogModel, CodexRequestContext};

const MAX_RESPONSE_ETAG_BYTES: usize = 256;
const ACCOUNT_CATALOG_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_ACCOUNT_CATALOG_MODELS: usize = 2_048;
const MAX_CLIENT_CATALOGS: usize = 32;
// 保持原先 32 份、每份 1 MiB 的原生正文缓存预算，不把缓存容量变成上游目录合同
const MAX_CLIENT_CATALOG_CACHE_BYTES: usize = 32 * 1024 * 1024;
const MODEL_CATALOG_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONCURRENT_CATALOGS: usize = 4;
const CLIENT_CATALOG_ERROR_TTL: Duration = Duration::from_secs(5);

/// OpenAI 按账号、凭据版本与套餐隔离的模型目录作用域。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CodexCatalogScope(ProviderCatalogScope);

impl CodexCatalogScope {
    /// 从账号已验证的套餐事实构造目录作用域。
    pub fn for_account(account: &ProviderAccount) -> Result<Self, CodexCredentialCatalogError> {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(account.id().as_str());
        digest.update([0]);
        digest.update(
            account
                .plan_type()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase(),
        );
        ProviderCatalogScope::new(format!(
            "account:{}:{}",
            hex::encode(digest.finalize()),
            account.revision().get()
        ))
        .map(Self)
        .map_err(|_| CodexCredentialCatalogError::InvalidCredentialData)
    }

    /// 返回稳定的 Provider-owned 作用域。
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Redis TTL cache 中一条可重建的账号模型目录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexAccountCatalog {
    scope: CodexCatalogScope,
    observed_at: DateTime<Utc>,
    models: Vec<String>,
}

impl CodexAccountCatalog {
    #[must_use]
    pub const fn new(
        scope: CodexCatalogScope,
        observed_at: DateTime<Utc>,
        models: Vec<String>,
    ) -> Self {
        Self {
            scope,
            observed_at,
            models,
        }
    }

    #[must_use]
    pub const fn scope(&self) -> &CodexCatalogScope {
        &self.scope
    }

    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    #[must_use]
    pub fn models(&self) -> &[String] {
        &self.models
    }
}

#[derive(Clone)]
pub struct CodexCredentialCatalogSnapshot {
    observed_at: SystemTime,
    models: Vec<CodexCatalogModel>,
    scope_catalogs: BTreeMap<CodexCatalogScope, CatalogScopeSnapshot>,
}

#[derive(Clone, PartialEq, Eq)]
struct CatalogScopeSnapshot {
    models: Vec<String>,
    accounts: BTreeSet<ProviderAccountId>,
}

impl CodexCredentialCatalogSnapshot {
    #[must_use]
    pub const fn observed_at(&self) -> SystemTime {
        self.observed_at
    }

    #[must_use]
    pub fn models(&self) -> &[CodexCatalogModel] {
        &self.models
    }

    pub fn account_models(
        &self,
        account: &ProviderAccount,
    ) -> Result<Option<&[String]>, CodexCredentialCatalogError> {
        let scope = CodexCatalogScope::for_account(account)?;
        Ok(self
            .scope_catalogs
            .get(&scope)
            .map(|catalog| catalog.models.as_slice()))
    }

    /// 将 Provider 私有目录作用域投影为模型来源账号，供 Core 结合冻结政策过滤。
    #[must_use]
    pub fn model_catalog_accounts(&self) -> BTreeMap<&str, BTreeSet<ProviderAccountId>> {
        let mut accounts = BTreeMap::<&str, BTreeSet<ProviderAccountId>>::new();
        for catalog in self.scope_catalogs.values() {
            for model in &catalog.models {
                accounts
                    .entry(model.as_str())
                    .or_default()
                    .extend(catalog.accounts.iter().cloned());
            }
        }
        accounts
    }
}

impl fmt::Debug for CodexCredentialCatalogSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexCredentialCatalogSnapshot")
            .field("observed_at", &self.observed_at)
            .field("model_count", &self.models.len())
            .field("scope_count", &self.scope_catalogs.len())
            .finish()
    }
}

#[derive(Debug, Clone, Error)]
pub enum CodexCredentialCatalogError {
    #[error("Codex model catalog has no eligible account")]
    NoEligibleCredential,
    #[error("Codex model catalog account data is invalid")]
    InvalidCredentialData,
    #[error("Codex model catalog upstream query failed: {detail}")]
    Upstream { detail: String },
    #[error("Codex model catalog cache is unavailable")]
    Cache,
    #[error("Codex model catalog ETag is invalid")]
    InvalidEtag,
    #[error("Codex model catalog changed during refresh")]
    ConcurrentUpdate,
}

#[derive(Default)]
struct CatalogEtagState {
    applied: Option<String>,
    pending: Option<String>,
    inflight: Option<String>,
}

#[derive(Default)]
struct CatalogCacheState {
    revision: u64,
    generation: u64,
    snapshot: Option<CodexCredentialCatalogSnapshot>,
}

#[derive(PartialEq, Eq)]
struct ClientCatalogKey {
    account_id: ProviderAccountId,
    revision: CredentialRevision,
    plan: Option<String>,
    upstream_account_id: Option<String>,
    client_version: String,
    profile: CodexWireProfile,
}

type ClientCatalogResult = Result<Vec<ProviderModelDescriptor>, CodexCredentialCatalogError>;

struct CachedClientCatalog {
    // 负缓存从请求结束计时，不能让一次慢失败耗尽后续请求的退避窗口。
    completed_at: Instant,
    result: ClientCatalogResult,
}

impl CachedClientCatalog {
    fn native_bytes(&self) -> usize {
        self.result.as_ref().map_or(0, |models| {
            models
                .iter()
                .map(|model| match &model.content {
                    gateway_core::routing::ProviderModelContent::Native(payload) => {
                        payload.body().len()
                    }
                    gateway_core::routing::ProviderModelContent::Adapted(_) => 0,
                })
                .sum()
        })
    }
}

struct ClientCatalogEntry {
    key: ClientCatalogKey,
    created_at: Instant,
    value: Arc<OnceCell<CachedClientCatalog>>,
}

impl ClientCatalogEntry {
    fn is_fresh(&self) -> bool {
        let Some(value) = self.value.get() else {
            return self.created_at.elapsed() < ACCOUNT_CATALOG_CACHE_TTL;
        };
        let ttl = if value.result.is_err() {
            CLIENT_CATALOG_ERROR_TTL
        } else {
            ACCOUNT_CATALOG_CACHE_TTL
        };
        value.completed_at.elapsed() < ttl
    }
}

struct FetchedAccountModels {
    models: Vec<CodexCatalogModel>,
    etag: Option<String>,
}

struct FetchedCatalog {
    snapshot: CodexCredentialCatalogSnapshot,
    etags: Vec<String>,
    refresh_error: Option<CodexCredentialCatalogError>,
}

impl From<CredentialRepositoryError> for CodexCredentialCatalogError {
    fn from(_: CredentialRepositoryError) -> Self {
        Self::InvalidCredentialData
    }
}

#[derive(Clone)]
pub struct CodexCredentialCatalogService {
    repository: CodexCredentialRepository,
    profile: CodexWireProfileState,
    http: reqwest::Client,
    base_url: String,
    account_catalog_cache: Arc<dyn ProviderCatalogCachePort>,
    cache: Arc<RwLock<CatalogCacheState>>,
    client_catalogs: Arc<Mutex<Vec<ClientCatalogEntry>>>,
    etags: Arc<Mutex<CatalogEtagState>>,
    etag_notification: Arc<Notify>,
}

impl CodexCredentialCatalogService {
    pub fn new(
        repository: CodexCredentialRepository,
        profile: CodexWireProfileState,
        http: reqwest::Client,
        base_url: String,
        account_catalog_cache: Arc<dyn ProviderCatalogCachePort>,
    ) -> Self {
        Self {
            repository,
            profile,
            http,
            base_url,
            account_catalog_cache,
            cache: Arc::new(RwLock::new(CatalogCacheState::default())),
            client_catalogs: Arc::new(Mutex::new(Vec::new())),
            etags: Arc::new(Mutex::new(CatalogEtagState::default())),
            etag_notification: Arc::new(Notify::new()),
        }
    }

    /// 合并 Key 范围内各账号的模型目录；同一模型优先保留 OAuth 原生对象。
    /// 两类账号各限并发 4，共享 15 秒预算；单账号查询合并并复用有界缓存。
    pub async fn client_model_catalog(
        &self,
        scope: &FrozenAccountScope,
        client_version: &str,
    ) -> ClientCatalogResult {
        let now = SystemTime::now();
        let mut accounts = self.repository.list_for_provider().await?;
        accounts
            .retain(|account| scope.allows(account.id()) && eligible_catalog_account(account, now));
        accounts.sort_by(|left, right| left.id().cmp(right.id()));
        let (api, oauth): (Vec<_>, Vec<_>) =
            accounts.into_iter().enumerate().partition(|(_, account)| {
                account.authentication_kind() == super::CODEX_AUTHENTICATION_KIND_API_KEY
            });
        let read = |(index, account): (usize, ProviderAccount)| async move {
            let result = self
                .cached_client_account_catalog(&account, client_version)
                .await;
            (index, account, result)
        };
        let mut catalogs = stream::select(
            stream::iter(oauth)
                .map(read)
                .buffer_unordered(MAX_CONCURRENT_CATALOGS),
            stream::iter(api)
                .map(read)
                .buffer_unordered(MAX_CONCURRENT_CATALOGS),
        );
        let deadline = Instant::now() + MODEL_CATALOG_TIMEOUT;
        let mut selected = BTreeMap::new();
        let mut available = false;
        let mut last_error = CodexCredentialCatalogError::NoEligibleCredential;
        loop {
            let (index, account, models) =
                match tokio::time::timeout_at(deadline, catalogs.next()).await {
                    Ok(Some((index, account, Ok(models)))) => (index, account, models),
                    Ok(Some((_, _, Err(error)))) => {
                        last_error = error;
                        continue;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        last_error = CodexCredentialCatalogError::Upstream {
                            detail: "model catalog timed out".to_owned(),
                        };
                        break;
                    }
                };
            available = true;
            let api = account.authentication_kind() == super::CODEX_AUTHENTICATION_KIND_API_KEY;
            for (position, model) in models.into_iter().enumerate() {
                if !account.model_access().allows(model.model.as_str())
                    || !scope.allows_model(account.id(), model.model.as_str())
                {
                    continue;
                }
                let order = (api, index, position);
                match selected.entry(model.model.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert((order, model));
                    }
                    Entry::Occupied(mut entry) if order < entry.get().0 => {
                        entry.insert((order, model));
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        if !available {
            return Err(last_error);
        }
        let mut models: Vec<_> = selected.into_values().collect();
        models.sort_unstable_by_key(|(order, _)| *order);
        Ok(models.into_iter().map(|(_, model)| model).collect())
    }

    async fn cached_client_account_catalog(
        &self,
        account: &ProviderAccount,
        client_version: &str,
    ) -> ClientCatalogResult {
        let profile = self.profile.snapshot();
        let key = ClientCatalogKey {
            account_id: account.id().clone(),
            revision: account.revision(),
            plan: account.plan_type().map(str::to_owned),
            upstream_account_id: account.upstream_account_id().map(str::to_owned),
            client_version: client_version.to_owned(),
            profile: profile.clone(),
        };
        let value = {
            let mut cache = self
                .client_catalogs
                .lock()
                .map_err(|_| CodexCredentialCatalogError::Cache)?;
            cache.retain(ClientCatalogEntry::is_fresh);
            if let Some(entry) = cache.iter().find(|entry| entry.key == key) {
                Arc::clone(&entry.value)
            } else {
                // 不驱逐正在读取的目录，避免并发请求绕过缓存容量限制制造额外出站。
                if cache.len() >= MAX_CLIENT_CATALOGS {
                    let Some(index) = cache.iter().position(|entry| entry.value.initialized())
                    else {
                        return Err(CodexCredentialCatalogError::Cache);
                    };
                    cache.remove(index);
                }
                let value = Arc::new(OnceCell::new());
                cache.push(ClientCatalogEntry {
                    key,
                    created_at: Instant::now(),
                    value: Arc::clone(&value),
                });
                value
            }
        };
        let result = value
            .get_or_init(|| async {
                // 冻结画像，确保实际请求与缓存身份一致；不传递客户端 Bearer 给上游。
                let client = CodexBackendClient::new(
                    self.http.clone(),
                    self.base_url.clone(),
                    CodexWireProfileState::new(profile),
                );
                let result = if account.authentication_kind()
                    == super::CODEX_AUTHENTICATION_KIND_API_KEY
                {
                    self.cached_or_refresh_account_catalog(account)
                        .await
                        .and_then(|catalog| {
                            catalog
                                .models()
                                .iter()
                                .map(|id| {
                                    Ok(ProviderModelDescriptor {
                                        model: UpstreamModelId::new(id.clone()).map_err(|_| {
                                            CodexCredentialCatalogError::InvalidCredentialData
                                        })?,
                                        content:
                                            gateway_core::routing::ProviderModelContent::Adapted(
                                                gateway_core::routing::ModelPresentation::new(
                                                    Some(id.clone()),
                                                    None,
                                                )
                                                .with_agent_tools(true, false),
                                            ),
                                    })
                                })
                                .collect()
                        })
                } else {
                    self.fetch_account_models(&client, account, Some(client_version))
                        .await
                        .map(|fetched| {
                            fetched
                                .models
                                .into_iter()
                                .map(|model| ProviderModelDescriptor {
                                    model: model.request_model().clone(),
                                    content: gateway_core::routing::ProviderModelContent::Native(
                                        model.document().clone(),
                                    ),
                                })
                                .collect()
                        })
                };
                CachedClientCatalog {
                    completed_at: Instant::now(),
                    result,
                }
            })
            .await;
        // 完成中的 OnceCell 仍由并发读者持有；移出缓存只影响后续查询，不丢弃本次成功结果
        let mut cache = self
            .client_catalogs
            .lock()
            .map_err(|_| CodexCredentialCatalogError::Cache)?;
        if result.native_bytes() > MAX_CLIENT_CATALOG_CACHE_BYTES {
            cache.retain(|entry| !Arc::ptr_eq(&entry.value, &value));
        } else {
            let mut bytes: usize = cache
                .iter()
                .filter_map(|entry| entry.value.get())
                .map(CachedClientCatalog::native_bytes)
                .sum();
            while bytes > MAX_CLIENT_CATALOG_CACHE_BYTES {
                let Some(index) = cache.iter().position(|entry| entry.value.initialized()) else {
                    break;
                };
                let entry = cache.remove(index);
                bytes -= entry
                    .value
                    .get()
                    .map_or(0, CachedClientCatalog::native_bytes);
            }
        }
        drop(cache);
        result.result.clone()
    }

    #[must_use]
    pub fn catalog_generation(&self) -> ProviderCatalogGeneration {
        let cache = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ProviderCatalogGeneration::new(cache.generation)
    }

    pub fn cached(
        &self,
    ) -> Result<Option<CodexCredentialCatalogSnapshot>, CodexCredentialCatalogError> {
        Ok(self
            .cache
            .read()
            .map_err(|_| CodexCredentialCatalogError::Cache)?
            .snapshot
            .as_ref()
            .cloned())
    }

    /// 读取账号当前凭据的模型 entitlement；不触发网络。
    pub fn cached_account_models(
        &self,
        account: &ProviderAccount,
    ) -> Result<Option<Vec<String>>, CodexCredentialCatalogError> {
        let Some(snapshot) = self.cached()? else {
            return Ok(None);
        };
        snapshot
            .account_models(account)
            .map(|models| models.map(<[String]>::to_vec))
    }

    pub fn observed_model_support(
        &self,
        account: &ProviderAccount,
        model: &str,
    ) -> Result<Option<bool>, CodexCredentialCatalogError> {
        let Some(snapshot) = self.cached()? else {
            return Ok(None);
        };
        // A missing scope is unobserved, not an authoritative empty model list.
        Ok(snapshot
            .account_models(account)?
            .map(|models| models.iter().any(|candidate| candidate == model)))
    }

    /// 优先读取账号目录 cache；缺失时才用当前账号自身实时填充。
    pub async fn cached_or_refresh_account_catalog(
        &self,
        account: &ProviderAccount,
    ) -> Result<CodexAccountCatalog, CodexCredentialCatalogError> {
        let scope = CodexCatalogScope::for_account(account)?;
        if let Some(catalog) = self.read_cached_catalog(&scope).await? {
            return Ok(catalog);
        }
        self.fetch_and_cache_account_catalog(account).await
    }

    /// 实时刷新指定账号本身的模型集合，并覆盖可重建 Redis cache。
    pub async fn refresh_account_catalog(
        &self,
        account_id: &ProviderAccountId,
    ) -> Result<CodexAccountCatalog, CodexCredentialCatalogError> {
        let account = self
            .repository
            .store()
            .get_account(account_id)
            .await
            .map_err(|_| CodexCredentialCatalogError::InvalidCredentialData)?
            .filter(|account| account.provider().as_str() == "openai")
            .ok_or(CodexCredentialCatalogError::NoEligibleCredential)?;
        self.fetch_and_cache_account_catalog(&account).await
    }

    async fn fetch_and_cache_account_catalog(
        &self,
        account: &ProviderAccount,
    ) -> Result<CodexAccountCatalog, CodexCredentialCatalogError> {
        let scope = CodexCatalogScope::for_account(account)?;
        let client = CodexBackendClient::new(
            self.http.clone(),
            self.base_url.clone(),
            self.profile.clone(),
        );
        let fetched = self.fetch_account_models(&client, account, None).await?;
        let catalog = CodexAccountCatalog::new(scope, Utc::now(), model_ids(&fetched.models));
        self.replace_account_catalog(&catalog).await?;
        Ok(catalog)
    }

    /// 读取当前账号自身的目录 cache，不触发上游请求。
    pub async fn read_account_catalog(
        &self,
        account: &ProviderAccount,
    ) -> Result<Option<CodexAccountCatalog>, CodexCredentialCatalogError> {
        self.read_cached_catalog(&CodexCatalogScope::for_account(account)?)
            .await
    }

    pub async fn synchronize(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        if let Some(cached) = self.cached()? {
            return Ok(cached);
        }
        let cache_revision = self.cache_revision()?;
        let fetched = self.fetch_catalog().await?;
        self.commit_catalog(cache_revision, fetched)
    }

    async fn fetch_catalog(&self) -> Result<FetchedCatalog, CodexCredentialCatalogError> {
        let previous = self.cached()?;
        let accounts = self.repository.list_for_provider().await?;
        let mut groups: Vec<_> = catalog_candidates_by_scope(accounts)?.into_iter().collect();
        groups.sort_by(|(_, left), (_, right)| left.id().cmp(right.id()));
        let client = CodexBackendClient::new(
            self.http.clone(),
            self.base_url.clone(),
            self.profile.clone(),
        );
        let mut union = BTreeMap::<String, CodexCatalogModel>::new();
        let mut union_order = Vec::new();
        let mut scope_catalogs = BTreeMap::new();
        let mut etags = Vec::new();
        let mut last_error = None;
        let mut fresh_scopes = 0;
        let client = &client;
        let mut catalogs = stream::iter(groups)
            .map(|(scope, account)| async move {
                let result = self.fetch_account_models(client, &account, None).await;
                (scope, account, result)
            })
            .buffered(MAX_CONCURRENT_CATALOGS);
        while let Some((scope, account, result)) = catalogs.next().await {
            let accounts = BTreeSet::from([account.id().clone()]);
            let fetched = match result {
                Ok(fetched) => {
                    fresh_scopes += 1;
                    self.replace_account_catalog(&CodexAccountCatalog::new(
                        scope.clone(),
                        Utc::now(),
                        model_ids(&fetched.models),
                    ))
                    .await?;
                    fetched
                }
                Err(error) => {
                    let retained = previous.as_ref().and_then(|snapshot| {
                        snapshot
                            .scope_catalogs
                            .get(&scope)
                            .map(|catalog| FetchedAccountModels {
                                models: snapshot
                                    .models
                                    .iter()
                                    .filter(|model| {
                                        catalog
                                            .models
                                            .iter()
                                            .any(|id| id == model.request_model().as_str())
                                    })
                                    .cloned()
                                    .collect(),
                                etag: None,
                            })
                    });
                    // Log only scope and classification: upstream text can contain secrets.
                    tracing::warn!(
                        scope = scope.as_str(),
                        retained = retained.is_some(),
                        "OpenAI model catalog scope refresh failed; preserving prior evidence or unknown support"
                    );
                    last_error = Some(error);
                    let Some(retained) = retained else {
                        continue;
                    };
                    // Retain only this exact scope, with current account membership below.
                    // Do not refresh its Redis TTL/observedAt or borrow another plan's models.
                    retained
                }
            };
            let entitlement = model_ids(&fetched.models);
            for model in &fetched.models {
                let id = model.request_model().as_str().to_owned();
                match union.entry(id) {
                    Entry::Vacant(entry) => {
                        union_order.push(entry.key().clone());
                        entry.insert(model.clone());
                    }
                    Entry::Occupied(mut entry) => {
                        // 原生目录含完整能力证据，同名 API ID 不能覆盖它，也不能跨来源拼字段。
                        if model.document().protocol() == "codex"
                            && entry.get().document().protocol() != "codex"
                        {
                            entry.insert(model.clone());
                        }
                    }
                }
            }
            scope_catalogs.insert(
                scope,
                CatalogScopeSnapshot {
                    models: entitlement,
                    accounts,
                },
            );
            etags.extend(fetched.etag);
        }
        if fresh_scopes == 0
            && let Some(error) = last_error.as_ref()
        {
            return Err(error.clone());
        }
        let observed_at = SystemTime::now();
        let snapshot = CodexCredentialCatalogSnapshot {
            observed_at,
            models: union_order
                .into_iter()
                .filter_map(|id| union.remove(&id))
                .collect(),
            scope_catalogs,
        };
        Ok(FetchedCatalog {
            snapshot,
            etags,
            refresh_error: last_error,
        })
    }

    async fn replace_account_catalog(
        &self,
        catalog: &CodexAccountCatalog,
    ) -> Result<(), CodexCredentialCatalogError> {
        let key = self.account_catalog_key(catalog.scope())?;
        let document = encode_account_catalog(catalog);
        self.account_catalog_cache
            .replace(&key, &document, ACCOUNT_CATALOG_CACHE_TTL)
            .await
            .map_err(|_| CodexCredentialCatalogError::Cache)
    }

    async fn read_cached_catalog(
        &self,
        scope: &CodexCatalogScope,
    ) -> Result<Option<CodexAccountCatalog>, CodexCredentialCatalogError> {
        let key = self.account_catalog_key(scope)?;
        self.account_catalog_cache
            .read(&key)
            .await
            .map_err(|_| CodexCredentialCatalogError::Cache)?
            .map(|document| decode_account_catalog(scope, document))
            .transpose()
    }

    fn account_catalog_key(
        &self,
        scope: &CodexCatalogScope,
    ) -> Result<ProviderCatalogCacheKey, CodexCredentialCatalogError> {
        let provider_kind =
            ProviderKind::new("openai").map_err(|_| CodexCredentialCatalogError::Cache)?;
        Ok(ProviderCatalogCacheKey::new(provider_kind, scope.0.clone()))
    }

    async fn fetch_account_models(
        &self,
        client: &CodexBackendClient,
        account: &ProviderAccount,
        client_version: Option<&str>,
    ) -> Result<FetchedAccountModels, CodexCredentialCatalogError> {
        tokio::time::timeout(MODEL_CATALOG_TIMEOUT, async {
            let credential = self
                .repository
                .load_runtime_credential(account)
                .await
                .map_err(|_| CodexCredentialCatalogError::InvalidCredentialData)?;
            let request_id = format!("catalog_{}", Uuid::now_v7().simple());
            let authorization = credential
                .authentication
                .authorization_header()
                .map_err(|_| CodexCredentialCatalogError::InvalidCredentialData)?;
            let result = client
                .for_account(account)
                .map_err(|error| CodexCredentialCatalogError::Upstream {
                    detail: error.to_string(),
                })?
                .with_authentication(&credential.authentication)
                .fetch_models_with_context(
                    CodexRequestContext::auxiliary(
                        authorization.expose_secret(),
                        account.upstream_account_id(),
                        &request_id,
                        None,
                    ),
                    client_version,
                )
                .await;
            let snapshot = result.map_err(|error| CodexCredentialCatalogError::Upstream {
                detail: error.to_string(),
            })?;
            Ok(FetchedAccountModels {
                models: snapshot.models().to_vec(),
                etag: snapshot.etag().map(str::to_owned),
            })
        })
        .await
        .map_err(|_| CodexCredentialCatalogError::Upstream {
            detail: "model catalog timed out".to_owned(),
        })?
    }

    pub fn invalidate(&self) -> Result<(), CodexCredentialCatalogError> {
        let mut cache = self
            .cache
            .write()
            .map_err(|_| CodexCredentialCatalogError::Cache)?;
        if cache.snapshot.take().is_some() {
            cache.generation = cache.generation.saturating_add(1);
        }
        cache.revision = cache.revision.saturating_add(1);
        drop(cache);
        self.invalidate_client_catalogs()?;
        Ok(())
    }

    fn invalidate_client_catalogs(&self) -> Result<(), CodexCredentialCatalogError> {
        self.client_catalogs
            .lock()
            .map_err(|_| CodexCredentialCatalogError::Cache)?
            .clear();
        Ok(())
    }

    /// 记录普通 Responses 响应声明的目录版本；相同版本只触发一次。
    pub fn observe_response_etag(&self, etag: &str) -> Result<bool, CodexCredentialCatalogError> {
        validate_response_etag(etag)?;
        let changed = {
            let mut state = self
                .etags
                .lock()
                .map_err(|_| CodexCredentialCatalogError::Cache)?;
            let already_observed = state.applied.as_deref() == Some(etag)
                || state.pending.as_deref() == Some(etag)
                || state.inflight.as_deref() == Some(etag);
            if already_observed {
                false
            } else {
                state.pending = Some(etag.to_owned());
                true
            }
        };
        if changed {
            self.invalidate_client_catalogs()?;
            self.etag_notification.notify_one();
        }
        Ok(changed)
    }

    /// 等待并认领一次需要强制刷新的 Provider 目录。
    pub async fn wait_for_etag_refresh(&self) {
        loop {
            if self.begin_pending_etag_refresh() {
                return;
            }
            self.etag_notification.notified().await;
        }
    }

    /// 立即刷新所有账号目录，但不接管 ETag daemon 已认领的刷新状态。
    ///
    /// 周期 worker 与 ETag daemon 可以同时请求目录；只有后者可以完成
    /// `inflight` ETag 的状态转换，避免周期刷新错误地确认另一个请求的版本。
    pub async fn refresh_catalogs(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        self.refresh_inner().await
    }

    /// 忽略当前 cache，按已认领的 ETag 变化强制生成一份完整新快照。
    pub async fn refresh(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        let result = self.refresh_inner().await;
        self.finish_etag_refresh(result.is_ok())?;
        result
    }

    async fn refresh_inner(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        let cache_revision = self.cache_revision()?;
        let fetched = self.fetch_catalog().await?;
        let error = fetched.refresh_error.clone();
        let snapshot = self.commit_catalog(cache_revision, fetched)?;
        // Healthy scopes can advance, but incomplete refreshes must not acknowledge
        // an ETag or be reported as a successful background reconciliation.
        match error {
            Some(error) => Err(error),
            None => Ok(snapshot),
        }
    }

    fn record_applied_catalog_etags(
        &self,
        etags: impl IntoIterator<Item = String>,
    ) -> Result<(), CodexCredentialCatalogError> {
        let mut distinct = etags.into_iter().collect::<std::collections::BTreeSet<_>>();
        if distinct.len() != 1 {
            return Ok(());
        }
        let Some(etag) = distinct.pop_first() else {
            return Ok(());
        };
        validate_response_etag(&etag)?;
        self.etags
            .lock()
            .map_err(|_| CodexCredentialCatalogError::Cache)?
            .applied = Some(etag);
        Ok(())
    }

    fn begin_pending_etag_refresh(&self) -> bool {
        let Ok(mut state) = self.etags.lock() else {
            return false;
        };
        let Some(pending) = state.pending.take() else {
            return false;
        };
        state.inflight = Some(pending);
        true
    }

    fn finish_etag_refresh(&self, succeeded: bool) -> Result<(), CodexCredentialCatalogError> {
        let should_retry = {
            let mut state = self
                .etags
                .lock()
                .map_err(|_| CodexCredentialCatalogError::Cache)?;
            let Some(etag) = state.inflight.take() else {
                return Ok(());
            };
            if succeeded {
                state.applied = Some(etag);
                false
            } else {
                if state.pending.is_none() {
                    state.pending = Some(etag);
                }
                true
            }
        };
        if should_retry {
            self.etag_notification.notify_one();
        }
        Ok(())
    }

    fn cache_revision(&self) -> Result<u64, CodexCredentialCatalogError> {
        self.cache
            .read()
            .map(|cache| cache.revision)
            .map_err(|_| CodexCredentialCatalogError::Cache)
    }

    fn commit_catalog(
        &self,
        expected_cache_revision: u64,
        fetched: FetchedCatalog,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        let snapshot = fetched.snapshot;
        let mut cache = self
            .cache
            .write()
            .map_err(|_| CodexCredentialCatalogError::Cache)?;
        if cache.revision != expected_cache_revision {
            return Err(CodexCredentialCatalogError::ConcurrentUpdate);
        }
        let changed = cache
            .snapshot
            .as_ref()
            .is_none_or(|existing| !same_catalog(existing, &snapshot));
        cache.snapshot = Some(snapshot.clone());
        cache.revision = cache.revision.saturating_add(1);
        if changed {
            cache.generation = cache.generation.saturating_add(1);
        }
        drop(cache);
        if changed {
            self.invalidate_client_catalogs()?;
        }
        if fetched.refresh_error.is_none() {
            self.record_applied_catalog_etags(fetched.etags)?;
        }
        Ok(snapshot)
    }
}

fn model_ids(models: &[CodexCatalogModel]) -> Vec<String> {
    models
        .iter()
        .map(|model| model.request_model().as_str().to_owned())
        .collect()
}

fn catalog_candidates_by_scope(
    accounts: Vec<ProviderAccount>,
) -> Result<BTreeMap<CodexCatalogScope, ProviderAccount>, CodexCredentialCatalogError> {
    let now = SystemTime::now();
    let mut groups = BTreeMap::<CodexCatalogScope, ProviderAccount>::new();
    for account in accounts
        .into_iter()
        .filter(|account| eligible_catalog_account(account, now))
    {
        let scope = CodexCatalogScope::for_account(&account)?;
        groups.insert(scope, account);
    }
    if groups.is_empty() {
        return Err(CodexCredentialCatalogError::NoEligibleCredential);
    }
    Ok(groups)
}

fn encode_account_catalog(catalog: &CodexAccountCatalog) -> OpaqueProviderData {
    let mut document = serde_json::Map::new();
    document.insert("version".to_owned(), serde_json::Value::from(1));
    document.insert(
        "scope".to_owned(),
        serde_json::Value::String(catalog.scope().as_str().to_owned()),
    );
    document.insert(
        "observedAt".to_owned(),
        serde_json::Value::String(catalog.observed_at().to_rfc3339()),
    );
    document.insert(
        "models".to_owned(),
        serde_json::Value::Array(
            catalog
                .models()
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    OpaqueProviderData::new(document)
}

fn decode_account_catalog(
    scope: &CodexCatalogScope,
    document: OpaqueProviderData,
) -> Result<CodexAccountCatalog, CodexCredentialCatalogError> {
    let mut fields = document.into_inner();
    if fields.remove("version").and_then(|value| value.as_u64()) != Some(1) {
        return Err(CodexCredentialCatalogError::Cache);
    }
    if fields
        .remove("scope")
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .as_deref()
        != Some(scope.as_str())
    {
        return Err(CodexCredentialCatalogError::Cache);
    }
    let observed_at = fields
        .remove("observedAt")
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or(CodexCredentialCatalogError::Cache)?;
    let values = fields
        .remove("models")
        .and_then(|value| value.as_array().cloned())
        .filter(|models| !models.is_empty() && models.len() <= MAX_ACCOUNT_CATALOG_MODELS)
        .ok_or(CodexCredentialCatalogError::Cache)?;
    if !fields.is_empty() {
        return Err(CodexCredentialCatalogError::Cache);
    }
    let mut seen = BTreeSet::new();
    let mut models = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or(CodexCredentialCatalogError::Cache)?;
        let model = UpstreamModelId::new(value).map_err(|_| CodexCredentialCatalogError::Cache)?;
        if !seen.insert(model.as_str().to_owned()) {
            return Err(CodexCredentialCatalogError::Cache);
        }
        models.push(model.as_str().to_owned());
    }
    Ok(CodexAccountCatalog::new(scope.clone(), observed_at, models))
}

fn same_catalog(
    left: &CodexCredentialCatalogSnapshot,
    right: &CodexCredentialCatalogSnapshot,
) -> bool {
    left.models == right.models && left.scope_catalogs == right.scope_catalogs
}

fn validate_response_etag(etag: &str) -> Result<(), CodexCredentialCatalogError> {
    if etag.is_empty() || etag.len() > MAX_RESPONSE_ETAG_BYTES || etag.chars().any(char::is_control)
    {
        return Err(CodexCredentialCatalogError::InvalidEtag);
    }
    Ok(())
}

fn eligible_catalog_account(account: &ProviderAccount, now: SystemTime) -> bool {
    account.enabled()
        && account
            .access_token_expires_at()
            .is_none_or(|expires_at| expires_at > now)
        && matches!(
            account.credential_state(),
            CredentialState::Unknown | CredentialState::Ready
        )
}
