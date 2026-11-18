//! Codex 账号 entitlement 的进程内快照，以及账号级 Redis 模型目录 cache。

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use gateway_core::account::{
    AccountCatalogObservation, CredentialState, OpaqueProviderData, ProviderAccount,
    ProviderAccountId,
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
use uuid::Uuid;

use super::repository::{CodexCredentialRepository, CredentialRepositoryError};
use crate::transport::profile::CodexWireProfileState;
use crate::transport::{CodexBackendClient, CodexCatalogModel, CodexRequestContext};

const MAX_RESPONSE_ETAG_BYTES: usize = 256;
const ACCOUNT_CATALOG_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_ACCOUNT_CATALOG_MODELS: usize = 2_048;
const MODEL_CATALOG_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONCURRENT_CATALOGS: usize = 4;
const CATALOG_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
const SCHEDULING_CATALOG_MAX_AGE: Duration = Duration::from_secs(30 * 60);

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
    observed_at: SystemTime,
    content: Arc<Vec<CodexCatalogModel>>,
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

type ClientCatalogResult = Result<Vec<ProviderModelDescriptor>, CodexCredentialCatalogError>;

struct FetchedAccountModels {
    observed_at: SystemTime,
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
    restored: Arc<OnceCell<()>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
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
            restored: Arc::new(OnceCell::new()),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            etags: Arc::new(Mutex::new(CatalogEtagState::default())),
            etag_notification: Arc::new(Notify::new()),
        }
    }

    /// 只读取当前账号目录证据；模型请求不等待上游目录网络调用。
    pub async fn client_model_catalog(
        &self,
        scope: &FrozenAccountScope,
        _client_version: &str,
    ) -> ClientCatalogResult {
        let snapshot = self.restored_snapshot().await?;
        let mut accounts = self.repository.list_for_provider().await?;
        accounts.retain(|a| scope.allows(a.id()) && eligible_catalog_account(a, SystemTime::now()));
        accounts.sort_by(|a, b| a.id().cmp(b.id()));
        let mut selected = BTreeMap::new();
        let mut available = false;
        for (account_index, account) in accounts.into_iter().enumerate() {
            let key = CodexCatalogScope::for_account(&account)?;
            let Some(catalog) = snapshot.scope_catalogs.get(&key) else {
                self.schedule_refresh();
                continue;
            };
            available = true;
            for (model_index, model) in catalog.content.iter().enumerate() {
                let id = model.request_model();
                if !account.model_access().allows(id.as_str())
                    || !scope.allows_model(account.id(), id.as_str())
                {
                    continue;
                }
                let native = model.document().protocol() == "codex";
                let descriptor = ProviderModelDescriptor {
                    model: id.clone(),
                    content: if native {
                        gateway_core::routing::ProviderModelContent::Native(
                            model.document().clone(),
                        )
                    } else {
                        gateway_core::routing::ProviderModelContent::Adapted(
                            gateway_core::routing::ModelPresentation::new(
                                Some(model.display_name().to_owned()),
                                None,
                            )
                            .with_agent_tools(true, false),
                        )
                    },
                };
                match selected.entry(id.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert((native, account_index, model_index, descriptor));
                    }
                    Entry::Occupied(mut entry) if native && !entry.get().0 => {
                        entry.insert((native, account_index, model_index, descriptor));
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        if !available {
            return Err(CodexCredentialCatalogError::NoEligibleCredential);
        }
        let mut selected: Vec<_> = selected.into_values().collect();
        selected.sort_by_key(|(native, account, model, _)| (!native, *account, *model));
        Ok(selected.into_iter().map(|(_, _, _, model)| model).collect())
    }

    fn schedule_refresh(&self) {
        self.etag_notification.notify_one();
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
        let scope = CodexCatalogScope::for_account(account)?;
        Ok(snapshot
            .scope_catalogs
            .get(&scope)
            .filter(|entry| {
                entry
                    .observed_at
                    .elapsed()
                    .is_ok_and(|age| age < SCHEDULING_CATALOG_MAX_AGE)
            })
            .map(|entry| entry.models.iter().any(|candidate| candidate == model)))
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
        let _refresh = self.refresh_lock.lock().await;
        let mut snapshot = match self.restored_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CodexCredentialCatalogError::NoEligibleCredential) => {
                snapshot_from_scopes(BTreeMap::new())
            }
            Err(error) => return Err(error),
        };
        let revision = self.cache_revision()?;
        let scope = CodexCatalogScope::for_account(account)?;
        let client = CodexBackendClient::new(
            self.http.clone(),
            self.base_url.clone(),
            CodexWireProfileState::new(self.profile.snapshot()),
        );
        let fetched = self.fetch_account_models(&client, account, None).await?;
        let catalog = CodexAccountCatalog::new(
            scope.clone(),
            fetched.observed_at.into(),
            model_ids(&fetched.models),
        );
        self.replace_account_catalog(&catalog).await?;
        snapshot
            .scope_catalogs
            .retain(|_, entry| !entry.accounts.contains(account.id()));
        snapshot.scope_catalogs.insert(
            scope,
            CatalogScopeSnapshot {
                models: catalog.models.clone(),
                accounts: BTreeSet::from([account.id().clone()]),
                observed_at: fetched.observed_at,
                content: Arc::new(fetched.models),
            },
        );
        self.commit_catalog(
            revision,
            FetchedCatalog {
                snapshot: snapshot_from_scopes(snapshot.scope_catalogs),
                etags: Vec::new(),
                refresh_error: None,
            },
        )?;
        Ok(catalog)
    }

    /// 读取当前账号自身的目录 cache，不触发上游请求。
    pub async fn read_account_catalog(
        &self,
        account: &ProviderAccount,
    ) -> Result<Option<CodexAccountCatalog>, CodexCredentialCatalogError> {
        let scope = CodexCatalogScope::for_account(account)?;
        if let Ok(Some(catalog)) = self.read_cached_catalog(&scope).await {
            return Ok(Some(catalog));
        }
        let snapshot = match self.restored_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CodexCredentialCatalogError::NoEligibleCredential) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(snapshot.scope_catalogs.get(&scope).map(|entry| {
            CodexAccountCatalog::new(
                scope.clone(),
                entry.observed_at.into(),
                entry.models.clone(),
            )
        }))
    }

    pub async fn restored_snapshot(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        if let Some(cached) = self.cached()? {
            return Ok(cached);
        }
        self.restored
            .get_or_try_init(|| async {
                if self.cached()?.is_none() {
                    self.restore_catalogs().await?;
                }
                Ok::<(), CodexCredentialCatalogError>(())
            })
            .await?;
        self.schedule_refresh();
        self.cached()?
            .ok_or(CodexCredentialCatalogError::NoEligibleCredential)
    }

    /// 显式目录同步；普通模型能力与 picker 请求仅使用 restored_snapshot。
    pub async fn synchronize(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        if let Some(snapshot) = self.cached()? {
            return Ok(snapshot);
        }
        let _refresh = self.refresh_lock.lock().await;
        let revision = self.cache_revision()?;
        let fetched = self.fetch_catalog(true).await?;
        self.commit_catalog(revision, fetched)
    }

    async fn restore_catalogs(&self) -> Result<(), CodexCredentialCatalogError> {
        let revision = self.cache_revision()?;
        let provider =
            ProviderKind::new("openai").map_err(|_| CodexCredentialCatalogError::Cache)?;
        let accounts = self.repository.list_for_provider().await?;
        let accounts: BTreeMap<_, _> = accounts
            .into_iter()
            .filter(|a| eligible_catalog_account(a, SystemTime::now()))
            .map(|a| (a.id().clone(), a))
            .collect();
        let stored = self
            .repository
            .store()
            .load_account_catalogs(&provider)
            .await
            .map_err(|_| CodexCredentialCatalogError::Cache)?;
        let mut scopes = BTreeMap::new();
        for group in stored {
            let Ok(decoded) = crate::transport::CodexModelCatalogSnapshot::restore(&group.document)
            else {
                continue;
            };
            let content = Arc::new(decoded.models().to_vec());
            for observation in group.accounts {
                let Some(account) = accounts.get(&observation.account_id).filter(|a| {
                    a.revision() == observation.revision
                        && a.plan_type() == observation.plan.as_deref()
                }) else {
                    continue;
                };
                scopes.insert(
                    CodexCatalogScope::for_account(account)?,
                    CatalogScopeSnapshot {
                        models: model_ids(&content),
                        accounts: BTreeSet::from([account.id().clone()]),
                        observed_at: observation.observed_at,
                        content: Arc::clone(&content),
                    },
                );
            }
        }
        // No observation is different from an observed empty catalog. Let Core
        // retain its existing cold-start catalog until a background fetch succeeds.
        if scopes.is_empty() {
            return Ok(());
        }
        let mut union = BTreeMap::new();
        for entry in scopes.values() {
            for model in entry.content.iter() {
                match union.entry(model.request_model().as_str().to_owned()) {
                    Entry::Vacant(entry) => {
                        entry.insert(model.clone());
                    }
                    Entry::Occupied(mut entry)
                        if model.document().protocol() == "codex"
                            && entry.get().document().protocol() != "codex" =>
                    {
                        entry.insert(model.clone());
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        self.commit_catalog(
            revision,
            FetchedCatalog {
                snapshot: CodexCredentialCatalogSnapshot {
                    observed_at: SystemTime::now(),
                    models: union.into_values().collect(),
                    scope_catalogs: scopes,
                },
                etags: vec![],
                refresh_error: None,
            },
        )?;
        Ok(())
    }

    async fn fetch_catalog(
        &self,
        force: bool,
    ) -> Result<FetchedCatalog, CodexCredentialCatalogError> {
        let previous = self.cached()?;
        let accounts = self.repository.list_for_provider().await?;
        let mut groups: Vec<_> = catalog_candidates_by_scope(accounts)?.into_iter().collect();
        groups.sort_by(|(_, left), (_, right)| left.id().cmp(right.id()));
        let client = CodexBackendClient::new(
            self.http.clone(),
            self.base_url.clone(),
            CodexWireProfileState::new(self.profile.snapshot()),
        );
        let mut union = BTreeMap::<String, CodexCatalogModel>::new();
        let mut union_order = Vec::new();
        let mut scope_catalogs = BTreeMap::new();
        let mut etags = Vec::new();
        let mut last_error = None;
        let mut fresh_scopes = 0;
        let client = &client;
        let previous_ref = previous.as_ref();
        let catalogs = stream::iter(groups)
            .then(|entry| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                entry
            })
            .map(|(scope, account)| async move {
                let cached = previous_ref.and_then(|s| s.scope_catalogs.get(&scope));
                let result = if !force
                    && let Some(entry) = cached.filter(|e| {
                        e.observed_at
                            .elapsed()
                            .is_ok_and(|age| age < CATALOG_REFRESH_INTERVAL)
                    }) {
                    Ok(FetchedAccountModels {
                        models: entry.content.as_ref().clone(),
                        etag: None,
                        observed_at: entry.observed_at,
                    })
                } else {
                    self.fetch_account_models(client, &account, None).await
                };
                (scope, account, result)
            })
            .buffered(MAX_CONCURRENT_CATALOGS);
        futures::pin_mut!(catalogs);
        while let Some((scope, account, result)) = catalogs.next().await {
            let accounts = BTreeSet::from([account.id().clone()]);
            let fetched = match result {
                Ok(fetched) => {
                    fresh_scopes += 1;
                    self.replace_account_catalog(&CodexAccountCatalog::new(
                        scope.clone(),
                        fetched.observed_at.into(),
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
                                models: catalog.content.as_ref().clone(),
                                etag: None,
                                observed_at: catalog.observed_at,
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
                    observed_at: fetched.observed_at,
                    content: Arc::new(fetched.models),
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
            let observed_at = SystemTime::now();
            let observation = AccountCatalogObservation {
                account_id: account.id().clone(),
                revision: account.revision(),
                plan: account.plan_type().map(str::to_owned),
                etag: snapshot.etag().map(str::to_owned),
                observed_at,
            };
            let saved = self
                .repository
                .store()
                .save_account_catalog(&observation, snapshot.document())
                .await
                .map_err(|_| CodexCredentialCatalogError::Cache)?;
            if !saved {
                return Err(CodexCredentialCatalogError::ConcurrentUpdate);
            }
            Ok(FetchedAccountModels {
                models: snapshot.models().to_vec(),
                etag: observation.etag,
                observed_at,
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
        // Keep unchanged accounts' negative entitlement evidence while refreshing.
        // Current account revision and plan still fence every lookup.
        cache.revision = cache.revision.saturating_add(1);
        drop(cache);
        self.schedule_refresh();
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
            self.schedule_refresh();
            self.etag_notification.notify_one();
        }
        Ok(changed)
    }

    /// 等待并认领一次需要强制刷新的 Provider 目录。
    pub async fn wait_for_etag_refresh(&self) -> bool {
        self.etag_notification.notified().await;
        self.begin_pending_etag_refresh()
    }

    /// 刷新缺失或过期的账号目录，但不接管 ETag daemon 已认领的刷新状态。
    ///
    /// 周期 worker 与 ETag daemon 可以同时请求目录；只有后者可以完成
    /// `inflight` ETag 的状态转换，避免周期刷新错误地确认另一个请求的版本。
    pub async fn refresh_catalogs(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        self.refresh_inner(false).await
    }

    /// 忽略当前 cache，按已认领的 ETag 变化强制生成一份完整新快照。
    pub async fn refresh(
        &self,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        let result = self.refresh_inner(true).await;
        self.finish_etag_refresh(result.is_ok())?;
        result
    }

    async fn refresh_inner(
        &self,
        force: bool,
    ) -> Result<CodexCredentialCatalogSnapshot, CodexCredentialCatalogError> {
        let _refresh = self.refresh_lock.lock().await;
        self.restored
            .get_or_try_init(|| async {
                if self.cached()?.is_none() {
                    self.restore_catalogs().await?;
                }
                Ok::<(), CodexCredentialCatalogError>(())
            })
            .await?;
        let cache_revision = self.cache_revision()?;
        let fetched = self.fetch_catalog(force).await?;
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
    left.models == right.models
        && left.scope_catalogs.len() == right.scope_catalogs.len()
        && left.scope_catalogs.iter().all(|(scope, a)| {
            right.scope_catalogs.get(scope).is_some_and(|b| {
                a.models == b.models && a.accounts == b.accounts && a.content == b.content
            })
        })
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

fn snapshot_from_scopes(
    scope_catalogs: BTreeMap<CodexCatalogScope, CatalogScopeSnapshot>,
) -> CodexCredentialCatalogSnapshot {
    let mut models: Vec<CodexCatalogModel> = Vec::new();
    let mut indices = BTreeMap::<String, usize>::new();
    for entry in scope_catalogs.values() {
        for model in entry.content.iter() {
            if let Some(index) = indices.get(model.request_model().as_str()).copied() {
                if model.document().protocol() == "codex"
                    && models[index].document().protocol() != "codex"
                {
                    models[index] = model.clone();
                }
            } else {
                indices.insert(model.request_model().as_str().to_owned(), models.len());
                models.push(model.clone());
            }
        }
    }
    CodexCredentialCatalogSnapshot {
        observed_at: SystemTime::now(),
        models,
        scope_catalogs,
    }
}
