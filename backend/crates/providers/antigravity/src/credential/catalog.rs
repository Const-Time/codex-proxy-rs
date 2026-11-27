//! 将资源服务确认的项目与模型绑定到授权注册和账号凭据版本

use crate::{
    credential::{CredentialService, oauth::error},
    profile::ProfileService,
    transport::{Catalog, CloudCodeClient, Model, unsent},
};
use gateway_admin::{
    model::provider_credentials::{ProviderModel, ProviderModels},
    ports::provider::{ProviderAdminError, ProviderAdminErrorKind as AdminKind},
};
use gateway_core::{
    account::{AccountCatalogObservation, ProviderAccountId, StoredAccountCatalog},
    error::{ProviderError, ProviderErrorKind as Kind},
    operation::OperationKind,
    routing::{ModelCapabilities, ModelPresentation, ProviderModelCapabilities, UpstreamModelId},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

pub(crate) struct CatalogService {
    credentials: Arc<CredentialService>,
    kind: gateway_core::routing::ProviderKind,
    accounts: Arc<dyn gateway_core::account::ProviderAccountStore>,
    registration: String,
    pub(crate) origin: url::Url,
    profile: Option<Arc<ProfileService>>,
    pub(crate) generation: AtomicU64,
    observed:
        Mutex<BTreeMap<ProviderAccountId, (gateway_core::account::CredentialRevision, SystemTime)>>,
    refresh_cursor: AtomicUsize,
}

pub(crate) struct RefreshCatalog {
    observation: AccountCatalogObservation,
    document: bytes::Bytes,
}

struct DiscoveredModel {
    presentation: ModelPresentation,
    max_output_tokens: Option<u64>,
    accounts: BTreeSet<ProviderAccountId>,
}

impl CatalogService {
    pub(crate) async fn capture_refresh_catalogs(
        &self,
        candidates: &[gateway_core::account::LoadedCredential],
    ) -> Result<BTreeMap<ProviderAccountId, RefreshCatalog>, ProviderAdminError> {
        let candidates: BTreeMap<_, _> = candidates
            .iter()
            .map(|loaded| (loaded.account.id(), &loaded.account))
            .collect();
        let stored = self
            .accounts
            .load_account_catalogs(&self.kind)
            .await
            .map_err(|_| error(AdminKind::Unavailable))?;
        let mut snapshots = BTreeMap::new();
        for catalog in stored {
            for observation in catalog.accounts {
                if candidates
                    .get(&observation.account_id)
                    .is_some_and(|account| {
                        account.revision() == observation.revision
                            && account.plan_type() == observation.plan.as_deref()
                            && observation.etag.as_ref() == Some(&self.registration)
                    })
                {
                    snapshots.insert(
                        observation.account_id.clone(),
                        RefreshCatalog {
                            observation,
                            document: catalog.document.clone(),
                        },
                    );
                }
            }
        }
        Ok(snapshots)
    }

    pub(crate) async fn after_credential_refresh(
        &self,
        account_id: &ProviderAccountId,
        revision: gateway_core::account::CredentialRevision,
        snapshot: Option<RefreshCatalog>,
    ) -> Result<(), ProviderAdminError> {
        if let Some(mut snapshot) = snapshot {
            // OAuth 续期已核对同一身份；沿用观测时间，存储仍校验新 revision 和原套餐
            snapshot.observation.revision = revision;
            if self
                .accounts
                .save_account_catalog(&snapshot.observation, &snapshot.document)
                .await
                .map_err(|_| error(AdminKind::Unavailable))?
            {
                self.invalidate();
                return Ok(());
            }
        }
        // 401 过期账号不会进入旧目录读取；新 AT 提交后立即尝试恢复，失败再由目录任务重试
        self.account_models(account_id, false).await.map(|_| ())
    }

    pub(crate) fn new(
        kind: gateway_core::routing::ProviderKind,
        accounts: Arc<dyn gateway_core::account::ProviderAccountStore>,
        credentials: Arc<CredentialService>,
        registration: String,
        origin: url::Url,
    ) -> Self {
        Self {
            kind,
            accounts,
            credentials,
            registration,
            origin,
            profile: None,
            generation: AtomicU64::new(0),
            observed: Mutex::default(),
            refresh_cursor: AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_profile(mut self, profile: Arc<ProfileService>) -> Self {
        self.profile = Some(profile);
        self
    }

    pub(crate) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) async fn catalogs(
        &self,
    ) -> Result<Vec<(Vec<AccountCatalogObservation>, Catalog)>, ProviderError> {
        let registration = &self.registration;
        let docs = self
            .accounts
            .load_account_catalogs(&self.kind)
            .await
            .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?;
        let mut result = Vec::new();
        for StoredAccountCatalog { accounts, document } in docs {
            let accounts: Vec<_> = accounts
                .into_iter()
                .filter(|a| a.etag.as_ref() == Some(registration))
                .collect();
            if !accounts.is_empty() {
                let catalog = serde_json::from_slice(&document)
                    .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?;
                result.push((accounts, catalog));
            }
        }
        let observations = result
            .iter()
            .flat_map(|(accounts, _)| accounts.iter())
            .map(|a| (a.account_id.clone(), (a.revision, a.observed_at)))
            .collect();
        let mut observed = self
            .observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *observed != observations {
            *observed = observations;
            self.invalidate();
        }
        Ok(result)
    }

    pub(crate) async fn capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Catalog::compile_capabilities(self.catalogs().await?)
    }

    pub(crate) async fn diagnostic_capabilities(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Catalog::compile_capabilities(self.diagnostic_catalogs(account).await?)
    }

    async fn diagnostic_catalogs(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Vec<(Vec<AccountCatalogObservation>, Catalog)>, ProviderError> {
        let stored = self
            .accounts
            .load_diagnostic_account_catalog(&self.kind, account)
            .await
            .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?;
        let Some((observation, document)) = stored.filter(|(observation, _)| {
            &observation.account_id == account
                && observation.etag.as_ref() == Some(&self.registration)
        }) else {
            return Ok(Vec::new());
        };
        let catalog = serde_json::from_slice(&document)
            .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?;
        Ok(vec![(vec![observation], catalog)])
    }

    pub(crate) async fn projects(
        &self,
        model: &UpstreamModelId,
        requested_output_tokens: Option<u64>,
        diagnostic_account: Option<&ProviderAccountId>,
        search: bool,
    ) -> Result<
        BTreeMap<ProviderAccountId, (gateway_core::account::CredentialRevision, String)>,
        ProviderError,
    > {
        let mut projects = BTreeMap::new();
        let catalogs = match diagnostic_account {
            Some(account) => self.diagnostic_catalogs(account).await?,
            None => self.catalogs().await?,
        };
        for (accounts, catalog) in catalogs {
            if catalog.supports_project_model(model, requested_output_tokens, search) {
                projects.extend(accounts.into_iter().map(|account| {
                    (
                        account.account_id,
                        (account.revision, catalog.project.clone()),
                    )
                }));
            }
        }
        Ok(projects)
    }

    pub(crate) async fn account_models(
        &self,
        id: &ProviderAccountId,
        refresh: bool,
    ) -> Result<ProviderModels, ProviderAdminError> {
        let current = self.credentials.current(id).await?;
        if !refresh {
            for (accounts, cat) in self
                .catalogs()
                .await
                .map_err(|_| error(AdminKind::Unavailable))?
            {
                if let Some(a) = accounts
                    .iter()
                    .find(|a| &a.account_id == id && a.revision == current.account.revision())
                {
                    return admin_models(cat, a.observed_at);
                }
            }
        }
        let material = self
            .credentials
            .material(&current, true)
            .map_err(|_| error(AdminKind::CredentialRefreshRequired))?;
        let profile = match &self.profile {
            Some(service) => Some(service.current().await?),
            None => None,
        };
        let client = CloudCodeClient::with_profile(
            &self.origin,
            current.account.outbound_proxy(),
            false,
            profile.as_ref(),
        )
        .map_err(|_| error(AdminKind::Unavailable))?;
        let observed_at = SystemTime::now();
        let catalog = client
            .catalog(&material.access_token)
            .await
            .map_err(|e| match e.kind() {
                Kind::Unauthorized => error(AdminKind::CredentialRefreshRequired),
                _ => error(AdminKind::Unavailable)
                    .with_public_message("Antigravity 项目或模型目录查询失败"),
            })?;
        self.publish(
            &current.account,
            current.account.plan_type().map(str::to_owned),
            catalog,
            observed_at,
        )
        .await
    }

    pub(crate) async fn publish(
        &self,
        account: &gateway_core::account::ProviderAccount,
        plan: Option<String>,
        mut catalog: Catalog,
        observed_at: SystemTime,
    ) -> Result<ProviderModels, ProviderAdminError> {
        // 模型目录只拥有能力事实，易变的额度由独立快照保存
        for model in catalog.available.models.values_mut() {
            model.quota = None;
        }
        let observed = AccountCatalogObservation {
            account_id: account.id().clone(),
            revision: account.revision(),
            plan,
            etag: Some(self.registration.clone()),
            observed_at,
        };
        let body = serde_json::to_vec(&catalog).map_err(|_| error(AdminKind::Internal))?;
        if !self
            .accounts
            .save_account_catalog(&observed, &body)
            .await
            .map_err(|_| error(AdminKind::Unavailable))?
        {
            return Err(error(AdminKind::Conflict));
        }
        self.generation.fetch_add(1, Ordering::Relaxed);
        admin_models(catalog, observed_at)
    }
}

impl Catalog {
    /// Compile routing capabilities from saved account catalogs.
    pub fn compile_capabilities(
        mut catalogs: Vec<(Vec<AccountCatalogObservation>, Catalog)>,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        let mut members = BTreeMap::<UpstreamModelId, DiscoveredModel>::new();
        let mut groups = BTreeMap::new();
        let mut search_models = BTreeMap::new();
        catalogs.sort_by(|(left, _), (right, _)| {
            left.first()
                .map(|account| &account.account_id)
                .cmp(&right.first().map(|account| &account.account_id))
        });
        for (accounts, catalog) in catalogs {
            super::model_groups::collect(&mut groups, &catalog, &accounts);
            if let Some(target) = catalog
                .web_search_target()
                .and_then(|id| UpstreamModelId::new(id).ok())
            {
                search_models.extend(
                    accounts
                        .iter()
                        .map(|account| (account.account_id.clone(), target.clone())),
                );
            }
            for (key, model) in catalog.available.models {
                let id = UpstreamModelId::new(key).map_err(|_| unsent(Kind::Protocol))?;
                let output_limit = model.max_output_tokens.filter(|limit| *limit != 0);
                members
                    .entry(id.clone())
                    .and_modify(|existing| {
                        existing.max_output_tokens = existing
                            .max_output_tokens
                            .zip(output_limit)
                            .map(|(previous, current)| previous.max(current));
                    })
                    .or_insert_with(|| DiscoveredModel {
                        presentation: presentation(&id, model),
                        max_output_tokens: output_limit,
                        accounts: BTreeSet::new(),
                    })
                    .accounts
                    .extend(accounts.iter().map(|account| account.account_id.clone()));
            }
        }
        let capabilities = |max_output_tokens| {
            ModelCapabilities::new(
                [OperationKind::Generate, OperationKind::CountTokens].into(),
                max_output_tokens,
            )
            .with_generation_protocol(gateway_protocol::genai::GENAI_PROTOCOL)
            .with_translated_generation_protocols([
                gateway_protocol::chat_completions::CHAT_PROTOCOL,
            ])
            .with_upstream_feature_validation()
            .with_dedicated_web_search(search_models.clone())
        };
        let mut models: BTreeMap<_, _> = members
            .into_iter()
            .map(|(id, model)| {
                let metadata = ProviderModelCapabilities::new(
                    id.clone(),
                    capabilities(model.max_output_tokens),
                )
                .with_presentation(model.presentation)
                .with_catalog_accounts(model.accounts);
                (id, metadata)
            })
            .collect();
        for (id, variants) in groups {
            models.entry(id.clone()).or_insert_with(|| {
                let owners = variants
                    .values()
                    .flat_map(|variant| variant.accounts.iter().cloned())
                    .collect();
                ProviderModelCapabilities::new(
                    id,
                    capabilities(None).with_reasoning_variants(variants),
                )
                .with_catalog_accounts(owners)
            });
        }
        Ok(models.into_values().collect())
    }

    /// Check whether this account catalog supports the resolved model and limit.
    #[must_use]
    pub fn supports_project_model(
        &self,
        model: &UpstreamModelId,
        requested_output_tokens: Option<u64>,
        search: bool,
    ) -> bool {
        let member = self.available.models.get(model.as_str());
        let declared = if search {
            self.web_search_target() == Some(model.as_str())
        } else {
            member.is_some()
        };
        declared
            && member
                .and_then(|data| data.max_output_tokens)
                .filter(|limit| *limit > 0)
                .zip(requested_output_tokens)
                .is_none_or(|(limit, requested)| requested <= limit)
    }
}

pub(crate) fn presentation(id: &UpstreamModelId, model: Model) -> ModelPresentation {
    let window = model.context_window.filter(|tokens| *tokens > 0);
    let description = model.description.or(model.tag_description);
    ModelPresentation::new(
        Some(model.name)
            .filter(|name| !name.is_empty())
            .or_else(|| Some(id.as_str().to_owned())),
        description,
    )
    .with_context_window_tokens(window)
    .with_max_context_window_tokens(window)
    .with_image_input(model.supports_images == Some(true))
}

fn admin_models(
    catalog: Catalog,
    observed: SystemTime,
) -> Result<ProviderModels, ProviderAdminError> {
    let models = catalog
        .available
        .models
        .into_iter()
        .map(|(id, m)| {
            Ok(ProviderModel {
                name: if m.name.is_empty() {
                    id.clone()
                } else {
                    m.name
                },
                id: UpstreamModelId::new(id).map_err(|_| error(AdminKind::Invalid))?,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(ProviderModels {
        models,
        observed_at: Some(observed.into()),
    })
}

pub(crate) struct CatalogTask(pub(crate) Arc<CatalogService>);
impl gateway_core::task::ScheduledTask for CatalogTask {
    fn run_cycle(
        &self,
        context: gateway_core::task::WorkerCycleContext,
    ) -> futures::future::BoxFuture<'_, Result<(), gateway_core::task::WorkerTaskError>> {
        Box::pin(async move { self.0.reconcile(context.cancellation()).await })
    }
}
impl CatalogService {
    pub(crate) async fn reconcile(
        &self,
        cancellation: &gateway_core::lifecycle::CancellationToken,
    ) -> Result<(), gateway_core::task::WorkerTaskError> {
        use futures::{StreamExt, stream};
        use gateway_core::task::WorkerTaskError;
        let catalogs = self
            .catalogs()
            .await
            .map_err(|_| WorkerTaskError::safe("读取 Antigravity 目录失败"))?;
        let observations: BTreeMap<_, _> = catalogs
            .into_iter()
            .flat_map(|(accounts, _)| accounts)
            .map(|a| (a.account_id.clone(), a))
            .collect();
        let mut due = self
            .accounts
            .list_for_provider(&self.kind)
            .await
            .map_err(|_| WorkerTaskError::safe("读取 Antigravity 账号失败"))?;
        due.retain(|account| {
            account.enabled()
                && account.authentication_kind() == "oauth"
                && observations.get(account.id()).is_none_or(|observation| {
                    observation.revision != account.revision()
                        || observation.observed_at.elapsed().unwrap_or_default()
                            >= Duration::from_secs(3600)
                })
        });
        if due.is_empty() {
            return Ok(());
        }
        due.sort_by(|a, b| a.id().cmp(b.id()));
        // 固定大小轮转批次，持续失败的账号不能阻塞后面的目录修复
        let offset = self.refresh_cursor.fetch_add(32, Ordering::Relaxed) % due.len();
        due.rotate_left(offset);
        due.truncate(32);
        let mut updates=stream::iter(due).map(|account|async move {
            tokio::select! {biased;_=cancellation.cancelled()=>Ok(()),result=self.account_models(account.id(),true)=>result.map(|_|())}
        }).buffer_unordered(4);
        let mut first_error = None;
        while let Some(result) = updates.next().await {
            if let Err(error) = result
                && !matches!(
                    error.kind(),
                    AdminKind::Conflict | AdminKind::CredentialRefreshRequired
                )
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), |_| {
            Err(WorkerTaskError::safe("部分 Antigravity 模型目录刷新失败"))
        })
    }
}
