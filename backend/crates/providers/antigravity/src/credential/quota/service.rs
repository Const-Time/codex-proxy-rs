//! AGY 额度刷新、持久观测与选号投影，凭据版本和观察时间由 Store 隔离

use super::document::Document;
use crate::{
    credential::catalog::CatalogService,
    credential::{CredentialService, oauth::error},
    profile::ProfileService,
    transport::CloudCodeClient,
};
use futures::{StreamExt, stream};
use gateway_admin::{
    model::provider_credentials::ProviderQuota,
    ports::provider::{ProviderAdminError, ProviderAdminErrorKind as Kind},
};
use gateway_core::{
    account::{
        AccountQuotaSignals, OpaqueProviderData, ProviderAccount, ProviderAccountId,
        ProviderAccountStore, QuotaObservation, QuotaWriteOutcome,
    },
    lifecycle::CancellationToken,
    routing::ProviderKind,
    task::{ScheduledTask, WorkerCycleContext, WorkerTaskError},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

pub(crate) struct QuotaService {
    kind: ProviderKind,
    accounts: Arc<dyn ProviderAccountStore>,
    credentials: Arc<CredentialService>,
    catalog: Arc<CatalogService>,
    origin: url::Url,
    profile: Option<Arc<ProfileService>>,
    cursor: AtomicUsize,
}

impl QuotaService {
    pub(crate) fn new(
        kind: ProviderKind,
        accounts: Arc<dyn ProviderAccountStore>,
        credentials: Arc<CredentialService>,
        catalog: Arc<CatalogService>,
        origin: url::Url,
    ) -> Self {
        Self {
            kind,
            accounts,
            credentials,
            catalog,
            origin,
            profile: None,
            cursor: AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_profile(mut self, profile: Arc<ProfileService>) -> Self {
        self.profile = Some(profile);
        self
    }

    pub(crate) async fn read(
        &self,
        id: &ProviderAccountId,
    ) -> Result<ProviderQuota, ProviderAdminError> {
        let account = self
            .accounts
            .get_account(id)
            .await
            .map_err(unavailable)?
            .filter(|a| a.provider() == &self.kind)
            .ok_or_else(|| error(Kind::NotFound))?;
        let observations = self
            .accounts
            .get_quotas(std::slice::from_ref(id))
            .await
            .map_err(unavailable)?;
        if let Some(observation) = observations
            .into_iter()
            .find(|o| &o.account_id == id && o.expected_revision == account.revision())
        {
            let doc = parse(&observation)?;
            return Ok(doc.project(observation.observed_at));
        }
        Ok(ProviderQuota {
            plan_type: account.plan_type().map(str::to_owned),
            observed_at: None,
            refresh_token_expires_at: None,
            windows: Vec::new(),
            credits: None,
            limit_reached: false,
            provider_data: None,
        })
    }

    pub(crate) async fn refresh(&self, id: &ProviderAccountId) -> Result<(), ProviderAdminError> {
        let current = self.credentials.current(id).await?;
        let material = self
            .credentials
            .material(&current, true)
            .map_err(|_| error(Kind::CredentialRefreshRequired))?;
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
        .map_err(upstream)?;
        // 用查询开始时间隔离交错返回的旧结果，失败不覆盖已有快照
        let observed_at = SystemTime::now();
        let info = client
            .account_info(&material.access_token)
            .await
            .map_err(upstream)?;
        let (catalog, summary) = tokio::try_join!(
            client.catalog_for_project(&material.access_token, info.project.clone()),
            client.quota_summary(&material.access_token, &info.project),
        )
        .map_err(upstream)?;
        let doc = Document {
            tier: info.paid_tier.or(info.current_tier),
            bucket_models: super::document::bucket_models(&summary, &catalog),
            summary,
            models: catalog
                .available
                .models
                .iter()
                .map(|(id, model)| {
                    (
                        id.clone(),
                        model
                            .quota
                            .clone()
                            .unwrap_or(crate::transport::account::ModelQuota {
                                remaining_fraction: None,
                                reset_time: None,
                            }),
                    )
                })
                .collect(),
        };
        let plan = doc.plan_type();
        let quota = serde_json::to_value(&doc).map_err(|_| error(Kind::Internal))?;
        let observation = QuotaObservation {
            account_id: id.clone(),
            expected_revision: current.account.revision(),
            quota: OpaqueProviderData::new(
                quota
                    .as_object()
                    .cloned()
                    .ok_or_else(|| error(Kind::Internal))?,
            ),
            plan_type: plan.clone(),
            observed_at,
            state: doc.access_with_credits(
                doc.access(observed_at),
                current.account.auto_use_quota_credits(),
                observed_at,
            ),
        };
        if self
            .accounts
            .compare_and_swap_quota(observation)
            .await
            .map_err(unavailable)?
            == QuotaWriteOutcome::Conflict
        {
            return Err(error(Kind::Conflict));
        }
        // 套餐与目录来自同次查询，保存当前套餐的目录避免新套餐短暂失去路由资格
        self.catalog
            .publish(
                &current.account,
                plan.or_else(|| current.account.plan_type().map(str::to_owned)),
                catalog,
                observed_at,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn scheduling(
        &self,
        accounts: &[ProviderAccount],
        model: &str,
    ) -> Result<BTreeMap<ProviderAccountId, (bool, Option<AccountQuotaSignals>)>, ProviderAdminError>
    {
        let owners: BTreeMap<_, _> = accounts.iter().map(|a| (a.id().clone(), a)).collect();
        let ids: Vec<_> = owners.keys().cloned().collect();
        let observations = self.accounts.get_quotas(&ids).await.map_err(unavailable)?;
        let now = SystemTime::now();
        observations
            .into_iter()
            .filter_map(|o| {
                owners
                    .get(&o.account_id)
                    .filter(|a| a.revision() == o.expected_revision)
                    .map(|account| (o, account))
            })
            .map(|(o, account)| {
                let result = parse(&o)?.model_signals(
                    model,
                    account.auto_use_quota_credits(),
                    account.quota_usage_limits(),
                    o.observed_at,
                    now,
                );
                Ok((o.account_id, result))
            })
            .collect()
    }

    pub(crate) async fn reconcile(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), WorkerTaskError> {
        let accounts = self
            .accounts
            .list_for_provider(&self.kind)
            .await
            .map_err(|_| WorkerTaskError::safe("读取 Antigravity 账号失败"))?;
        let ids: Vec<_> = accounts.iter().map(|a| a.id().clone()).collect();
        let mut observations: BTreeMap<_, _> = self
            .accounts
            .get_quotas(&ids)
            .await
            .map_err(|_| WorkerTaskError::safe("读取 Antigravity 额度版本失败"))?
            .into_iter()
            .map(|o| (o.account_id.clone(), o))
            .collect();
        let now = SystemTime::now();
        let interval = Duration::from_secs(300);
        let stale_before = now - interval;
        let mut due = Vec::new();
        for account in accounts
            .iter()
            .filter(|a| a.enabled() && a.authentication_kind() == "oauth")
        {
            let snapshot = observations
                .remove(account.id())
                .filter(|s| s.expected_revision == account.revision());
            match snapshot {
                None => due.push(account.id().clone()),
                Some(_) if account.quota().exhaustion_refresh_due(now, interval) => {
                    due.push(account.id().clone())
                }
                Some(s) if !account.quota().is_exhausted() && s.observed_at <= stale_before => {
                    due.push(account.id().clone())
                }
                Some(_) => {}
            }
        }
        let mut outcome = Ok(());
        if due.is_empty() {
            return outcome;
        }
        due.sort_unstable();
        let offset = self.cursor.fetch_add(32, Ordering::Relaxed) % due.len();
        due.rotate_left(offset);
        due.truncate(32);
        let mut results = stream::iter(due)
            .map(|id| async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Ok(()),
                    result = self.refresh(&id) => result,
                }
            })
            .buffer_unordered(4);
        while let Some(result) = results.next().await {
            if let Err(e) = result
                && !matches!(e.kind(), Kind::Conflict | Kind::CredentialRefreshRequired)
                && outcome.is_ok()
            {
                outcome = Err(WorkerTaskError::safe("部分 Antigravity 额度刷新失败"));
            }
        }
        outcome
    }
}

fn parse(observation: &QuotaObservation) -> Result<Document, ProviderAdminError> {
    serde_json::from_value(serde_json::Value::Object(
        observation.quota.expose_to_provider().clone(),
    ))
    .map_err(|_| error(Kind::Internal))
}
fn unavailable(_: gateway_core::error::StoreError) -> ProviderAdminError {
    error(Kind::Unavailable)
}
fn upstream(e: gateway_core::error::ProviderError) -> ProviderAdminError {
    if e.kind() == gateway_core::error::ProviderErrorKind::Unauthorized {
        error(Kind::CredentialRefreshRequired)
    } else {
        error(Kind::Unavailable).with_public_message("Antigravity 套餐或额度查询失败")
    }
}

pub(crate) struct QuotaTask(pub(crate) Arc<QuotaService>);
impl ScheduledTask for QuotaTask {
    fn run_cycle(
        &self,
        context: WorkerCycleContext,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move { self.0.reconcile(context.cancellation()).await })
    }
}
