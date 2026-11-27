use crate::credential::CredentialService;
use crate::credential::catalog::{CatalogService, CatalogTask};
use crate::profile::{ReleaseService, ReleaseTask};
use futures::{StreamExt, future::BoxFuture, stream};
use gateway_admin::ports::provider::ProviderAdminErrorKind as Kind;
use gateway_core::task::{ScheduledTask, WorkerCycleContext, WorkerTaskError};
use gateway_core::task::{
    WorkerContribution, WorkerDefinitionError, WorkerId, WorkerKind, WorkerLeaseRequest,
    WorkerRegistration, WorkerRunnable, WorkerSchedule,
};
use std::{sync::Arc, time::Duration};

pub(crate) fn release_contribution(
    release: Arc<ReleaseService>,
) -> Result<WorkerContribution, WorkerDefinitionError> {
    WorkerRegistration::try_new(
        WorkerId::try_new(
            WorkerKind::QuotaCatalogHealth,
            "antigravity-client-releases",
        )?,
        WorkerRunnable::Scheduled {
            schedule: WorkerSchedule::try_new(
                Duration::from_secs(300),
                Duration::from_secs(5),
                Duration::from_secs(300),
                Duration::from_secs(900),
                Duration::from_secs(300),
            )?,
            lease: None,
            task: Box::new(ReleaseTask(release)),
        },
    )
    .map(WorkerContribution::Registration)
}

pub(crate) fn contributions(
    credentials: Arc<CredentialService>,
    catalog: Arc<CatalogService>,
    quota: Arc<crate::credential::quota::QuotaService>,
) -> Result<Vec<WorkerContribution>, WorkerDefinitionError> {
    let refresh_id = WorkerId::try_new(WorkerKind::OAuthRefresh, "antigravity")?;
    let ttl = Duration::from_secs(900);
    let refresh = WorkerRegistration::try_new(
        refresh_id.clone(),
        WorkerRunnable::Scheduled {
            schedule: WorkerSchedule::try_new(
                Duration::from_secs(30),
                Duration::from_secs(1),
                Duration::from_secs(60),
                ttl,
                Duration::from_secs(300),
            )?,
            lease: Some(WorkerLeaseRequest::try_new(refresh_id, ttl)?),
            task: Box::new(RefreshTask {
                credentials,
                catalog: catalog.clone(),
            }),
        },
    )?;
    let catalog = WorkerRegistration::try_new(
        WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "antigravity-model-catalog")?,
        WorkerRunnable::Scheduled {
            schedule: WorkerSchedule::try_new(
                Duration::from_secs(30),
                Duration::from_secs(2),
                Duration::from_secs(60),
                ttl,
                Duration::from_secs(300),
            )?,
            lease: None,
            task: Box::new(CatalogTask(catalog)),
        },
    )?;
    let quota_id = WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "antigravity-quota")?;
    let quota = WorkerRegistration::try_new(
        quota_id.clone(),
        WorkerRunnable::Scheduled {
            schedule: WorkerSchedule::try_new(
                Duration::from_secs(30),
                Duration::from_secs(3),
                Duration::from_secs(60),
                ttl,
                Duration::from_secs(300),
            )?,
            lease: Some(WorkerLeaseRequest::try_new(quota_id, ttl)?),
            task: Box::new(crate::credential::quota::QuotaTask(quota)),
        },
    )?;
    Ok(vec![
        WorkerContribution::Registration(quota),
        WorkerContribution::Registration(refresh),
        WorkerContribution::Registration(catalog),
    ])
}

struct RefreshTask {
    credentials: Arc<CredentialService>,
    catalog: Arc<CatalogService>,
}

impl ScheduledTask for RefreshTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let (candidates, concurrency) = self.credentials.refresh_candidates().await?;
            if candidates.is_empty() || context.cancellation().is_cancelled() {
                return Ok(());
            }
            let mut catalogs = self
                .catalog
                .capture_refresh_catalogs(&candidates)
                .await
                .unwrap_or_else(|_| {
                    tracing::warn!("Antigravity 目录读取失败，凭据续期后重新查询目录");
                    Default::default()
                });
            let mut updates = stream::iter(candidates)
                .map(|loaded| {
                    let cancellation = context.cancellation();
                    let catalog = catalogs.remove(loaded.account.id());
                    async move {
                        if cancellation.is_cancelled() {
                            return Ok(());
                        }
                        let revision = self.credentials.refresh_due(&loaded).await?;
                        self.catalog
                            .after_credential_refresh(loaded.account.id(), revision, catalog)
                            .await
                    }
                })
                .buffer_unordered(concurrency);
            // 取消时停止发起新请求，已交换的 refresh token 仍要等 CAS 完成
            let mut first_error = None;
            while let Some(result) = updates.next().await {
                if let Err(e) = result
                    && !matches!(e.kind(), Kind::Conflict | Kind::CredentialRefreshRequired)
                {
                    first_error.get_or_insert(e);
                }
            }
            if first_error.is_some() {
                Err(WorkerTaskError::safe("部分 Antigravity 账号续期失败"))
            } else {
                Ok(())
            }
        })
    }
}
