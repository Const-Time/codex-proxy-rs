//! 运行时快照的原子发布、健康状态与跨进程版本收敛。

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as SyncMutex, RwLock, Weak};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::{FutureExt as _, Stream, StreamExt as _, pin_mut, select_biased};
use futures_timer::Delay;

use crate::engine::provider::ProviderCatalogGeneration;
use crate::health::{HealthProbe, HealthState};
use crate::identity::ProviderKind;
use crate::lifecycle::CancellationToken;
use crate::routing::ConfigRevision;
use crate::routing::snapshot::{
    RuntimeSnapshot, RuntimeSnapshotCompileError, RuntimeSnapshotCompiler,
};
use crate::task::{
    DaemonRestartPolicy, DaemonTask, ScheduledTask, WorkerContribution, WorkerCycleContext,
    WorkerDefinitionError, WorkerId, WorkerKind, WorkerRegistration, WorkerRunnable,
    WorkerSchedule, WorkerTaskError,
};

const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAXIMUM_BACKOFF: Duration = Duration::from_secs(30);
const UNUSED_LEASE_TTL: Duration = Duration::from_secs(30);
const UNUSED_LEASE_RENEWAL: Duration = Duration::from_secs(10);

/// 不泄漏订阅基础设施细节的通知错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("runtime snapshot notification is unavailable")]
pub struct SnapshotSubscriptionError;

impl SnapshotSubscriptionError {
    #[must_use]
    pub const fn unavailable() -> Self {
        Self
    }
}

/// 可丢失的配置 revision 通知流；权威 revision 始终由 Store 端口读取。
pub type SnapshotRevisionStream =
    Pin<Box<dyn Stream<Item = Result<ConfigRevision, SnapshotSubscriptionError>> + Send + 'static>>;

/// 跨进程 revision 通知的基础设施中立端口。
pub trait SnapshotSubscriptionPort: Send + Sync {
    fn publish_snapshot_revision(
        &self,
        revision: ConfigRevision,
    ) -> BoxFuture<'_, Result<(), SnapshotSubscriptionError>>;

    fn subscribe_snapshot_revisions(
        &self,
    ) -> BoxFuture<'_, Result<SnapshotRevisionStream, SnapshotSubscriptionError>>;
}

/// 请求级冻结失败；此状态必须 fail closed。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("runtime snapshot is unavailable")]
pub struct RuntimeSnapshotUnavailable;

/// RuntimeSnapshot 原子发布和请求级冻结句柄。
#[derive(Clone, Default)]
pub struct RuntimeSnapshotHandle {
    current: Arc<RwLock<PublishedSnapshot>>,
    changed: Arc<event_listener::Event>,
}
struct PublishedSnapshot {
    snapshot: Option<Arc<RuntimeSnapshot>>,
    access: Vec<Weak<ClientAccess>>,
    next_access_cleanup: usize,
    publication: u64,
    availability: CancellationToken,
}
impl Default for PublishedSnapshot {
    fn default() -> Self {
        let availability = CancellationToken::new();
        availability.cancel();
        Self {
            snapshot: None,
            access: Vec::new(),
            next_access_cleanup: 64,
            publication: 0,
            availability,
        }
    }
}
/// Read-only notification of facts actually published by Core, never a Store commit notice.
#[derive(Clone)]
pub struct SnapshotPublications {
    current: Arc<RwLock<PublishedSnapshot>>,
    changed: Arc<event_listener::Event>,
}
/// Suspension immediately revokes the availability lease, even if notifications coalesce.
#[derive(Clone)]
pub struct SnapshotPublication {
    revision: Option<ConfigRevision>,
    availability: CancellationToken,
}
impl SnapshotPublication {
    #[must_use]
    pub fn revision(&self) -> Option<ConfigRevision> {
        self.revision
    }
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        // Consumers may end their own lease, never Core's shared availability period.
        CancellationToken::linked([self.availability.clone()])
    }
}
impl SnapshotPublications {
    /// Each subscriber receives the latest state first. Slow readers coalesce publications.
    pub fn subscribe(&self) -> Pin<Box<dyn Stream<Item = SnapshotPublication> + Send + 'static>> {
        Box::pin(futures::stream::unfold(
            (self.clone(), None),
            |(source, mut last)| async move {
                loop {
                    let changed = source.changed.listen();
                    let next = {
                        let current = read_unpoisoned(&source.current);
                        (last != Some(current.publication)).then(|| {
                            last = Some(current.publication);
                            SnapshotPublication {
                                revision: current.snapshot.as_ref().map(|s| s.revision()),
                                availability: current.availability.clone(),
                            }
                        })
                    };
                    if let Some(next) = next {
                        return Some((next, (source, last)));
                    }
                    changed.await;
                }
            },
        ))
    }
}
struct ClientAccess {
    policy: crate::policy::ClientPolicy,
    cancellation: CancellationToken,
}
/// Live authorization for delegated calls; it never changes the execution's frozen billing facts.
#[derive(Clone)]
pub struct ClientAccessGuard {
    access: Arc<ClientAccess>,
    cancellation: CancellationToken,
}
impl ClientAccessGuard {
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    #[must_use]
    pub fn with_revocation(self, revocation: CancellationToken) -> Self {
        Self {
            access: self.access,
            cancellation: CancellationToken::linked([self.cancellation, revocation]),
        }
    }
}

impl RuntimeSnapshotHandle {
    #[must_use]
    pub fn new(initial: RuntimeSnapshot) -> Self {
        Self {
            current: Arc::new(RwLock::new(PublishedSnapshot {
                snapshot: Some(Arc::new(initial)),
                access: Vec::new(),
                next_access_cleanup: 64,
                publication: 0,
                availability: CancellationToken::new(),
            })),
            changed: Arc::default(),
        }
    }

    #[must_use]
    pub fn publications(&self) -> SnapshotPublications {
        SnapshotPublications {
            current: self.current.clone(),
            changed: self.changed.clone(),
        }
    }

    pub fn publish(&self, snapshot: RuntimeSnapshot) {
        let mut current = write_unpoisoned(&self.current);
        current.access.retain(|weak| {
            let Some(access) = weak.upgrade() else {
                return false;
            };
            if !snapshot.contains_client_policy(&access.policy) {
                access.cancellation.cancel();
            }
            !access.cancellation.is_cancelled()
        });
        current.next_access_cleanup = current.access.len().saturating_mul(2).max(64);
        if current.snapshot.is_none() {
            current.availability = CancellationToken::new();
        }
        current.snapshot = Some(Arc::new(snapshot));
        current.publication = current.publication.wrapping_add(1);
        self.changed.notify(usize::MAX);
    }

    pub fn suspend(&self) {
        let mut current = write_unpoisoned(&self.current);
        for access in current.access.drain(..).filter_map(|weak| weak.upgrade()) {
            access.cancellation.cancel();
        }
        current.next_access_cleanup = 64;
        current.snapshot = None;
        current.availability.cancel();
        current.publication = current.publication.wrapping_add(1);
        self.changed.notify(usize::MAX);
    }

    #[must_use]
    pub fn revision(&self) -> Option<ConfigRevision> {
        read_unpoisoned(&self.current)
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.revision())
    }

    #[must_use]
    pub fn provider_catalog_generations(
        &self,
    ) -> Option<BTreeMap<ProviderKind, ProviderCatalogGeneration>> {
        read_unpoisoned(&self.current)
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.provider_catalog_generations().clone())
    }

    /// Registration and revocation share the publication lock; disable/re-enable cannot resurrect a lease.
    pub fn client_access(
        &self,
        policy: &crate::policy::ClientPolicy,
    ) -> Result<ClientAccessGuard, RuntimeSnapshotUnavailable> {
        let mut current = write_unpoisoned(&self.current);
        if !current
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.contains_client_policy(policy))
        {
            return Err(RuntimeSnapshotUnavailable);
        }
        if current.access.len() >= current.next_access_cleanup {
            current.access.retain(|weak| weak.strong_count() != 0);
            current.next_access_cleanup = current.access.len().saturating_mul(2).max(64);
        }
        let access = Arc::new(ClientAccess {
            policy: policy.clone(),
            cancellation: CancellationToken::new(),
        });
        current.access.push(Arc::downgrade(&access));
        Ok(ClientAccessGuard {
            cancellation: access.cancellation.clone(),
            access,
        })
    }
    /// 冻结当前 Arc；后续发布不改变已经开始的请求。
    pub fn acquire(&self) -> Result<Arc<RuntimeSnapshot>, RuntimeSnapshotUnavailable> {
        read_unpoisoned(&self.current)
            .snapshot
            .clone()
            .ok_or(RuntimeSnapshotUnavailable)
    }
}

impl HealthProbe for RuntimeSnapshotHandle {
    fn name(&self) -> &'static str {
        "runtime_snapshot"
    }

    fn check(&self) -> BoxFuture<'_, HealthState> {
        Box::pin(async move {
            if self.revision().is_some() {
                HealthState::Healthy
            } else {
                HealthState::Unhealthy("Runtime snapshot is unavailable".to_owned())
            }
        })
    }
}

impl crate::routing::ModelCatalogValidator for RuntimeSnapshotHandle {
    fn validate_model_aliases(
        &self,
        aliases: Vec<crate::routing::ContributedModelAlias>,
    ) -> Result<(), RuntimeSnapshotCompileError> {
        self.acquire()
            .map_err(|_| RuntimeSnapshotCompileError::StoreUnavailable)?
            .as_ref()
            .clone()
            .with_model_aliases(aliases)
            .map(|_| ())
    }
}

/// Admin 提交配置后触发本进程刷新与跨进程通知的对象安全端口。
pub trait SnapshotControl: Send + Sync {
    fn publish_committed(&self, committed_revision: ConfigRevision) -> BoxFuture<'_, ()>;
}

#[derive(Default)]
struct RefreshPriorityState {
    pending_commits: usize,
    active_background: Option<CancellationToken>,
}

struct CommittedRefreshGuard {
    priority: Arc<SyncMutex<RefreshPriorityState>>,
}

impl CommittedRefreshGuard {
    fn begin(priority: Arc<SyncMutex<RefreshPriorityState>>) -> Self {
        let active = {
            let mut state = lock_unpoisoned(&priority);
            state.pending_commits += 1;
            state.active_background.clone()
        };
        // 先登记待发布提交，再取消正在等待目录的后台刷新，避免后者抢先重入。
        if let Some(active) = active {
            active.cancel();
        }
        Self { priority }
    }
}

impl Drop for CommittedRefreshGuard {
    fn drop(&mut self) {
        lock_unpoisoned(&self.priority).pending_commits -= 1;
    }
}

struct BackgroundRefreshGuard {
    priority: Arc<SyncMutex<RefreshPriorityState>>,
    cancellation: CancellationToken,
}

impl BackgroundRefreshGuard {
    fn begin(priority: Arc<SyncMutex<RefreshPriorityState>>) -> Option<Self> {
        let mut state = lock_unpoisoned(&priority);
        if state.pending_commits != 0 {
            return None;
        }
        let cancellation = CancellationToken::new();
        state.active_background = Some(cancellation.clone());
        drop(state);
        Some(Self {
            priority,
            cancellation,
        })
    }
}

impl Drop for BackgroundRefreshGuard {
    fn drop(&mut self) {
        lock_unpoisoned(&self.priority).active_background = None;
    }
}

// 仅中断未完成的后台读取或编译；配置提交负责发布最新事实并发送通知。
async fn preempt_for_committed_refresh<T>(
    cancellation: Option<&CancellationToken>,
    operation: impl Future<Output = T>,
) -> Result<T, RuntimeSnapshotCompileError> {
    let Some(cancellation) = cancellation else {
        return Ok(operation.await);
    };
    let cancelled = cancellation.cancelled().fuse();
    let operation = operation.fuse();
    pin_mut!(cancelled, operation);
    select_biased! {
        _ = cancelled => Err(RuntimeSnapshotCompileError::RevisionChanged),
        result = operation => Ok(result),
    }
}

/// 配置提交后的本进程快照发布与跨进程失效通知。
#[derive(Clone)]
pub struct RuntimeSnapshotPublisher {
    compiler: Arc<RuntimeSnapshotCompiler>,
    snapshots: RuntimeSnapshotHandle,
    subscriptions: Arc<dyn SnapshotSubscriptionPort>,
    refresh_lock: Arc<Mutex<()>>,
    refresh_priority: Arc<SyncMutex<RefreshPriorityState>>,
}

enum RefreshMode {
    Required,
    Reconcile,
    Committed,
}

impl RuntimeSnapshotPublisher {
    #[must_use]
    pub fn new(
        compiler: Arc<RuntimeSnapshotCompiler>,
        snapshots: RuntimeSnapshotHandle,
        subscriptions: Arc<dyn SnapshotSubscriptionPort>,
    ) -> Self {
        Self {
            compiler,
            snapshots,
            subscriptions,
            refresh_lock: Arc::new(Mutex::new(())),
            refresh_priority: Arc::new(SyncMutex::new(RefreshPriorityState::default())),
        }
    }

    /// 串行重编译并替换本进程快照；无法确认配置时暂停新请求。
    pub async fn refresh(&self) -> Result<ConfigRevision, RuntimeSnapshotCompileError> {
        self.refresh_with_mode(RefreshMode::Required).await
    }

    async fn refresh_with_mode(
        &self,
        mode: RefreshMode,
    ) -> Result<ConfigRevision, RuntimeSnapshotCompileError> {
        // 提交先登记并中断慢后台读取；发布/暂停仍在完整临界区内串行执行。
        let _committed = matches!(mode, RefreshMode::Committed)
            .then(|| CommittedRefreshGuard::begin(Arc::clone(&self.refresh_priority)));
        let _refresh = self.refresh_lock.lock().await;
        self.refresh_locked_with_priority(mode).await
    }

    async fn refresh_locked_with_priority(
        &self,
        mode: RefreshMode,
    ) -> Result<ConfigRevision, RuntimeSnapshotCompileError> {
        if matches!(mode, RefreshMode::Committed) {
            return self.refresh_locked(mode, None).await;
        }
        let background = BackgroundRefreshGuard::begin(Arc::clone(&self.refresh_priority))
            .ok_or(RuntimeSnapshotCompileError::RevisionChanged)?;
        self.refresh_locked(mode, Some(&background.cancellation))
            .await
    }

    async fn refresh_locked(
        &self,
        mode: RefreshMode,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ConfigRevision, RuntimeSnapshotCompileError> {
        let configuration_changed = if matches!(mode, RefreshMode::Reconcile) {
            let persisted_revision = preempt_for_committed_refresh(
                cancellation,
                self.compiler.store().current_config_revision(),
            )
            .await?
            .map_err(|_| {
                self.snapshots.suspend();
                RuntimeSnapshotCompileError::StoreUnavailable
            })?;
            let changed = runtime_revision_needs_refresh(
                self.published_revision().map(ConfigRevision::get),
                persisted_revision.get(),
            );
            if !changed && !self.provider_catalogs_need_refresh() {
                return Ok(persisted_revision);
            }
            changed
        } else {
            true
        };
        let compiled = if matches!(mode, RefreshMode::Committed) {
            match self.snapshots.acquire().ok() {
                Some(previous) => self.compiler.compile_with_cached_catalog(&previous).await,
                None => self.compiler.compile().await,
            }
        } else {
            preempt_for_committed_refresh(cancellation, self.compiler.compile()).await?
        };
        let snapshot = match compiled {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // 目录对账失败可继续服务旧快照；配置或权限变化无法确认时须暂停。
                if configuration_changed {
                    self.snapshots.suspend();
                }
                return Err(error);
            }
        };
        let revision = snapshot.revision();
        self.snapshots.publish(snapshot);
        Ok(revision)
    }

    #[must_use]
    pub fn published_revision(&self) -> Option<ConfigRevision> {
        self.snapshots.revision()
    }

    #[must_use]
    fn provider_catalogs_need_refresh(&self) -> bool {
        self.snapshots.provider_catalog_generations().as_ref()
            != Some(&self.compiler.provider_catalog_generations())
    }

    /// 数据库提交不能被目录或通知基础设施的暂时故障伪装成回滚。
    async fn publish_committed_inner(&self, committed_revision: ConfigRevision) {
        let _ = self.refresh_with_mode(RefreshMode::Committed).await;
        let _ = self
            .subscriptions
            .publish_snapshot_revision(committed_revision)
            .await;
    }

    /// 交给 Host 的周期对账与长驻订阅任务。
    pub fn worker_contributions(&self) -> Result<Vec<WorkerContribution>, WorkerDefinitionError> {
        let reconciliation_id = WorkerId::try_new(
            WorkerKind::RuntimeSnapshotReconciliation,
            "runtime_snapshot",
        )?;
        let schedule = WorkerSchedule::try_new(
            RECONCILIATION_INTERVAL,
            INITIAL_BACKOFF,
            MAXIMUM_BACKOFF,
            UNUSED_LEASE_TTL,
            UNUSED_LEASE_RENEWAL,
        )?;
        let reconciliation = WorkerRegistration::try_new(
            reconciliation_id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: None,
                task: Box::new(RuntimeSnapshotReconciliationTask {
                    publisher: self.clone(),
                }),
            },
        )?;
        let subscription_id =
            WorkerId::try_new(WorkerKind::RuntimeChangeSubscription, "runtime_snapshot")?;
        let restart = DaemonRestartPolicy::try_new(INITIAL_BACKOFF, MAXIMUM_BACKOFF)?;
        let subscription = WorkerRegistration::try_new(
            subscription_id,
            WorkerRunnable::Daemon {
                restart,
                task: Box::new(RuntimeSnapshotSubscriptionTask {
                    subscriptions: Arc::clone(&self.subscriptions),
                    publisher: self.clone(),
                }),
            },
        )?;
        Ok(vec![
            WorkerContribution::Registration(reconciliation),
            WorkerContribution::Registration(subscription),
        ])
    }
}

impl SnapshotControl for RuntimeSnapshotPublisher {
    fn publish_committed(&self, committed_revision: ConfigRevision) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.publish_committed_inner(committed_revision).await;
        })
    }
}

struct RuntimeSnapshotReconciliationTask {
    publisher: RuntimeSnapshotPublisher,
}

impl ScheduledTask for RuntimeSnapshotReconciliationTask {
    fn run_cycle(
        &self,
        _context: WorkerCycleContext,
    ) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            self.publisher
                .refresh_with_mode(RefreshMode::Reconcile)
                .await
                .map(|_| ())
                .map_err(|_| WorkerTaskError::safe("runtime snapshot reconciliation failed"))
        })
    }
}

struct RuntimeSnapshotSubscriptionTask {
    subscriptions: Arc<dyn SnapshotSubscriptionPort>,
    publisher: RuntimeSnapshotPublisher,
}

impl DaemonTask for RuntimeSnapshotSubscriptionTask {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let mut retry_delay = INITIAL_BACKOFF;
            loop {
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                let subscription = self.subscriptions.subscribe_snapshot_revisions().await;
                let mut subscription = match subscription {
                    Ok(subscription) => {
                        retry_delay = INITIAL_BACKOFF;
                        subscription
                    }
                    Err(_) => {
                        wait_or_cancel(&cancellation, retry_delay).await;
                        retry_delay = (retry_delay * 2).min(MAXIMUM_BACKOFF);
                        continue;
                    }
                };
                loop {
                    let cancelled = cancellation.cancelled().fuse();
                    let next = subscription.next().fuse();
                    pin_mut!(cancelled, next);
                    let notified = select_biased! {
                        _ = cancelled => return Ok(()),
                        next = next => next,
                    };
                    match notified {
                        Some(Ok(_)) => {
                            let _ = self.publisher.refresh().await;
                        }
                        Some(Err(_)) | None => break,
                    }
                }
            }
        })
    }
}

async fn wait_or_cancel(cancellation: &CancellationToken, duration: Duration) {
    let cancelled = cancellation.cancelled().fuse();
    let delay = Delay::new(duration).fuse();
    pin_mut!(cancelled, delay);
    select_biased! {
        _ = cancelled => {},
        _ = delay => {},
    }
}

/// 当前发布版本与持久版本不一致时必须重载；缺失和回退同样 fail closed。
#[must_use]
pub fn runtime_revision_needs_refresh(
    published_revision: Option<u64>,
    persisted_revision: u64,
) -> bool {
    published_revision != Some(persisted_revision)
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_unpoisoned<T>(lock: &SyncMutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
