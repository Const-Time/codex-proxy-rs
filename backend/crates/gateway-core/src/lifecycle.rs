//! Host 与 API 之间的连接注册、drain 与取消契约。

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use event_listener::Event;

/// Maximum interval of recoverable request ownership after its maintenance guard is lost.
/// This is independent of the explicit execution timeout.
pub const REQUEST_LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// Optional total execution budget. Absence means no total deadline, not a distant timestamp.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Deadline(Option<std::time::SystemTime>);
impl Deadline {
    pub fn from_timeout(
        started_at: std::time::SystemTime,
        timeout: Option<std::time::Duration>,
    ) -> Option<Self> {
        match timeout {
            Some(timeout) => started_at.checked_add(timeout).map(|at| Self(Some(at))),
            None => Some(Self(None)),
        }
    }
    pub const fn at(self) -> Option<std::time::SystemTime> {
        self.0
    }
    pub fn remaining(self) -> Option<std::time::Duration> {
        self.0.map(|at| {
            at.duration_since(std::time::SystemTime::now())
                .unwrap_or_default()
        })
    }
    pub fn is_elapsed(self) -> bool {
        self.remaining().is_some_and(|left| left.is_zero())
    }
    pub fn bounded(self, maximum: std::time::Duration) -> std::time::Duration {
        self.remaining().map_or(maximum, |left| left.min(maximum))
    }
    pub fn cap(self, at: std::time::SystemTime) -> Self {
        Self(Some(self.0.map_or(at, |current| current.min(at))))
    }
    pub fn lease_deadline(self) -> std::time::SystemTime {
        self.cap(std::time::SystemTime::now() + REQUEST_LEASE_TTL)
            .at()
            .expect("bounded recovery lease")
    }
    pub fn wait(self) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async move {
            loop {
                match self.remaining() {
                    None => futures::future::pending::<()>().await,
                    Some(left) if left.is_zero() => return,
                    Some(left) => futures_timer::Delay::new(left.min(REQUEST_LEASE_TTL)).await,
                }
            }
        })
    }
}
impl From<std::time::SystemTime> for Deadline {
    fn from(at: std::time::SystemTime) -> Self {
        Self(Some(at))
    }
}

struct CancellationState {
    cancelled: AtomicBool,
    event: Event,
    parents: RwLock<Vec<CancellationToken>>,
}

// Serialize additions so simultaneous inverse links cannot create a reference cycle.
static CANCELLATION_LINKS: Mutex<()> = Mutex::new(());

/// 可克隆的请求、任务与连接取消信号。
#[derive(Clone)]
pub struct CancellationToken(Arc<CancellationState>);

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(CancellationState {
            cancelled: AtomicBool::new(false),
            event: Event::new(),
            parents: RwLock::new(Vec::new()),
        }))
    }

    /// A local cancellation scope revoked by any parent. Cancelling the child never
    /// cancels a parent or sibling, and no background propagation task is required.
    #[must_use]
    pub fn linked(parents: impl IntoIterator<Item = Self>) -> Self {
        Self(Arc::new(CancellationState {
            cancelled: AtomicBool::new(false),
            event: Event::new(),
            parents: RwLock::new(parents.into_iter().collect()),
        }))
    }

    /// Add a revocation source after an asynchronous policy has been consulted.
    /// Existing clones and waiters observe it; cancellation remains one-way.
    /// Reject cycles without retaining the proposed parent.
    #[must_use]
    pub fn link_parent(&self, parent: Self) -> bool {
        let _links = CANCELLATION_LINKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pending = vec![parent.clone()];
        let mut visited = std::collections::HashSet::new();
        while let Some(token) = pending.pop() {
            if Arc::ptr_eq(&self.0, &token.0) {
                return false;
            }
            if visited.insert(Arc::as_ptr(&token.0) as usize) {
                pending.extend(token.parents());
            }
        }
        let mut parents = self
            .0
            .parents
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !parents.iter().any(|p| Arc::ptr_eq(&p.0, &parent.0)) {
            parents.push(parent);
            self.0.event.notify(usize::MAX);
        }
        true
    }

    fn parents(&self) -> Vec<Self> {
        self.0
            .parents
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn cancel(&self) {
        if self.0.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        self.0.event.notify(usize::MAX);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        if self.0.cancelled.load(Ordering::Acquire) {
            return true;
        }
        let mut pending = self.parents();
        let mut visited = std::collections::HashSet::new();
        while let Some(token) = pending.pop() {
            if !visited.insert(Arc::as_ptr(&token.0) as usize) {
                continue;
            }
            if token.0.cancelled.load(Ordering::Acquire) {
                return true;
            }
            pending.extend(token.parents());
        }
        false
    }

    pub async fn cancelled(&self) {
        use futures::{StreamExt as _, stream::FuturesUnordered};
        loop {
            let mut listeners = FuturesUnordered::new();
            let mut pending = vec![self.clone()];
            let mut visited = std::collections::HashSet::new();
            while let Some(token) = pending.pop() {
                if !visited.insert(Arc::as_ptr(&token.0) as usize) {
                    continue;
                }
                // Register before reading sources/state. A source addition wakes
                // this loop to subscribe to its events without a background task.
                listeners.push(token.0.event.listen());
                if token.0.cancelled.load(Ordering::Acquire) {
                    return;
                }
                pending.extend(token.parents());
            }
            listeners.next().await;
        }
    }
}

/// 进程已进入 drain，新连接不得再注册。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionDraining;

impl fmt::Display for ConnectionDraining {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("connection lifecycle is draining")
    }
}

impl std::error::Error for ConnectionDraining {}

/// 一次成功的活跃连接注册。
///
/// 实现必须在 guard `Drop` 时原子减少活跃连接计数。
pub trait ConnectionGuard: Send + 'static {}

/// API 消费、Host 实现的进程连接生命周期。
pub trait ConnectionLifecycle: Send + Sync {
    /// 原子地检查 drain 状态并注册一个活跃连接。
    ///
    /// 当本方法成功时，drain 必须等待返回的 guard 被释放；
    /// 当 drain 已经线性化生效时，本方法必须返回 [`ConnectionDraining`]。
    fn try_register(&self) -> Result<Box<dyn ConnectionGuard>, ConnectionDraining>;

    fn cancellation(&self) -> CancellationToken;

    fn is_draining(&self) -> bool;
}
