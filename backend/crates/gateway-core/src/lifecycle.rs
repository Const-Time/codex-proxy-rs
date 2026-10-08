//! Host 与 API 之间的连接注册、drain 与取消契约。

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use event_listener::Event;

/// Maximum interval of recoverable request ownership after its maintenance guard is lost.
/// This is independent of the explicit execution timeout.
pub const REQUEST_LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

struct CancellationState {
    cancelled: AtomicBool,
    event: Event,
    parents: Vec<CancellationToken>,
}

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
            parents: Vec::new(),
        }))
    }

    /// A local cancellation scope revoked by any parent. Cancelling the child never
    /// cancels a parent or sibling, and no background propagation task is required.
    #[must_use]
    pub fn linked(parents: impl IntoIterator<Item = Self>) -> Self {
        Self(Arc::new(CancellationState {
            cancelled: AtomicBool::new(false),
            event: Event::new(),
            parents: parents.into_iter().collect(),
        }))
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
        let mut pending: Vec<_> = self.0.parents.iter().collect();
        while let Some(token) = pending.pop() {
            if token.0.cancelled.load(Ordering::Acquire) {
                return true;
            }
            pending.extend(&token.0.parents);
        }
        false
    }

    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        // 等待 future 被 select 丢弃时自动注销，不积累已失效的等待项。
        let listener = self.0.event.listen();
        // 注册后复查，避免取消发生在首次检查与注册之间时丢失通知。
        if self.is_cancelled() {
            return;
        }
        if self.0.parents.is_empty() {
            listener.await;
            return;
        }
        use futures::{StreamExt as _, stream::FuturesUnordered};
        let mut listeners = FuturesUnordered::new();
        listeners.push(listener);
        let mut pending: Vec<_> = self.0.parents.iter().collect();
        while let Some(token) = pending.pop() {
            let listener = token.0.event.listen();
            if token.0.cancelled.load(Ordering::Acquire) {
                return;
            }
            listeners.push(listener);
            pending.extend(&token.0.parents);
        }
        listeners.next().await;
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
