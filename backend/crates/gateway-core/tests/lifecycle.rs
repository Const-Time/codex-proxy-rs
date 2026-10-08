use futures::FutureExt as _;
use std::future::Future as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Wake, Waker};

use gateway_core::lifecycle::CancellationToken;

#[test]
fn cancellation_token_should_wake_current_state() {
    let token = CancellationToken::new();
    token.cancel();

    assert!(token.is_cancelled());
}
use gateway_core::lifecycle::{ConnectionDraining, ConnectionGuard, ConnectionLifecycle};

#[derive(Default)]
struct LifecycleState {
    draining: AtomicBool,
    active: AtomicUsize,
}

struct TestGuard {
    state: Arc<LifecycleState>,
}

impl ConnectionGuard for TestGuard {}

impl Drop for TestGuard {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct TestLifecycle {
    state: Arc<LifecycleState>,
    cancellation: CancellationToken,
}

impl TestLifecycle {
    fn new() -> Self {
        Self {
            state: Arc::new(LifecycleState::default()),
            cancellation: CancellationToken::new(),
        }
    }

    fn begin_draining(&self) {
        self.state.draining.store(true, Ordering::Release);
        self.cancellation.cancel();
    }

    fn active(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }
}

impl ConnectionLifecycle for TestLifecycle {
    fn try_register(&self) -> Result<Box<dyn ConnectionGuard>, ConnectionDraining> {
        if self.state.draining.load(Ordering::Acquire) {
            return Err(ConnectionDraining);
        }
        self.state.active.fetch_add(1, Ordering::AcqRel);
        if self.state.draining.load(Ordering::Acquire) {
            self.state.active.fetch_sub(1, Ordering::AcqRel);
            return Err(ConnectionDraining);
        }
        Ok(Box::new(TestGuard {
            state: Arc::clone(&self.state),
        }))
    }

    fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn is_draining(&self) -> bool {
        self.state.draining.load(Ordering::Acquire)
    }
}

#[test]
fn connection_guard_drop_releases_active_registration() {
    let lifecycle = TestLifecycle::new();
    let guard = lifecycle.try_register().expect("registration before drain");
    drop(guard);

    assert_eq!(lifecycle.active(), 0);
}

#[test]
fn connection_registration_rejects_after_drain_linearization() {
    let lifecycle = TestLifecycle::new();
    lifecycle.begin_draining();

    assert!(matches!(lifecycle.try_register(), Err(ConnectionDraining)));
}

#[test]
fn connection_lifecycle_contract_is_object_safe() {
    let lifecycle = TestLifecycle::new();
    let object: &dyn ConnectionLifecycle = &lifecycle;

    assert!(!object.is_draining());
}

#[test]
fn discarded_cancellation_branches_release_allocations() {
    let token = CancellationToken::new();
    // 预热一次，允许取消信号保留固定大小的通知状态。
    discard_cancellation_branch(&token);

    let allocations = allocation_counter::measure(|| {
        for _ in 0..10_000 {
            discard_cancellation_branch(&token);
        }
    });

    assert_eq!(allocations.count_current, 0, "{allocations:?}");
    assert_eq!(allocations.bytes_current, 0, "{allocations:?}");
    assert!(!token.is_cancelled());
}

fn discard_cancellation_branch(token: &CancellationToken) {
    // select 先轮询取消分支，再由另一个就绪分支获胜并丢弃取消等待。
    let selected = futures::future::select(Box::pin(token.cancelled()), futures::future::ready(()))
        .now_or_never();
    assert!(matches!(selected, Some(futures::future::Either::Right(_))));
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn cancellation_wakes_all_live_waiters_after_other_waits_are_dropped() {
    let token = CancellationToken::new();
    let shared = token.clone();
    let tokens = [&token, &shared];
    let mut waiters = Vec::new();

    for token in tokens.into_iter().cycle().take(64) {
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut waiting = Box::pin(token.cancelled());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        waiters.push((waiting, counter));
    }
    for _ in 0..100 {
        discard_cancellation_branch(&token);
    }

    shared.cancel();
    token.cancel();

    for (waiting, counter) in waiters {
        assert!(counter.0.load(Ordering::Relaxed) > 0);
        assert!(waiting.now_or_never().is_some());
        assert_eq!(Arc::strong_count(&counter), 1);
    }
}

#[test]
fn cancellation_racing_with_wait_registration_and_drop_does_not_lose_wakeup() {
    for index in 0..256 {
        let parent = CancellationToken::new();
        let token = if index % 2 == 0 {
            parent.clone()
        } else {
            CancellationToken::linked([parent.clone()])
        };
        let mut dropped_wait = Box::pin(token.cancelled());
        assert!(dropped_wait.as_mut().now_or_never().is_none());
        let start = Barrier::new(2);
        let finished = Barrier::new(2);
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));

        std::thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                parent.cancel();
                finished.wait();
            });

            start.wait();
            drop(dropped_wait);
            let mut waiting = Box::pin(token.cancelled());
            let pending = waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending();
            finished.wait();

            // 取消完成后再检查，既验证就绪结果，也验证 Pending 路径实际收到唤醒。
            if pending {
                assert!(counter.0.load(Ordering::Relaxed) > 0);
                assert!(waiting.now_or_never().is_some());
            }
        });
        assert!(token.is_cancelled());
        assert_eq!(Arc::strong_count(&counter), 2);
    }
}

#[test]
fn linked_cancellation_wakes_on_either_parent_without_revoking_siblings() {
    let first = CancellationToken::new();
    let second = CancellationToken::new();
    let child = CancellationToken::linked([first.clone(), second.clone()]);
    let sibling = CancellationToken::linked([first.clone()]);
    assert!(!child.is_cancelled());
    assert!(child.cancelled().now_or_never().is_none());
    child.cancel();
    assert!(child.is_cancelled());
    assert!(!first.is_cancelled());
    assert!(!second.is_cancelled());
    assert!(!sibling.is_cancelled());
    first.cancel();
    assert!(sibling.is_cancelled());
    for selected in 0..2 {
        let parents = [CancellationToken::new(), CancellationToken::new()];
        let child = CancellationToken::linked(parents.clone());
        let future = child.cancelled();
        futures::pin_mut!(future);
        assert!(future.as_mut().now_or_never().is_none());
        parents[selected].cancel();
        assert!(future.now_or_never().is_some());
        assert!(child.is_cancelled());
        assert!(!parents[1 - selected].is_cancelled());
    }
}
#[test]
fn linked_cancellation_observes_ancestors_and_cancelled_parents_without_a_runtime() {
    let root = CancellationToken::new();
    let middle = CancellationToken::linked([root.clone()]);
    let leaf = CancellationToken::linked([middle.clone()]);
    for _ in 0..1000 {
        assert!(leaf.cancelled().now_or_never().is_none());
    }
    let future = leaf.cancelled();
    futures::pin_mut!(future);
    assert!(future.as_mut().now_or_never().is_none());
    root.cancel();
    assert!(future.now_or_never().is_some());
    assert!(middle.is_cancelled());
    let late = CancellationToken::linked([root]);
    assert!(late.cancelled().now_or_never().is_some());
    let independent = CancellationToken::linked([]);
    assert!(!independent.is_cancelled());
    independent.cancel();
    assert!(independent.cancelled().now_or_never().is_some());
}

#[test]
fn discarded_linked_cancellation_waits_release_all_registrations() {
    let parents = [CancellationToken::new(), CancellationToken::new()];
    let child = CancellationToken::linked(parents);
    discard_cancellation_branch(&child);
    let allocations = allocation_counter::measure(|| {
        for _ in 0..10000 {
            discard_cancellation_branch(&child);
        }
    });
    assert_eq!(allocations.count_current, 0, "{allocations:?}");
    assert_eq!(allocations.bytes_current, 0, "{allocations:?}");
    assert!(!child.is_cancelled());
}
