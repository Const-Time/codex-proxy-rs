//! Processing budget suspension, resumption and retirement use the shared SDK clock.
use gateway_plugin_sdk::client::CallDeadline;
use std::time::Duration;
use tokio::time::{Instant, advance};

#[tokio::test(start_paused = true)]
async fn pending_waiter_tracks_pause_and_retains_only_unspent_time_on_resume() {
    let deadline = CallDeadline::new(Instant::now() + Duration::from_secs(10));
    let shared = deadline.clone();
    let waiter = tokio::spawn(async move { shared.wait().await });
    tokio::task::yield_now().await;
    advance(Duration::from_secs(3)).await;
    let pause = deadline.pause().unwrap();
    assert!(deadline.pause().is_none());
    advance(Duration::from_secs(100)).await;
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    assert_eq!(deadline.remaining(), Some(Duration::from_secs(7)));
    drop(pause);
    advance(Duration::from_secs(6)).await;
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    advance(Duration::from_secs(1)).await;
    waiter.await.unwrap();
    assert!(deadline.is_elapsed());
    assert!(deadline.pause().is_none());
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_cannot_be_revived_by_next() {
    let deadline = CallDeadline::new(Instant::now() + Duration::from_secs(1));
    advance(Duration::from_secs(1)).await;
    assert!(deadline.pause().is_none());
    deadline.wait().await;
    assert_eq!(deadline.remaining(), Some(Duration::ZERO));
}

#[tokio::test(start_paused = true)]
async fn resource_stream_clear_survives_a_late_pause_guard_drop() {
    let deadline = CallDeadline::new(Instant::now() + Duration::from_secs(1));
    let pause = deadline.pause().unwrap();
    deadline.clear();
    drop(pause);
    let shared = deadline.clone();
    let waiter = tokio::spawn(async move { shared.wait().await });
    advance(Duration::from_secs(100)).await;
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    assert_eq!(deadline.remaining(), None);
    assert!(deadline.pause().is_none());
    waiter.abort();
}
