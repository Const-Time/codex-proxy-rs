use gateway_core::engine::middleware::{
    MiddlewareError,
    http::{Response, empty_body, upgrade::Upgraded},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn upgraded(status: http::StatusCode) -> (Upgraded, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let starts = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let held = DropCount(drops.clone());
    let active = starts.clone();
    let task = Box::pin(async move {
        let _held = held;
        active.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
        Ok(())
    });
    (Upgraded::new(status, task).unwrap(), starts, drops)
}

#[test]
fn changed_handshake_cannot_start_task_and_all_response_clones_share_single_consumption() {
    for status in [http::StatusCode::SWITCHING_PROTOCOLS, http::StatusCode::OK] {
        let (upgrade, starts, drops) = upgraded(status);
        let other = upgrade.clone();
        assert!(upgrade.take_task(http::StatusCode::FORBIDDEN).is_none());
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        let task = other.take_task(status).unwrap();
        assert!(upgrade.take_task(status).is_none());
        drop(upgrade);
        drop(other);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(task);
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn replacing_or_dropping_response_releases_unstarted_session_task() {
    let (upgrade, starts, drops) = upgraded(http::StatusCode::SWITCHING_PROTOCOLS);
    let mut response = Response::new(empty_body());
    response.extensions_mut().insert(upgrade.clone());
    drop(upgrade);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    response = Response::new(empty_body());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    drop(response);
}

#[test]
fn non_upgrade_status_cannot_own_a_session_task() {
    assert!(matches!(
        Upgraded::new(http::StatusCode::FORBIDDEN, Box::pin(async { Ok(()) })),
        Err(MiddlewareError::InvalidState)
    ));
}
