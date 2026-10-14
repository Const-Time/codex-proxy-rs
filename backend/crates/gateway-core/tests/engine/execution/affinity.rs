//! Affinity reads retain the existing parent and never acquire execution resources.
use super::*;
use gateway_core::provider_ports::{
    ProviderSessionAffinityKey, ProviderSessionAffinityPort, ProviderStoreError,
    ProviderStoreErrorKind,
};

#[derive(Default)]
struct Affinity {
    account: Mutex<Option<ProviderAccountId>>,
    error: Mutex<Option<ProviderStoreErrorKind>>,
    pending: AtomicBool,
    loads: AtomicUsize,
    finished: AtomicUsize,
}
impl ProviderSessionAffinityPort for Affinity {
    fn load<'a>(
        &'a self,
        provider: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>> {
        Box::pin(async move {
            assert_eq!(provider.as_str(), "openai");
            assert_eq!(key.expose_to_store(), "fixture-affinity");
            self.loads.fetch_add(1, Ordering::SeqCst);
            struct Finished<'a>(&'a AtomicUsize);
            impl Drop for Finished<'_> {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _finished = Finished(&self.finished);
            if self.pending.load(Ordering::SeqCst) {
                futures::future::pending::<()>().await;
            }
            if let Some(kind) = *self.error.lock().unwrap() {
                return Err(ProviderStoreError::new(kind, "fixture affinity read"));
            }
            Ok(self.account.lock().unwrap().clone())
        })
    }
    fn bind<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        _: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        unreachable!("read-only lookup must not bind")
    }
    fn claim_or_load<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        _: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        unreachable!("read-only lookup must not claim")
    }
    fn compare_and_bind<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        _: &'a ProviderAccountId,
        _: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        unreachable!("read-only lookup must not replace")
    }
    fn clear<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        unreachable!("read-only lookup must not clear")
    }
}
fn inputs() -> (ProviderKind, ProviderSessionAffinityKey) {
    (
        ProviderKind::new("openai").unwrap(),
        ProviderSessionAffinityKey::try_new("fixture-affinity").unwrap(),
    )
}

#[test]
fn existing_affinity_is_only_a_scoped_hint_and_never_starts_or_charges_execution() {
    let port = Arc::new(Affinity::default());
    let admissions = Arc::new(Admissions::default());
    let budget = Arc::new(Budget::default());
    let service = service(admissions.clone(), budget.clone()).with_session_affinity(port.clone());
    let parent = prepare(&service);
    let authority = parent.nested_context();
    let (provider, key) = inputs();
    for (stored, expected) in [
        (None, None),
        (Some("acct_start"), Some("acct_start")),
        (Some("acct_outside"), None),
    ] {
        *port.account.lock().unwrap() = stored.map(|id| ProviderAccountId::new(id).unwrap());
        let account =
            block_on(service.lookup_session_affinity(&authority, &provider, &key)).unwrap();
        assert_eq!(account.as_ref().map(ProviderAccountId::as_str), expected);
    }
    assert_eq!(port.loads.load(Ordering::SeqCst), 3);
    assert!(!admissions.active.load(Ordering::SeqCst));
    assert!(!budget.active.load(Ordering::SeqCst));
}

#[test]
fn provider_outside_scope_and_ended_parent_are_rejected_before_reading() {
    let port = Arc::new(Affinity::default());
    let service = service(Arc::default(), Arc::default()).with_session_affinity(port.clone());
    let parent = prepare(&service);
    let authority = parent.nested_context();
    let (provider, key) = inputs();
    assert_failure(
        block_on(service.lookup_session_affinity(
            &authority,
            &ProviderKind::new("xai").unwrap(),
            &key,
        )),
        GatewayErrorKind::PolicyDenied,
    );
    drop(parent);
    assert_failure(
        block_on(service.lookup_session_affinity(&authority, &provider, &key)),
        GatewayErrorKind::Cancelled,
    );
    assert_eq!(port.loads.load(Ordering::SeqCst), 0);
}

#[test]
fn an_authorized_account_of_another_provider_is_not_an_affinity_hit() {
    let port = Arc::new(Affinity::default());
    *port.account.lock().unwrap() = Some(ProviderAccountId::new("acct_wrong_provider").unwrap());
    let openai = ProviderKind::new("openai").unwrap();
    let xai = ProviderKind::new("xai").unwrap();
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([
            (
                ProviderAccountId::new("acct_start").unwrap(),
                RuntimeAccount::new(openai, BTreeSet::new()),
            ),
            (
                ProviderAccountId::new("acct_wrong_provider").unwrap(),
                RuntimeAccount::new(xai, BTreeSet::new()),
            ),
        ]))),
        ClientRoutingScope::all_accounts(),
    ));
    let policy = ClientPolicy::new(
        ClientApiKeyId::new("key_start_test").unwrap(),
        PlaintextClientApiKey::new("sk_start_test").unwrap(),
        scope,
        true,
        RateLimits::unlimited(),
    );
    let service = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(start_snapshot_with_clients(1, vec![policy])),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::default(),
        Arc::new(Admissions::default()),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
    .with_session_affinity(port);
    let parent = prepare(&service);
    let (provider, key) = inputs();
    assert_eq!(
        block_on(service.lookup_session_affinity(&parent.nested_context(), &provider, &key))
            .unwrap(),
        None
    );
}

#[test]
fn cancelled_and_revoked_parents_drop_pending_reads_without_late_hints() {
    for revoke_key in [false, true] {
        let port = Arc::new(Affinity::default());
        port.pending.store(true, Ordering::SeqCst);
        let service = service(Arc::default(), Arc::default()).with_session_affinity(port.clone());
        let parent = prepare(&service);
        let authority = parent.nested_context();
        let (provider, key) = inputs();
        block_on(async {
            let mut read = Box::pin(service.lookup_session_affinity(&authority, &provider, &key));
            assert!(futures::poll!(read.as_mut()).is_pending());
            if revoke_key {
                parent
                    .client()
                    .bound_access()
                    .unwrap()
                    .cancellation()
                    .cancel();
            } else {
                parent.cancellation().cancel();
            }
            assert_failure(
                read.await,
                if revoke_key {
                    GatewayErrorKind::Unauthorized
                } else {
                    GatewayErrorKind::Cancelled
                },
            );
        });
        assert_eq!(port.loads.load(Ordering::SeqCst), 1);
        assert_eq!(port.finished.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn original_model_deadline_and_coordination_timeout_bound_the_same_read() {
    for short_parent in [false, true] {
        let port = Arc::new(Affinity::default());
        port.pending.store(true, Ordering::SeqCst);
        let service = service(Arc::default(), Arc::default()).with_session_affinity(port.clone());
        let mut parent = prepare(&service);
        if short_parent {
            let mut values = parent.settings().execution_values().unwrap();
            values.timeout_ms = Some(20);
            let settings = parent
                .settings()
                .replace_execution(&values, "fixture-affinity")
                .unwrap();
            parent = parent.with_settings(settings).unwrap();
        }
        let authority = parent.nested_context();
        let (provider, key) = inputs();
        let started = std::time::Instant::now();
        assert_failure(
            block_on(service.lookup_session_affinity(&authority, &provider, &key)),
            if short_parent {
                GatewayErrorKind::Timeout
            } else {
                GatewayErrorKind::ProviderInfrastructureUnavailable
            },
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(port.finished.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn store_errors_keep_safe_gateway_classes_and_missing_port_is_explicit() {
    let port = Arc::new(Affinity::default());
    let service = service(Arc::default(), Arc::default()).with_session_affinity(port.clone());
    let parent = prepare(&service);
    let authority = parent.nested_context();
    let (provider, key) = inputs();
    for (stored, expected) in [
        (
            ProviderStoreErrorKind::Unavailable,
            GatewayErrorKind::ProviderInfrastructureUnavailable,
        ),
        (
            ProviderStoreErrorKind::InvalidData,
            GatewayErrorKind::Internal,
        ),
        (ProviderStoreErrorKind::Conflict, GatewayErrorKind::Internal),
    ] {
        *port.error.lock().unwrap() = Some(stored);
        assert_failure(
            block_on(service.lookup_session_affinity(&authority, &provider, &key)),
            expected,
        );
    }
    let unavailable = super::service(Arc::default(), Arc::default());
    let parent = prepare(&unavailable);
    assert_failure(
        block_on(unavailable.lookup_session_affinity(&parent.nested_context(), &provider, &key)),
        GatewayErrorKind::Unsupported,
    );
}
