use super::*;
use gateway_admin::plugins::PluginManager;
use gateway_plugin_runtime::PluginEngine;

struct RootFixture {
    manager: PluginManager,
    digest: String,
    _cache: tempfile::TempDir,
}
impl RootFixture {
    async fn new(source: core::Source) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let engine = Arc::new(
            PluginEngine::new(
                cache.path().into(),
                "3.11.2".parse().unwrap(),
                store.clone(),
            )
            .with_services(source),
        );
        let manager = PluginManager::new(store, engine);
        let artifact = manager
            .upload(authentication_archive("1.0.0"), &actor())
            .await
            .unwrap();
        Self {
            manager,
            digest: artifact.sha256,
            _cache: cache,
        }
    }
    fn command(&self, revision: u64, registration: bool) -> PluginUpdate {
        let mut command = update(&self.digest, revision, true);
        command.frontend_authentication = Some(PluginAuthenticationBinding {
            failure_policy: PluginAuthenticationPolicy::Reject,
            identities: std::collections::BTreeMap::from([("alice".into(), "key_alice".into())]),
        });
        if registration {
            command.configuration = json!({"adapter_mode":"registration-service"});
        }
        command
    }
}
fn source(probe: &Arc<Probe>) -> core::Source {
    let factory = factory(probe);
    Arc::new(move || Ok(Some(factory.clone())))
}
fn origin(probe: &Arc<Probe>) -> core::Origin {
    core::Origin {
        context: core::Context {
            operation: "host.services.call",
            request_id: "authenticated-ingress".into(),
            call_id: "auth-origin".into(),
            parent_call_id: None,
            cancellation: Default::default(),
            deadline_at: Default::default(),
            call_scope: Default::default(),
            dispatcher: None,
        },
        factory: Some(factory(probe)),
    }
}

#[tokio::test]
async fn registration_services_freeze_rpc_source_and_source_failure_preserves_old_generation() {
    let probe = Arc::new(Probe::default());
    let failed = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let f = RootFixture::new({
        let port = factory(&probe);
        let failed = failed.clone();
        let reads = reads.clone();
        Arc::new(move || {
            reads.fetch_add(1, Ordering::SeqCst);
            if failed.load(Ordering::SeqCst) {
                Err(core::Error::unavailable("source unavailable"))
            } else {
                Ok(Some(port.clone()))
            }
        })
    })
    .await;
    let installed = f
        .manager
        .update(None, f.command(0, true), &actor())
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 3);
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].instance, installed.id);
        assert_eq!(facts[0].revision, 1);
        assert_eq!(facts[0].context.request_id, facts[0].context.call_id);
        assert!(facts[0].context.cancellation.is_cancelled());
        assert!(facts[0].context.call_scope.excluded_instances().is_empty());
        assert_eq!(facts[0].context.call_id, facts[1].context.call_id);
    }
    failed.store(true, Ordering::SeqCst);
    assert!(
        f.manager
            .update(Some(&installed.id), f.command(1, true), &actor())
            .await
            .is_err()
    );
    assert_eq!(f.manager.list().await.unwrap()[0].revision, 1);
    assert!(
        f.manager
            .authenticate("Bearer external-alice".into(), None)
            .await
            .is_err()
    );
    failed.store(false, Ordering::SeqCst);
    assert!(matches!(
        f.manager
            .authenticate("Bearer external-alice".into(), None)
            .await
            .unwrap(),
        PluginAuthenticationDecision::Authenticated { .. }
    ));
    f.manager.shutdown().await;
}

#[tokio::test]
async fn authentication_services_preserve_ingress_factory_and_identity_without_global_fallback() {
    let root = Arc::new(Probe::default());
    let f = RootFixture::new(source(&root)).await;
    let installed = f
        .manager
        .update(None, f.command(0, false), &actor())
        .await
        .unwrap();
    let probe = Arc::new(Probe::default());
    for _ in 0..35 {
        let result = f
            .manager
            .authenticate("Bearer external-service".into(), Some(origin(&probe)))
            .await
            .unwrap();
        let PluginAuthenticationDecision::Authenticated { key, .. } = result else {
            panic!()
        };
        assert_eq!(key.as_str(), "key_alice");
    }
    let mut unavailable = origin(&probe);
    unavailable.factory = None;
    assert!(
        f.manager
            .authenticate("Bearer external-service".into(), Some(unavailable))
            .await
            .is_err()
    );
    assert!(root.facts.lock().unwrap().is_empty());
    {
        let facts = probe.facts.lock().unwrap();
        assert_eq!(facts.len(), 35);
        for fact in facts.iter() {
            assert_eq!(fact.instance, installed.id);
            assert_eq!(fact.context.request_id, "authenticated-ingress");
            assert!(fact.context.cancellation.is_cancelled());
        }
    }
    let _ = f
        .manager
        .authenticate("Bearer external-service".into(), None)
        .await
        .unwrap();
    assert_eq!(root.facts.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn authentication_services_obey_original_cancel_and_plugin_disable_drops_waiter() {
    for disable in [false, true] {
        let f = RootFixture::new(source(&Arc::new(Probe::default()))).await;
        let installed = f
            .manager
            .update(None, f.command(0, false), &actor())
            .await
            .unwrap();
        let probe = Arc::new(Probe::default());
        probe.wait.store(true, Ordering::SeqCst);
        let origin = origin(&probe);
        let cancellation = origin.context.cancellation.clone();
        let call = f
            .manager
            .authenticate("Bearer external-service".into(), Some(origin));
        tokio::pin!(call);
        tokio::select! { result=&mut call=>panic!("callback must wait: {}",result.is_err()), ()=probe.entered.notified()=>{} }
        if disable {
            let mut command = f.command(1, false);
            command.enabled = false;
            f.manager
                .update(Some(&installed.id), command, &actor())
                .await
                .unwrap();
            assert!(!cancellation.is_cancelled());
        } else {
            cancellation.cancel();
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut call)
                .await
                .unwrap()
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !probe.dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            probe.facts.lock().unwrap()[0]
                .context
                .cancellation
                .is_cancelled()
        );
        f.manager.shutdown().await;
    }
}
