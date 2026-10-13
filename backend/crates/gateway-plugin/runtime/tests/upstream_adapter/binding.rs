use super::execution::{Account, invocation};
use super::*;
use async_trait::async_trait;
use futures::StreamExt;
use gateway_admin::{
    model::{AdminError, plugins::*},
    ports::plugins::PluginProcess,
};
use gateway_core::{
    account::{AccountSelectionPolicy, RotationStrategy, scope::*},
    engine::{
        AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
        upstream_adapter::UpstreamAdapterPlan,
    },
    identity::ProviderKind,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    routing::{PublicModelId, UpstreamModelId},
};
use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

// Exercise real preparation/publication while production enablement remains gated.
// The only bypass is enabled=false during validation; declarations and schemas still validate.
struct CandidateRuntime(Arc<PluginEngine>);
#[async_trait]
impl PluginRuntime for CandidateRuntime {
    fn describe(&self, info: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        self.0.describe(info)
    }
    async fn inspect(&self, archive: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        self.0.inspect(archive).await
    }
    async fn validate(
        &self,
        record: &PluginRecord,
        artifact: &PluginArtifact,
        previous: Option<&PluginArtifact>,
    ) -> Result<(), AdminError> {
        let mut record = record.clone();
        record.enabled = false;
        self.0.validate(&record, artifact, previous).await
    }
    async fn prepare(
        &self,
        record: &PluginRecord,
        artifact: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        self.0.prepare(record, artifact).await
    }
}
struct Fixture {
    manager: PluginManager,
    runtime: Arc<CandidateRuntime>,
    store: Arc<MemoryStore>,
    digest: String,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(CandidateRuntime(Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ))));
        let manager = PluginManager::new(store.clone(), runtime.clone());
        let digest = manager
            .upload(upstream_archive("1.0.0"), &actor())
            .await
            .unwrap()
            .sha256;
        Self {
            manager,
            runtime,
            store,
            digest,
            _cache: cache,
        }
    }
    fn command(&self, revision: u64, scope: PluginExecutionScope) -> PluginUpdate {
        let mut command = update(&self.digest, revision, true);
        command.upstream_adapter = Some(PluginUpstreamAdapterBinding { scope });
        command
    }
    async fn install(&self, scope: PluginExecutionScope) -> PluginView {
        self.manager
            .update(None, self.command(0, scope), &actor())
            .await
            .unwrap()
    }
}
fn keys(key: &str) -> PluginExecutionScope {
    PluginExecutionScope::Keys {
        ids: vec![key.into()],
    }
}
fn context(key: &str, model: &str, group: Option<&str>) -> AttemptContext {
    let mut account = AccountAttemptContext::new(BTreeSet::new(), None, None);
    if let Some(group) = group {
        let group = AccountGroupId::new(group).unwrap();
        account = account.with_account_scope(Arc::new(FrozenAccountScope::new(
            Arc::default(),
            ClientRoutingScope::restricted(
                vec![RoutingGroupSnapshot::new(group.clone(), "fixture".into())],
                BTreeSet::from([group]),
                BTreeSet::new(),
            )
            .unwrap(),
        )));
    }
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_fixture").unwrap(),
            ClientApiKeyId::new(key).unwrap(),
        )
        .with_request_metadata(Some(PublicModelId::new(model).unwrap()), "http_json"),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(20),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(1).unwrap(),
            Duration::ZERO,
        ),
        account,
        None,
        CancellationToken::new(),
    )
}
fn select(
    registry: &gateway_admin::plugins::middleware::PluginMiddlewareRegistry,
    key: &str,
    model: &str,
    provider: &str,
) -> Result<
    Option<Arc<dyn gateway_core::engine::upstream_adapter::UpstreamAdapter>>,
    gateway_core::error::ProviderError,
> {
    registry.select(
        &context(key, model, None),
        &ProviderKind::new(provider).unwrap(),
        &UpstreamModelId::new("native-model").unwrap(),
    )
}
#[tokio::test]
async fn upstream_binding_selects_public_model_and_key_then_revokes_old_publication() {
    let f = Fixture::new().await;
    let view = f.install(keys("key_fixture")).await;
    assert!(view.upstream_adapter.is_some());
    let registry = f.manager.middleware_registry().unwrap();
    assert!(
        select(&registry, "key_other", "public-model", "openai")
            .unwrap()
            .is_none()
    );
    assert!(
        select(&registry, "key_fixture", "native-model", "openai")
            .unwrap()
            .is_none()
    );
    assert!(
        select(&registry, "key_fixture", "public-model", "xai")
            .unwrap()
            .is_none()
    );
    let adapter = select(&registry, "key_fixture", "public-model", "openai")
        .unwrap()
        .unwrap();
    let mut stream = invocation("synthetic", Account::new(), None, CancellationToken::new())
        .into_stream(adapter);
    let mut count = 0;
    while let Some(event) = stream.next().await {
        event.unwrap();
        count += 1;
    }
    assert_eq!(count, 2);
    let mut command = f.command(view.revision, keys("key_fixture"));
    command.enabled = false;
    f.manager
        .update(Some(&view.id), command, &actor())
        .await
        .unwrap();
    assert!(registry.cancellation().is_cancelled());
    assert!(select(&registry, "key_fixture", "public-model", "openai").is_err());
    assert!(
        select(
            &f.manager.middleware_registry().unwrap(),
            "key_fixture",
            "public-model",
            "openai"
        )
        .unwrap()
        .is_none()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn upstream_binding_rejects_overlapping_candidates_without_replacing_the_old_generation() {
    let f = Fixture::new().await;
    let first = f.install(keys("key_fixture")).await;
    let registry = f.manager.middleware_registry().unwrap();
    let access = f.manager.access(&first.id, first.revision).unwrap();
    for scope in [
        PluginExecutionScope::All {},
        keys("key_fixture"),
        PluginExecutionScope::Groups {
            ids: vec!["grp_11111111111111111111111111111111".into()],
        },
    ] {
        assert!(
            f.manager
                .update(None, f.command(0, scope), &actor())
                .await
                .is_err()
        );
        assert!(!registry.cancellation().is_cancelled());
        assert!(!access.is_revoked());
        assert_eq!(f.store.list().await.unwrap().len(), 1);
    }
    f.install(keys("key_other")).await;
    assert!(registry.cancellation().is_cancelled());
    let registry = f.manager.middleware_registry().unwrap();
    assert!(
        select(&registry, "key_fixture", "public-model", "openai")
            .unwrap()
            .is_some()
    );
    assert!(
        select(&registry, "key_other", "public-model", "openai")
            .unwrap()
            .is_some()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn upstream_binding_matches_frozen_groups_and_rejects_failed_restore_in_its_scope() {
    let mut f = Fixture::new().await;
    let group = "grp_11111111111111111111111111111111";
    let view = f
        .install(PluginExecutionScope::Groups {
            ids: vec![group.into()],
        })
        .await;
    let registry = f.manager.middleware_registry().unwrap();
    let provider = ProviderKind::new("openai").unwrap();
    let model = UpstreamModelId::new("native-model").unwrap();
    assert!(
        registry
            .select(
                &context("key_fixture", "public-model", Some(group)),
                &provider,
                &model
            )
            .unwrap()
            .is_some()
    );
    assert!(
        registry
            .select(
                &context("key_fixture", "public-model", None),
                &provider,
                &model
            )
            .unwrap()
            .is_none()
    );
    f.manager.shutdown().await;
    f.store
        .records
        .lock()
        .unwrap()
        .get_mut(&view.id)
        .unwrap()
        .configuration = json!({"adapter_mode":"models"});
    f.manager = PluginManager::new(f.store.clone(), f.runtime.clone());
    f.manager.restore().await.unwrap();
    assert_eq!(f.manager.list().await.unwrap()[0].status, "failed");
    let registry = f.manager.middleware_registry().unwrap();
    assert!(
        registry
            .select(
                &context("key_fixture", "public-model", Some(group)),
                &provider,
                &model
            )
            .is_err()
    );
    assert!(
        registry
            .select(
                &context("key_fixture", "public-model", None),
                &provider,
                &model
            )
            .unwrap()
            .is_none()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn upstream_binding_failed_upgrade_preserves_old_scope_and_uncertain_save_blocks_stale_plans()
{
    let f = Fixture::new().await;
    let view = f.install(keys("key_fixture")).await;
    let registry = f.manager.middleware_registry().unwrap();
    let access = f.manager.access(&view.id, view.revision).unwrap();
    for mode in ["models", "overlap"] {
        let mut bad = f.command(view.revision, PluginExecutionScope::All {});
        bad.configuration = json!({"adapter_mode":mode});
        assert!(
            f.manager
                .update(Some(&view.id), bad, &actor())
                .await
                .is_err()
        );
        assert!(!access.is_revoked());
        assert!(!registry.cancellation().is_cancelled());
    }
    f.store
        .save_fault
        .store(3, std::sync::atomic::Ordering::SeqCst);
    assert!(
        f.manager
            .update(
                Some(&view.id),
                f.command(view.revision, keys("key_other")),
                &actor()
            )
            .await
            .is_err()
    );
    assert!(access.is_revoked());
    assert!(registry.cancellation().is_cancelled());
    assert!(
        select(
            &f.manager.middleware_registry().unwrap(),
            "key_other",
            "public-model",
            "openai"
        )
        .is_err()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn upstream_binding_rejects_a_disabled_binding_on_a_non_adapter_artifact() {
    let f = Fixture::new().await;
    let artifact = f
        .manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let mut command = f.command(0, PluginExecutionScope::All {});
    command.artifact_sha256 = artifact.sha256;
    command.enabled = false;
    assert!(f.manager.update(None, command, &actor()).await.is_err());
    assert!(f.store.list().await.unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn upstream_binding_links_selected_process_revocation_without_canceling_unmatched_plans() {
    let f = Fixture::new().await;
    let view = f.install(keys("key_fixture")).await;
    let selected = f.manager.middleware_registry().unwrap();
    let unmatched = f.manager.middleware_registry().unwrap();
    assert!(
        select(&selected, "key_fixture", "public-model", "openai")
            .unwrap()
            .is_some()
    );
    assert!(
        select(&unmatched, "key_other", "public-model", "openai")
            .unwrap()
            .is_none()
    );
    f.manager.access(&view.id, view.revision).unwrap().revoke();
    assert!(selected.cancellation().is_cancelled());
    assert!(!unmatched.cancellation().is_cancelled());
    assert!(select(&unmatched, "key_fixture", "public-model", "openai").is_err());
    f.manager.shutdown().await;
}
