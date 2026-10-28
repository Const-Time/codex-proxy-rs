use super::support::*;
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, plugins::*},
    plugins::PluginManager,
    ports::plugins::{PluginProcess, PluginRuntime, PluginService, PluginStore},
};
use gateway_plugin_runtime::{
    PluginEngine,
    updates::{allowed_url, repository, select_release},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
#[ignore = "read-only GitHub release download; no production accounts"]
async fn published_paired_plugins_download_with_verified_identity_and_digest() {
    for (id, current) in [
        ("local.model-health", "0.8.0"),
        ("cpr.shared-budget-manager", "0.3.0"),
    ] {
        let artifact = PluginArtifactInfo {
            plugin_id: id.into(),
            version: current.into(),
            sha256: String::new(),
            display_name: String::new(),
            manifest: json!({}),
            unsupported_capabilities: vec![],
            version_warning: None,
        };
        let release = gateway_plugin_runtime::updates::check(&artifact)
            .await
            .unwrap()
            .expect("new release");
        let bytes = gateway_plugin_runtime::updates::download(&artifact, &release)
            .await
            .unwrap();
        let package = gateway_plugin_runtime::ValidatedPackage::read(
            bytes,
            Some(&release.sha256),
            gateway_plugin_runtime::PackageLimits::default(),
        )
        .unwrap();
        assert_eq!(package.manifest().plugin_id().unwrap(), id);
        assert_eq!(package.manifest().version.to_string(), release.version);
    }
}

fn release_json() -> serde_json::Value {
    json!({"draft":false,"prerelease":false,"tag_name":"v0.10.0","assets":[{
        "name":"local.model-health-0.10.0-x86_64-unknown-linux-gnu.tar.gz","size":123,
        "digest":format!("sha256:{}","a".repeat(64)),
        "browser_download_url":"https://github.com/Const-Time/codex-model-health-plugin/releases/download/v0.10.0/local.model-health-0.10.0-x86_64-unknown-linux-gnu.tar.gz"
    }]})
}
#[test]
fn curated_updates_select_plugin_platform_and_newer_stable_version() {
    let select = |v: &serde_json::Value, current: &str, platform: &str| {
        select_release(
            &serde_json::to_vec(v).unwrap(),
            "local.model-health",
            current,
            platform,
        )
    };
    let value = release_json();
    assert_eq!(
        select(&value, "0.9.0", "x86_64-unknown-linux-gnu")
            .unwrap()
            .unwrap()
            .version,
        "0.10.0"
    );
    assert!(
        select(&value, "0.10.0", "x86_64-unknown-linux-gnu")
            .unwrap()
            .is_none()
    );
    assert!(
        select(&value, "0.9.0", "aarch64-unknown-linux-gnu")
            .unwrap()
            .is_none()
    );
    for (field, bad) in [
        ("digest", json!(null)),
        ("size", json!(33554433)),
        (
            "browser_download_url",
            json!("https://attacker.invalid/plugin.tar.gz"),
        ),
    ] {
        let mut v = value.clone();
        v["assets"][0][field] = bad;
        assert!(select(&v, "0.9.0", "x86_64-unknown-linux-gnu").is_err());
    }
    let mut v = value;
    v["prerelease"] = json!(true);
    assert!(
        select(&v, "0.9.0", "x86_64-unknown-linux-gnu")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repository("cpr.shared-budget-manager"),
        Some("Const-Time/codex-proxy-rs")
    );
    assert!(repository("untrusted.plugin").is_none());
    for address in [
        "http://github.com/a",
        "https://github.com.evil.test/a",
        "https://127.0.0.1/a",
        "https://secret@github.com/a",
        "https://github.com:8443/a",
    ] {
        assert!(!allowed_url(&address.parse().unwrap()));
    }
    assert!(allowed_url(&"https://release-assets.githubusercontent.com/github-production-release-asset/example?signature=opaque".parse().unwrap()));
}
struct Runtime {
    engine: PluginEngine,
    package: PluginArtifact,
    store: Arc<MemoryStore>,
    edit_during_download: AtomicBool,
}
#[async_trait]
impl PluginRuntime for Runtime {
    async fn online_release(
        &self,
        _: &PluginArtifactInfo,
    ) -> Result<Option<PluginOnlineRelease>, AdminError> {
        Ok(Some(PluginOnlineRelease {
            version: self.package.info.version.clone(),
            sha256: self.package.info.sha256.clone(),
            asset_name: "fixture".into(),
            size: self.package.archive.len() as u64,
            release_url:
                "https://github.com/Const-Time/codex-model-health-plugin/releases/tag/fixture"
                    .into(),
        }))
    }
    async fn download_release(
        &self,
        _: &PluginArtifactInfo,
        _: &PluginOnlineRelease,
    ) -> Result<Arc<[u8]>, AdminError> {
        if self.edit_during_download.load(Ordering::SeqCst) {
            for r in self.store.records.lock().unwrap().values_mut() {
                r.revision += 1;
                r.name = "Concurrent change".into();
            }
        }
        Ok(self.package.archive.clone())
    }
    fn describe(&self, a: PluginArtifactInfo) -> Result<PluginArtifactInfo, AdminError> {
        self.engine.describe(a)
    }
    async fn inspect(&self, a: Arc<[u8]>) -> Result<PluginArtifact, AdminError> {
        self.engine.inspect(a).await
    }
    async fn validate(
        &self,
        r: &PluginRecord,
        a: &PluginArtifactInfo,
        p: Option<&PluginArtifactInfo>,
    ) -> Result<(), AdminError> {
        self.engine.validate(r, a, p).await
    }
    async fn prepare(
        &self,
        r: &PluginRecord,
        a: PluginArtifact,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        self.engine.prepare(r, a).await
    }
    async fn prepare_state(
        &self,
        r: &PluginRecord,
        a: &PluginArtifactInfo,
        p: Option<&Arc<dyn PluginProcess>>,
    ) -> Result<Option<PluginStateCommit>, AdminError> {
        self.engine.prepare_state(r, a, p).await
    }
}
#[tokio::test]
async fn update_preserves_instance_configuration_secrets_and_private_state_and_rejects_stale_revision()
 {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let engine = PluginEngine::new(
        cache.path().into(),
        "3.15.0".parse().unwrap(),
        store.clone(),
    );
    let package = engine.inspect(archive("2.0.0", false)).await.unwrap();
    let runtime = Arc::new(Runtime {
        engine,
        package,
        store: store.clone(),
        edit_during_download: AtomicBool::new(false),
    });
    let manager = PluginManager::new(store.clone(), runtime.clone());
    let old = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let mut command = update(&old.sha256, 0, true);
    command.name = "Keep my instance".into();
    command.configuration = json!({"schedule":"saved-plan"});
    let instance = manager.update(None, command, &actor()).await.unwrap();
    manager
        .handle(&instance.id, instance.revision, request("state"))
        .await
        .unwrap();
    let before = store
        .state_snapshot(&instance.id, instance.revision)
        .await
        .unwrap();
    assert_eq!(before.records.len(), 1);
    let original = store.list().await.unwrap().remove(0);
    let release = manager
        .check_update(&instance.id, instance.revision)
        .await
        .unwrap()
        .unwrap();
    let mut mismatched = release.clone();
    mismatched.sha256 = "0".repeat(64);
    assert!(
        manager
            .install_update(&instance.id, instance.revision, mismatched, &actor())
            .await
            .is_err()
    );
    assert!(store.list().await.unwrap()[0] == original);
    assert!(
        before
            == store
                .state_snapshot(&instance.id, instance.revision)
                .await
                .unwrap()
    );
    // A rejected candidate must leave the old process callable as well as preserving its record.
    manager
        .handle(&instance.id, instance.revision, request("echo"))
        .await
        .unwrap();
    let upgraded = manager
        .install_update(&instance.id, instance.revision, release.clone(), &actor())
        .await
        .unwrap();
    assert_eq!(upgraded.id, instance.id);
    assert_eq!(upgraded.name, original.name);
    assert_eq!(upgraded.configuration, original.configuration);
    assert!(upgraded.enabled);
    let after = store.list().await.unwrap().remove(0);
    assert_eq!(after.secrets, original.secrets);
    assert!(
        before
            == store
                .state_snapshot(&upgraded.id, upgraded.revision)
                .await
                .unwrap()
    );
    assert!(
        manager
            .install_update(&instance.id, instance.revision, release, &actor())
            .await
            .is_err()
    );
    manager.shutdown().await;
}
#[tokio::test]
async fn update_keeps_disabled_instance_disabled_and_does_not_overwrite_concurrent_settings() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let engine = PluginEngine::new(
        cache.path().into(),
        "3.15.0".parse().unwrap(),
        store.clone(),
    );
    let package = engine.inspect(archive("2.0.0", false)).await.unwrap();
    let runtime = Arc::new(Runtime {
        engine,
        package,
        store: store.clone(),
        edit_during_download: AtomicBool::new(false),
    });
    let manager = PluginManager::new(store.clone(), runtime.clone());
    let old = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = manager
        .update(None, update(&old.sha256, 0, false), &actor())
        .await
        .unwrap();
    let release = manager
        .check_update(&first.id, first.revision)
        .await
        .unwrap()
        .unwrap();
    let next = manager
        .install_update(&first.id, first.revision, release.clone(), &actor())
        .await
        .unwrap();
    assert!(!next.enabled);
    assert_eq!(next.status, "disabled");
    runtime.edit_during_download.store(true, Ordering::SeqCst);
    assert!(
        manager
            .install_update(&next.id, next.revision, release, &actor())
            .await
            .is_err()
    );
    assert_eq!(store.list().await.unwrap()[0].name, "Concurrent change");
    manager.shutdown().await;
}
