pub(crate) mod binding;
pub(crate) mod callbacks;
mod execution;
mod native;
use crate::support::*;
use gateway_admin::{
    plugins::PluginManager,
    ports::plugins::{PluginService, PluginStore},
};
use gateway_plugin_runtime::{PluginEngine, upstream_adapter::RegisteredAdapter};
use gateway_plugin_sdk::{ContributionDeclaration, Stage, call::upstream_adapter::*};
use serde_json::json;
use std::sync::Arc;
fn contribution() -> ContributionDeclaration {
    ContributionDeclaration {
        id: "local.fixture.upstream".into(),
        version: 4,
        stages: vec![Stage::Upstream],
        input_formats: vec!["openai".into()],
        output_formats: vec!["openai".into()],
    }
}
fn declaration() -> UpstreamAdapterDeclaration {
    UpstreamAdapterDeclaration {
        id: "adapter".into(),
        provider: BuiltinProvider::OpenAi,
        base_url: "https://example.test/base/".into(),
        paths: vec![
            UpstreamPath {
                path: "responses".into(),
                purpose: UpstreamPathPurpose::Inference,
            },
            UpstreamPath {
                path: "audit".into(),
                purpose: UpstreamPathPurpose::Auxiliary,
            },
        ],
        authentication_kinds: vec!["oauth".into()],
        transport: UpstreamTransport::HttpSse,
        protocol: "openai".into(),
        models: vec!["public-model".into()],
    }
}
#[test]
fn upstream_adapter_target_classifies_paths_without_treating_them_as_an_allowlist() {
    let adapter = RegisteredAdapter::compile(declaration(), &contribution()).unwrap();
    let (target, purpose) = adapter
        .resolve_target("audit", &[("q".into(), "a&b".into())])
        .unwrap();
    assert_eq!(purpose, UpstreamPathPurpose::Auxiliary);
    assert_eq!(target.as_str(), "https://example.test/base/audit?q=a%26b");
    for (path, expected) in [
        ("other", "https://example.test/base/other"),
        ("/absolute", "https://example.test/absolute"),
        ("https://other.test/endpoint", "https://other.test/endpoint"),
    ] {
        let (target, purpose) = adapter.resolve_target(path, &[]).unwrap();
        assert_eq!(purpose, UpstreamPathPurpose::Inference);
        assert_eq!(target.as_str(), expected);
    }
    assert!(adapter.resolve_target("file:///tmp/data", &[]).is_err());
    assert!(
        adapter
            .resolve_target("responses", &vec![("q".into(), "v".into()); 65])
            .is_err()
    );
}
#[test]
fn upstream_adapter_registration_rejects_ambiguous_base_paths_and_model_ranges() {
    for base in [
        "https://example.test/base",
        "https://u:p@example.test/",
        "https://example.test/?q=v",
        "https://example.test/#fragment",
        "file:///tmp/",
    ] {
        let mut value = declaration();
        value.base_url = base.into();
        assert!(
            RegisteredAdapter::compile(value, &contribution()).is_err(),
            "{base}"
        );
    }
    for path in [
        "",
        "/responses",
        "../responses",
        "path/../responses",
        "responses?x=1",
        "responses#fragment",
        "a//b",
        "a/*",
    ] {
        let mut value = declaration();
        value.paths[0].path = path.into();
        assert!(
            RegisteredAdapter::compile(value, &contribution()).is_err(),
            "{path}"
        );
    }
    let mut value = declaration();
    value.models = vec!["model".into(); 257];
    assert!(RegisteredAdapter::compile(value, &contribution()).is_err());
    let mut value = declaration();
    value.paths.push(value.paths[0].clone());
    assert!(RegisteredAdapter::compile(value, &contribution()).is_err());
}
#[tokio::test]
async fn sdk_upstream_adapter_registration_is_validated_before_the_instance_can_be_published() {
    for contract in [4] {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = PluginManager::new(store.clone(), runtime);
        let artifact = manager
            .upload(upstream_archive_contract("1.0.0", contract), &actor())
            .await
            .unwrap();
        assert!(
            artifact.unsupported_capabilities.is_empty(),
            "contract v{contract}"
        );
        let mut command = update(&artifact.sha256, 0, true);
        command.upstream_adapter = Some(
            gateway_admin::model::plugins::PluginUpstreamAdapterBinding {
                scope: gateway_admin::model::plugins::PluginExecutionScope::All {},
            },
        );
        let installed = manager.update(None, command, &actor()).await.unwrap();
        assert_eq!(installed.status, "ready");
        let original = store.list().await.unwrap().remove(0);
        let access = manager.access(&installed.id, installed.revision).unwrap();
        for mode in [
            "id",
            "provider",
            "base",
            "path",
            "path-purpose",
            "auth",
            "auth-id",
            "protocol",
            "transport",
            "models",
            "unknown",
            "empty",
            "too-many",
            "duplicate",
            "oversized",
            "envelope",
        ] {
            let mut command = update(&artifact.sha256, installed.revision, true);
            command.upstream_adapter = installed.upstream_adapter.clone();
            command.configuration["adapter_mode"] = json!(mode);
            assert!(
                manager
                    .update(Some(&installed.id), command, &actor())
                    .await
                    .is_err(),
                "v{contract}: {mode}"
            );
            assert!(!access.is_revoked());
            assert!(store.list().await.unwrap()[0] == original);
            assert_eq!(manager.list().await.unwrap()[0].status, "ready");
        }
        manager.shutdown().await;
    }
}
