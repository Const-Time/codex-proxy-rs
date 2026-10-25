pub(crate) mod environment;
use crate::support::{actor, update};
use environment::Environment;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_plugin_runtime::PluginEngine;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use sqlx::Row as _;
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

pub(crate) fn binary() -> &'static PathBuf {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let runtime = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fixture = runtime.join("tests/services/upstream_sdk/fixture");
        // Generated build output must stay outside the integration source tree.
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(
                || {
                    runtime
                        .ancestors()
                        .nth(3)
                        .expect("runtime belongs to backend workspace")
                        .join("target")
                },
                PathBuf::from,
            )
            .join("upstream-service-fixture");
        let output = std::process::Command::new("rustup")
            .args([
                "run",
                "1.99.1",
                "cargo",
                "build",
                "--locked",
                "--manifest-path",
            ])
            .arg(fixture.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(&target)
            .env_remove("RUSTC")
            .env_remove("RUSTUP_TOOLCHAIN")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "original upstream SDK fixture must build: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        target.join("debug").join(format!(
            "upstream-service-fixture{}",
            std::env::consts::EXE_SUFFIX
        ))
    })
}
fn archive() -> Arc<[u8]> {
    let executable = std::fs::read(binary()).unwrap();
    let manifest = json!({
        "manifestVersion":2,"name":"upstream-service-fixture","displayName":"Upstream service fixture","publisher":"local","version":"1.0.0","description":"Unmodified upstream SDK contract","license":"Apache-2.0",
        "engines":{"codex-proxy-rs":">=3.11.0,<4.0.0"},"main":"bin/plugin","runtime":"trustedProcess",
        "contributes":{"maintenance":{"id":"local.upstream-service-fixture.maintenance","version":1,"stages":["maintenance"]},"middleware":{"id":"local.upstream-service-fixture.middleware","version":5,"stages":["service"]}},
        "configurationSchema":{"type":"object","properties":{"maintenance":{"type":"boolean"},"mutate":{"type":"boolean"},"observation":{"type":"string"},"rewrite":{"type":"string","enum":["actor","request"]}},"additionalProperties":false},"state":[],"resources":{},
        "package":{"protocolVersion":gateway_plugin_sdk::PROTOCOL_VERSION,"target":{"os":std::env::consts::OS,"architecture":std::env::consts::ARCH},"files":{"bin/plugin":hex::encode(Sha256::digest(&executable))}}
    });
    let mut tar = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    for (name, bytes) in [
        ("plugin.json", serde_json::to_vec(&manifest).unwrap()),
        ("bin/plugin", executable),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o700);
        header.set_cksum();
        tar.append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    tar.into_inner().unwrap().finish().unwrap().into()
}
pub(super) async fn manager(environment: &Environment) -> Arc<PluginManager> {
    Arc::new_cyclic(|manager: &std::sync::Weak<PluginManager>| {
        let services = manager.clone();
        let runtime = PluginEngine::new(
            environment.cache.path().join("plugins"),
            "3.11.2".parse().unwrap(),
            environment.store.plugin_store(),
        )
        .with_services(Arc::new(move || {
            let registry = services
                .upgrade()
                .and_then(|manager| manager.middleware_registry())
                .unwrap();
            Ok(registry.service_factory())
        }));
        PluginManager::new(environment.store.plugin_store(), Arc::new(runtime))
            .with_services(environment.admin.services().service_settings())
    })
}
#[tokio::test]
async fn original_sdk_root_services_write_real_store_with_native_plugin_audit_and_publication() {
    let Some(environment) = Environment::new().await else {
        return;
    };
    let manager = manager(&environment).await;
    let artifact = manager.upload(archive(), &actor()).await.unwrap();
    let before = environment
        .admin
        .services()
        .settings()
        .load()
        .await
        .unwrap()
        .config_revision;
    environment.published.lock().unwrap().clear();
    let mut command = update(&artifact.sha256, 0, true);
    command.secrets.clear();
    command.configuration = json!({"mutate":true});
    let installed = manager.update(None, command, &actor()).await.unwrap();
    let after = environment
        .admin
        .services()
        .settings()
        .load()
        .await
        .unwrap()
        .config_revision;
    assert_eq!(after.get(), before.get() + 2);
    assert!(
        !environment
            .admin
            .services()
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    assert_eq!(environment.published.lock().unwrap().len(), 2);
    let rows=sqlx::query("select actor_ref,actor_admin_user_id,admin_request_id from admin_audit_events where actor_kind='plugin' order by created_at,id").fetch_all(&environment.pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(
            row.get::<String, _>("actor_ref"),
            format!("plugin:{}@1", installed.id)
        );
        assert!(
            row.get::<Option<String>, _>("actor_admin_user_id")
                .is_none()
        );
        assert_ne!(row.get::<String, _>("admin_request_id"), "forged-request");
    }
    manager.shutdown().await;
    drop(manager);
    environment.close().await;
}

#[tokio::test]
async fn original_sdk_typed_service_middleware_preserves_real_writes_and_native_plugin_audit() {
    let Some(environment) = Environment::new().await else {
        return;
    };
    let manager = manager(&environment).await;
    let artifact = manager.upload(archive(), &actor()).await.unwrap();
    let mut layer = update(&artifact.sha256, 0, true);
    layer.secrets.clear();
    let observation = environment.cache.path().join("sdk-input.json");
    layer.configuration = json!({"observation":observation});
    layer.middleware = Some(PluginMiddlewareBinding {
        order: 0,
        mounts: vec![PluginMiddlewareMount::Service],
        scope: PluginExecutionScope::All {},
    });
    manager.update(None, layer, &actor()).await.unwrap();
    let before = environment
        .admin
        .services()
        .settings()
        .load()
        .await
        .unwrap()
        .config_revision;
    environment.published.lock().unwrap().clear();
    let mut command = update(&artifact.sha256, 0, true);
    command.secrets.clear();
    command.configuration = json!({"mutate":true});
    let installed = manager.update(None, command, &actor()).await.unwrap();
    let observed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(observation).unwrap()).unwrap();
    assert_eq!(observed["actor"], "system");
    assert!(observed["decode_error"].is_null(), "{observed}");
    let request_id = observed["request_id"].as_str().unwrap();
    assert_ne!(request_id, "forged-request");
    assert_eq!(
        environment
            .admin
            .services()
            .settings()
            .load()
            .await
            .unwrap()
            .config_revision
            .get(),
        before.get() + 2
    );
    assert!(
        !environment
            .admin
            .services()
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    assert_eq!(environment.published.lock().unwrap().len(), 2);
    let rows=sqlx::query("select actor_ref,actor_admin_user_id,admin_request_id from admin_audit_events where actor_kind='plugin' order by created_at,id").fetch_all(&environment.pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(
            row.get::<String, _>("actor_ref"),
            format!("plugin:{}@1", installed.id)
        );
        assert!(
            row.get::<Option<String>, _>("actor_admin_user_id")
                .is_none()
        );
        assert_eq!(row.get::<String, _>("admin_request_id"), request_id);
    }
    manager.shutdown().await;
    drop(manager);
    environment.close().await;
}

#[tokio::test]
async fn original_sdk_middleware_identity_rewrite_is_rejected_before_real_store_mutation() {
    for field in ["actor", "request"] {
        let Some(environment) = Environment::new().await else {
            return;
        };
        let manager = manager(&environment).await;
        let artifact = manager.upload(archive(), &actor()).await.unwrap();
        let observation = environment.cache.path().join("sdk-input.json");
        let mut layer = update(&artifact.sha256, 0, true);
        layer.secrets.clear();
        layer.configuration = json!({"observation":observation,"rewrite":field});
        layer.middleware = Some(PluginMiddlewareBinding {
            order: 0,
            mounts: vec![PluginMiddlewareMount::Service],
            scope: PluginExecutionScope::All {},
        });
        manager.update(None, layer, &actor()).await.unwrap();
        let before = environment
            .admin
            .services()
            .settings()
            .load()
            .await
            .unwrap()
            .config_revision;
        environment.published.lock().unwrap().clear();
        let mut command = update(&artifact.sha256, 0, true);
        command.secrets.clear();
        command.configuration = json!({"mutate":true});
        assert!(manager.update(None, command, &actor()).await.is_err());
        let observed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(observation).unwrap()).unwrap();
        assert_eq!(observed["actor"], "system");
        assert!(observed["decode_error"].is_null());
        assert_eq!(
            environment
                .admin
                .services()
                .settings()
                .load()
                .await
                .unwrap()
                .config_revision,
            before
        );
        assert!(
            !environment
                .admin
                .services()
                .settings()
                .admin_api_key_exists()
                .await
                .unwrap()
        );
        assert!(environment.published.lock().unwrap().is_empty());
        let writes: i64 =
            sqlx::query_scalar("select count(*) from admin_audit_events where actor_kind='plugin'")
                .fetch_one(&environment.pool)
                .await
                .unwrap();
        assert_eq!(writes, 0);
        manager.shutdown().await;
        drop(manager);
        environment.close().await;
    }
}

#[tokio::test]
async fn original_sdk_maintenance_worker_writes_native_sql_through_typed_middleware() {
    use gateway_core::{
        lifecycle::CancellationToken,
        task::{WorkerContribution, WorkerRunnable},
    };
    use std::time::Duration;
    let Some(environment) = Environment::new().await else {
        return;
    };
    let manager = manager(&environment).await;
    let artifact = manager.upload(archive(), &actor()).await.unwrap();
    let observation = environment.cache.path().join("maintenance-sdk-input.json");
    let mut layer = update(&artifact.sha256, 0, true);
    layer.secrets.clear();
    layer.configuration = json!({"observation":observation});
    layer.middleware = Some(PluginMiddlewareBinding {
        order: 0,
        mounts: vec![PluginMiddlewareMount::Service],
        scope: PluginExecutionScope::All {},
    });
    manager.update(None, layer, &actor()).await.unwrap();
    let mut command = update(&artifact.sha256, 0, true);
    command.secrets.clear();
    command.configuration = json!({"maintenance":true});
    let installed = manager.update(None, command, &actor()).await.unwrap();
    let before = environment
        .admin
        .services()
        .settings()
        .load()
        .await
        .unwrap()
        .config_revision;
    environment.published.lock().unwrap().clear();
    assert!(!observation.exists());
    let WorkerContribution::Registration(registration) = manager
        .maintenance_worker(environment.core.snapshot_publications())
        .unwrap()
    else {
        panic!("maintenance registration");
    };
    let WorkerRunnable::Daemon { task, .. } = registration.runnable else {
        panic!("maintenance daemon");
    };
    let stop = CancellationToken::new();
    let cancellation = stop.clone();
    let worker = tokio::spawn(async move { task.run(cancellation).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let writes: i64 = sqlx::query_scalar(
                "select count(*) from admin_audit_events where actor_kind='plugin'",
            )
            .fetch_one(&environment.pool)
            .await
            .unwrap();
            if writes == 2 && environment.published.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let observed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(observation).unwrap()).unwrap();
    assert_eq!(observed["actor"], "system");
    assert!(observed["decode_error"].is_null());
    assert_ne!(observed["request_id"], "forged-request");
    assert_eq!(
        environment
            .admin
            .services()
            .settings()
            .load()
            .await
            .unwrap()
            .config_revision
            .get(),
        before.get() + 2
    );
    assert!(
        !environment
            .admin
            .services()
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    let rows = sqlx::query("select actor_ref,actor_admin_user_id,admin_request_id from admin_audit_events where actor_kind='plugin' order by created_at,id").fetch_all(&environment.pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(
            row.get::<String, _>("actor_ref"),
            format!("plugin:{}@1", installed.id)
        );
        assert!(
            row.get::<Option<String>, _>("actor_admin_user_id")
                .is_none()
        );
        assert_eq!(
            row.get::<String, _>("admin_request_id"),
            observed["request_id"].as_str().unwrap()
        );
    }
    manager.shutdown().await;
    drop(manager);
    environment.close().await;
}
