//! Official and locally rebuilt artifacts exercise native PostgreSQL-backed business calls.
use crate::services::upstream_sdk::environment::Environment;
use crate::support::{actor, update as fixture_update};
use gateway_admin::{
    model::{
        MutationActor, MutationContext, auth::LoginCommand, client_keys::CreateClientKey,
        plugins::*,
    },
    plugins::PluginManager,
    ports::plugins::PluginService,
};
use gateway_core::{policy::RateLimits, routing::AccountGroupId};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

struct TextServer {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl TextServer {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/fixture", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0 && request.len() + count <= 4096);
                request.extend_from_slice(&buffer[..count]);
            }
            assert!(request.starts_with(b"GET /fixture HTTP/1.1\r\n"));
            let text = "native 文本";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",text.len()).as_bytes()).await.unwrap();
        });
        Self { url, task }
    }
}
impl Drop for TextServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn update(digest: &str, revision: u64, enabled: bool) -> PluginUpdate {
    let mut update = fixture_update(digest, revision, enabled);
    update.secrets.clear();
    update
}

async fn request(
    manager: &PluginManager,
    installed: &PluginView,
    context: &MutationContext,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let response = manager
        .handle(
            &installed.id,
            installed.revision,
            PluginHttpRequest {
                invocation: None,
                actor: context.clone(),
                method: method.into(),
                path: path.into(),
                query: String::new(),
                content_type: body.as_ref().map(|_| "application/json".into()),
                body: body.map_or_else(Vec::new, |body| serde_json::to_vec(&body).unwrap()),
            },
        )
        .await
        .unwrap();
    (
        response.status,
        serde_json::from_slice(&response.body).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires verified official and rebuilt Linux x86_64 artifacts and isolated PostgreSQL/Redis"]
async fn official_and_rebuilt_workbench_execute_management_business_without_rewriting_the_plugin() {
    crate::callbacks::log::initialize_capture();
    assert_eq!(
        (std::env::consts::OS, std::env::consts::ARCH),
        ("linux", "x86_64")
    );
    for (variable, expected) in [
        (
            "CPR_PLUGIN_WORKBENCH_ORIGINAL",
            "5f173067aa892de3f34532d63fd5d163e4123c495b7176d7cca5518fb33620b0",
        ),
        (
            "CPR_PLUGIN_WORKBENCH_REBUILT",
            "f24f5d999e703e320272566bbbc3c655b22831c82571161c901f83a4c8720b1b",
        ),
    ] {
        let path = std::env::var_os(variable).expect("provide verified workbench artifacts");
        let bytes = std::fs::read(path).unwrap();
        if !expected.is_empty() {
            assert_eq!(hex::encode(Sha256::digest(&bytes)), expected);
        }
        let env = Environment::with_login_admin()
            .await
            .expect("isolated PostgreSQL and Redis required");
        let store = env.store.plugin_store();
        let manager = Arc::new_cyclic(|plugins| {
            PluginManager::new(
                store.clone(),
                Arc::new(crate::middleware::plan::KernelRuntime(
                    PluginEngine::new(
                        env.cache.path().join("plugins"),
                        "3.11.2".parse().unwrap(),
                        store.clone(),
                    )
                    .with_data(env.admin.services().plugin_data())
                    .with_models(
                        Arc::new(gateway_api::openai::middleware::PluginModelExecution::new(
                            env.core.execution_service(),
                            plugins.clone(),
                        )),
                        env.admin.services().client_keys(),
                    ),
                )),
            )
        });
        let artifact = manager.upload(Arc::from(bytes), &actor()).await.unwrap();
        assert_eq!(artifact.plugin_id, "codex-proxy.capability-workbench");
        assert_eq!(artifact.version, "0.3.1");
        assert!(
            artifact
                .unsupported_capabilities
                .contains(&"middleware".into())
        );
        assert!(
            artifact
                .unsupported_capabilities
                .contains(&"command_line".into())
        );
        // Explicit kernel composition keeps production enablement gates intact.
        let installed = manager
            .update(None, update(&artifact.sha256, 0, true), &actor())
            .await
            .unwrap();
        let scope = PluginMutationScope {
            instance_id: installed.id.clone(),
            revision: installed.revision,
            actor: actor(),
        };
        let group = env
            .admin
            .services()
            .plugin_resources()
            .ensure_group(
                scope,
                PluginGroupEnsure {
                    resource_key: "workbench-tests".into(),
                    name: "Workbench native scope".into(),
                    color: "#2563EBFF".into(),
                    description: None,
                },
            )
            .await
            .unwrap();
        let admin_login = env
            .admin
            .services()
            .auth()
            .login(LoginCommand {
                username: Some("sdk-fixture@fixture.test".into()),
                password: "sdk-business-fixture-password".into(),
            })
            .await
            .unwrap();
        let admin_id = env
            .admin
            .services()
            .auth()
            .resolve_admin_user_id(Some(&admin_login.session_id))
            .await
            .unwrap()
            .unwrap();
        let admin_actor = MutationContext {
            actor: MutationActor::AdminSession {
                admin_user_id: admin_id,
            },
            request_id: "workbench-user-create".into(),
        };
        let key = env
            .admin
            .services()
            .client_keys()
            .create(
                &admin_actor,
                CreateClientKey {
                    name: "Workbench native key".into(),
                    label: None,
                    group_ids: vec![AccountGroupId::new(group.id).unwrap()],
                    limits: RateLimits::unlimited(),
                },
            )
            .await
            .unwrap();
        let key_id = key.secret.record.id.to_string();
        let (status, snapshot) = request(
            &manager,
            &installed,
            &admin_actor,
            "GET",
            "api/snapshot",
            None,
        )
        .await;
        assert_eq!(status, 200, "{snapshot}");
        assert_eq!(snapshot["contractVersion"], 1);
        assert!(
            snapshot["keys"]
                .as_array()
                .unwrap()
                .iter()
                .any(|key| key["id"] == key_id)
        );
        assert_eq!(
            request(
                &manager,
                &installed,
                &admin_actor,
                "POST",
                "api/echo",
                Some(json!({"message":"native business"}))
            )
            .await,
            (200, json!({"message":"native business"}))
        );
        let (status, models) = request(
            &manager,
            &installed,
            &admin_actor,
            "POST",
            "api/models",
            Some(json!({"clientKeyId":key_id})),
        )
        .await;
        assert_eq!(status, 200, "{models}");
        assert!(models["models"].is_array());
        assert_eq!(
            request(&manager, &installed, &admin_actor, "GET", "api/tasks", None).await,
            (200, json!({"version":null,"value":null}))
        );
        let value = json!({"selectedId":null,"entries":[]});
        let (status, saved) = request(
            &manager,
            &installed,
            &admin_actor,
            "POST",
            "api/tasks",
            Some(json!({"expectedVersion":null,"value":value})),
        )
        .await;
        assert_eq!(status, 200, "{saved}");
        assert_eq!(saved["version"], 1);
        assert_eq!(
            request(&manager, &installed, &admin_actor, "GET", "api/tasks", None).await,
            (200, json!({"version":1,"value":value}))
        );
        let persisted = store
            .state_get(
                &PluginStateScope {
                    instance_id: installed.id.clone(),
                    revision: installed.revision,
                    namespace: "workbench".into(),
                    schema_version: 1,
                    maximum_records: 1,
                    maximum_bytes: 262144,
                },
                "tasks",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.value, value);
        assert_eq!(persisted.version, 1);
        let (status, stale) = request(
            &manager,
            &installed,
            &admin_actor,
            "POST",
            "api/tasks",
            Some(json!({"expectedVersion":null,"value":value})),
        )
        .await;
        assert_eq!(status, 409, "{stale}");
        assert_eq!(
            request(
                &manager,
                &installed,
                &admin_actor,
                "POST",
                "api/log",
                Some(json!({}))
            )
            .await,
            (200, json!({"recorded":true}))
        );
        let mut text = TextServer::new().await;
        assert_eq!(
            request(
                &manager,
                &installed,
                &admin_actor,
                "POST",
                "api/fetch-text",
                Some(json!({"url":text.url}))
            )
            .await,
            (
                200,
                json!({"url":text.url,"status":200,"contentType":"text/plain; charset=utf-8","text":"native 文本","truncated":false,"bytes":"native 文本".len()})
            )
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut text.task)
            .await
            .unwrap()
            .unwrap();
        let revision = installed.revision;
        manager
            .update(
                Some(&installed.id),
                update(&artifact.sha256, revision, false),
                &actor(),
            )
            .await
            .unwrap();
        assert!(
            manager
                .handle(
                    &installed.id,
                    revision,
                    PluginHttpRequest {
                        invocation: None,
                        actor: actor(),
                        method: "GET".into(),
                        path: "api/snapshot".into(),
                        query: String::new(),
                        content_type: None,
                        body: vec![]
                    }
                )
                .await
                .is_err()
        );
        let restored = manager
            .update(
                Some(&installed.id),
                update(&artifact.sha256, revision + 1, true),
                &actor(),
            )
            .await
            .unwrap();
        assert_eq!(
            request(&manager, &restored, &admin_actor, "GET", "api/tasks", None).await,
            (200, json!({"version":1,"value":value}))
        );
        manager.shutdown().await;
        drop(manager);
        env.close().await;
    }
}
