//! Official and locally rebuilt artifacts exercise native PostgreSQL-backed business calls.
use crate::services::upstream_sdk::environment::Environment;
use crate::support::{actor, api::ApiServer, update as fixture_update};
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

struct ModelServer {
    url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl ModelServer {
    async fn new(websocket: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let captured = captured.clone();
                while connections.try_join_next().is_some() {}
                connections.spawn(async move {
                    use futures::{SinkExt as _, StreamExt as _};
                    let mut probe = vec![0; 64 * 1024];
                    let headers = loop {
                        let count = socket.peek(&mut probe).await.unwrap();
                        assert!(count > 0);
                        if let Some(end) = probe[..count].windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                            break String::from_utf8(probe[..end].to_vec()).unwrap();
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    };
                    if headers.to_ascii_lowercase().contains("upgrade: websocket") {
                        if !websocket {
                            // Preserve the original preferred-WebSocket decision:
                            // an opening exceeding its preparation budget falls
                            // back to HTTP before sending a business request.
                            std::future::pending::<()>().await;
                            return;
                        }
                        let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
                        while let Some(message) = websocket.next().await {
                            match message.unwrap() {
                                tokio_tungstenite::tungstenite::Message::Text(body) => {
                                    let event: Value = serde_json::from_str(&body).unwrap();
                                    assert_eq!(event["type"],"response.create");
                                    captured.lock().unwrap().push(format!("{headers}\r\n\r\n{event}"));
                                    for event in response_events() {
                                        websocket.send(tokio_tungstenite::tungstenite::Message::Text(event.to_string().into())).await.unwrap();
                                    }
                                }
                                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                                tokio_tungstenite::tungstenite::Message::Ping(body) => { websocket.send(tokio_tungstenite::tungstenite::Message::Pong(body)).await.unwrap(); }
                                other => panic!("unexpected native websocket fixture message: {other:?}"),
                            }
                        }
                        return;
                    }
                    let mut bytes = Vec::new();
                    let mut buffer = [0; 4096];
                    loop {
                        let count = socket.read(&mut buffer).await.unwrap();
                        assert!(count > 0 && bytes.len() + count <= 64 * 1024);
                        bytes.extend_from_slice(&buffer[..count]);
                        if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                            let length = headers.lines().find_map(|line| line.strip_prefix("content-length: ")).map_or(0, |value| value.parse::<usize>().unwrap());
                            if bytes.len() >= end + 4 + length { break; }
                        }
                    }
                    let end = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n").unwrap();
                    let request = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let (content_type, body) = if request.starts_with("GET /codex/models?") {
                        ("application/json", json!({"models":[{"slug":"gpt-5.4","display_name":"Native fixture model","supported_in_api":true,"input_modalities":["text"]}]}).to_string())
                    } else {
                        assert!(request.starts_with("POST /codex/responses "), "unexpected fixture request: {}", request.lines().next().unwrap());
                        let body = if request.to_ascii_lowercase().contains("content-encoding: zstd") {
                            zstd::stream::decode_all(&bytes[end + 4..]).unwrap()
                        } else { bytes[end + 4..].to_vec() };
                        captured.lock().unwrap().push(format!("{request}\r\n\r\n{}",String::from_utf8(body).unwrap()));
                        ("text/event-stream", response_events().into_iter().map(|event| format!("event: {}\ndata: {event}\n\n",event["type"].as_str().unwrap())).collect())
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
}

fn response_events() -> [Value; 2] {
    let id = format!("resp_{}", uuid::Uuid::new_v4().simple());
    [
        json!({"type":"response.created","response":{"id":id,"object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}),
        json!({"type":"response.completed","response":{"id":id,"object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12,"input_tokens_details":{"cached_tokens":0}}}}),
    ]
}
impl Drop for ModelServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn verified_workbench_archives() -> Vec<Arc<[u8]>> {
    assert_eq!(
        (std::env::consts::OS, std::env::consts::ARCH),
        ("linux", "x86_64")
    );
    [
        (
            "CPR_PLUGIN_WORKBENCH_ORIGINAL",
            "5f173067aa892de3f34532d63fd5d163e4123c495b7176d7cca5518fb33620b0",
        ),
        (
            "CPR_PLUGIN_WORKBENCH_REBUILT",
            "f24f5d999e703e320272566bbbc3c655b22831c82571161c901f83a4c8720b1b",
        ),
    ]
    .into_iter()
    .map(|(variable, expected)| {
        let bytes = std::fs::read(
            std::env::var_os(variable).expect("provide verified workbench artifacts"),
        )
        .unwrap();
        assert_eq!(hex::encode(Sha256::digest(&bytes)), expected, "{variable}");
        Arc::from(bytes)
    })
    .collect()
}

fn native_account(id: &str, access: &str) -> gateway_core::account::NewProviderAccount {
    use provider_openai::credential::{
        CodexAccountProfile, CodexCredentialAdmin, CodexOAuthSecret, ImportCodexOAuthCredential,
    };
    CodexCredentialAdmin
        .prepare_import(ImportCodexOAuthCredential {
            account_id: id.into(),
            name: id.into(),
            enabled: true,
            secret: CodexOAuthSecret {
                access_token: secrecy::SecretString::from(access),
                refresh_token: None,
                id_token: None,
            },
            verified_account: CodexAccountProfile {
                oauth_subject: format!("subject-{id}"),
                poid: None,
                chatgpt_account_id: format!("chatgpt-{id}"),
                chatgpt_user_id: format!("user-{id}"),
                email: None,
                plan_type: Some("pro".into()),
                access_token_expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            },
            next_refresh_at: None,
        })
        .unwrap()
}

async fn model_call(
    manager: &PluginManager,
    caller: &PluginView,
    context: &MutationContext,
    key: &str,
) -> Value {
    request(manager, caller, context, "POST", "echo", Some(json!({
        "operation":"models", "mode":"raw", "method":"host.model.execute",
        "params":{"client_key_id":key,"model":"gpt-5.4","protocol":"openai","operation":"generate"},
        "body":{"model":"gpt-5.4","input":"mixedCase","metadata":{"capability_workbench":"true","capability_workbench_uppercase":"true"}}
    }))).await.1
}

async fn settled(env: &Environment, count: i64) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let rows: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM model_requests WHERE completed_at IS NOT NULL",
            )
            .fetch_one(&env.pool)
            .await
            .unwrap();
            let charges: i64 = sqlx::query_scalar("SELECT count(*) FROM user_group_charge_events")
                .fetch_one(&env.pool)
                .await
                .unwrap();
            if rows == count && charges == count {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original PostgreSQL requests and shared ledger must settle once");
}

impl ApiServer {
    async fn get(&self, path: &str, authorization: &str) -> reqwest::Response {
        self.client
            .get(format!("http://{}{path}", self.address))
            .header("authorization", authorization)
            .send()
            .await
            .unwrap()
    }
    async fn model(&self, authorization: &str) -> reqwest::Response {
        self.client
            .post(format!("http://{}/v1/responses", self.address))
            .header("authorization", authorization)
            .json(&json!({
                "model":"gpt-5.4", "input":"mixedCase", "stream":false,
                "metadata":{"capability_workbench":"true","capability_workbench_uppercase":"true"}
            }))
            .send()
            .await
            .unwrap()
    }
    async fn websocket_model(
        &self,
        authorization: &str,
        upstream_websocket: bool,
    ) -> Result<(), Vec<String>> {
        use futures::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest as _};
        let mut handshake = format!("ws://{}/v1/responses", self.address)
            .into_client_request()
            .unwrap();
        handshake
            .headers_mut()
            .insert("authorization", authorization.parse().unwrap());
        let stream = tokio::net::TcpStream::connect(self.address).await.unwrap();
        let (mut socket, response) = tokio_tungstenite::client_async(handshake, stream)
            .await
            .unwrap();
        assert_eq!(response.status(), 101);
        socket.send(Message::Text(json!({
            "type":"response.create", "model":"gpt-5.4", "input":"mixedCase",
            "use_websocket":upstream_websocket,
            "metadata":{"capability_workbench":"true","capability_workbench_uppercase":"true"}
        }).to_string().into())).await.unwrap();
        let mut seen = vec![];
        let completion = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let mut started = false;
            while let Some(message) = socket.next().await {
                let Message::Text(body) = message.unwrap() else {
                    continue;
                };
                let body: Value = serde_json::from_str(&body).unwrap();
                seen.push(body["type"].as_str().unwrap_or("missing-type").to_owned());
                assert_ne!(body["type"], "response.failed", "{body}");
                assert_ne!(body["type"], "error", "{body}");
                if body["type"] == "response.created" {
                    started = true;
                }
                if body["type"] == "response.completed" {
                    assert!(started);
                    assert_eq!(body["response"]["usage"]["input_tokens"], 10);
                    socket.close(None).await.unwrap();
                    return;
                }
            }
            panic!("API WebSocket closed without the native terminal");
        })
        .await;
        if completion.is_ok() {
            Ok(())
        } else {
            Err(seen)
        }
    }
}

async fn completed_evidence(
    manager: &PluginManager,
    installed: &PluginView,
    context: &MutationContext,
    count: usize,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let (_, snapshot) =
                request(manager, installed, context, "GET", "api/snapshot", None).await;
            if snapshot["facts"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["event"] == "request_completed")
                .count()
                == count
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original observer must report each completed request once")
}

#[tokio::test]
#[ignore = "requires verified original/rebuilt artifacts and isolated PostgreSQL/Redis"]
async fn official_and_rebuilt_workbench_execute_native_models_and_scoped_affinity_with_single_shared_charge()
 {
    use gateway_admin::model::account_groups::{GroupModelMultiplier, UpdateAccountGroup};
    use gateway_core::{
        account::ProviderAccountId, identity::ProviderKind,
        provider_ports::ProviderSessionAffinityKey,
    };
    use std::{collections::BTreeMap, time::Duration};
    crate::callbacks::log::initialize_capture();
    for archive in verified_workbench_archives() {
        for websocket in [true, false] {
            let server = ModelServer::new(websocket).await;
            let allowed =
                ProviderAccountId::new(format!("acct_allowed_{}", uuid::Uuid::new_v4().simple()))
                    .unwrap();
            let foreign =
                ProviderAccountId::new(format!("acct_foreign_{}", uuid::Uuid::new_v4().simple()))
                    .unwrap();
            let mut env = Environment::with_login_admin_and_openai_accounts(
                &server.url,
                vec![
                    native_account(allowed.as_str(), "native-workbench-allowed"),
                    native_account(foreign.as_str(), "native-workbench-foreign"),
                ],
            )
            .await
            .expect("isolated PostgreSQL and Redis required");
            env.start_store_writers();
            let store = env.store.plugin_store();
            let manager = Arc::new_cyclic(|plugins| {
                PluginManager::new(
                    store.clone(),
                    Arc::new(crate::middleware::plan::KernelRuntime(
                        PluginEngine::new(
                            env.cache.path().join("native-workbench"),
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
            let caller_artifact = manager
                .upload(crate::support::archive("1.0.0", false), &actor())
                .await
                .unwrap();
            let caller = manager
                .update(
                    None,
                    fixture_update(&caller_artifact.sha256, 0, true),
                    &actor(),
                )
                .await
                .unwrap();
            let scope = PluginMutationScope {
                instance_id: caller.id.clone(),
                revision: caller.revision,
                actor: actor(),
            };
            let resources = env.admin.services().plugin_resources();
            let group = resources
                .ensure_group(
                    scope.clone(),
                    PluginGroupEnsure {
                        resource_key: "native-models".into(),
                        name: "Native workbench models".into(),
                        color: "#2563EBFF".into(),
                        description: None,
                    },
                )
                .await
                .unwrap();
            resources
                .change_members(
                    scope,
                    PluginGroupMembers {
                        resource_key: "native-models".into(),
                        add: vec![allowed.to_string()],
                        remove: vec![],
                    },
                )
                .await
                .unwrap();
            env.admin
                .services()
                .account_groups()
                .update(
                    &actor(),
                    UpdateAccountGroup {
                        id: AccountGroupId::new(group.id.clone()).unwrap(),
                        name: group.name.clone(),
                        description: None,
                        color: gateway_admin::model::account_groups::AccountGroupColor::parse(
                            "#2563EBFF",
                        )
                        .unwrap(),
                        budget: gateway_core::engine::budget::ClientBudgetLimits::default(),
                        model_multipliers: BTreeMap::from([(
                            "gpt-5.4".into(),
                            GroupModelMultiplier::All("1.5".into()),
                        )]),
                    },
                )
                .await
                .unwrap();
            let login = env
                .admin
                .services()
                .auth()
                .login(LoginCommand {
                    username: Some("sdk-fixture@fixture.test".into()),
                    password: "sdk-business-fixture-password".into(),
                })
                .await
                .unwrap();
            let user = env
                .admin
                .services()
                .auth()
                .resolve_admin_user_id(Some(&login.session_id))
                .await
                .unwrap()
                .unwrap();
            let context = MutationContext {
                actor: MutationActor::AdminSession {
                    admin_user_id: user.clone(),
                },
                request_id: "workbench-models".into(),
            };
            let key = env
                .admin
                .services()
                .client_keys()
                .create(
                    &context,
                    CreateClientKey {
                        name: "Workbench native model key".into(),
                        label: None,
                        group_ids: vec![AccountGroupId::new(group.id.clone()).unwrap()],
                        limits: RateLimits::unlimited(),
                    },
                )
                .await
                .unwrap();
            let key_id = key.secret.record.id.to_string();
            let baseline = model_call(&manager, &caller, &context, &key_id).await;
            assert!(baseline.get("error").is_none(), "{baseline}");
            settled(&env, 1).await;
            let artifact = manager.upload(archive.clone(), &actor()).await.unwrap();
            let mut command = update(&artifact.sha256, 0, true);
            command.middleware = Some(PluginMiddlewareBinding {
                order: 1,
                mounts: vec![
                    PluginMiddlewareMount::Request,
                    PluginMiddlewareMount::Attempt,
                ],
                scope: PluginExecutionScope::All {},
            });
            command.model_router = Some(PluginModelRouterBinding {
                order: 1,
                scope: PluginExecutionScope::All {},
                failure_policy: PluginPolicyFailurePolicy::Reject,
            });
            command.account_scheduler = Some(PluginAccountSchedulerBinding {
                order: 1,
                scope: PluginExecutionScope::All {},
                failure_policy: PluginPolicyFailurePolicy::Reject,
            });
            command.observer = Some(PluginObserverBinding {
                order: 1,
                request_completed: Some(PluginExecutionScope::All {}),
                websocket_response: Some(PluginExecutionScope::All {}),
            });
            command.frontend_authentication = Some(PluginAuthenticationBinding {
                failure_policy: PluginAuthenticationPolicy::Delegate,
                identities: BTreeMap::from([(
                    "capability-workbench-demo-user".into(),
                    key_id.clone(),
                )]),
            });
            let installed = manager.update(None, command, &actor()).await.unwrap();
            let provider = ProviderKind::new("openai").unwrap();
            let affinity_key = ProviderSessionAffinityKey::try_new("capability-workbench").unwrap();
            let affinity = env.store.provider_ports().session_affinity();
            affinity
                .bind(&provider, &affinity_key, &allowed, Duration::from_secs(60))
                .await
                .unwrap();
            let result = model_call(&manager, &caller, &context, &key_id).await;
            assert!(result.get("error").is_none(), "{result}");
            settled(&env, 2).await;
            let (_, snapshot) =
                request(&manager, &installed, &context, "GET", "api/snapshot", None).await;
            let entries = snapshot["facts"]["entries"].as_array().unwrap();
            assert!(
                entries
                    .iter()
                    .any(|event| event["event"] == "request_processed"
                        && event["details"]["mount"] == "request"
                        && event["details"]["uppercased"] == true),
                "{snapshot}"
            );
            assert!(
                entries
                    .iter()
                    .any(|event| event["event"] == "request_processed"
                        && event["details"]["mount"] == "attempt"
                        && event["details"]["affinityAccountId"] == allowed.as_str()),
                "{snapshot}"
            );
            assert_eq!(
                affinity.load(&provider, &affinity_key).await.unwrap(),
                Some(allowed.clone())
            );
            affinity
                .bind(&provider, &affinity_key, &foreign, Duration::from_secs(60))
                .await
                .unwrap();
            let result = model_call(&manager, &caller, &context, &key_id).await;
            assert!(result.get("error").is_none(), "{result}");
            settled(&env, 3).await;
            let (_, snapshot) =
                request(&manager, &installed, &context, "GET", "api/snapshot", None).await;
            let entries = snapshot["facts"]["entries"].as_array().unwrap();
            let attempt = entries
                .iter()
                .rev()
                .find(|event| {
                    event["event"] == "request_processed" && event["details"]["mount"] == "attempt"
                })
                .unwrap();
            assert!(
                attempt["details"].get("affinityAccountId").is_none(),
                "{attempt}"
            );
            assert_eq!(
                affinity.load(&provider, &affinity_key).await.unwrap(),
                Some(foreign)
            );
            let mut api = ApiServer::new(&env, manager.clone()).await;
            let demo = "CapabilityWorkbench demo";
            let catalog = api.get("/v1/models", demo).await;
            assert_eq!(catalog.status(), 200);
            let catalog: Value = catalog.json().await.unwrap();
            assert!(
                catalog["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|model| model["id"] == "gpt-5.4"),
                "{catalog}"
            );
            assert_eq!(
                api.get("/v1/models", "CapabilityWorkbench invalid")
                    .await
                    .status(),
                401
            );
            assert_eq!(
                api.get(
                    "/v1/models",
                    &format!("Bearer {}", key.secret.expose_for_response())
                )
                .await
                .status(),
                200
            );
            assert_eq!(
                api.get("/api/admin/plugins", demo).await.status(),
                401,
                "external model identity cannot become an admin session"
            );
            let response = api.model(demo).await;
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["x-cpr-capability-workbench"], "observed");
            let response: Value = response.json().await.unwrap();
            assert_eq!(response["status"], "completed", "{response}");
            assert_eq!(response["usage"]["input_tokens"], 10);
            settled(&env, 4).await;
            // A client WS request asks for its chosen upstream transport. The
            // HTTP scenario explicitly keeps the client on WS while selecting
            // HTTP upstream; native HTTP calls above exercise preferred-WS fallback.
            if let Err(seen) = api.websocket_model(demo, websocket).await {
                let (_, snapshot) =
                    request(&manager, &installed, &context, "GET", "api/snapshot", None).await;
                let requests: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('id',id,'completed',completed_at IS NOT NULL,'error',error_kind,'attempts',attempt_count,'transport',upstream_transport) FROM model_requests").fetch_all(&env.pool).await.unwrap();
                panic!(
                    "native API WebSocket timeout: events={seen:?}; requests={requests:?}; upstream_count={}; facts={snapshot}",
                    server.requests.lock().unwrap().len()
                );
            }
            settled(&env, 5).await;
            assert!(
                manager
                    .command_help(&installed.id, installed.revision, Some("ping"))
                    .await
                    .unwrap()
                    .contains("ping")
            );
            let pong = manager
                .run_system_command(
                    &installed.id,
                    PluginCommandAction::Execute {
                        name: "ping".into(),
                        arguments: vec![],
                    },
                    gateway_core::lifecycle::CancellationToken::new(),
                    gateway_core::engine::nested::ExtensionCallScope::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                (
                    pong.stdout.as_str(),
                    pong.stderr.as_str(),
                    pong.exit_code,
                    pong.saved_accounts
                ),
                ("capability-workbench pong\n", "", 0, 0)
            );
            let snapshot = completed_evidence(&manager, &installed, &context, 4).await;
            let entries = snapshot["facts"]["entries"].as_array().unwrap();
            let observed: std::collections::BTreeSet<_> = entries
                .iter()
                .filter(|entry| entry["event"] == "request_completed")
                .map(|entry| entry["requestId"].as_str().unwrap().to_owned())
                .collect();
            assert_eq!(observed.len(), 4, "{snapshot}");
            for event in ["route_decided", "account_ordered"] {
                let decisions: std::collections::BTreeSet<_> = entries
                    .iter()
                    .filter(|entry| entry["event"] == event)
                    .map(|entry| entry["requestId"].as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(decisions, observed, "{snapshot}");
            }
            for event in [
                "demo_identity_authenticated",
                "invalid_demo_identity_rejected",
                "unrelated_identity_delegated",
                "ping_executed",
            ] {
                assert!(
                    entries.iter().any(|entry| entry["event"] == event),
                    "{snapshot}"
                );
            }
            for entry in entries.iter().filter(|entry| {
                entry["event"] == "account_ordered" || entry["event"] == "request_completed"
            }) {
                assert_eq!(entry["accountId"], allowed.as_str(), "{entry}");
                assert_eq!(entry["provider"], "openai", "{entry}");
            }
            let frames: Vec<_> = entries
                .iter()
                .filter(|entry| entry["event"] == "websocket_response")
                .collect();
            if websocket {
                assert!(
                    !frames.is_empty(),
                    "native WebSocket observation must reach the unchanged plugin: {snapshot}"
                );
                let mut sequences = std::collections::BTreeMap::<String, u64>::new();
                for frame in frames {
                    let id = frame["requestId"].as_str().unwrap();
                    assert!(observed.contains(id));
                    assert_eq!(frame["provider"], "openai");
                    assert_eq!(frame["accountId"], allowed.as_str());
                    assert_eq!(frame["details"]["payloadIncluded"], true);
                    let sequence = frame["details"]["sequence"].as_u64().unwrap();
                    if let Some(previous) = sequences.insert(id.to_owned(), sequence) {
                        assert!(sequence > previous);
                    }
                }
            } else {
                assert!(
                    frames.is_empty(),
                    "client WebSocket transport cannot invent upstream WebSocket observations: {snapshot}"
                );
            }
            let recorded: std::collections::BTreeSet<String> =
                sqlx::query_scalar("SELECT id FROM model_requests")
                    .fetch_all(&env.pool)
                    .await
                    .unwrap()
                    .into_iter()
                    .collect();
            assert!(observed.is_subset(&recorded));
            {
                let captured = server.requests.lock().unwrap();
                assert_eq!(captured.len(), 5);
                for (index, request) in captured.iter().enumerate() {
                    let (headers, body) = request.split_once("\r\n\r\n").unwrap();
                    assert!(
                        headers
                            .to_ascii_lowercase()
                            .contains("authorization: bearer native-workbench-allowed")
                    );
                    assert!(!request.contains("native-workbench-foreign"));
                    if websocket {
                        assert!(headers.to_ascii_lowercase().contains("upgrade: websocket"));
                    } else {
                        assert!(
                            headers
                                .to_ascii_lowercase()
                                .contains("content-encoding: zstd")
                        );
                    }
                    let body: Value = serde_json::from_str(body).unwrap();
                    assert_eq!(
                        body["input"][0]["content"][0]["text"],
                        if index == 0 { "mixedCase" } else { "MIXEDCASE" }
                    );
                    if index > 0 {
                        assert!(body.get("metadata").is_none());
                        let headers = headers.to_ascii_lowercase();
                        assert!(headers.contains("x-cpr-workbench-request: observed"));
                        assert!(headers.contains("x-cpr-workbench-attempt: observed"));
                    }
                }
            }
            let rows: Vec<(String,String,String,String)> = sqlx::query_as("SELECT user_id,account_group_id,client_api_key_ref,amount_usd::text FROM user_group_charge_events ORDER BY completed_at").fetch_all(&env.pool).await.unwrap();
            let metering: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('input',input_tokens,'output',output_tokens,'multiplier',billing_multiplier::text,'cost',cost_amount::text,'billed',billed_cost_amount::text,'transport',upstream_transport) FROM model_requests").fetch_all(&env.pool).await.unwrap();
            let ledger: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('raw',raw_amount_usd::text,'multiplier',billing_multiplier::text,'scope',billing_multiplier_scope,'billed',amount_usd::text) FROM user_group_charge_events ORDER BY completed_at").fetch_all(&env.pool).await.unwrap();
            assert_eq!(rows.len(), 5);
            for row in &rows {
                assert_eq!((&row.0, &row.1, &row.2), (&user, &group.id, &key_id));
                assert_eq!(row.3, rows[0].3, "requests={metering:?}; ledger={ledger:?}");
            }
            let priced: (i64,Option<bool>) = sqlx::query_as("SELECT count(*),bool_and(input_tokens=10 AND output_tokens=2 AND billing_multiplier=1.5 AND cost_amount>0 AND billed_cost_amount=round(cost_amount*1.5,10)) FROM model_requests").fetch_one(&env.pool).await.unwrap();
            assert_eq!(priced, (5, Some(true)), "{metering:?}");
            let transports: Vec<String> =
                sqlx::query_scalar("SELECT upstream_transport FROM model_requests")
                    .fetch_all(&env.pool)
                    .await
                    .unwrap();
            assert_eq!(transports.len(), 5);
            assert!(
                transports
                    .iter()
                    .all(|transport| transport == if websocket { "websocket" } else { "http_sse" }),
                "{transports:?}"
            );
            let balanced: bool = sqlx::query_scalar("SELECT bool_and(w.daily_used_usd=e.amount AND w.weekly_used_usd=e.amount) FROM user_group_budget_windows w JOIN (SELECT user_id,account_group_id,sum(amount_usd) amount FROM user_group_charge_events GROUP BY user_id,account_group_id) e USING(user_id,account_group_id)").fetch_one(&env.pool).await.unwrap();
            assert!(balanced);
            let PluginAuthenticationDecision::Authenticated { access, .. } =
                manager.authenticate(demo.into(), None).await.unwrap()
            else {
                panic!("native authentication binding")
            };
            manager
                .update(
                    Some(&installed.id),
                    update(&artifact.sha256, installed.revision, false),
                    &actor(),
                )
                .await
                .unwrap();
            assert!(access.is_revoked());
            assert_eq!(api.model(demo).await.status(), 401);
            assert!(
                manager
                    .command_help(&installed.id, installed.revision, Some("ping"))
                    .await
                    .is_err()
            );
            settled(&env, 5).await;
            api.close().await;
            manager.shutdown().await;
            drop(manager);
            env.close().await;
        }
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
