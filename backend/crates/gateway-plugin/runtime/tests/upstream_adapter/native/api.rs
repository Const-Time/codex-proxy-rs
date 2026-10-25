//! Normal enablement, native API transport and the original PostgreSQL ledger.
use super::super::execution::Server;
use super::websocket::Socket;
use crate::services::upstream_sdk::environment::Environment;
use crate::support::{
    actor,
    api::ApiServer,
    update, upstream_archive_contract,
    xai::{Proxy, Trust},
};
use futures::{SinkExt as _, StreamExt as _};
use gateway_admin::{
    model::{
        MutationActor, MutationContext, account_groups::*, auth::LoginCommand,
        client_keys::CreateClientKey, plugins::*,
    },
    plugins::PluginManager,
    ports::plugins::PluginService,
};
use gateway_core::{account::NewProviderAccount, policy::RateLimits, routing::AccountGroupId};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest as _};

fn account(id: &str, token: &str) -> NewProviderAccount {
    use provider_openai::credential::*;
    CodexCredentialAdmin
        .prepare_import(ImportCodexOAuthCredential {
            account_id: id.into(),
            name: id.into(),
            enabled: true,
            secret: CodexOAuthSecret {
                access_token: secrecy::SecretString::from(token),
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

fn manager(env: &Environment) -> Arc<PluginManager> {
    let store = env.store.plugin_store();
    Arc::new(PluginManager::new(
        store.clone(),
        Arc::new(
            PluginEngine::new(
                env.cache.path().join("api-adapter"),
                "3.11.2".parse().unwrap(),
                store,
            )
            .with_credentials(env.admin.services().plugin_credentials()),
        ),
    ))
}

fn configuration(url: &str, websocket: bool, provider: &str, model: &str) -> Value {
    json!({"adapter_url":url,"adapter_transport":if websocket {"websocket"} else {"http_sse"},
        "adapter_provider":provider,"adapter_models":[model]})
}

fn input(allowed: &str, websocket: bool, model: &str) -> Value {
    json!({"model":model,"input":"native adapter API","fixture_api":true,
        "behavior":if websocket {"websocket"} else {"http"},
        "fixture_account":allowed,"fixture_credential_revision":1,"fixture_model":model})
}

async fn http_model(api: &ApiServer, authorization: &str, mut body: Value, stream: bool) {
    body["stream"] = json!(stream);
    let response = api
        .client
        .post(format!("http://{}/v1/responses", api.address))
        .header("authorization", authorization)
        .json(&body)
        .send()
        .await
        .unwrap();
    if response.status() != 200 {
        panic!(
            "native API status {}: {}",
            response.status(),
            response.text().await.unwrap()
        );
    }
    if stream {
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        let text = response.text().await.unwrap();
        assert!(text.contains("response.created"), "{text}");
        assert!(text.contains("response.completed"), "{text}");
        assert!(!text.contains("response.failed"), "{text}");
    } else {
        let response: Value = response.json().await.unwrap();
        assert_eq!(response["id"], "adapter-response");
        assert_eq!(response["status"], "completed");
    }
}

async fn websocket_model(api: &ApiServer, authorization: &str, mut body: Value) {
    let mut handshake = format!("ws://{}/v1/responses", api.address)
        .into_client_request()
        .unwrap();
    handshake
        .headers_mut()
        .insert("authorization", authorization.parse().unwrap());
    let stream = tokio::net::TcpStream::connect(api.address).await.unwrap();
    let (mut socket, response) = tokio_tungstenite::client_async(handshake, stream)
        .await
        .unwrap();
    assert_eq!(response.status(), 101);
    body["type"] = json!("response.create");
    socket
        .send(Message::Text(body.to_string().into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut started = false;
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            let event: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(event["type"], "error", "{event}");
            assert_ne!(event["type"], "response.failed", "{event}");
            if event["type"] == "response.created" {
                started = true;
            }
            if event["type"] == "response.completed" {
                assert!(started);
                assert_eq!(event["response"]["id"], "adapter-response");
                break;
            }
        }
    })
    .await
    .expect("native API WebSocket terminal");
    socket.close(None).await.unwrap();
}

async fn settled(env: &Environment, count: i64, key: &str, account: &str, provider: &str) {
    let ports = env.store.provider_ports();
    let request_interval = Duration::from_millis(
        env.admin
            .services()
            .settings()
            .load()
            .await
            .unwrap()
            .request_interval_ms,
    );
    let key = gateway_core::policy::ClientApiKeyId::new(key).unwrap();
    let provider = gateway_core::identity::ProviderKind::new(provider).unwrap();
    let accounts = [gateway_core::account::ProviderAccountId::new(account).unwrap()];
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let requests: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM model_requests WHERE completed_at IS NOT NULL",
            )
            .fetch_one(&env.pool)
            .await
            .unwrap();
            let charges: i64 = sqlx::query_scalar("SELECT count(*) FROM user_group_charge_events")
                .fetch_one(&env.pool)
                .await
                .unwrap();
            let leases = ports
                .leases()
                .load_state(&key, &provider, &accounts)
                .await
                .unwrap();
            let idle = leases.signals().get(&accounts[0]).is_none_or(|state| {
                state.in_flight == 0
                    && state.last_started_at.is_none_or(|started| {
                        started
                            .elapsed()
                            .is_ok_and(|elapsed| elapsed >= request_interval)
                    })
            });
            if requests == count && charges == count && idle {
                break;
            }
            // Finishing a request does not expire the configured account pacing
            // interval. Fast fixtures must wait for eligibility, not just billing.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("native request and unique shared charge must settle");
}

#[tokio::test]
async fn normal_v1_v2_adapters_survive_restore_and_upgrade_through_api_json_sse_and_websocket() {
    run("openai", None).await;
}

#[tokio::test]
async fn normal_xai_v1_v2_adapters_charge_native_ledger_through_api_restore_and_upgrade() {
    let root = match std::env::var_os("CPR_NATIVE_XAI_TRUST") {
        Some(root) => PathBuf::from(root),
        None => {
            for key in ["CPR_TEST_DATABASE_URL", "CPR_TEST_REDIS_URL"] {
                if std::env::var_os(key).is_none() {
                    let ci = std::env::var("CI")
                        .ok()
                        .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
                    assert!(
                        !ci,
                        "{key} is required for native xAI API integration tests in CI"
                    );
                    return;
                }
            }
            Trust::new().run_with_timeout(
                "upstream_adapter::native::api::normal_xai_v1_v2_adapters_charge_native_ledger_through_api_restore_and_upgrade",
                Duration::from_secs(180),
            ).await;
            return;
        }
    };
    run("xai", Some(&root)).await;
}

async fn run(provider: &str, root: Option<&Path>) {
    let model = if provider == "xai" {
        "grok-4.5"
    } else {
        "gpt-5.4"
    };
    for contract in [4] {
        for websocket in [false, true] {
            eprintln!("adapter v{contract}, upstream websocket={websocket}: enable/API");
            let http = Server::new().await;
            let socket = Socket::new(false, false).await;
            let allowed = format!("acct_allowed_{}", uuid::Uuid::new_v4().simple());
            let forbidden = format!("acct_forbidden_{}", uuid::Uuid::new_v4().simple());
            let url = if websocket { &socket.url } else { &http.url };
            let proxy = if let Some(root) = root {
                Some(Proxy::start(root, &format!("subject-{allowed}"), url, "api").await)
            } else {
                None
            };
            let accounts = [
                (&allowed, "fixture-provider-secret"),
                (&forbidden, "fixture-forbidden-secret"),
            ]
            .into_iter()
            .map(|(id, token)| {
                if let Some(proxy) = &proxy {
                    let mut prepared = crate::support::xai::account(
                        &gateway_core::account::ProviderAccountId::new(id.clone()).unwrap(),
                        token,
                    );
                    prepared.account = prepared.account.with_outbound_proxy(Some(
                        gateway_core::account::OutboundProxy::parse(&proxy.url).unwrap(),
                    ));
                    prepared
                } else {
                    account(id, token)
                }
            })
            .collect();
            let Some(mut env) = Environment::with_login_admin_and_accounts(
                (provider == "openai").then_some(http.url.as_str()),
                accounts,
            )
            .await
            else {
                return;
            };
            let initial_requests = http.requests.lock().unwrap().len();
            for request in http.requests.lock().unwrap().iter() {
                let head = request.lines().next().unwrap();
                assert!(
                    head.starts_with("GET ") && head.contains("/models"),
                    "{head}"
                );
            }
            env.start_store_writers();
            let mut plugins = manager(&env);
            let artifact = plugins
                .upload(upstream_archive_contract("1.0.0", contract), &actor())
                .await
                .unwrap();
            assert!(artifact.unsupported_capabilities.is_empty());
            let mut command = update(&artifact.sha256, 0, true);
            command.configuration = configuration(url, websocket, provider, model);
            command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
                scope: PluginExecutionScope::All {},
            });
            let installed = plugins.update(None, command, &actor()).await.unwrap();
            assert_eq!(installed.status, "ready");

            let scope = PluginMutationScope {
                instance_id: installed.id.clone(),
                revision: installed.revision,
                actor: actor(),
            };
            let resources = env.admin.services().plugin_resources();
            let group = resources
                .ensure_group(
                    scope.clone(),
                    PluginGroupEnsure {
                        resource_key: "api-models".into(),
                        name: "Adapter API models".into(),
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
                        resource_key: "api-models".into(),
                        add: vec![allowed.clone()],
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
                        color: AccountGroupColor::parse("#2563EBFF").unwrap(),
                        budget: Default::default(),
                        model_multipliers: BTreeMap::from([(
                            model.into(),
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
                request_id: "adapter-api".into(),
            };
            let key = env
                .admin
                .services()
                .client_keys()
                .create(
                    &context,
                    CreateClientKey {
                        name: "Adapter native API".into(),
                        label: None,
                        group_ids: vec![AccountGroupId::new(group.id.clone()).unwrap()],
                        limits: RateLimits::unlimited(),
                    },
                )
                .await
                .unwrap();
            let authorization = format!("Bearer {}", key.secret.expose_for_response());
            let mut api = ApiServer::new(&env, plugins.clone()).await;
            let body = input(&allowed, websocket, model);
            let denied = api
                .client
                .post(format!("http://{}/v1/responses", api.address))
                .bearer_auth("invalid-key")
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(denied.status(), 401);
            http_model(&api, &authorization, body.clone(), false).await;
            settled(&env, 1, key.secret.record.id.as_str(), &allowed, provider).await;
            http_model(&api, &authorization, body.clone(), true).await;
            settled(&env, 2, key.secret.record.id.as_str(), &allowed, provider).await;
            websocket_model(&api, &authorization, body.clone()).await;
            settled(&env, 3, key.secret.record.id.as_str(), &allowed, provider).await;
            let raw = "{ \"future\": 9007199254740993, \"text\": \"雪☃\", \"status\": \"completed\", \"id\": \"adapter-response\" }\n";
            for (index, (mode, status)) in [("chunks", 200), ("tail-fault", 502)]
                .into_iter()
                .enumerate()
            {
                let mut raw_input = body.clone();
                raw_input["stream"] = json!(false);
                raw_input["raw_body"] = json!(mode);
                let response = api
                    .client
                    .post(format!("http://{}/v1/responses", api.address))
                    .header("authorization", &authorization)
                    .json(&raw_input)
                    .send()
                    .await
                    .unwrap();
                let received_status = response.status().as_u16();
                let received = response.bytes().await.unwrap();
                assert_eq!(
                    received_status,
                    status,
                    "raw body {mode}: {}",
                    String::from_utf8_lossy(&received)
                );
                if status == 200 {
                    assert_eq!(received.as_ref(), raw.as_bytes());
                } else {
                    assert!(!String::from_utf8_lossy(&received).contains("private fixture"));
                    assert!(!String::from_utf8_lossy(&received).contains("9007199254740993"));
                }
                settled(
                    &env,
                    4 + index as i64,
                    key.secret.record.id.as_str(),
                    &allowed,
                    provider,
                )
                .await;
            }

            api.close().await;
            drop(api);
            plugins.shutdown().await;
            plugins = manager(&env);
            plugins.restore().await.unwrap();
            eprintln!("adapter v{contract}, upstream websocket={websocket}: restored/API");
            assert_eq!(plugins.list().await.unwrap()[0].status, "ready");
            let mut api = ApiServer::new(&env, plugins.clone()).await;
            http_model(&api, &authorization, body.clone(), false).await;
            settled(&env, 6, key.secret.record.id.as_str(), &allowed, provider).await;

            let replacement = plugins
                .upload(upstream_archive_contract("1.0.1", contract), &actor())
                .await
                .unwrap();
            let mut command = update(&replacement.sha256, installed.revision, true);
            command.configuration = configuration(url, websocket, provider, model);
            command.upstream_adapter = installed.upstream_adapter.clone();
            let upgraded = plugins
                .update(Some(&installed.id), command, &actor())
                .await
                .unwrap();
            eprintln!("adapter v{contract}, upstream websocket={websocket}: upgraded/API");
            assert_eq!(upgraded.revision, installed.revision + 1);
            http_model(&api, &authorization, body, false).await;
            settled(&env, 7, key.secret.record.id.as_str(), &allowed, provider).await;

            let requests: Vec<(String,String,String,String,bool)> = sqlx::query_as(
                "SELECT provider_kind,provider_account_id,client_api_key_ref,upstream_transport,\
                 input_tokens=10 AND output_tokens=2 AND billing_multiplier=1.5 AND cost_amount>0 \
                 AND billed_cost_amount=round(cost_amount*1.5,10) FROM model_requests ORDER BY started_at"
            ).fetch_all(&env.pool).await.unwrap();
            assert_eq!(requests.len(), 7);
            let outcomes: Vec<(String, i64)> = sqlx::query_as(
                "SELECT outcome,count(*) FROM model_requests GROUP BY outcome ORDER BY outcome",
            )
            .fetch_all(&env.pool)
            .await
            .unwrap();
            assert_eq!(
                outcomes,
                vec![("failed".into(), 1), ("succeeded".into(), 6)]
            );
            for row in requests {
                assert_eq!(row.0, provider);
                assert_eq!(row.1, allowed);
                assert_eq!(row.2, key.secret.record.id.to_string());
                assert_eq!(row.3, if websocket { "websocket" } else { "http_sse" });
                assert!(row.4);
            }
            let ledger: Vec<(String,String,String,bool)> = sqlx::query_as(
                "SELECT user_id,account_group_id,client_api_key_ref,\
                 raw_amount_usd>0 AND billing_multiplier=1.5 AND amount_usd=round(raw_amount_usd*1.5,10) \
                 FROM user_group_charge_events"
            ).fetch_all(&env.pool).await.unwrap();
            assert_eq!(ledger.len(), 7);
            for row in ledger {
                assert_eq!(row.0, user);
                assert_eq!(row.1, group.id);
                assert_eq!(row.2, key.secret.record.id.to_string());
                assert!(row.3);
            }
            let matching: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM user_group_charge_events e JOIN model_requests r ON r.id=e.request_id \
                 WHERE e.raw_amount_usd=r.cost_amount AND e.amount_usd=r.billed_cost_amount"
            ).fetch_one(&env.pool).await.unwrap();
            assert_eq!(
                matching, 7,
                "each native request must own its exact original charge"
            );
            let balanced: bool = sqlx::query_scalar("SELECT bool_and(w.daily_used_usd=e.amount AND w.weekly_used_usd=e.amount) FROM user_group_budget_windows w JOIN (SELECT user_id,account_group_id,sum(amount_usd) amount FROM user_group_charge_events GROUP BY user_id,account_group_id) e USING(user_id,account_group_id)")
                .fetch_one(&env.pool).await.unwrap();
            assert!(balanced);
            if websocket {
                assert_eq!(http.requests.lock().unwrap().len(), initial_requests);
                assert_eq!(socket.pings.load(Ordering::SeqCst), 7);
                for headers in socket.headers.lock().unwrap().iter() {
                    assert_eq!(headers["authorization"], "Bearer fixture-provider-secret");
                    if provider == "xai" {
                        assert_eq!(headers["x-grok-user-id"], format!("subject-{allowed}"));
                        assert_eq!(headers["x-xai-token-auth"], "xai-grok-cli");
                        assert!(!headers.contains_key("chatgpt-account-id"));
                    } else {
                        assert_eq!(headers["chatgpt-account-id"], format!("chatgpt-{allowed}"));
                    }
                    assert!(!headers.contains_key("cookie"));
                }
            } else {
                let requests = http.requests.lock().unwrap();
                assert_eq!(requests.len(), initial_requests + 7);
                for request in &requests[initial_requests..] {
                    assert!(request.contains("authorization: Bearer fixture-provider-secret"));
                    assert!(request.contains("x-fixture: adapter"));
                    if provider == "xai" {
                        assert!(request.contains(&format!("x-grok-user-id: subject-{allowed}")));
                        assert!(request.contains("x-xai-token-auth: xai-grok-cli"));
                        assert!(!request.contains("chatgpt-account-id"));
                    } else {
                        assert!(
                            request.contains(&format!("chatgpt-account-id: chatgpt-{allowed}"))
                        );
                    }
                    assert!(!request.contains("cookie:"));
                    assert!(!request.contains("fixture-forbidden-secret"));
                }
                assert_eq!(socket.pings.load(Ordering::SeqCst), 0);
            }
            api.close().await;
            drop(api);
            plugins.shutdown().await;
            drop(plugins);
            drop(resources);
            env.close().await;
            if let Some(proxy) = proxy {
                let events = proxy.events();
                let catalog: Vec<_> = events
                    .iter()
                    .filter(|event| event["kind"] == "catalog")
                    .collect();
                assert!(
                    catalog.iter().any(|event| event["path"] == "/v1/models"),
                    "native startup must fetch the real Provider catalog"
                );
                assert!(catalog.iter().all(|event| event["authorization"] == true));
                assert!(
                    !events.iter().any(|event| event["kind"] == "forbidden"),
                    "only owned offline endpoints may be contacted: {events:?}"
                );
                let sends: Vec<_> = events
                    .iter()
                    .filter(|event| event["kind"] == "inference")
                    .collect();
                assert_eq!(sends.len(), 7, "each API request must send exactly once");
                assert!(sends.iter().all(|event| event["authorization"] == true));
                proxy.stop().await;
            }
        }
    }
}
