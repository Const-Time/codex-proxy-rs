//! Public HTTP, native SDK networking and original account transactions share one login grant.
use super::*;
use crate::{command_line::credentials::create, support::api::ApiServer};
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Query, State},
    http::{StatusCode, header::LOCATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use gateway_admin::ports::plugins::PluginCredentialService;
use gateway_core::{engine::nested::ExtensionCallScope, lifecycle::CancellationToken};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;

#[derive(Default)]
struct Exchanges {
    codes: Mutex<BTreeMap<String, Value>>,
    calls: AtomicUsize,
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
}
struct Issuer {
    url: String,
    state: Arc<Exchanges>,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
#[derive(serde::Deserialize)]
struct Authorization {
    redirect_uri: String,
}
async fn authorize(
    State(state): State<Arc<Exchanges>>,
    Query(input): Query<Authorization>,
) -> Response {
    let code = uuid::Uuid::new_v4().simple().to_string();
    let request = create(&format!("oauth-{code}"));
    let material = &request["facts"]["material"];
    state.codes.lock().unwrap().insert(code.clone(), json!({"access_token":material["access_token"],"refresh_token":material["refresh_token"],"token_type":"Bearer","expires_in":3600}));
    let mut redirect = url::Url::parse(&input.redirect_uri).unwrap();
    redirect.query_pairs_mut().append_pair("code", &code);
    (StatusCode::FOUND, [(LOCATION, redirect.as_str())]).into_response()
}
async fn exchange(
    State(state): State<Arc<Exchanges>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    assert!(!headers.contains_key("authorization"));
    assert!(!headers.contains_key("cookie"));
    let form: BTreeMap<String, String> = url::form_urlencoded::parse(&body).into_owned().collect();
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("authorization_code")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("public-callback-fixture")
    );
    state.calls.fetch_add(1, Ordering::SeqCst);
    let token = state
        .codes
        .lock()
        .unwrap()
        .remove(form.get("code").unwrap());
    state.entered.notify_one();
    if state.hold.load(Ordering::SeqCst) {
        state.release.notified().await;
    }
    match token {
        Some(value) => (StatusCode::OK, axum::Json(value)).into_response(),
        None => StatusCode::BAD_REQUEST.into_response(),
    }
}
impl Issuer {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Exchanges::default());
        let app = Router::new()
            .route("/authorize", get(authorize))
            .route("/token", post(exchange))
            .with_state(state.clone());
        let cancellation = CancellationToken::new();
        let stopping = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { stopping.cancelled().await })
                .await
                .unwrap();
        });
        Self {
            url,
            state,
            cancellation,
            task,
        }
    }
    async fn redirect(&self, callback: &url::Url) -> url::Url {
        let response = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
            .get(format!("{}/authorize", self.url))
            .query(&[("redirect_uri", callback.as_str())])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        url::Url::parse(response.headers()[LOCATION].to_str().unwrap()).unwrap()
    }
    async fn close(&mut self) {
        self.state.release.notify_waiters();
        self.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for Issuer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

struct Credentials {
    inner: Arc<dyn PluginCredentialService>,
    actors: Mutex<Vec<MutationContext>>,
}
#[async_trait]
impl PluginCredentialService for Credentials {
    async fn accounts(
        &self,
        scope: PluginMutationScope,
        actor: MutationContext,
        query: PluginCredentialQuery,
    ) -> Result<PluginCredentialPage, gateway_admin::model::AdminError> {
        self.inner.accounts(scope, actor, query).await
    }
    async fn credential(
        &self,
        scope: PluginMutationScope,
        actor: MutationContext,
        account: gateway_core::account::ProviderAccountId,
    ) -> Result<
        gateway_admin::model::provider_credentials::ProviderExportCredentialInput,
        gateway_admin::model::AdminError,
    > {
        self.inner.credential(scope, actor, account).await
    }
    async fn save(
        &self,
        scope: PluginMutationScope,
        actor: MutationContext,
        command: PluginCredentialSave,
        cleanup: ExtensionCallScope,
        cancellation: CancellationToken,
    ) -> Result<PluginCredentialSaved, gateway_admin::model::AdminError> {
        self.actors.lock().unwrap().push(actor.clone());
        self.inner
            .save(scope, actor, command, cleanup, cancellation)
            .await
    }
}
fn setup(
    env: &Environment,
    credentials: Arc<Credentials>,
    http: Arc<gateway_plugin_runtime::HttpDispatcher>,
) -> Arc<PluginManager> {
    Arc::new_cyclic(|manager: &std::sync::Weak<PluginManager>| {
        let source = manager.clone();
        PluginManager::new(
            env.store.plugin_store(),
            Arc::new(
                PluginEngine::new(
                    env.cache.path().join("oauth-plugins"),
                    "3.11.2".parse().unwrap(),
                    env.store.plugin_store(),
                )
                .with_credentials(credentials)
                .with_data(env.admin.services().plugin_data())
                .with_http(http)
                .with_services(Arc::new(move || {
                    source
                        .upgrade()
                        .and_then(|manager| manager.middleware_registry())
                        .map(|registry| registry.service_factory())
                        .ok_or_else(|| {
                            gateway_core::engine::middleware::service::Error::unavailable(
                                "native service publication absent",
                            )
                        })
                })),
            ),
        )
        .with_services(env.admin.services().service_settings())
        .with_management_callbacks(
            env.store.provider_ports().oauth_pending(),
            env.store.admin_ports().auth(),
        )
    })
}
async fn install_oauth(manager: &PluginManager, issuer: &Issuer) -> PluginView {
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let mut settings = update(&artifact.sha256, 0, true);
    settings.configuration =
        json!({"public_callbacks":true,"callback_oauth_url":format!("{}/token",issuer.url)});
    manager.update(None, settings, &actor()).await.unwrap()
}
async fn login(api: &ApiServer) -> String {
    let response = api.client.post(format!("http://{}/api/admin/auth/login", api.address))
        .json(&json!({"username":"sdk-fixture@fixture.test","password":"sdk-business-fixture-password"})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .into()
}
async fn ticket(api: &ApiServer, installed: &PluginView, cookie: &str) -> url::Url {
    let response = api.client.post(format!("http://{}/api/admin/plugins/callback-ticket",api.address))
        .header("cookie",cookie).json(&json!({"id":installed.id,"revision":installed.revision,"path":"oauth","ttlSeconds":60})).send().await.unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut callback =
        url::Url::parse(&format!("http://{}/api/plugins/callback", api.address)).unwrap();
    callback
        .query_pairs_mut()
        .append_pair("id", &installed.id)
        .append_pair("revision", &installed.revision.to_string())
        .append_pair("path", "oauth")
        .append_pair("state", body["data"]["state"].as_str().unwrap());
    callback
}
async fn count(env: &Environment) -> i64 {
    sqlx::query_scalar("select count(*) from provider_accounts")
        .fetch_one(&env.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn native_public_oauth_http_callback_exchanges_once_commits_provider_account_and_inherits_actual_admin_service_authority()
 {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let mut issuer = Issuer::new().await;
    let credentials = Arc::new(Credentials {
        inner: env.admin.services().plugin_credentials(),
        actors: Mutex::new(vec![]),
    });
    let http = Arc::new(gateway_plugin_runtime::HttpDispatcher::default());
    let manager = setup(&env, credentials.clone(), http.clone());
    let installed = install_oauth(&manager, &issuer).await;
    let mut api = ApiServer::new(&env, manager.clone()).await;
    http.attach(&api.dispatcher);
    let cookie = login(&api).await;
    let callback = ticket(&api, &installed, &cookie).await;
    let redirect = issuer.redirect(&callback).await;
    let response = api
        .client
        .get(redirect.clone())
        .header("cookie", "cpr_admin_session=forged")
        .header("authorization", "Bearer forged")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response: Value = response.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["saved"]["credential_revision"], 1, "{response}");
    assert_eq!(response["api_key_exists"], false);
    assert!(!response.to_string().contains("refresh_token"));
    assert!(!response.to_string().contains("access_token"));
    assert_eq!(count(&env).await, 1);
    assert_eq!(env.published.lock().unwrap().len(), 1);
    let actors = credentials.actors.lock().unwrap().clone();
    assert_eq!(actors.len(), 1);
    let admin = administrator(&env).await;
    assert_eq!(actors[0].actor, admin.actor);
    let audits: Vec<(String,String,Vec<String>)> = sqlx::query_as("select actor_kind,actor_ref,changed_fields from admin_audit_events where actor_kind='plugin'").fetch_all(&env.pool).await.unwrap();
    assert_eq!(
        audits,
        vec![(
            "plugin".into(),
            format!("plugin:{}@{}", installed.id, installed.revision),
            vec!["credentials".into()]
        )]
    );
    let again = api.client.get(redirect.clone()).send().await.unwrap();
    assert_eq!(again.status(), StatusCode::BAD_REQUEST);
    assert_eq!(issuer.state.calls.load(Ordering::SeqCst), 1);
    let invalid = ticket(&api, &installed, &cookie).await;
    let mut invalid = invalid;
    invalid
        .query_pairs_mut()
        .append_pair("code", "already-consumed-code");
    assert!(
        api.client
            .get(invalid.clone())
            .send()
            .await
            .unwrap()
            .status()
            .is_server_error()
    );
    assert_eq!(
        api.client.get(invalid).send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(count(&env).await, 1);
    assert_eq!(credentials.actors.lock().unwrap().len(), 1);
    assert_eq!(issuer.state.calls.load(Ordering::SeqCst), 2);
    api.close().await;
    manager.shutdown().await;
    issuer.close().await;
    drop(api);
    drop(manager);
    env.close().await;
}

#[tokio::test]
async fn public_oauth_deadline_before_exchange_does_not_save_and_inflight_commit_is_drained_without_replay()
 {
    for during_commit in [false, true] {
        let Some(env) = Environment::with_login_admin().await else {
            return;
        };
        let mut issuer = Issuer::new().await;
        issuer.state.hold.store(!during_commit, Ordering::SeqCst);
        let credentials = Arc::new(Credentials {
            inner: env.admin.services().plugin_credentials(),
            actors: Mutex::new(vec![]),
        });
        let http = Arc::new(gateway_plugin_runtime::HttpDispatcher::default());
        let manager = setup(&env, credentials.clone(), http.clone());
        let installed = install_oauth(&manager, &issuer).await;
        let mut api = ApiServer::with_timeout(&env, manager.clone(), Some(2)).await;
        http.attach(&api.dispatcher);
        let cookie = login(&api).await;
        let callback = ticket(&api, &installed, &cookie).await;
        let redirect = issuer.redirect(&callback).await;
        // Block the original Store transaction, rather than an artificial SDK receipt.
        let mut lock = if during_commit {
            let mut transaction = env.pool.begin().await.unwrap();
            sqlx::query("select id from runtime_settings where id=1 for update")
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
            Some(transaction)
        } else {
            None
        };
        let lock_pid: Option<i32> = if let Some(transaction) = lock.as_mut() {
            Some(
                sqlx::query_scalar("select pg_backend_pid()")
                    .fetch_one(&mut **transaction)
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let running = {
            let client = api.client.clone();
            let address = redirect.clone();
            tokio::spawn(async move { client.get(address).send().await.unwrap() })
        };
        if let Some(lock_pid) = lock_pid {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = sqlx::query_scalar("select exists(select 1 from pg_stat_activity where datname=current_database() and query like 'update runtime_settings%' and $1=any(pg_blocking_pids(pid)))")
                        .bind(lock_pid).fetch_one(&env.pool).await.unwrap();
                    if blocked { break; }
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(5), issuer.state.entered.notified())
                .await
                .unwrap();
        }
        assert_eq!(count(&env).await, 0);
        let response = tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(
            api.client.get(redirect).send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(issuer.state.calls.load(Ordering::SeqCst), 1);
        let mut stop = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.shutdown().await })
        };
        if let Some(lock) = lock.take() {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut stop)
                    .await
                    .is_err()
            );
            lock.rollback().await.unwrap();
        } else {
            issuer.state.release.notify_one();
        }
        tokio::time::timeout(Duration::from_secs(5), &mut stop)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count(&env).await, i64::from(during_commit));
        assert_eq!(
            credentials.actors.lock().unwrap().len(),
            usize::from(during_commit)
        );
        assert_eq!(
            env.published.lock().unwrap().len(),
            usize::from(during_commit)
        );
        api.close().await;
        issuer.close().await;
        drop(api);
        drop(manager);
        env.close().await;
    }
}

#[tokio::test]
async fn public_oauth_rechecks_actual_administrator_after_exchange_before_native_save() {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let mut issuer = Issuer::new().await;
    issuer.state.hold.store(true, Ordering::SeqCst);
    let credentials = Arc::new(Credentials {
        inner: env.admin.services().plugin_credentials(),
        actors: Mutex::new(vec![]),
    });
    let http = Arc::new(gateway_plugin_runtime::HttpDispatcher::default());
    let manager = setup(&env, credentials.clone(), http.clone());
    let installed = install_oauth(&manager, &issuer).await;
    let mut api = ApiServer::new(&env, manager.clone()).await;
    http.attach(&api.dispatcher);
    let cookie = login(&api).await;
    let admin_user_id = match administrator(&env).await.actor {
        MutationActor::AdminSession { admin_user_id } => admin_user_id,
        _ => unreachable!(),
    };
    let callback = ticket(&api, &installed, &cookie).await;
    let redirect = issuer.redirect(&callback).await;
    let running = {
        let client = api.client.clone();
        let address = redirect.clone();
        tokio::spawn(async move { client.get(address).send().await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(5), issuer.state.entered.notified())
        .await
        .unwrap();
    sqlx::query("update users set enabled=false where id=$1")
        .bind(&admin_user_id)
        .execute(&env.pool)
        .await
        .unwrap();
    issuer.state.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
    assert!(response.status().is_server_error());
    assert_eq!(count(&env).await, 0);
    assert!(env.published.lock().unwrap().is_empty());
    assert_eq!(credentials.actors.lock().unwrap().len(), 1);
    assert_eq!(
        api.client.get(redirect).send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(issuer.state.calls.load(Ordering::SeqCst), 1);
    api.close().await;
    manager.shutdown().await;
    issuer.close().await;
    drop(api);
    drop(manager);
    env.close().await;
}
