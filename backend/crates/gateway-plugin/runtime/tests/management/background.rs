//! Exercise the actual HTTP placement, administrator session and SDK subprocess.
use super::*;
use crate::support::api::ApiServer;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

fn envelope(id: &str, input: Value) -> Value {
    json!({"id":id,"revision":1,"method":"POST","path":"echo",
        "contentType":"application/json","body":STANDARD.encode(input.to_string())})
}

async fn invoke(api: &ApiServer, cookie: &str, id: &str, input: Value) -> Value {
    let response = api
        .client
        .post(format!("http://{}/api/admin/plugins/invoke", api.address))
        .header("cookie", cookie)
        .json(&envelope(id, input))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["data"]["status"], 200, "{value}");
    serde_json::from_slice(
        &STANDARD
            .decode(value["data"]["body"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap()
}

fn supports(headers: &Value) -> bool {
    headers
        .as_array()
        .unwrap()
        .iter()
        .any(|h| h["name"] == "x-cpr-background-version" && h["value"] == json!([49]))
}

#[tokio::test]
async fn background_model_callbacks_keep_personal_key_authority_after_browser_cancellation() {
    use crate::models::fixture::Fixture;
    use gateway_core::engine::{execution::ExecutionService as _, middleware::http::Context};
    use std::sync::atomic::Ordering;
    for (owner, key, expected) in [
        ("alice", "key_alice", 3),
        ("bob", "key_alice", 0),
        ("alice", "key_disabled", 0),
    ] {
        let fixture = Fixture::new().await;
        let mut request = crate::support::request("background");
        request.actor.actor = MutationActor::AdminSession {
            admin_user_id: owner.into(),
        };
        request.body = serde_json::to_vec(
            &json!({"operation":"background","limit":3,"delay":100,"model_probe":true,"key":key}),
        )
        .unwrap();
        let context = Context {
            request_id: request.actor.request_id.clone(),
            call_id: "browser-http".into(),
            parent_call_id: None,
            call_scope: Default::default(),
            model_parent: None,
            cancellation: Default::default(),
            deadline_at: Default::default(),
        };
        let cancellation = context.cancellation.clone();
        request.invocation = Some(PluginInvocationContext {
            context,
            http: None,
            services: None,
        });
        fixture
            .manager
            .handle(&fixture.id, 1, request)
            .await
            .unwrap();
        cancellation.cancel();
        // An error must end the chain, not repeatedly retry the same billed operation.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut request = crate::support::request("background_capability");
                request.actor.actor = MutationActor::AdminSession {
                    admin_user_id: owner.into(),
                };
                let response = fixture
                    .manager
                    .handle(&fixture.id, 1, request)
                    .await
                    .unwrap();
                let headers: Value = serde_json::from_slice(&response.body).unwrap();
                if !headers
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|h| h["name"] == "x-cpr-background-active")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fixture.core.starts.load(Ordering::SeqCst), expected);
        assert_eq!(fixture.core.charges.lock().unwrap().len(), expected);
        let response = fixture
            .manager
            .handle(&fixture.id, 1, crate::support::request("state_read"))
            .await
            .unwrap();
        let state: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(state["record"]["value"], if expected == 0 { 1 } else { 3 });

        // A native model root still carries borrowed model authority, even with
        // no parent_call_id and a root graph. It must not start a detached chain.
        let parent = fixture
            .execution
            .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
            .unwrap();
        let mut request = crate::support::request("background_capability");
        request.actor.actor = MutationActor::AdminSession {
            admin_user_id: owner.into(),
        };
        request.invocation = Some(PluginInvocationContext {
            context: Context::from_execution(parent.nested_context()),
            http: None,
            services: None,
        });
        let response = fixture
            .manager
            .handle(&fixture.id, 1, request)
            .await
            .unwrap();
        assert!(!supports(&serde_json::from_slice(&response.body).unwrap()));
        fixture.manager.shutdown().await;
    }
}

#[tokio::test]
async fn background_root_context_detaches_but_borrowed_authority_is_rejected() {
    use gateway_core::engine::middleware::http::Context;
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let manager = PluginManager::new(
        store.clone(),
        Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.12.4".parse().unwrap(),
            store,
        )),
    );
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = install(&manager, &artifact.sha256).await;
    let root = Context {
        request_id: "browser-request".into(),
        call_id: "browser-http".into(),
        parent_call_id: None,
        call_scope: Default::default(),
        model_parent: None,
        cancellation: Default::default(),
        deadline_at: Default::default(),
    };
    let make_request = |context: Context, operation: &str| {
        let mut request = crate::support::request(operation);
        request.actor.actor = MutationActor::AdminSession {
            admin_user_id: "alice".into(),
        };
        request.body =
            serde_json::to_vec(&json!({"operation":operation,"limit":3,"delay":100})).unwrap();
        request.invocation = Some(PluginInvocationContext {
            context,
            http: None,
            services: None,
        });
        request
    };
    let child = root.child("nested-fixture".into()).unwrap();
    let mut parent_id_only = root.clone();
    parent_id_only.parent_call_id = Some("parent".into());
    let mut child_graph_only = child.clone();
    child_graph_only.parent_call_id = None;
    let mut cancelled = root.clone();
    cancelled.cancellation = Default::default();
    cancelled.cancellation.cancel();
    let mut expired = root.clone();
    expired.deadline_at = gateway_core::lifecycle::Deadline::from_timeout(
        std::time::SystemTime::now(),
        Some(Duration::ZERO),
    )
    .unwrap();
    for context in [child, parent_id_only, child_graph_only, cancelled, expired] {
        let response = manager
            .handle(
                &installed.id,
                1,
                make_request(context.clone(), "background_capability"),
            )
            .await
            .unwrap();
        assert!(!supports(&serde_json::from_slice(&response.body).unwrap()));
        assert!(
            manager
                .handle(&installed.id, 1, make_request(context, "background"))
                .await
                .is_err()
        );
    }
    for actor in [MutationActor::System, MutationActor::AdminApiKey] {
        let mut request = make_request(root.clone(), "background_capability");
        request.actor.actor = actor;
        let response = manager.handle(&installed.id, 1, request).await.unwrap();
        assert!(!supports(&serde_json::from_slice(&response.body).unwrap()));
    }
    manager
        .handle(&installed.id, 1, make_request(root.clone(), "background"))
        .await
        .unwrap();
    root.cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = manager
                .handle(&installed.id, 1, crate::support::request("state_read"))
                .await
                .unwrap();
            let state: Value = serde_json::from_slice(&result.body).unwrap();
            if state["record"]["value"] == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    manager.shutdown().await;
}

#[tokio::test]
async fn background_real_http_advertises_capability_and_survives_response_end() {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let manager = Arc::new(super::manager(&env));
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = install(&manager, &artifact.sha256).await;
    let mut api = ApiServer::new(&env, manager.clone()).await;
    let login = api.client.post(format!("http://{}/api/admin/auth/login", api.address))
        .json(&json!({"username":"sdk-fixture@fixture.test","password":"sdk-business-fixture-password"})).send().await.unwrap();
    assert_eq!(login.status(), reqwest::StatusCode::OK);
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let capability = invoke(
        &api,
        &cookie,
        &installed.id,
        json!({"operation":"background_capability"}),
    )
    .await;
    assert!(
        supports(&capability),
        "normal authenticated HTTP management requests must advertise background v1: {capability}"
    );

    // Borrowed internal HTTP authority must not be detached, even with a valid admin cookie.
    use gateway_core::engine::middleware::http as core;
    use http_body_util::BodyExt as _;
    let root = core::Context {
        request_id: "nested-background".into(),
        call_id: "parent".into(),
        parent_call_id: None,
        call_scope: Default::default(),
        model_parent: None,
        cancellation: Default::default(),
        deadline_at: Default::default(),
    };
    let nested = http::Request::builder()
        .method("POST")
        .uri("/api/admin/plugins/invoke")
        .header("cookie", &cookie)
        .header("content-type", "application/json")
        .body(
            http_body_util::Full::new(bytes::Bytes::from(
                envelope(&installed.id, json!({"operation":"background_capability"})).to_string(),
            ))
            .map_err(|never| match never {})
            .boxed_unsync(),
        )
        .unwrap();
    let response = api
        .dispatcher
        .dispatch(root.child("nested-fixture".into()).unwrap(), nested)
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let headers: Value = serde_json::from_slice(
        &STANDARD
            .decode(value["data"]["body"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert!(
        !supports(&headers),
        "nested HTTP must not gain detached authority"
    );

    // The response has been fully consumed before the delayed step executes.
    // No browser-driven steps; callbacks must still write after the HTTP scope ends.
    let started = invoke(
        &api,
        &cookie,
        &installed.id,
        json!({"operation":"background","limit":3,"delay":100}),
    )
    .await;
    assert_eq!(started["count"], 0);
    assert!(supports(&started["headers"]));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = invoke(
                &api,
                &cookie,
                &installed.id,
                json!({"operation":"state_read"}),
            )
            .await;
            if state["record"]["value"] == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    api.close().await;
    manager.shutdown().await;
    drop(manager);
    env.close().await;
}
