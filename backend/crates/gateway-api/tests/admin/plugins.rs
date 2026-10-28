use super::*;

#[tokio::test]
async fn online_plugin_updates_require_admin_and_explicit_trust_and_reject_forged_fields() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    let service = Arc::new(super::plugin_models::Plugins(
        gateway_admin::model::plugins::PluginAccess::default(),
    ));
    let state = AdminTestState(fixture.services.clone().with_plugins(service));
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(state);
    for path in ["check-update", "install-update"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/admin/plugins/{path}"))
                    .header("x-request-id", "online-update-auth-test")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"plugin","revision":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    for (path, body, expected) in [
        (
            "check-update",
            serde_json::json!({"id":"plugin","revision":1,"url":"http://127.0.0.1/private"}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "install-update",
            serde_json::json!({"id":"plugin","revision":1,"trustedProcess":false,"release":{"version":"2.0.0","sha256":"a".repeat(64),"assetName":"fixture.tar.gz","size":1,"releaseUrl":"https://github.com/example"}}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/admin/plugins/{path}"))
                    .header("x-request-id", "online-update-input-test")
                    .header("content-type", "application/json")
                    .header("cookie", "cpr_admin_session=admin-session")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
async fn plugin_management_login_tickets_require_admin_and_callbacks_are_bounded_public_documents()
{
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    let service = Arc::new(super::plugin_models::Plugins(
        gateway_admin::model::plugins::PluginAccess::default(),
    ));
    let state = AdminTestState(fixture.services.clone().with_plugins(service));
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(state);
    let ticket_request = |session: bool, forged: bool| {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/api/admin/plugins/callback-ticket")
            .header("content-type", "application/json")
            .header("x-request-id", "ticket-fixture");
        if session {
            builder = builder.header("cookie", "cpr_admin_session=admin-session");
        }
        let mut body =
            serde_json::json!({"id":"plugin","revision":1,"path":"oauth","ttlSeconds":60});
        if forged {
            body["actor"] = serde_json::json!({"admin_user_id":"forged"});
        }
        builder
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(ticket_request(false, false))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(ticket_request(true, true))
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let response = app
        .clone()
        .oneshot(ticket_request(true, false))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let envelope: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(envelope["data"]["state"], "opaque-ticket");
    assert_eq!(envelope["data"]["expiresAtMs"], 123);
    let callback = |query: &str, method: &str, content_type: bool, body: &'static str| {
        let mut builder = Request::builder().method(method).uri(format!(
            "/api/plugins/callback?id=plugin&revision=1&path=oauth&{query}"
        ));
        if content_type {
            builder = builder.header("content-type", "text/plain");
        }
        builder.body(Body::from(body)).unwrap()
    };
    let response = app
        .clone()
        .oneshot(callback(
            "state=opaque-ticket&code=synthetic",
            "GET",
            false,
            "",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/plain");
    assert_eq!(
        response.headers()["content-security-policy"],
        "sandbox allow-scripts allow-popups"
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "no-referrer");
    assert_eq!(response.headers().get_all("x-callback").iter().count(), 2);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap(),
        "id=plugin&revision=1&path=oauth&state=opaque-ticket&code=synthetic"
    );
    for (query, method, content_type, body) in [
        ("code=synthetic", "GET", false, ""),
        ("state=opaque-ticket&state=forged", "GET", false, ""),
        ("state=opaque-ticket", "HEAD", false, ""),
        ("state=opaque-ticket", "GET", true, ""),
        ("state=opaque-ticket", "GET", false, "body"),
    ] {
        assert!(
            app.clone()
                .oneshot(callback(query, method, content_type, body))
                .await
                .unwrap()
                .status()
                .is_client_error()
        );
    }
}
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;
#[tokio::test]
async fn public_plugin_resources_need_no_session_and_remain_json_with_revision_checks() {
    use base64::Engine as _;
    let fixture = AdminTestFixture::new().await;
    let access = gateway_admin::model::plugins::PluginAccess::default();
    let service = Arc::new(super::plugin_models::Plugins(access.clone()));
    let state = AdminTestState(fixture.services.clone().with_plugins(service));
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(state);
    let request = |path: &str| {
        Request::builder()
            .uri(format!(
                "/api/plugins/resource?id=plugin&revision=1&path={path}"
            ))
            .body(Body::empty())
            .unwrap()
    };
    let response = app.clone().oneshot(request("public.html")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let envelope: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(envelope["data"]["contentType"], "text/html");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(envelope["data"]["body"].as_str().unwrap())
            .unwrap(),
        b"<script>fixture-public-resource</script>"
    );
    for path in ["private.html", "../plugin.json"] {
        assert_eq!(
            app.clone().oneshot(request(path)).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/plugins/resource?id=plugin&revision=2&path=public.html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    access.revoke();
    assert_eq!(
        app.oneshot(request("public.html")).await.unwrap().status(),
        StatusCode::CONFLICT
    );
}
#[tokio::test]
async fn plugin_install_configuration_resources_and_calls_all_require_admin_identity() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    fixture
        .auth
        .insert_user_session("ordinary-session", "ordinary");
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(fixture.state());
    for (method, path) in [
        ("GET", "/api/admin/plugins"),
        ("POST", "/api/admin/plugins"),
        ("GET", "/api/admin/plugins/artifacts"),
        ("POST", "/api/admin/plugins/artifacts"),
        ("POST", "/api/admin/plugins/artifacts/remove"),
        ("POST", "/api/admin/plugins/update"),
        ("POST", "/api/admin/plugins/uninstall"),
        (
            "GET",
            "/api/admin/plugins/resource?id=instance&revision=1&path=index.html",
        ),
        ("POST", "/api/admin/plugins/invoke"),
    ] {
        for (cookie, expected) in [
            ("", StatusCode::UNAUTHORIZED),
            ("cpr_admin_session=ordinary-session", StatusCode::FORBIDDEN),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("cookie", cookie)
                        .header("x-request-id", "plugin-auth-test")
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{method} {path}");
        }
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/admin/plugins")
                .header("cookie", "cpr_admin_session=admin-session")
                .header("x-request-id", "plugin-auth-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "missing runtime must never present an empty successful plugin manager"
    );
}

#[tokio::test]
async fn static_plugin_routes_decode_valid_admin_requests_before_service_dispatch() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(fixture.state());
    let settings = serde_json::json!({"revision":0,"name":"fixture","artifactSha256":"a".repeat(64),"enabled":false,"configuration":{},"secrets":{}});
    let mut update = settings.clone();
    update["id"] = serde_json::json!("fixture");
    for (method, path, body) in [
        ("POST", "/api/admin/plugins", settings),
        ("POST", "/api/admin/plugins/update", update),
        (
            "POST",
            "/api/admin/plugins/uninstall",
            serde_json::json!({"id":"fixture","revision":1}),
        ),
        (
            "POST",
            "/api/admin/plugins/artifacts/remove",
            serde_json::json!({"sha256":"a".repeat(64)}),
        ),
        (
            "GET",
            "/api/admin/plugins/resource?id=fixture&revision=1&path=index.html",
            serde_json::json!({}),
        ),
        (
            "POST",
            "/api/admin/plugins/invoke",
            serde_json::json!({"id":"fixture","revision":1,"method":"POST","path":"echo","contentType":"application/json","body":"e30="}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("cookie", "cpr_admin_session=admin-session")
                    .header("x-request-id", "plugin-wire-test")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "valid request must reach unconfigured service: {method} {path}"
        );
    }
}

#[tokio::test]
async fn plugin_management_mutation_identity_comes_from_auth_not_plugin_payload() {
    use base64::Engine as _;
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    let service = Arc::new(super::plugin_models::Plugins(
        gateway_admin::model::plugins::PluginAccess::default(),
    ));
    let state = AdminTestState(fixture.services.clone().with_plugins(service));
    let app = gateway_api::admin::plugins::router::<AdminTestState>().with_state(state);
    let request = |body: serde_json::Value| {
        Request::builder()
            .method("POST")
            .uri("/api/admin/plugins/invoke")
            .header("cookie", "cpr_admin_session=admin-session")
            .header("x-request-id", "plugin-actor-test")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    };
    let body = serde_json::json!({"id":"plugin","revision":1,"method":"POST","path":"echo","contentType":"application/json","body":base64::engine::general_purpose::STANDARD.encode(br#"{"actor":"forged"}"#)});
    let response = app.clone().oneshot(request(body.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let envelope: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(envelope["data"]["body"].as_str().unwrap())
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(
        result["actor"],
        "AdminSession { admin_user_id: \"admin_1\" }"
    );
    assert_eq!(result["requestId"], "plugin-actor-test");
    let mut forged = body;
    forged["actor"] = serde_json::json!({"admin_user_id":"forged"});
    assert_eq!(
        app.oneshot(request(forged)).await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}
