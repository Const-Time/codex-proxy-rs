use super::*;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;
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
