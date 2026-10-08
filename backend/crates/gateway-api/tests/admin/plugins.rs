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
