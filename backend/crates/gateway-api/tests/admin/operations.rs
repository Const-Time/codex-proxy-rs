use super::{AdminTestFixture, AdminTestState};
use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware,
};
use gateway_api::admin;
use tower::ServiceExt;

fn app(state: AdminTestState) -> Router {
    admin::router::<AdminTestState>()
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin::operations::audit_request::<AdminTestState>,
        ))
        .with_state(state)
}

#[tokio::test]
async fn audit_rejects_untrusted_identity_and_never_records_headers_body_or_query() {
    let fixture = AdminTestFixture::new().await;
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/admin/users/update?password=private-secret")
        .header("authorization", "Bearer private-secret")
        .header("cookie", "cpr_admin_session=private-secret")
        .header("x-request-id", "private-secret")
        .header("x-forwarded-for", "192.0.2.99, private-secret")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"password":"private-secret","email":"attacker@example.com"}"#,
        ))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:1234".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let response = app(fixture.state()).oneshot(request).await.unwrap();
    assert!(!response.status().is_success());
    let events = fixture.auth.operations.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].auth_method, "anonymous");
    assert!(events[0].actor_user_id.is_none());
    assert_eq!(events[0].client_ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(events[0].forwarded_ip.as_deref(), Some("192.0.2.99"));
    assert_eq!(events[0].path, "/api/admin/users/update");
    assert!(uuid::Uuid::parse_str(&events[0].request_id).is_ok());
    let serialized = serde_json::to_string(&*events).unwrap();
    assert!(!serialized.contains("private-secret"));
    assert!(!serialized.contains("attacker@example.com"));
}

#[tokio::test]
async fn audit_records_user_actor_on_logout_and_failed_mutations_but_not_read_only_polls() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("valid-session");
    for (method, path, expected) in [
        ("GET", "/api/profile", StatusCode::OK),
        (
            "POST",
            "/api/admin/client-keys/update",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        ("POST", "/api/admin/auth/logout", StatusCode::OK),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("cookie", "cpr_admin_session=valid-session")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            app(fixture.state())
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    let events = fixture.auth.operations.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|e| e.actor_user_id.as_deref() == Some("admin_1") && e.auth_method == "session")
    );
    assert_eq!(events[0].status, 422);
    assert_eq!(events[1].path, "/api/admin/auth/logout");
}
