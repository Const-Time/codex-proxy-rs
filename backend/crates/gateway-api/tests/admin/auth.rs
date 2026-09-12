use gateway_admin::model::auth::{LoginCommand, LoginError};
use gateway_api::admin::auth::{
    AdminLoginData, AdminLoginRequest, AdminLogoutData, AdminSessionStatusData,
};
use serde_json::json;

use super::AdminTestFixture;

#[tokio::test]
async fn user_actions_are_admin_only_and_password_modes_are_exclusive() {
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt as _;
    let fixture = AdminTestFixture::new().await;
    fixture
        .auth
        .insert_user_session("ordinary-session", "ordinary");
    fixture.auth.insert_session("admin-session");
    for path in ["delete", "status", "password"] {
        for (cookie, expected) in [
            ("", StatusCode::UNAUTHORIZED),
            ("cpr_admin_session=ordinary-session", StatusCode::FORBIDDEN),
        ] {
            let response = gateway_api::admin::users::router::<super::AdminTestState>()
                .with_state(fixture.state())
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/admin/users/{path}"))
                        .header(header::COOKIE, cookie)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(r#"{"id":"ordinary","reset":true}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
    }
    for body in [
        r#"{"id":"ordinary"}"#,
        r#"{"id":"ordinary","reset":true,"newPassword":"valid-password-123"}"#,
        r#"{"id":"ordinary","newPassword":"short"}"#,
    ] {
        let response = gateway_api::admin::users::router::<super::AdminTestState>()
            .with_state(fixture.state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/users/password")
                    .header(header::COOKIE, "cpr_admin_session=admin-session")
                    .header("x-request-id", "password-validation")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn subscriptions_are_admin_only_and_reset_requires_valid_request_id() {
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt as _;
    let fixture = AdminTestFixture::new().await;
    fixture
        .auth
        .insert_user_session("ordinary-session", "ordinary");
    fixture.auth.insert_session("admin-session");
    for (method, path) in [
        ("GET", "/api/admin/subscriptions"),
        ("POST", "/api/admin/subscriptions/reset"),
    ] {
        let response = gateway_api::admin::users::router::<super::AdminTestState>()
            .with_state(fixture.state())
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(header::COOKIE, "cpr_admin_session=ordinary-session")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"requestId":"00000000-0000-4000-8000-000000000001"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for (body, expected) in [
        (
            r#"{"requestId":"invalid","targets":[]}"#,
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"requestId":"00000000-0000-4000-8000-000000000001"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            r#"{"requestId":"00000000-0000-4000-8000-000000000001","targets":[]}"#,
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = gateway_api::admin::users::router::<super::AdminTestState>()
            .with_state(fixture.state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/subscriptions/reset")
                    .header(header::COOKIE, "cpr_admin_session=admin-session")
                    .header("x-request-id", "subscriptions-validation")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[test]
fn login_request_should_deny_unknown_fields_and_redact_password_debug() {
    let password = "admin-password-must-not-leak";
    let request = serde_json::from_value::<AdminLoginRequest>(json!({
        "username": "admin@example.invalid",
        "password": password
    }))
    .expect("deserialize login request");

    assert!(!format!("{request:?}").contains(password));
    let (username, parsed_password) = request.into_parts();
    assert_eq!(username.as_deref(), Some("admin@example.invalid"));
    assert_eq!(parsed_password, password);
    assert!(
        serde_json::from_value::<AdminLoginRequest>(json!({
            "password": password,
            "rememberMe": true
        }))
        .is_err()
    );
}

#[test]
fn auth_responses_should_keep_stable_wire_shapes() {
    assert_eq!(
        serde_json::to_value(AdminLoginData::new("2026-07-18T08:00:00+08:00".to_owned()))
            .expect("serialize login"),
        json!({ "expiresAt": "2026-07-18T08:00:00+08:00" })
    );
    assert_eq!(
        serde_json::to_value(AdminSessionStatusData::new(true)).expect("serialize status"),
        json!({ "authenticated": true, "user": null })
    );
    assert_eq!(
        serde_json::to_value(AdminLogoutData::new()).expect("serialize logout"),
        json!({ "message": "Logged out successfully" })
    );
}

#[tokio::test]
async fn default_auth_service_should_initialize_login_validate_and_logout() {
    let fixture = AdminTestFixture::new().await;
    let service = fixture.services.auth();
    let session = service
        .login(LoginCommand {
            username: Some("admin@example.com".to_owned()),
            password: "strong-admin-password".to_owned(),
        })
        .await
        .expect("login succeeds");
    assert!(
        service
            .validate_session(Some(&session.session_id))
            .await
            .expect("validate session")
    );
    assert_eq!(
        service
            .resolve_admin_user_id(Some(&session.session_id))
            .await
            .expect("resolve session")
            .as_deref(),
        Some("admin_1")
    );
    service
        .logout(&session.session_id)
        .await
        .expect("logout session");
    assert!(
        !service
            .validate_session(Some(&session.session_id))
            .await
            .expect("validate logged-out session")
    );
    assert_eq!(fixture.auth.audit_count(), 2);
}

#[tokio::test]
async fn default_auth_service_should_verify_only_full_plaintext_admin_key() {
    let fixture = AdminTestFixture::new().await;
    let key = format!("admin-{}", "a".repeat(64));
    fixture.auth.set_api_key(&key);

    assert!(
        fixture
            .services
            .auth()
            .verify_admin_api_key(&key)
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .services
            .auth()
            .verify_admin_api_key("admin-short")
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .services
            .auth()
            .verify_admin_api_key(&format!("admin-{}", "b".repeat(64)))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn audit_failure_should_revoke_new_session_before_returning_it() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.fail_audit(true);

    assert_eq!(
        fixture
            .services
            .auth()
            .login(LoginCommand {
                username: Some("admin@example.com".to_owned()),
                password: "strong-admin-password".to_owned(),
            })
            .await
            .expect_err("audit failure rejects login"),
        LoginError::Unavailable
    );
    assert_eq!(fixture.auth.session_count(), 0);
}
