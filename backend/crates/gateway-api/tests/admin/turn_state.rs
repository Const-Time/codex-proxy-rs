use super::*;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gateway_admin::{
    model::{AdminError, turn_state::*},
    ports::turn_state::TurnStateService,
};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Default)]
struct StateService(Mutex<Vec<(String, bool, bool, String)>>);

fn view() -> TurnStateView {
    TurnStateView {
        revision: 7,
        policy: TurnStatePolicy::default(),
        pools: vec![],
        targets: vec![],
        accounts: vec![],
    }
}

#[async_trait]
impl TurnStateService for StateService {
    async fn view(&self) -> Result<TurnStateView, AdminError> {
        Ok(view())
    }
    async fn configure_account(
        &self,
        id: &str,
        input: TurnStateAccountUpdate,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        if input.revision != 7 {
            return Err(AdminError::conflict("reload"));
        }
        self.0.lock().unwrap().push((
            id.to_owned(),
            input.fingerprint_convergence,
            input.takeover,
            context.request_id.clone(),
        ));
        Ok(view())
    }
    async fn configure(
        &self,
        _: TurnStateSettingsInput,
        _: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        Ok(view())
    }
    async fn action(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &MutationContext,
    ) -> Result<TurnStateView, AdminError> {
        Ok(view())
    }
    async fn test_pool(&self, _: &str) -> Result<TurnStatePoolTest, AdminError> {
        Err(AdminError::not_found("pool"))
    }
}

#[tokio::test]
async fn turn_state_routes_require_admin_and_preserve_cas_and_mutation_context() {
    let mut fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("admin-session");
    fixture
        .auth
        .insert_user_session("ordinary-session", "ordinary");
    let service = Arc::new(StateService::default());
    fixture.services = fixture
        .services
        .clone()
        .with_turn_state(Some(service.clone()));
    let app =
        gateway_api::admin::turn_state::router::<AdminTestState>().with_state(fixture.state());
    let valid =
        json!({"accountId":"acct_a","revision":7,"fingerprintConvergence":true,"takeover":false});
    for (method, path) in [
        ("GET", "/api/admin/turn-state"),
        ("POST", "/api/admin/turn-state"),
        ("POST", "/api/admin/turn-state/account"),
        ("POST", "/api/admin/turn-state/action"),
        ("POST", "/api/admin/turn-state/test-pool"),
    ] {
        for (cookie, status) in [
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
                        .header("x-request-id", "req_state")
                        .header("content-type", "application/json")
                        .body(Body::from(valid.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{method} {path}");
        }
    }
    for (body, content_type, expected) in [
        (valid.clone(), "application/json", StatusCode::OK),
        (
            json!({"accountId":"acct_a","revision":6,"fingerprintConvergence":false,"takeover":true}),
            "application/json",
            StatusCode::CONFLICT,
        ),
        (
            json!({"accountId":"acct_a","revision":7,"fingerprintConvergence":false,"takeover":true,"token":"not accepted"}),
            "application/json",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            valid.clone(),
            "text/plain",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/turn-state/account")
                    .header("cookie", "cpr_admin_session=admin-session")
                    .header("x-request-id", "req_state")
                    .header("content-type", content_type)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await.unwrap())
                .unwrap();
        assert!(!body.to_string().contains("not accepted"));
    }
    assert_eq!(
        *service.0.lock().unwrap(),
        vec![("acct_a".to_owned(), true, false, "req_state".to_owned())]
    );
}
