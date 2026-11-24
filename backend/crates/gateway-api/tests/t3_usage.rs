use std::{sync::Arc, time::SystemTime};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
    response::Response,
};
use chrono::{DateTime, Utc};
use gateway_admin::model::{
    Revision,
    accounts::{
        AccountPageItem, AccountRecord, AccountStatusFacts, CredentialState, QuotaState,
        resolve_account_status,
    },
    provider_credentials::{
        ProviderQuota, ProviderQuotaWindow, ProviderQuotaWindowRole, QuotaLocalUsageAttribution,
    },
};
use gateway_api::{ApiConfig, T3UsageConfig};
use gateway_core::routing::ProviderKind;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;

use crate::{
    admin::AdminTestFixture,
    health::UnusedExecution,
    openai::{EmptyWorkerHealth, TestLifecycle},
};

const ACCOUNT: &str = "acct_t3_fixture";
const KEY: &str = "t3-local-test-only-credential-000001";
const URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const LIST: &str = "/v0/management/auth-files";
const CALL: &str = "/v0/management/api-call";

#[tokio::test]
async fn t3_reads_account_wide_codex_windows_in_the_cliproxy_wire_format() {
    let fixture = fixture().await;
    let router = router(&fixture, Some(config()));
    let listed = request(&router, LIST, Some(KEY), None).await;
    assert_eq!(listed.status(), StatusCode::OK);
    assert_eq!(listed.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        body(listed).await,
        json!({"files": [{"id": ACCOUNT, "auth_index": ACCOUNT, "provider": "codex"}]})
    );

    let result = quota(&router).await;
    assert_eq!(result["status_code"], 200);
    let usage: Value = serde_json::from_str(result["body"].as_str().unwrap()).unwrap();
    assert_eq!(
        usage,
        json!({
            "plan_type": "pro",
            "rate_limit": {
                "primary_window": { "used_percent": 12.5, "reset_at": 1795500000, "limit_window_seconds": 18000 },
                "secondary_window": { "used_percent": 64.0, "reset_at": 1796050000, "limit_window_seconds": 604800 },
            }
        })
    );

    // Missing usage is distinct from a known zero-percent window.
    let stored = fixture.stored_quota.lock().unwrap().clone().unwrap();
    *fixture.stored_quota.lock().unwrap() = Some(ProviderQuota {
        windows: Vec::new(),
        plan_type: None,
        ..stored
    });
    let result = quota(&router).await;
    let usage: Value = serde_json::from_str(result["body"].as_str().unwrap()).unwrap();
    assert_eq!(usage, json!({"plan_type": "plus", "rate_limit": null}));
}

#[tokio::test]
async fn t3_access_is_scoped_to_the_read_only_key_and_eligible_allowlisted_accounts() {
    let fixture = fixture().await;
    let router = router(&fixture, Some(config()));
    fixture
        .auth
        .set_api_key("fixture-admin-credential-0000000001");
    for key in [None, Some("fixture-admin-credential-0000000001")] {
        assert_eq!(
            request(&router, LIST, key, None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request(&router, CALL, key, Some(call(ACCOUNT, "GET", URL)))
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        request(&router, "/api/admin/accounts", Some(KEY), None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    // The excluded ID exists and is otherwise eligible.
    fixture.account.lock().unwrap().as_mut().unwrap().account.id = "acct_excluded".to_owned();
    assert_eq!(
        request(
            &router,
            CALL,
            Some(KEY),
            Some(call("acct_excluded", "GET", URL))
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        body(request(&router, LIST, Some(KEY), None).await).await,
        json!({"files": []})
    );
    fixture.account.lock().unwrap().as_mut().unwrap().account.id = ACCOUNT.to_owned();
    for (method, url) in [
        ("POST", URL),
        ("GET", "https://example.invalid/quota"),
        (
            "GET",
            "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits",
        ),
    ] {
        assert_eq!(
            request(&router, CALL, Some(KEY), Some(call(ACCOUNT, method, url)))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    for (provider, authentication, enabled) in [
        ("openai", "oauth", false),
        ("openai", "api_key", true),
        ("xai", "oauth", true),
    ] {
        {
            let mut item = fixture.account.lock().unwrap();
            let account = &mut item.as_mut().unwrap().account;
            account.provider_kind = ProviderKind::new(provider).unwrap();
            account.authentication_kind = authentication.to_owned();
            account.enabled = enabled;
        }
        assert_eq!(
            body(request(&router, LIST, Some(KEY), None).await).await,
            json!({"files": []})
        );
        assert_eq!(
            request(&router, CALL, Some(KEY), Some(call(ACCOUNT, "GET", URL)))
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn t3_routes_are_opt_in_and_require_a_digest_and_explicit_account_scope() {
    let fixture = fixture().await;
    let router = router(&fixture, None);
    assert_eq!(
        request(&router, LIST, Some(KEY), None).await.status(),
        StatusCode::NOT_FOUND
    );
    let mut config = config();
    config.account_ids.clear();
    assert!(matches!(
        initialize(&fixture, Some(config)),
        Err(gateway_api::ApiError::Config(
            gateway_api::ApiConfigError::InvalidT3Usage
        ))
    ));
}

fn config() -> T3UsageConfig {
    T3UsageConfig {
        key_sha256: hex::encode(Sha256::digest(KEY)),
        account_ids: [ACCOUNT.to_owned(), "acct_missing".to_owned()].into(),
    }
}

fn initialize(
    fixture: &AdminTestFixture,
    t3_usage: Option<T3UsageConfig>,
) -> Result<gateway_api::ApiBundle, gateway_api::ApiError> {
    gateway_api::initialize(
        ApiConfig {
            ingress: Default::default(),
            asset_directory: std::env::temp_dir().join("cpr-t3-fixture-no-assets"),
            cors_allowed_origins: Vec::new(),
            request_timeout_seconds: None,
            request_id_header: "x-request-id".into(),
            t3_usage,
        },
        Arc::new(UnusedExecution),
        fixture.services.clone(),
        Vec::new(),
        Arc::new(EmptyWorkerHealth),
        Arc::new(TestLifecycle::default()),
    )
}

fn router(fixture: &AdminTestFixture, t3_usage: Option<T3UsageConfig>) -> Router {
    initialize(fixture, t3_usage).unwrap().router()
}

async fn request(router: &Router, path: &str, key: Option<&str>, value: Option<Value>) -> Response {
    let mut request = Request::builder().uri(path);
    if let Some(key) = key {
        request = request.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let request = if let Some(value) = value {
        request
            .method(Method::POST)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
    } else {
        request.body(Body::empty())
    };
    router.clone().oneshot(request.unwrap()).await.unwrap()
}

fn call(account: &str, method: &str, url: &str) -> Value {
    json!({"auth_index": account, "method": method, "url": url, "header": { "Authorization": "Bearer $TOKEN$" } })
}

async fn quota(router: &Router) -> Value {
    let response = request(router, CALL, Some(KEY), Some(call(ACCOUNT, "GET", URL))).await;
    assert_eq!(response.status(), StatusCode::OK);
    body(response).await
}

async fn body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap()
}

async fn fixture() -> AdminTestFixture {
    let fixture = AdminTestFixture::new().await;
    let now = Utc::now();
    let facts = AccountStatusFacts {
        enabled: true,
        credential_state: CredentialState::Ready,
        access_token_expires_at: None,
        quota: QuotaState::unknown(),
        rate_limited_until: None,
        last_error_reason: None,
        last_error_message: None,
    };
    *fixture.account.lock().unwrap() = Some(AccountPageItem {
        account: AccountRecord {
            dispatch_hours: Default::default(),
            quota_usage_limits: Default::default(),
            auto_use_quota_credits: false,
            id: ACCOUNT.to_owned(),
            provider_kind: ProviderKind::new("openai").unwrap(),
            groups: Vec::new(),
            name: "synthetic account".to_owned(),
            email: Some("private@example.test".to_owned()),
            upstream_user_id: Some("private-user-id".to_owned()),
            upstream_account_id: Some("private-account-id".to_owned()),
            plan_type: Some("plus".to_owned()),
            authentication_kind: "oauth".to_owned(),
            credential_revision: Revision::new(1).unwrap(),
            has_refresh_token: true,
            access_token_expires_at: None,
            next_refresh_at: None,
            enabled: facts.enabled,
            concurrency_limit: None,
            weight: Default::default(),
            model_access: Default::default(),
            outbound_proxy: None,
            credential_state: facts.credential_state,
            credential_observed_at: now,
            quota: facts.quota,
            last_error_reason: None,
            last_error_message: None,
            created_at: now,
            updated_at: now,
        },
        projection: resolve_account_status(&facts, SystemTime::now()),
    });
    let window =
        |role: ProviderQuotaWindowRole, percent, seconds, reset, attribution| ProviderQuotaWindow {
            key: role.as_str().to_owned(),
            group: "general".to_owned(),
            label: "synthetic window".to_owned(),
            limit_id: None,
            limit_name: None,
            role: Some(role),
            local_usage_attribution: attribution,
            estimated_quota: None,
            estimate_hint: None,
            window_seconds: Some(seconds),
            used_percent: Some(percent),
            reset_at: DateTime::from_timestamp(reset, 0),
            limit_reached: false,
            local_usage: None,
            provider_data: None,
        };
    *fixture.stored_quota.lock().unwrap() = Some(ProviderQuota {
        plan_type: Some("pro".to_owned()),
        observed_at: Some(now),
        refresh_token_expires_at: None,
        windows: vec![
            window(
                ProviderQuotaWindowRole::Primary,
                90.0,
                18000,
                1795500000,
                QuotaLocalUsageAttribution::Unavailable,
            ),
            window(
                ProviderQuotaWindowRole::Primary,
                12.5,
                18000,
                1795500000,
                QuotaLocalUsageAttribution::AccountWide,
            ),
            window(
                ProviderQuotaWindowRole::Secondary,
                64.0,
                604800,
                1796050000,
                QuotaLocalUsageAttribution::AccountWide,
            ),
        ],
        credits: None,
        limit_reached: false,
        provider_data: None,
    });
    fixture
}
