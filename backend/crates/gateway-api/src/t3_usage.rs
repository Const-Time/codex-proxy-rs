//! Read-only subset of the CLIProxyAPI management protocol used by T3's Codex usage reader.

use std::{collections::BTreeSet, fmt, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, State},
    http::{StatusCode, header, request::Parts},
    response::IntoResponse,
    routing::{get, post},
};
use gateway_admin::model::{
    AdminError,
    accounts::CodexUsageAccount,
    provider_credentials::{ProviderQuotaWindowRole, QuotaLocalUsageAttribution},
};
use gateway_core::{
    account::ProviderAccountId,
    routing::{FrozenAccountScope, ProviderKind},
    runtime::ClientAccessGuard,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;

use crate::{
    ApiConfigError, ApiState,
    openai::auth::{ClientApiKeyAuthError, bearer_client_api_key},
};

const CODEX_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const MAX_ACCOUNTS: usize = 128;
const NO_STORE: [(header::HeaderName, &str); 1] = [(header::CACHE_CONTROL, "no-store")];

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct T3UsageConfig {
    pub key_sha256: String,
    pub account_ids: BTreeSet<String>,
}

impl fmt::Debug for T3UsageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("T3UsageConfig")
            .field("account_count", &self.account_ids.len())
            .finish_non_exhaustive()
    }
}

impl T3UsageConfig {
    pub(crate) fn validate(&self) -> Result<(), ApiConfigError> {
        if self.key_sha256.len() != 64
            || !self
                .key_sha256
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            || self.account_ids.is_empty()
            || self.account_ids.len() > MAX_ACCOUNTS
            || self
                .account_ids
                .iter()
                .any(|id| ProviderAccountId::new(id.as_str()).is_err())
        {
            return Err(ApiConfigError::InvalidT3Usage);
        }
        Ok(())
    }
}

pub(crate) fn router() -> Router<ApiState> {
    Router::new()
        .route("/v0/management/auth-files", get(auth_files))
        .route("/v0/management/api-call", post(api_call))
        .layer(DefaultBodyLimit::max(4096))
}

enum UsageAuth {
    Dedicated(Arc<T3UsageConfig>),
    Client {
        scope: Arc<FrozenAccountScope>,
        access: ClientAccessGuard,
    },
}

impl FromRequestParts<ApiState> for UsageAuth {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        let mut values = parts.headers.get_all(header::AUTHORIZATION).iter();
        let (scheme, key) = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if values.next().is_some() || !scheme.eq_ignore_ascii_case("bearer") {
            return Err(StatusCode::UNAUTHORIZED);
        }
        if let Some(config) = &state.t3_usage
            && (32..=256).contains(&key.len())
            && key.bytes().all(|byte| byte.is_ascii_graphic())
        {
            let digest = hex::encode(Sha256::digest(key.as_bytes()));
            if bool::from(digest.as_bytes().ct_eq(config.key_sha256.as_bytes())) {
                return Ok(Self::Dedicated(config.clone()));
            }
        }
        let key = bearer_client_api_key(&parts.headers).map_err(|_| StatusCode::UNAUTHORIZED)?;
        let service = state.openai();
        let client = service
            .authenticate_read_only(key)
            .map_err(|error| match error {
                ClientApiKeyAuthError::RuntimeUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::UNAUTHORIZED,
            })?;
        let access = service
            .client_access(&client)
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let auth = Self::Client {
            scope: client.policy().account_scope().clone(),
            access,
        };
        auth.check_current()?;
        Ok(auth)
    }
}

impl UsageAuth {
    fn candidates(&self) -> Vec<ProviderAccountId> {
        match self {
            Self::Dedicated(config) => config
                .account_ids
                .iter()
                .map(|id| {
                    ProviderAccountId::new(id.as_str()).expect("validated Codex usage account")
                })
                .collect(),
            Self::Client { scope, .. } => {
                scope.account_ids(&ProviderKind::new("openai").expect("valid provider"))
            }
        }
    }

    fn allows(&self, account: &ProviderAccountId) -> bool {
        match self {
            Self::Dedicated(config) => config.account_ids.contains(account.as_str()),
            Self::Client { scope, .. } => scope.allows(account),
        }
    }

    fn check_current(&self) -> Result<(), StatusCode> {
        match self {
            Self::Client { access, .. } if access.cancellation().is_cancelled() => {
                Err(StatusCode::UNAUTHORIZED)
            }
            _ => Ok(()),
        }
    }
}

fn quota_unavailable(error: AdminError) -> StatusCode {
    tracing::warn!(kind = ?error.kind(), "T3 Codex usage snapshot unavailable");
    StatusCode::SERVICE_UNAVAILABLE
}

#[derive(Serialize)]
struct AuthFiles {
    files: Vec<AuthFile>,
}

#[derive(Serialize)]
struct AuthFile {
    id: String,
    auth_index: String,
    provider: &'static str,
}

async fn auth_files(
    auth: UsageAuth,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let files = state
        .admin
        .accounts()
        .codex_usage_account_ids(&auth.candidates())
        .await
        .map_err(quota_unavailable)?
        .into_iter()
        .map(|id| AuthFile {
            auth_index: id.clone(),
            id,
            provider: "codex",
        })
        .collect();
    auth.check_current()?;
    Ok((NO_STORE, Json(AuthFiles { files })))
}

#[derive(Deserialize)]
struct ApiCall {
    auth_index: String,
    method: String,
    url: String,
    data: Option<String>,
}

#[derive(Serialize)]
struct ApiCallResult {
    status_code: u16,
    body: String,
}

async fn api_call(
    auth: UsageAuth,
    State(state): State<ApiState>,
    Json(call): Json<ApiCall>,
) -> Result<impl IntoResponse, StatusCode> {
    // The nested URL is a protocol selector. No caller URL, headers or token
    // placeholder are ever used to build an outbound request.
    if call.method != "GET" || call.url != CODEX_USAGE_URL || call.data.is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    let account_id = ProviderAccountId::new(call.auth_index).map_err(|_| StatusCode::NOT_FOUND)?;
    if !auth.allows(&account_id) {
        return Err(StatusCode::NOT_FOUND);
    }
    let account = state
        .admin
        .accounts()
        .codex_usage_account(&account_id)
        .await
        .map_err(quota_unavailable)?
        .ok_or(StatusCode::NOT_FOUND)?;
    auth.check_current()?;
    let body = serde_json::to_string(&CodexUsage::from(account))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        NO_STORE,
        Json(ApiCallResult {
            status_code: StatusCode::OK.as_u16(),
            body,
        }),
    ))
}

#[derive(Serialize)]
struct CodexUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_type: Option<String>,
    rate_limit: Option<CodexRateLimit>,
}

#[derive(Serialize)]
struct CodexRateLimit {
    primary_window: Option<CodexWindow>,
    secondary_window: Option<CodexWindow>,
}

#[derive(Serialize)]
struct CodexWindow {
    used_percent: f64,
    reset_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_window_seconds: Option<u64>,
}

impl From<CodexUsageAccount> for CodexUsage {
    fn from(account: CodexUsageAccount) -> Self {
        let plan_type = account.quota.plan_type.or(account.plan_type);
        let rate_limit = {
            let window = |role| {
                account.quota.windows.iter().find_map(|window| {
                    if window.role != Some(role)
                        || window.local_usage_attribution != QuotaLocalUsageAttribution::AccountWide
                    {
                        return None;
                    }
                    Some(CodexWindow {
                        used_percent: window.used_percent.filter(|value| value.is_finite())?,
                        reset_at: window.reset_at.map(|time| time.timestamp()),
                        limit_window_seconds: window.window_seconds,
                    })
                })
            };
            let primary_window = window(ProviderQuotaWindowRole::Primary);
            let secondary_window = window(ProviderQuotaWindowRole::Secondary);
            (primary_window.is_some() || secondary_window.is_some()).then_some(CodexRateLimit {
                primary_window,
                secondary_window,
            })
        };
        Self {
            plan_type,
            rate_limit,
        }
    }
}
