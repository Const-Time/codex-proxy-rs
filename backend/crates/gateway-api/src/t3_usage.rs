//! Read-only subset of the CLIProxyAPI management protocol used by T3's Codex usage reader.

use std::{collections::BTreeSet, fmt, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, State},
    http::{StatusCode, header, request::Parts},
    response::IntoResponse,
    routing::{get, post},
};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use gateway_admin::{
    AdminServices,
    model::{
        accounts::CodexUsageAccount,
        provider_credentials::{ProviderQuotaWindowRole, QuotaLocalUsageAttribution},
    },
};
use gateway_core::account::ProviderAccountId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;

use crate::{ApiConfigError, ApiState};

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

#[derive(Clone)]
struct UsageState {
    config: Arc<T3UsageConfig>,
    admin: AdminServices,
}

pub(crate) fn router(config: T3UsageConfig, admin: AdminServices) -> Router<ApiState> {
    Router::new()
        .route("/v0/management/auth-files", get(auth_files))
        .route("/v0/management/api-call", post(api_call))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(UsageState {
            config: Arc::new(config),
            admin,
        })
}

struct UsageAuth;

impl FromRequestParts<UsageState> for UsageAuth {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &UsageState,
    ) -> Result<Self, Self::Rejection> {
        let mut values = parts.headers.get_all(header::AUTHORIZATION).iter();
        let (scheme, key) = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if values.next().is_some()
            || !scheme.eq_ignore_ascii_case("bearer")
            || !(32..=256).contains(&key.len())
            || key.bytes().any(|byte| !byte.is_ascii_graphic())
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let digest = hex::encode(Sha256::digest(key.as_bytes()));
        if bool::from(digest.as_bytes().ct_eq(state.config.key_sha256.as_bytes())) {
            Ok(Self)
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

impl UsageState {
    async fn account(
        &self,
        id: &str,
        include_quota: bool,
    ) -> Result<Option<CodexUsageAccount>, StatusCode> {
        let account_id = ProviderAccountId::new(id).map_err(|_| StatusCode::NOT_FOUND)?;
        self.admin
            .accounts()
            .codex_usage_account(&account_id, include_quota)
            .await
            .map_err(|error| {
                tracing::warn!(kind = ?error.kind(), "T3 Codex usage snapshot unavailable");
                StatusCode::SERVICE_UNAVAILABLE
            })
    }
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
    _auth: UsageAuth,
    State(state): State<UsageState>,
) -> Result<impl IntoResponse, StatusCode> {
    let state = &state;
    let files = stream::iter(state.config.account_ids.iter().cloned())
        .map(|id| async move {
            Ok::<_, StatusCode>(state.account(&id, false).await?.map(|_| AuthFile {
                auth_index: id.clone(),
                id,
                provider: "codex",
            }))
        })
        .buffered(8)
        .try_filter_map(|account| async { Ok(account) })
        .try_collect()
        .await?;
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
    _auth: UsageAuth,
    State(state): State<UsageState>,
    Json(call): Json<ApiCall>,
) -> Result<impl IntoResponse, StatusCode> {
    // The nested URL is a protocol selector. No caller URL, headers or token
    // placeholder are ever used to build an outbound request.
    if call.method != "GET" || call.url != CODEX_USAGE_URL || call.data.is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    if !state.config.account_ids.contains(&call.auth_index) {
        return Err(StatusCode::NOT_FOUND);
    }
    let account = state
        .account(&call.auth_index, true)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
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
        let plan_type = account
            .quota
            .as_ref()
            .and_then(|quota| quota.plan_type.clone())
            .or(account.plan_type);
        let rate_limit = account.quota.and_then(|quota| {
            let window = |role| {
                quota.windows.iter().find_map(|window| {
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
        });
        Self {
            plan_type,
            rate_limit,
        }
    }
}
