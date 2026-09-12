//! Administrator-managed users and personal profile endpoints.

use super::{
    AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminResponse, AdminSessionState,
    auth::UserAuth, wire::map_admin_service_error,
};
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use gateway_admin::model::users::{CreateUser, UpdateUser, UserRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserView {
    id: String,
    username: String,
    role: &'static str,
    enabled: bool,
    group_ids: Vec<String>,
    quota_multipliers: std::collections::BTreeMap<String, String>,
    max_concurrency: u64,
    requests_per_minute: u64,
    created_at: String,
    updated_at: String,
}

impl From<UserRecord> for UserView {
    fn from(user: UserRecord) -> Self {
        Self {
            id: user.id,
            username: user.username,
            role: user.role.as_str(),
            enabled: user.enabled,
            max_concurrency: user.limits.max_concurrency,
            requests_per_minute: user.limits.requests_per_minute,
            group_ids: user.group_ids,
            quota_multipliers: user.quota_multipliers,
            created_at: user.created_at.to_rfc3339(),
            updated_at: user.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateUserRequest {
    #[serde(default)]
    max_concurrency: u64,
    #[serde(default)]
    requests_per_minute: u64,
    username: String,
    password: String,
    #[serde(default)]
    group_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateUserRequest {
    #[serde(default)]
    quota_multipliers: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    max_concurrency: u64,
    #[serde(default)]
    requests_per_minute: u64,
    id: String,
    enabled: bool,
    group_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

pub fn router<S>() -> Router<S>
where
    S: AdminSessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/admin/users", get(list::<S>))
        .route("/api/admin/subscriptions", get(subscriptions::<S>))
        .route(
            "/api/admin/subscriptions/reset",
            post(reset_subscriptions::<S>),
        )
        .route("/api/admin/users/create", post(create::<S>))
        .route("/api/admin/users/update", post(update::<S>))
        .route("/api/profile", get(profile::<S>))
        .route("/api/profile/groups", get(groups::<S>))
        .route("/api/profile/password", post(change_password::<S>))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionView {
    user_id: String,
    username: String,
    enabled: bool,
    group_id: String,
    group_name: String,
    quota_multiplier: String,
    daily_limit_usd: String,
    weekly_limit_usd: String,
    daily_used_usd: String,
    weekly_used_usd: String,
    daily_resets_at: Option<chrono::DateTime<chrono::Utc>>,
    weekly_resets_at: Option<chrono::DateTime<chrono::Utc>>,
    last_reset_at: Option<chrono::DateTime<chrono::Utc>>,
    last_reset_reason: Option<String>,
}

async fn subscriptions<S>(
    State(state): State<S>,
    _auth: AdminAuth,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let data = state
        .admin_services()
        .auth()
        .subscriptions()
        .await
        .map_err(map_admin_service_error)?;
    let data = data
        .into_iter()
        .map(|s| SubscriptionView {
            user_id: s.user_id,
            username: s.username,
            enabled: s.enabled,
            group_id: s.group_id,
            group_name: s.group_name,
            quota_multiplier: s.quota_multiplier,
            daily_limit_usd: s.daily_limit_usd,
            weekly_limit_usd: s.weekly_limit_usd,
            daily_used_usd: s.daily_used_usd,
            weekly_used_usd: s.weekly_used_usd,
            daily_resets_at: s.daily_resets_at,
            weekly_resets_at: s.weekly_resets_at,
            last_reset_at: s.last_reset_at,
            last_reset_reason: s.last_reset_reason,
        })
        .collect::<Vec<_>>();
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResetSubscriptionsRequest {
    request_id: String,
    targets: Vec<gateway_admin::model::users::SubscriptionTarget>,
}

async fn reset_subscriptions<S>(
    State(state): State<S>,
    auth: AdminAuth,
    AdminJson(body): AdminJson<ResetSubscriptionsRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let request_id = uuid::Uuid::parse_str(&body.request_id)
        .map_err(|_| AdminError::bad_request("requestId 必须是 UUID"))?;
    let count = state
        .admin_services()
        .auth()
        .reset_subscriptions(
            request_id,
            &body.targets,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(count)))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserGroupView {
    id: String,
    name: String,
    color: String,
    enabled: bool,
    daily_limit_usd: String,
    weekly_limit_usd: String,
    daily_used_usd: String,
    weekly_used_usd: String,
    daily_resets_at: Option<chrono::DateTime<chrono::Utc>>,
    weekly_resets_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn groups<S>(State(state): State<S>, auth: UserAuth) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let groups = state
        .admin_services()
        .auth()
        .user_groups(&auth.user.id)
        .await
        .map_err(map_admin_service_error)?;
    let data = groups
        .into_iter()
        .map(|group| UserGroupView {
            id: group.id,
            name: group.name,
            color: group.color,
            enabled: group.enabled,
            daily_limit_usd: group.budget.limits.daily_usd.canonical(),
            weekly_limit_usd: group.budget.limits.weekly_usd.canonical(),
            daily_used_usd: group.budget.daily_used_usd.canonical(),
            weekly_used_usd: group.budget.weekly_used_usd.canonical(),
            daily_resets_at: group.budget.daily_resets_at.map(Into::into),
            weekly_resets_at: group.budget.weekly_resets_at.map(Into::into),
        })
        .collect::<Vec<_>>();
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

async fn list<S>(State(state): State<S>, _auth: AdminAuth) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let users = state
        .admin_services()
        .auth()
        .list_users()
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(users.into_iter().map(UserView::from).collect::<Vec<_>>()),
    ))
}

async fn create<S>(
    State(state): State<S>,
    auth: AdminAuth,
    AdminJson(body): AdminJson<CreateUserRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let user = state
        .admin_services()
        .auth()
        .create_user(
            CreateUser {
                username: body.username,
                password: body.password,
                group_ids: body.group_ids,
                limits: gateway_core::policy::RateLimits {
                    max_concurrency: body.max_concurrency,
                    requests_per_minute: body.requests_per_minute,
                },
            },
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::CREATED,
        AdminEnvelope::ok(UserView::from(user)),
    ))
}

async fn update<S>(
    State(state): State<S>,
    auth: AdminAuth,
    AdminJson(body): AdminJson<UpdateUserRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let user = state
        .admin_services()
        .auth()
        .update_user(
            UpdateUser {
                quota_multipliers: body.quota_multipliers,
                id: body.id,
                enabled: body.enabled,
                group_ids: body.group_ids,
                limits: gateway_core::policy::RateLimits {
                    max_concurrency: body.max_concurrency,
                    requests_per_minute: body.requests_per_minute,
                },
            },
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(UserView::from(user)),
    ))
}

async fn profile<S>(State(_state): State<S>, auth: UserAuth) -> impl IntoResponse
where
    S: AdminSessionState + Send + Sync,
{
    AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(UserView::from(auth.user)))
}

async fn change_password<S>(
    State(state): State<S>,
    auth: UserAuth,
    AdminJson(body): AdminJson<ChangePasswordRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    state
        .admin_services()
        .auth()
        .change_password(
            &auth.user,
            &body.current_password,
            &body.new_password,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(())))
}
