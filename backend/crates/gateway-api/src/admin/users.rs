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
            group_ids: user.group_ids,
            created_at: user.created_at.to_rfc3339(),
            updated_at: user.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateUserRequest {
    username: String,
    password: String,
    #[serde(default)]
    group_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateUserRequest {
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
        .route("/api/admin/users/create", post(create::<S>))
        .route("/api/admin/users/update", post(update::<S>))
        .route("/api/profile", get(profile::<S>))
        .route("/api/profile/groups", get(groups::<S>))
        .route("/api/profile/password", post(change_password::<S>))
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
                id: body.id,
                enabled: body.enabled,
                group_ids: body.group_ids,
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
