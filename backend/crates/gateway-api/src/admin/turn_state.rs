//! 仅管理员可读写；复用既有鉴权、JSON 请求约束、错误信封和审计上下文。
use super::{AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminResponse, AdminSessionState};
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use gateway_admin::model::turn_state::{
    TurnStateAccountUpdate, TurnStateMaintenance, TurnStateRecordQuery, TurnStateSettingsInput,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Action {
    account_id: String,
    model: String,
    action: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolId {
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccountUpdate {
    account_id: String,
    revision: u64,
    fingerprint_convergence: bool,
    takeover: bool,
    #[serde(default)]
    maintenance: Option<TurnStateMaintenance>,
}

pub fn router<S: AdminSessionState + Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/api/admin/turn-state", get(view::<S>).post(configure::<S>))
        .route(
            "/api/admin/turn-state/account",
            post(configure_account::<S>),
        )
        .route("/api/admin/turn-state/action", post(action::<S>))
        .route("/api/admin/turn-state/test-pool", post(test_pool::<S>))
        .route("/api/admin/turn-state/records", post(records::<S>))
}

async fn records<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
    AdminJson(query): AdminJson<TurnStateRecordQuery>,
) -> Result<impl IntoResponse, AdminError> {
    query.validate().map_err(error)?;
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .records(query)
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn configure_account<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<AccountUpdate>,
) -> Result<impl IntoResponse, AdminError> {
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .configure_account(
            &input.account_id,
            TurnStateAccountUpdate {
                revision: input.revision,
                fingerprint_convergence: input.fingerprint_convergence,
                takeover: input.takeover,
                maintenance: input.maintenance,
            },
            &auth.context().mutation_context(),
        )
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

fn error(e: gateway_admin::model::AdminError) -> AdminError {
    super::wire::map_admin_service_error(e)
}

async fn view<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
) -> Result<impl IntoResponse, AdminError> {
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .view()
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn configure<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<TurnStateSettingsInput>,
) -> Result<impl IntoResponse, AdminError> {
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .configure(input, &auth.context().mutation_context())
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn action<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<Action>,
) -> Result<impl IntoResponse, AdminError> {
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .action(
            &input.account_id,
            &input.model,
            &input.action,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn test_pool<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<PoolId>,
) -> Result<impl IntoResponse, AdminError> {
    let result = state
        .admin_services()
        .turn_state()
        .map_err(error)?
        .test_pool(&input.id)
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}
