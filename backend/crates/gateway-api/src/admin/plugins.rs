//! 所有插件安装和调用均使用现有管理员鉴权；页面由父窗口桥转发，不授予 iframe 管理会话。
use super::{AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminResponse, AdminSessionState};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine as _;
use gateway_admin::model::plugins::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub fn router<S: AdminSessionState + Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/api/admin/plugins", get(list::<S>).post(create::<S>))
        .route(
            "/api/admin/plugins/artifacts",
            get(artifacts::<S>)
                .post(upload::<S>)
                .layer(DefaultBodyLimit::max(32 * 1024 * 1024)),
        )
        .route(
            "/api/admin/plugins/artifacts/remove",
            post(remove_artifact::<S>),
        )
        .route("/api/admin/plugins/update", post(update::<S>))
        .route("/api/admin/plugins/uninstall", post(uninstall::<S>))
        .route("/api/admin/plugins/resource", get(resource::<S>))
        .route("/api/admin/plugins/invoke", post(invoke::<S>))
}
fn error(e: gateway_admin::model::AdminError) -> AdminError {
    super::wire::map_admin_service_error(e)
}
async fn list<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
) -> Result<impl IntoResponse, AdminError> {
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(
            state
                .admin_services()
                .plugins()
                .map_err(error)?
                .list()
                .await
                .map_err(error)?,
        ),
    ))
}
async fn artifacts<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
) -> Result<impl IntoResponse, AdminError> {
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(
            state
                .admin_services()
                .plugins()
                .map_err(error)?
                .artifacts()
                .await
                .map_err(error)?,
        ),
    ))
}
async fn upload<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    body: Bytes,
) -> Result<impl IntoResponse, AdminError> {
    let info = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .upload(Arc::from(body.as_ref()), &auth.context().mutation_context())
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(info)))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactTarget {
    sha256: String,
}
async fn remove_artifact<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<ArtifactTarget>,
) -> Result<impl IntoResponse, AdminError> {
    let services = state.admin_services().clone();
    let actor = auth.context().mutation_context();
    tokio::spawn(async move {
        services
            .plugins()?
            .delete_artifact(&input.sha256, &actor)
            .await
    })
    .await
    .map_err(|_| AdminError::internal())?
    .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(serde_json::json!({})),
    ))
}
async fn create<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<PluginUpdate>,
) -> Result<impl IntoResponse, AdminError> {
    mutate(state, auth, None, input).await
}
#[derive(Deserialize)]
struct Update {
    id: String,
    #[serde(flatten)]
    settings: PluginUpdate,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    id: String,
    revision: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resource {
    id: String,
    revision: u64,
    path: String,
}
async fn update<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<Update>,
) -> Result<impl IntoResponse, AdminError> {
    mutate(state, auth, Some(input.id), input.settings).await
}
async fn mutate<S: AdminSessionState>(
    state: S,
    auth: AdminAuth,
    id: Option<String>,
    input: PluginUpdate,
) -> Result<impl IntoResponse, AdminError> {
    let services = state.admin_services().clone();
    let actor = auth.context().mutation_context();
    // Mutation owns completion even when the HTTP caller disconnects during commit/publication.
    let view = tokio::spawn(async move {
        services
            .plugins()?
            .update(id.as_deref(), input, &actor)
            .await
    })
    .await
    .map_err(|_| AdminError::internal())?
    .map_err(error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(view)))
}
async fn uninstall<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(Target { id, revision }): AdminJson<Target>,
) -> Result<impl IntoResponse, AdminError> {
    let services = state.admin_services().clone();
    let actor = auth.context().mutation_context();
    tokio::spawn(async move { services.plugins()?.delete(&id, revision, &actor).await })
        .await
        .map_err(|_| AdminError::internal())?
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(serde_json::json!({})),
    ))
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Reply {
    status: u16,
    content_type: String,
    body: String,
}
fn reply(response: PluginHttpResponse) -> Reply {
    Reply {
        status: response.status,
        content_type: response.content_type,
        body: base64::engine::general_purpose::STANDARD.encode(response.body),
    }
}
async fn resource<S: AdminSessionState + Send + Sync>(
    _: AdminAuth,
    State(state): State<S>,
    Query(Resource { id, revision, path }): Query<Resource>,
) -> Result<impl IntoResponse, AdminError> {
    let body = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .resource(&id, revision, &path)
        .await
        .map_err(error)?;
    // JSON envelope prevents plugin HTML from becoming an executable same-origin top-level document.
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(reply(body)),
    ))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Invocation {
    id: String,
    revision: u64,
    method: String,
    path: String,
    #[serde(default)]
    query: String,
    content_type: Option<String>,
    #[serde(default)]
    body: String,
}
async fn invoke<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<Invocation>,
) -> Result<impl IntoResponse, AdminError> {
    let body = base64::engine::general_purpose::STANDARD
        .decode(input.body)
        .map_err(|_| AdminError::bad_request("插件正文编码无效"))?;
    let result = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .handle(
            &input.id,
            input.revision,
            PluginHttpRequest {
                invocation: crate::middleware::plugin_invocation(
                    state.admin_services(),
                    &auth.context().mutation_context(),
                ),
                actor: auth.context().mutation_context(),
                method: input.method,
                path: input.path,
                query: input.query,
                content_type: input.content_type,
                body,
            },
        )
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(reply(result)),
    ))
}
