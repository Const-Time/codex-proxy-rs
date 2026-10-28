//! 插件管理使用现有管理员鉴权；公开资源只读取当前代次的显式声明。
use super::{AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminResponse, AdminSessionState};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
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
        .route("/api/admin/plugins/check-update", post(check_update::<S>))
        .route(
            "/api/admin/plugins/install-update",
            post(install_update::<S>),
        )
        .route("/api/admin/plugins/uninstall", post(uninstall::<S>))
        .route("/api/admin/plugins/resource", get(resource::<S>))
        .route("/api/plugins/resource", get(public_resource::<S>))
        .route("/api/admin/plugins/invoke", post(invoke::<S>))
        .route(
            "/api/admin/plugins/callback-ticket",
            post(callback_ticket::<S>),
        )
        .route(
            "/api/plugins/callback",
            get(callback::<S>).layer(DefaultBodyLimit::max(0)),
        )
}
fn error(e: gateway_admin::model::AdminError) -> AdminError {
    super::wire::map_admin_service_error(e)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CallbackTicket {
    id: String,
    revision: u64,
    path: String,
    ttl_seconds: u32,
}
async fn callback_ticket<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<CallbackTicket>,
) -> Result<impl IntoResponse, AdminError> {
    let ticket = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .callback_ticket(
            &input.id,
            input.revision,
            &input.path,
            input.ttl_seconds,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(ticket),
    ))
}
#[derive(Deserialize)]
struct CallbackTarget {
    id: String,
    revision: u64,
    path: String,
}
async fn callback<S: AdminSessionState + Send + Sync>(
    State(state): State<S>,
    Query(CallbackTarget { id, revision, path }): Query<CallbackTarget>,
    RawQuery(query): RawQuery,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Result<axum::response::Response, AdminError> {
    let query = query.unwrap_or_default();
    if method != Method::GET
        || !body.is_empty()
        || headers.contains_key("content-type")
        || query.len() > 8192
        || query.bytes().any(|b| b.is_ascii_control())
    {
        return Err(AdminError::bad_request("插件登录回调只接受有界 GET 查询"));
    }
    let states = url::form_urlencoded::parse(query.as_bytes())
        .filter(|(name, _)| name == "state")
        .map(|(_, value)| value.into_owned())
        .collect::<Vec<_>>();
    if states.len() != 1 {
        return Err(AdminError::bad_request("插件登录票据缺失或重复"));
    }
    let origin = crate::middleware::current();
    let request_id = uuid::Uuid::now_v7().simple().to_string();
    let services = state.admin_services().clone();
    let invocation_services = services.clone();
    let response = services
        .plugins()
        .map_err(error)?
        .callback(
            &id,
            revision,
            &path,
            &states[0],
            PluginCallbackRequest {
                request_id,
                query,
                headers: headers
                    .iter()
                    .map(|(name, value)| {
                        gateway_core::engine::middleware::MiddlewareHeader::new(
                            name.as_str(),
                            value.as_bytes().to_vec().into(),
                        )
                    })
                    .collect(),
                invocation: Arc::new(move |actor| {
                    crate::middleware::plugin_invocation_from(
                        origin.clone(),
                        &invocation_services,
                        actor,
                    )
                }),
            },
        )
        .await
        .map_err(error)?;
    let mut output = (
        StatusCode::from_u16(response.response.status).map_err(|_| AdminError::internal())?,
        response.response.body,
    )
        .into_response();
    output.headers_mut().insert(
        "content-type",
        response
            .response
            .content_type
            .parse()
            .map_err(|_| AdminError::internal())?,
    );
    let mut replaced = std::collections::HashSet::new();
    for header in response.headers {
        let name = axum::http::HeaderName::from_bytes(header.name().as_bytes())
            .map_err(|_| AdminError::internal())?;
        let value = axum::http::HeaderValue::from_bytes(header.value())
            .map_err(|_| AdminError::internal())?;
        if replaced.insert(name.clone()) {
            output.headers_mut().remove(&name);
        }
        output.headers_mut().append(name, value);
    }
    // A callback is a public plugin document, never an administrator same-origin page.
    output.headers_mut().insert(
        "content-security-policy",
        "sandbox allow-scripts allow-popups"
            .parse()
            .expect("constant CSP"),
    );
    output.headers_mut().insert(
        "cache-control",
        "no-store".parse().expect("constant cache policy"),
    );
    output.headers_mut().insert(
        "referrer-policy",
        "no-referrer".parse().expect("constant referrer policy"),
    );
    output.headers_mut().insert(
        "x-content-type-options",
        "nosniff".parse().expect("constant content policy"),
    );
    Ok(output)
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
async fn check_update<S: AdminSessionState + Send + Sync>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<Target>,
) -> Result<impl IntoResponse, AdminError> {
    let release = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .check_update(&input.id, input.revision)
        .await
        .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(release),
    ))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstallUpdate {
    id: String,
    revision: u64,
    release: PluginOnlineRelease,
    trusted_process: bool,
}
async fn install_update<S: AdminSessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<InstallUpdate>,
) -> Result<impl IntoResponse, AdminError> {
    if !input.trusted_process {
        return Err(error(gateway_admin::model::AdminError::invalid(
            "更新前必须确认可信插件来源",
        )));
    }
    let services = state.admin_services().clone();
    let actor = auth.context().mutation_context();
    let result = tokio::spawn(async move {
        services
            .plugins()?
            .install_update(&input.id, input.revision, input.release, &actor)
            .await
    })
    .await
    .map_err(|_| AdminError::internal())?
    .map_err(error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
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
async fn public_resource<S: AdminSessionState + Send + Sync>(
    State(state): State<S>,
    Query(Resource { id, revision, path }): Query<Resource>,
) -> Result<impl IntoResponse, AdminError> {
    let body = state
        .admin_services()
        .plugins()
        .map_err(error)?
        .public_resource(&id, revision, &path)
        .await
        .map_err(error)?;
    // Keep the same JSON envelope: public HTML cannot execute as a host document.
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
