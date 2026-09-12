//! Audits successful control-plane changes and failed requests without reading bodies.
use super::{
    AdminAuth, AdminEnvelope, AdminError, AdminQuery, AdminResponse, AdminSessionState,
    auth::UserAuth,
};
use axum::{
    Router,
    extract::{ConnectInfo, MatchedPath, Request, State},
    http::{StatusCode, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use gateway_admin::model::{auth::AdminPrincipal, operations::*};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Default)]
pub(crate) struct AuditIdentity(Arc<Mutex<Option<AdminPrincipal>>>);

pub(crate) fn mark_actor(parts: &Parts, principal: AdminPrincipal) {
    if let Some(identity) = parts.extensions.get::<AuditIdentity>()
        && let Ok(mut actor) = identity.0.lock()
    {
        *actor = Some(principal);
    }
}

pub fn router<S: AdminSessionState + Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/api/admin/operation-logs", get(list::<S>))
        .route("/api/admin/usage/key-options", get(keys::<S>))
}

async fn list<S: AdminSessionState + Send + Sync>(
    State(state): State<S>,
    _auth: AdminAuth,
    AdminQuery(query): AdminQuery<OperationLogQuery>,
) -> Result<impl IntoResponse, AdminError> {
    let end = query.end_time.unwrap_or_else(chrono::Utc::now);
    let start = query.start_time.unwrap_or(end - chrono::Duration::days(7));
    if start >= end
        || end - start > chrono::Duration::days(366)
        || !(1..=100_000).contains(&query.page.unwrap_or(1))
        || [
            &query.email,
            &query.action,
            &query.ip,
            &query.method,
            &query.auth_method,
            &query.result,
        ]
        .iter()
        .any(|v| v.as_ref().is_some_and(|v| v.len() > 256))
    {
        return Err(AdminError::bad_request("日志查询范围无效，最多查询 366 天"));
    }
    let data = state
        .admin_services()
        .auth()
        .operation_logs(query)
        .await
        .map_err(super::wire::map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyQuery {
    #[serde(default)]
    personal: bool,
}
async fn keys<S: AdminSessionState + Send + Sync>(
    State(state): State<S>,
    auth: UserAuth,
    AdminQuery(query): AdminQuery<KeyQuery>,
) -> Result<impl IntoResponse, AdminError> {
    let owner = (query.personal || auth.user.role != gateway_admin::model::users::UserRole::Admin)
        .then_some(auth.user.id.as_str());
    let data = state
        .admin_services()
        .auth()
        .usage_key_options(owner)
        .await
        .map_err(super::wire::map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

/// Attach to embedded admin routers to retain the same audit guarantees as the host.
pub async fn audit_request<S: AdminSessionState + Clone + Send + Sync + 'static>(
    State(state): State<S>,
    mut request: Request,
    next: Next,
) -> Response {
    if !request.uri().path().starts_with("/api/admin")
        && !request.uri().path().starts_with("/api/profile")
    {
        return next.run(request).await;
    }
    let started = Instant::now();
    let identity = AuditIdentity::default();
    request.extensions_mut().insert(identity.clone());
    // Matched route templates exclude attacker-controlled query strings and path values.
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("/api/admin/{unmatched}", MatchedPath::as_str)
        .to_owned();
    let method = request.method().as_str().to_owned();
    let client_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string());
    // Retain a proxy-reported candidate separately from the transport peer.
    // These headers are not authenticated and must not be used for authorization.
    let forwarded_ip = ["x-forwarded-for", "x-real-ip", "cf-connecting-ip"]
        .into_iter()
        .find_map(|name| {
            let value = request.headers().get(name)?.to_str().ok()?;
            let candidate = if name == "x-forwarded-for" {
                value.split(',').next()?
            } else {
                value
            };
            candidate.trim().parse::<IpAddr>().ok()
        })
        .map(|ip| ip.to_string());
    // An audit-owned ID prevents a caller from forging links to other audit events.
    let request_id = uuid::Uuid::now_v7().to_string();
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        request
            .extensions_mut()
            .insert(tower_http::request_id::RequestId::new(value.clone()));
        request.headers_mut().insert("x-request-id", value);
    }
    let mut response = next.run(request).await;
    let status = response.status().as_u16();
    if method == "GET"
        && status < 400
        && !["/reveal", "/export", "/download"]
            .iter()
            .any(|suffix| path.ends_with(suffix))
    {
        return response;
    }
    let actor = response
        .extensions_mut()
        .remove::<AdminPrincipal>()
        .or_else(|| identity.0.lock().ok().and_then(|v| v.clone()));
    let (auth_method, actor_user_id) = match actor {
        Some(AdminPrincipal::Session { admin_user_id }) => ("session", Some(admin_user_id)),
        Some(AdminPrincipal::ApiKey) => ("api_key", None),
        None => ("anonymous", None),
    };
    let event = OperationLog {
        id: request_id.clone(),
        occurred_at: chrono::Utc::now(),
        actor_user_id,
        email: None,
        username: None,
        auth_method: auth_method.into(),
        method,
        path,
        status,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        client_ip,
        forwarded_ip,
        request_id,
    };
    if state
        .admin_services()
        .auth()
        .record_operation(event)
        .await
        .is_err()
    {
        tracing::error!(target: "security_audit", "failed to persist operation audit metadata");
    }
    response
}
