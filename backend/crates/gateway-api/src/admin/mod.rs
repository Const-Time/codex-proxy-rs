//! 管理端 HTTP adapter、wire contract 与固定路由。

use axum::{
    Router,
    http::{HeaderValue, header},
    middleware,
    response::Response,
    routing::any,
};

pub mod account_groups;
pub mod accounts;
pub mod auth;
pub mod backups;
pub mod client_keys;
mod extract;
pub mod observability;
pub mod operations;
pub(crate) mod plugin_models;
pub mod plugins;
pub mod presenter;
pub mod proxies;
pub mod settings;
pub mod system;
pub mod turn_state;
pub mod users;
pub mod wire;

pub use auth::{AdminAuth, AdminSessionState};
pub use extract::{AdminJson, AdminQuery};
pub use wire::{
    ADMIN_OK_CODE, ADMIN_OK_MESSAGE, AdminEnvelope, AdminError, AdminErrorBody, AdminErrorCode,
    AdminPageData, AdminResponse, PageMeta, WireValidationError,
};

/// These host-owned routes must remain reachable to recover a broken HTTP plugin.
/// Their handlers still perform the normal login, administrator and revision checks.
/// Plugin execution/resource/model routes deliberately remain outside this set.
pub(crate) fn is_recovery_route(path: &str) -> bool {
    matches!(
        path,
        "/api/admin/auth/login"
            | "/api/admin/auth/status"
            | "/api/admin/auth/logout"
            | "/api/admin/plugins"
            | "/api/admin/plugins/artifacts"
            | "/api/admin/plugins/artifacts/remove"
            | "/api/admin/plugins/update"
            | "/api/admin/plugins/uninstall"
    )
}

/// 构造完整且固定的 `/api/admin` 路由。
pub fn router<S>() -> Router<S>
where
    S: AdminSessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .merge(account_groups::router::<S>())
        .merge(proxies::router::<S>())
        .merge(plugins::router::<S>())
        .merge(accounts::router::<S>())
        .merge(auth::router::<S>())
        .merge(users::router::<S>())
        .merge(operations::router::<S>())
        .merge(backups::router::<S>())
        .merge(client_keys::router::<S>())
        .merge(observability::router::<S>())
        .merge(settings::router::<S>())
        .merge(system::router::<S>())
        .merge(turn_state::router::<S>())
        .method_not_allowed_fallback(method_not_allowed)
        .route("/api/admin", any(admin_not_found))
        .route("/api/admin/{*path}", any(admin_not_found))
        .layer(middleware::map_response(no_store))
}

async fn method_not_allowed() -> AdminError {
    AdminError::method_not_allowed()
}

async fn admin_not_found() -> AdminError {
    AdminError::admin_route_not_found()
}

async fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
    response
}
