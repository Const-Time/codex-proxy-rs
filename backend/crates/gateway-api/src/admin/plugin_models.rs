//! Plugin page inference uses a personally owned Key and the normal Responses execution/settlement path.
use super::{AdminAuth, AdminError, AdminJson, AdminSessionState, auth::UserAuth};
use crate::ApiState;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Extension, State, connect_info::ConnectInfo},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::post,
};
use futures::{StreamExt, stream};
use gateway_core::{lifecycle::CancellationToken, policy::ClientApiKeyId};
use serde::Deserialize;
use std::{io, net::SocketAddr};

pub(crate) fn router() -> Router<ApiState> {
    Router::new()
        .route("/api/admin/plugins/models/responses", post(responses))
        .layer(DefaultBodyLimit::max(9 * 1024 * 1024))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelRequest {
    id: String,
    revision: u64,
    client_key_id: String,
    body: serde_json::Value,
}
async fn responses(
    _: AdminAuth,
    user: UserAuth,
    State(state): State<ApiState>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress: Option<Extension<tower_http::request_id::RequestId>>,
    AdminJson(input): AdminJson<ModelRequest>,
) -> Result<Response, AdminError> {
    let plugins = state
        .admin_services()
        .plugins()
        .map_err(super::wire::map_admin_service_error)?;
    let plugin_access = plugins
        .access(&input.id, input.revision)
        .map_err(super::wire::map_admin_service_error)?;
    if !input.body.is_object() {
        return Err(AdminError::bad_request("Responses 正文必须是 JSON 对象"));
    }
    let body = serde_json::to_vec(&input.body)
        .map_err(|_| AdminError::bad_request("Responses 正文无效"))?;
    if body.len() > 8 * 1024 * 1024 {
        return Err(AdminError::bad_request("Responses 正文超过 8 MiB"));
    }
    let key_id = ClientApiKeyId::new(input.client_key_id)
        .map_err(|_| AdminError::bad_request("Key ID 无效"))?;
    // This owner check precedes authentication; knowing another user's ID grants no access.
    let secret = state
        .admin_services()
        .client_keys()
        .reveal(&user.user.id, &key_id)
        .await
        .map_err(super::wire::map_admin_service_error)?;
    let client = match state.openai().authenticate(secret.expose_for_response()) {
        Ok(client) => client,
        Err(error) => {
            return Ok(crate::openai::auth::client_access_error_response(
                crate::openai::auth::ClientAccessError::Authentication(error),
            ));
        }
    };
    let client_access = state.openai().client_access(&client).ok_or_else(|| {
        AdminError::invalid_request(
            StatusCode::SERVICE_UNAVAILABLE,
            "模型授权已变更或不可用，请重试",
        )
    })?;
    let revoked = client_access.cancellation();
    let mut headers = HeaderMap::new();
    let mut authorization =
        HeaderValue::from_str(&format!("Bearer {}", secret.expose_for_response()))
            .map_err(|_| AdminError::internal())?;
    authorization.set_sensitive(true);
    headers.insert(AUTHORIZATION, authorization);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    // Never accept a plugin's arbitrary Authorization, Cookie or diagnostic identity headers.
    let response = tokio::select! { biased;
        _=plugin_access.revoked()=>return Err(AdminError::invalid_request(StatusCode::CONFLICT,"插件版本已失效")),
        _=revoked.cancelled()=>return Err(AdminError::invalid_request(StatusCode::FORBIDDEN,"Key 或用户权限已撤销")),
        response=crate::openai::responses::responses_at_endpoint(State(state),peer,ingress,headers,Bytes::from(body),"/api/admin/plugins/models/responses")=>response,
    };
    let (mut parts, body) = response.into_parts();
    parts.headers.remove(axum::http::header::SET_COOKIE);
    // The pump owns the Core body even while the browser stops pulling; revocation drops it immediately.
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let (send, receive) = tokio::sync::mpsc::channel(1);
    let delivery_plugin = plugin_access.clone();
    let delivery_revoked = revoked.clone();
    let delivery_access = client_access.clone();
    tokio::spawn(async move {
        let _authorization = client_access;
        let mut chunks = body.into_data_stream();
        loop {
            let chunk = tokio::select! { biased;
                _=stopped.cancelled()=>break,
                _=plugin_access.revoked()=>{ let _=send.try_send(Err(io::Error::other("plugin revision revoked"))); break; },
                _=revoked.cancelled()=>{ let _=send.try_send(Err(io::Error::other("client access revoked"))); break; },
                chunk=chunks.next()=>match chunk {Some(chunk)=>chunk.map_err(io::Error::other),None=>break},
            };
            tokio::select! { biased;
                _=stopped.cancelled()=>break,
                _=plugin_access.revoked()=>break,
                _=revoked.cancelled()=>break,
                result=send.send(chunk)=>if result.is_err(){break;},
            }
        }
    });
    let body = Body::from_stream(stream::unfold(
        Some((
            receive,
            CancelOnDrop(stop),
            delivery_plugin,
            delivery_revoked,
            delivery_access,
        )),
        |state| async move {
            let (mut receive, guard, plugin, client, access) = state?;
            tokio::select! { biased;
                _=plugin.revoked()=>Some((Err(io::Error::other("plugin revision revoked")),None)),
                _=client.cancelled()=>Some((Err(io::Error::other("client access revoked")),None)),
                chunk=receive.recv()=>chunk.map(|chunk|(chunk,Some((receive,guard,plugin,client,access)))),
            }
        },
    ));
    Ok(Response::from_parts(parts, body).into_response())
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
