//! The public login RPC borrows the original authenticated invocation and process scope.
pub(crate) mod tasks;

use crate::{RpcSession, resources::InvocationCalls};
use gateway_admin::model::{AdminError, plugins::*};
use gateway_plugin_sdk::{Stage, call::management::*};
use std::sync::Arc;

pub(super) async fn invoke(
    session: &RpcSession,
    calls: &Arc<InvocationCalls>,
    access: &PluginAccess,
    management: &ManagementRegistration,
    call: PluginCallbackCall,
) -> Result<PluginCallbackResponse, AdminError> {
    let request = call.request;
    let declaration = management
        .callbacks
        .iter()
        .find(|callback| callback.path == request.path)
        .ok_or_else(|| AdminError::invalid("插件登录回调未注册"))?;
    if access.is_revoked() || call.incarnation != session.incarnation() {
        return Err(AdminError::conflict("插件登录回调进程已变化"));
    }
    if request.method != "GET"
        || !request.body.is_empty()
        || request.content_type.is_some()
        || request.query.len() > 8192
        || request.query.bytes().any(|b| b.is_ascii_control())
        || !valid_headers(&call.headers)
    {
        return Err(AdminError::invalid("插件登录回调请求无效"));
    }
    let params = serde_json::to_value(ManagementRequest {
        method: request.method,
        path: request.path,
        query: request.query,
        content_type: None,
        headers: call
            .headers
            .into_iter()
            .map(
                |header| gateway_plugin_sdk::call::middleware::MiddlewareHeader {
                    name: header.name().into(),
                    value: header.value().to_vec(),
                },
            )
            .collect(),
    })
    .map_err(|_| AdminError::invalid("插件登录回调请求无效"))?;
    let mut context = session.context(
        Stage::PublicManagement,
        session.budget(gateway_plugin_sdk::Stage::Management),
    );
    context.request_id = Some(request.actor.request_id.clone());
    context.request = request
        .invocation
        .as_ref()
        .and_then(|v| crate::request_context::wire(&v.context.call_scope));
    let _caller = calls
        .enter(
            context.clone(),
            request.actor,
            request.invocation,
            access.cancellation(),
            None,
        )
        .map_err(|_| AdminError::unavailable("管理回调容量已耗尽，请重新发起登录"))?;
    let reply = session
        .call("management.callback", context, params, vec![])
        .await
        .map_err(|_| AdminError::bad_gateway("插件登录回调未完成；票据已消费，请重新发起登录"))?;
    let response: ManagementResponse = session
        .decode_response(Stage::PublicManagement, reply.result)
        .map_err(|_| AdminError::bad_gateway("插件登录响应无效；票据已消费"))?;
    let headers = response
        .headers
        .into_iter()
        .map(|header| {
            gateway_core::engine::middleware::MiddlewareHeader::new(
                header.name,
                header.value.into(),
            )
        })
        .collect::<Vec<_>>();
    if !(200..=599).contains(&response.status)
        || !declaration
            .response_content_types
            .contains(&response.content_type)
        || reply.payload.len() > 1024 * 1024
        || !valid_headers(&headers)
    {
        session.invalid_response(Stage::PublicManagement);
        return Err(AdminError::bad_gateway(
            "插件登录响应状态、类型、头部或大小无效；票据已消费",
        ));
    }
    Ok(PluginCallbackResponse {
        headers,
        response: PluginHttpResponse {
            status: response.status,
            content_type: response.content_type,
            body: reply.payload,
        },
    })
}
fn valid_headers(headers: &[gateway_core::engine::middleware::MiddlewareHeader]) -> bool {
    headers.len() <= 256
        && headers
            .iter()
            .map(|h| h.name().len() + h.value().len())
            .sum::<usize>()
            <= 64 * 1024
        && headers.iter().all(|h| {
            http::HeaderName::from_bytes(h.name().as_bytes()).is_ok()
                && http::HeaderValue::from_bytes(h.value()).is_ok()
        })
}
