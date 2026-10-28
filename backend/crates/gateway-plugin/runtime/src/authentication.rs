//! Authentication wire is bounded and never includes cookies, host mappings or a local Key secret.
use crate::RpcSession;
use gateway_admin::model::{AdminError, plugins::*};
use gateway_plugin_sdk::{Stage, call::frontend_authentication::*};
use serde_json::json;
use std::sync::Arc;
pub(crate) async fn register(session: &Arc<RpcSession>) -> Result<(), AdminError> {
    let reply = session
        .call(
            "frontend_auth.identifier",
            session.context(Stage::Registration, session.budget(Stage::Registration)),
            json!({}),
            vec![],
        )
        .await
        .map_err(|_| AdminError::invalid("认证器注册失败"))?;
    let identifier: FrontendAuthenticationIdentifier = session
        .decode_response(Stage::Registration, reply.result)
        .map_err(|_| AdminError::invalid("认证器标识无效"))?;
    let id = identifier.identifier;
    if !reply.payload.is_empty()
        || id.is_empty()
        || id.len() > 128
        || !id.as_bytes()[0].is_ascii_lowercase()
        || !id
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"-_".contains(&c))
    {
        return Err(AdminError::invalid("认证器标识无效"));
    }
    Ok(())
}
pub(crate) async fn authenticate(
    session: &Arc<RpcSession>,
    services: Arc<crate::services::Registry>,
    authorization: String,
    origin: Option<gateway_core::engine::middleware::service::Origin>,
) -> Result<PluginAuthenticationResult, AdminError> {
    if authorization.is_empty()
        || authorization.len() > 8192
        || authorization.chars().any(char::is_control)
    {
        return Err(AdminError::invalid("认证信封无效"));
    }
    let input = FrontendAuthenticationRequest { authorization };
    let mut context = session.context(Stage::Authentication, session.budget(Stage::Authentication));
    let origin = match origin {
        Some(origin) => origin,
        None => services
            .root_origin(&context)
            .map_err(|_| AdminError::unavailable("认证服务端口不可用"))?,
    };
    context.request_id = Some(origin.context.request_id.clone());
    context.request = crate::request_context::wire(&origin.context.call_scope);
    let cancellation = origin.context.cancellation.clone();
    let deadline = origin.context.deadline_at;
    let _services = services
        .bind_origin(&context, origin)
        .map_err(|_| AdminError::unavailable("认证服务范围不可用"))?;
    let call = session.call(
        "frontend_auth.authenticate",
        context,
        json!({}),
        serde_json::to_vec(&input).map_err(|_| AdminError::internal("认证编码失败"))?,
    );
    let reply = tokio::select! {biased;
        ()=cancellation.cancelled()=>return Err(AdminError::unavailable("认证调用已取消")),
        ()=deadline.wait()=>return Err(AdminError::unavailable("认证调用已到期")),
        reply=call=>reply.map_err(|_| AdminError::unavailable("认证器暂不可用"))?,
    };
    if reply.result != json!({}) || reply.payload.len() > 4096 {
        return Err(AdminError::unavailable("认证器响应无效"));
    }
    let result: FrontendAuthenticationResult = serde_json::from_slice(&reply.payload)
        .map_err(|_| AdminError::unavailable("认证器响应无效"))?;
    match result {
        FrontendAuthenticationResult::Authenticated { principal }
            if valid_plugin_principal(&principal) =>
        {
            Ok(PluginAuthenticationResult::Authenticated(principal))
        }
        FrontendAuthenticationResult::Authenticated { .. } => {
            Err(AdminError::unavailable("认证器身份无效"))
        }
        FrontendAuthenticationResult::NotMatched {} => Ok(PluginAuthenticationResult::NotMatched),
        FrontendAuthenticationResult::Rejected {} => Ok(PluginAuthenticationResult::Rejected),
    }
}
