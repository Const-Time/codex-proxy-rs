//! Service JSON is a typed Result on the wire; no HTTP body handles or principals.
use super::registry::MiddlewareCallbacks;
use crate::{RpcError, RpcReply, RpcSession};
use futures::future::BoxFuture;
use gateway_core::{engine::middleware::service as core, lifecycle::CancellationToken};
use gateway_plugin_sdk::{
    Capability, ErrorCode, PluginFault, Stage,
    call::{
        middleware::{HANDLE_METHOD, NEXT_METHOD},
        services as wire,
    },
};
use std::sync::{Arc, Mutex};

pub(super) struct Invocation {
    instance: String,
    dispatcher: Option<Arc<dyn core::Dispatcher>>,
    next: Mutex<Option<core::Next>>,
    pub(super) cancellation: CancellationToken,
    pub(super) closed: CancellationToken,
}
impl Invocation {
    pub(super) fn call(
        self: Arc<Self>,
        method: String,
        params: core::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        Box::pin(async move {
            if method == wire::CALL_METHOD {
                let dispatcher = self.dispatcher.clone().ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "service authority is not bound",
                    )
                })?;
                return call_service(dispatcher, self.instance.clone(), params, payload).await;
            }
            if method != NEXT_METHOD || !payload.is_empty() {
                return Err(PluginFault::new(
                    ErrorCode::InvalidInput,
                    "invalid service continuation",
                ));
            }
            let next = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "service continuation already consumed",
                    )
                })?;
            let result: wire::Response =
                next.run(params).await.map_err(|error| wire::ServiceError {
                    kind: error.kind,
                    message: error.message,
                    details: error.details,
                });
            Ok(RpcReply {
                result: serde_json::to_value(result).map_err(|_| {
                    PluginFault::new(ErrorCode::Fault, "service result encoding failed")
                })?,
                payload: vec![],
            })
        })
    }
}
pub(super) async fn invoke(
    callbacks: Arc<MiddlewareCallbacks>,
    session: Arc<RpcSession>,
    context: core::Context,
    input: core::Value,
    next: core::Next,
) -> Result<core::Value, core::Error> {
    if session.capability_version(Capability::Middleware) != Some(4)
        || !session.capability_supports_stage(Capability::Middleware, Stage::Service)
    {
        return Err(core::Error::unavailable("服务中间件未声明"));
    }
    let remaining = context.deadline_at.bounded(session.maximum_call_timeout());
    if context.cancellation.is_cancelled() || remaining.is_zero() {
        return Err(core::Error::unavailable("服务调用已结束"));
    }
    let mut call = session.context(Stage::Service, remaining);
    let closed = CancellationToken::new();
    let mut callback_context = context.clone();
    callback_context.cancellation =
        CancellationToken::linked([context.cancellation.clone(), closed.clone()]);
    let invocation = Arc::new(Invocation {
        instance: call.instance_id.clone(),
        dispatcher: context
            .dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.scoped(&callback_context)),
        next: Mutex::new(Some(next)),
        cancellation: context.cancellation.clone(),
        closed,
    });
    call.request_id = Some(context.request_id.clone());
    let _binding = callbacks
        .bind_service(&call, invocation)
        .map_err(|_| core::Error::unavailable("服务调用上下文不可用"))?;
    let request = wire::Call {
        operation: context.operation.into(),
        input,
        request_id: context.request_id,
        call_id: context.call_id,
        parent_call_id: context.parent_call_id,
    };
    let reply = tokio::select! {biased;
        ()=context.cancellation.cancelled()=>return Err(core::Error::unavailable("服务调用已取消")),
        ()=context.deadline_at.wait()=>return Err(core::Error::unavailable("服务调用已到期")),
        reply=session.call(HANDLE_METHOD,call,serde_json::to_value(request).map_err(|_|core::Error::invalid("服务输入编码失败"))?,vec![])=>reply.map_err(rpc_error)?,
    };
    if !reply.payload.is_empty() {
        session.invalid_response(Stage::Service);
        return Err(core::Error::invalid("服务返回了意外正文"));
    }
    let response: wire::Response = session
        .decode_response(Stage::Service, reply.result)
        .map_err(|_| core::Error::invalid("服务结果类型无效"))?;
    response.map_err(|error| core::Error {
        kind: error.kind,
        message: error.message,
        details: error.details,
    })
}

pub(crate) async fn call_service(
    dispatcher: Arc<dyn core::Dispatcher>,
    instance: String,
    params: core::Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if !payload.is_empty() {
        return Err(PluginFault::new(
            ErrorCode::InvalidInput,
            "service call has unexpected payload",
        ));
    }
    let request: wire::Request = serde_json::from_value(params)
        .map_err(|_| PluginFault::new(ErrorCode::InvalidInput, "invalid service call"))?;
    let response: wire::Response = dispatcher
        .call(instance, request.operation, request.input)
        .await
        .map_err(|error| wire::ServiceError {
            kind: error.kind,
            message: error.message,
            details: error.details,
        });
    Ok(RpcReply {
        result: serde_json::to_value(response)
            .map_err(|_| PluginFault::new(ErrorCode::Fault, "service result encoding failed"))?,
        payload: vec![],
    })
}
pub(crate) fn service_cancelled() -> PluginFault {
    let mut fault = PluginFault::new(
        ErrorCode::Uncertain,
        "service call cancelled; refresh state before retrying",
    );
    fault.send_state = gateway_plugin_sdk::SendState::Ambiguous;
    fault
}
fn rpc_error(error: RpcError) -> core::Error {
    let fault = match error {
        RpcError::Remote(fault) => fault,
        error => {
            let code = match error {
                RpcError::Timeout => ErrorCode::Timeout,
                RpcError::Cancelled => ErrorCode::Cancelled,
                RpcError::Capacity => ErrorCode::Capacity,
                _ => ErrorCode::Fault,
            };
            PluginFault::new(code, error.diagnostic().1)
        }
    };
    core::Error {
        kind: "unavailable".into(),
        message: fault.message.clone(),
        details: Some(serde_json::json!(fault)),
    }
}
