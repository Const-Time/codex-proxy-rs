//! One raw message owns its continuation and handles until its unary reply transfers them.
use super::{registry::MiddlewareCallbacks, websocket::Invocation};
use crate::RpcSession;
use gateway_core::engine::middleware::{MiddlewareError, websocket as core};
use gateway_plugin_sdk::{
    Capability, Stage,
    call::middleware::{HANDLE_METHOD, MiddlewareHeader, websocket as wire},
};
use std::sync::Arc;

pub(super) async fn invoke(
    callbacks: Arc<MiddlewareCallbacks>,
    session: Arc<RpcSession>,
    mut context: core::Context,
    message: core::Message,
    next: core::Next,
) -> Result<Option<core::Message>, MiddlewareError> {
    let origin = context.service_origin(None);
    context.cancellation = origin.context.cancellation.clone();
    context.deadline_at = origin.context.deadline_at;
    context.origin = Some(origin);
    if !matches!(
        session.capability_version(Capability::Middleware),
        Some(4 | 5)
    ) || !session.capability_supports_stage(Capability::Middleware, Stage::WebSocket)
    {
        return Err(MiddlewareError::InvalidState);
    }
    let remaining = context
        .deadline_at
        .bounded(session.budget(gateway_plugin_sdk::Stage::WebSocket));
    if context.cancellation.is_cancelled() || remaining.is_zero() {
        return Err(MiddlewareError::Fault);
    }
    let invocation = Invocation::new(next, &context);
    let input = wire::Call {
        connection_id: context.connection_id.clone(),
        direction: match context.direction {
            core::Direction::Incoming => wire::Direction::Incoming,
            core::Direction::Outgoing => wire::Direction::Outgoing,
        },
        headers: context
            .headers
            .iter()
            .map(|h| MiddlewareHeader {
                name: h.name().into(),
                value: h.value().to_vec(),
            })
            .collect(),
        message: invocation
            .encode(message)
            .map_err(|_| MiddlewareError::Fault)?,
    };
    let mut call = session.context(Stage::WebSocket, remaining);
    call.request = context
        .origin
        .as_ref()
        .and_then(|v| crate::request_context::wire(&v.context.call_scope));
    call.request_id = Some(context.connection_id.clone());
    let _binding =
        callbacks.bind_websocket(&call, invocation.clone(), context.service_origin(None))?;
    let reply = tokio::select! {biased;
        ()=context.cancellation.cancelled()=>return Err(MiddlewareError::Fault),
        ()=context.deadline_at.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
        reply=session.call(HANDLE_METHOD, call, serde_json::to_value(input).map_err(|_|MiddlewareError::InvalidState)?, vec![])=>reply,
    };
    if let Some(error) = invocation.take_error() {
        return Err(error);
    }
    let reply = reply.map_err(super::http::rpc_error)?;
    let message: Option<wire::Message> = session
        .decode_response(Stage::WebSocket, reply.result)
        .map_err(super::http::rpc_error)?;
    match message {
        Some(message) => invocation
            .decode(message, reply.payload)
            .map(Some)
            .map_err(|_| {
                session.invalid_response(Stage::WebSocket);
                MiddlewareError::InvalidState
            }),
        None if reply.payload.is_empty() => Ok(None),
        None => {
            session.invalid_response(Stage::WebSocket);
            Err(MiddlewareError::InvalidState)
        }
    }
}
