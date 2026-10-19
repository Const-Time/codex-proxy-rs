//! The existing RPC scope survives through stream EOF or the accepted session task.
use super::*;
use crate::{RpcSession, middleware::registry::MiddlewareCallbacks};
use futures::stream;
use gateway_plugin_sdk::{Capability, Stage, call::middleware::HANDLE_METHOD};
use http_body::Frame;
use http_body_util::{BodyExt as _, StreamBody};

pub(crate) async fn invoke(
    callbacks: Arc<MiddlewareCallbacks>,
    session: Arc<RpcSession>,
    context: core::Context,
    request: core::Request,
    next: core::Next,
) -> Result<core::Response, MiddlewareError> {
    if !matches!(
        session.capability_version(Capability::Middleware),
        Some(4 | 5)
    ) || !session.capability_supports_stage(Capability::Middleware, Stage::Http)
    {
        return Err(MiddlewareError::InvalidState);
    }
    let remaining = context.deadline_at.bounded(session.maximum_call_timeout());
    if context.cancellation.is_cancelled() || remaining.is_zero() {
        return Err(MiddlewareError::Fault);
    }
    let settings_sources = request
        .extensions()
        .get::<core::Settings>()
        .and_then(|s| s.runtime.as_ref())
        .map_or(serde_json::Value::Null, |s| s.inspect());
    let mut call = session.context(Stage::Http, remaining);
    call.request_id = Some(context.request_id.clone());
    call.request = crate::request_context::wire(&context.call_scope);
    call.resource_stream = true;
    let (invocation, request) =
        Invocation::new(request, next, call.instance_id.clone(), context.clone())?;
    let binding = callbacks.bind_http(&call, invocation.clone())?;
    let mut stream = tokio::select! {
        biased;
        ()=context.cancellation.cancelled()=>return Err(MiddlewareError::Fault),
        ()=context.deadline_at.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
        result=session.call_stream(HANDLE_METHOD,call,serde_json::to_value(wire::Call {settings_sources,request_id:context.request_id.clone(),call_id:context.call_id.clone(),parent_call_id:context.parent_call_id.clone(),request}).map_err(|_|MiddlewareError::InvalidState)?,vec![])=>{
            result.map_err(|error|invocation.take_downstream_error().unwrap_or_else(||rpc_error(error)))?
        }
    };
    if let Some(error) = invocation.take_downstream_error() {
        return Err(error);
    }
    let response: wire::Response = session
        .decode_response(Stage::Http, std::mem::take(&mut stream.initial.result))
        .map_err(rpc_error)?;
    let payload = std::mem::take(&mut stream.initial.payload);
    if response.session {
        let (status, source) = invocation
            .session_source()
            .ok_or(MiddlewareError::InvalidState)?;
        if response.status != status.as_u16()
            || response.response.as_deref() != Some(source.as_str())
            || !matches!(response.body, wire::Body::Empty)
            || !payload.is_empty()
        {
            session.invalid_response(Stage::Http);
            return Err(MiddlewareError::InvalidState);
        }
        let mut response = invocation
            .complete(response, payload, None)
            .await
            .inspect_err(|_| {
                session.invalid_response(Stage::Http);
            })?;
        let task = Box::pin(async move {
            let _binding = binding;
            tokio::select! {
                biased;
                ()=context.cancellation.cancelled()=>Err(MiddlewareError::Fault),
                ()=context.deadline_at.wait()=>Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
                result=async {
                    invocation.session_ready().await?;
                    match stream.next().await {
                        Ok(None)=>Ok(()),
                        Ok(Some(_))=>{session.invalid_response(Stage::Http);Err(MiddlewareError::InvalidState)},
                        Err(error)=>Err(invocation.take_downstream_error().unwrap_or_else(||rpc_error(error))),
                    }
                }=>result,
            }
        });
        response
            .extensions_mut()
            .insert(core::upgrade::Upgraded::new(status, task)?);
        return Ok(response);
    }
    if matches!(response.body, wire::Body::Stream) {
        if !payload.is_empty() {
            session.invalid_response(Stage::Http);
            return Err(MiddlewareError::InvalidState);
        }
        let cancellation = context.cancellation.clone();
        let body = StreamBody::new(stream::try_unfold(
            (stream, invocation.clone(), binding, session.clone(), false),
            |(mut stream, invocation, binding, session, trailers_seen)| async move {
                let Some(chunk) = stream.next().await.map_err(|e| {
                    Box::new(
                        invocation
                            .take_downstream_error()
                            .unwrap_or_else(|| rpc_error(e)),
                    ) as Box<dyn std::error::Error + Send + Sync>
                })?
                else {
                    return Ok(None);
                };
                let parsed = match chunk.split_first() {
                    Some((0, data)) if !trailers_seen => {
                        Ok((Frame::data(bytes::Bytes::copy_from_slice(data)), false))
                    }
                    Some((1, data)) if !trailers_seen => serde_json::from_slice(data)
                        .map_err(|_| invalid())
                        .and_then(headers)
                        .map(|headers| (Frame::trailers(headers), true)),
                    _ => Err(invalid()),
                };
                let (frame, trailers_seen) = parsed.map_err(|_| {
                    session.invalid_response(Stage::Http);
                    Box::new(MiddlewareError::InvalidState)
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                Ok(Some((
                    frame,
                    (stream, invocation, binding, session, trailers_seen),
                )))
            },
        ))
        .boxed_unsync();
        let response = invocation
            .complete(response, payload, Some(body))
            .await
            .inspect_err(|_| {
                session.invalid_response(Stage::Http);
            })?;
        return Ok(response.map(|body| core::guard_body(body, cancellation, context.deadline_at)));
    }
    // Resolve pass-through ownership before End removes callback-only resources.
    let response = invocation
        .complete(response, payload, None)
        .await
        .inspect_err(|_| {
            session.invalid_response(Stage::Http);
        })?;
    tokio::select! {
        biased;
        ()=context.cancellation.cancelled()=>return Err(MiddlewareError::Fault),
        ()=context.deadline_at.wait()=>return Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),
        result=stream.next()=>match result {
            Ok(None)=>{},
            Ok(Some(_))=>{session.invalid_response(Stage::Http);return Err(MiddlewareError::InvalidState);},
            Err(error)=>return Err(invocation.take_downstream_error().unwrap_or_else(||rpc_error(error))),
        }
    };
    drop(binding);
    Ok(response.map(|body| core::guard_body(body, context.cancellation, context.deadline_at)))
}
