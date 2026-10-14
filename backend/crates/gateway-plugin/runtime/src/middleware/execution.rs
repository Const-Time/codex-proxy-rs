//! v4 wire invocation and lazy body transformation. Core facts remain in the host.
use super::{
    MiddlewareBodyAuthority, MiddlewareCompletionBody, MiddlewareInvocation,
    registry::MiddlewareCallbacks,
};
use crate::{RpcError, RpcSession, RpcStream};
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::engine::middleware::*;
use gateway_plugin_sdk::{
    Capability, ErrorCode, Stage,
    call::middleware::{
        HANDLE_METHOD, MiddlewareBodyDisposition, MiddlewareBodyFrame,
        MiddlewareMount as WireMiddlewareMount, MiddlewareRequestHead, MiddlewareResponseHead,
        MiddlewareTransport as WireMiddlewareTransport,
    },
};
use gateway_protocol::openai::sse::parse_sse_events;
use std::sync::{Arc, Mutex};

const MAXIMUM_MAPPED_FRAMES_PER_SOURCE: usize = 64;
const MAXIMUM_MAPPED_BYTES_PER_SOURCE: usize = 8 * 1024 * 1024;

pub(super) async fn invoke(
    callbacks: Arc<MiddlewareCallbacks>,
    session: Arc<RpcSession>,
    context: MiddlewareContext,
    request: MiddlewareRequest,
    next: MiddlewareNext,
) -> Result<MiddlewareResponse, MiddlewareError> {
    if session.capability_version(Capability::Middleware) != Some(4) {
        return Err(MiddlewareError::InvalidState);
    }
    let (stage, mount) = match context.mount {
        MiddlewareMount::Request => (Stage::Request, WireMiddlewareMount::Request),
        MiddlewareMount::Attempt => (Stage::Attempt, WireMiddlewareMount::Attempt),
        _ => return Err(MiddlewareError::InvalidState),
    };
    if !session.capability_supports_stage(Capability::Middleware, stage) {
        return Err(MiddlewareError::InvalidState);
    }
    let remaining = context.deadline_at.bounded(session.maximum_call_timeout());
    if remaining.is_zero() || context.cancellation.is_cancelled() {
        return Err(MiddlewareError::Fault);
    }
    let mut call = session.context(stage, remaining);
    let settings_sources = request
        .settings()
        .map_or(serde_json::Value::Null, |s| s.inspect());
    let settings = if stage == Stage::Request {
        request
            .settings()
            .and_then(|s| s.execution_values())
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| MiddlewareError::InvalidState)?
            .unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::Value::Null
    };
    let invocation = MiddlewareInvocation::new(&context, call.instance_id.clone(), request, next);
    let (protocol, headers, payload) = invocation
        .request_parts()
        .map_err(|_| MiddlewareError::InvalidState)?;
    call.request_id = Some(context.request_id.as_str().to_owned());
    call.attempt_id = context
        .attempt_index
        .map(|n| format!("{}:{n}", context.request_id.as_str()));
    call.account_id = context.account_id.as_ref().map(|a| a.as_str().to_owned());
    let binding = callbacks.bind(&call, invocation.clone())?;
    let head = MiddlewareRequestHead {
        settings_sources,
        settings,
        request_id: context.request_id.as_str().into(),
        client_key_id: context.client_key_id.as_str().into(),
        account_group_ids: context
            .account_group_ids
            .iter()
            .map(|g| g.as_str().into())
            .collect(),
        mount,
        attempt_index: context.attempt_index,
        operation: context.operation.as_str().into(),
        protocol,
        endpoint: context.endpoint,
        transport: wire_transport(context.transport),
        provider: context.provider.map(|p| p.as_str().to_owned()),
        model: context.model,
        account_id: context.account_id.map(|a| a.as_str().to_owned()),
        headers,
    };
    let stream = tokio::select! {
        biased;
        () = context.cancellation.cancelled() => return Err(MiddlewareError::Fault),
        () = context.deadline_at.wait() => return Err(MiddlewareError::Fault),
        result = session.call_stream(HANDLE_METHOD, call, serde_json::to_value(head).map_err(|_| MiddlewareError::InvalidState)?, payload) => {
            result.map_err(|e| invocation.take_downstream_error().unwrap_or_else(|| rpc_error(e)))?
        }
    };
    if !stream.initial.payload.is_empty() {
        session.invalid_response(stage);
        return Err(MiddlewareError::InvalidState);
    }
    let response: MiddlewareResponseHead = session
        .decode_response(stage, stream.initial.result.clone())
        .map_err(rpc_error)?;
    if let Some(error) = invocation.take_downstream_error() {
        return Err(error);
    }
    let completion = invocation
        .resolve_response(response)
        .await
        .inspect_err(|_| {
            session.invalid_response(stage);
        })?;
    let body: Box<dyn MiddlewareBody> = match completion.body {
        MiddlewareCompletionBody::PassThrough(body) => {
            drain_empty_stream(stream, &invocation, &session, stage, &context.cancellation).await?;
            body
        }
        MiddlewareCompletionBody::Empty => {
            drain_empty_stream(stream, &invocation, &session, stage, &context.cancellation).await?;
            Box::new(EmptyMiddlewareBody)
        }
        MiddlewareCompletionBody::Stream {
            framing,
            downstream,
        } => Box::new(PluginMiddlewareBody {
            session,
            stage,
            stream: Some(stream),
            framing,
            terminal_seen: false,
            next_source_id: 0,
            active_source: None,
            active_output_count: 0,
            active_output_bytes: 0,
            pending_transformed: false,
            downstream_error: invocation.downstream_error_slot(),
            downstream,
            _invocation: invocation,
            _binding: binding,
            cancellation: context.cancellation,
        }),
    };
    let response = MiddlewareResponse::new(
        completion.protocol,
        completion.status,
        completion.headers,
        body,
    );
    Ok(match completion.envelope {
        Some(envelope) => response.with_envelope(envelope),
        None => response,
    })
}

fn rpc_error(error: RpcError) -> MiddlewareError {
    let rejected = matches!(&error, RpcError::Remote(fault) if fault.code == ErrorCode::Rejected);
    MiddlewareError::Remote {
        source: Box::new(error),
        rejected,
    }
}
async fn drain_empty_stream(
    mut stream: RpcStream,
    invocation: &MiddlewareInvocation,
    session: &crate::RpcSession,
    stage: Stage,
    cancellation: &gateway_core::lifecycle::CancellationToken,
) -> Result<(), MiddlewareError> {
    match tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(RpcError::Cancelled),
        result = stream.next() => result,
    } {
        Ok(None) => invocation.take_downstream_error().map_or(Ok(()), Err),
        Ok(Some(_)) => {
            session.invalid_response(stage);
            Err(MiddlewareError::InvalidState)
        }
        Err(error) => invocation
            .take_downstream_error()
            .map_or_else(|| Err(rpc_error(error)), Err),
    }
}

struct EmptyMiddlewareBody;

impl MiddlewareBody for EmptyMiddlewareBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async { Ok(None) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

struct PluginMiddlewareBody {
    session: Arc<crate::RpcSession>,
    stage: Stage,
    stream: Option<RpcStream>,
    framing: MiddlewareFraming,
    terminal_seen: bool,
    next_source_id: u64,
    active_source: Option<u64>,
    active_output_count: usize,
    active_output_bytes: usize,
    pending_transformed: bool,
    downstream_error: Arc<Mutex<Option<MiddlewareError>>>,
    downstream: Option<MiddlewareBodyAuthority>,
    _invocation: Arc<MiddlewareInvocation>,
    _binding: super::registry::Binding,
    cancellation: gateway_core::lifecycle::CancellationToken,
}

impl MiddlewareBody for PluginMiddlewareBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            let invalid = || {
                self.session.invalid_response(self.stage);
                MiddlewareError::InvalidState
            };
            if let Some(error) = take_error(&self.downstream_error) {
                self.stream.take();
                return Err(error);
            }
            loop {
                let Some(stream) = self.stream.as_mut() else {
                    return Ok(None);
                };
                let chunk = match tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => Err(RpcError::Cancelled),
                    result = stream.next() => result,
                } {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        self.stream.take();
                        return Err(
                            take_error(&self.downstream_error).unwrap_or_else(|| rpc_error(error))
                        );
                    }
                };
                let Some(chunk) = chunk else {
                    self.stream.take();
                    if self.active_source.is_some()
                        || self.pending_transformed
                        || match &self.downstream {
                            Some(downstream) => downstream.has_pending_sources().await,
                            None => false,
                        }
                    {
                        return Err(invalid());
                    }
                    return take_error(&self.downstream_error).map_or(Ok(None), Err);
                };
                if self.terminal_seen {
                    self.stream.take();
                    return Err(invalid());
                }
                let wire_frame = MiddlewareBodyFrame::decode(&chunk).map_err(|_| invalid())?;
                let source_id = wire_frame.source_id();
                let disposition = wire_frame.disposition();
                let frame = match (&self.downstream, disposition) {
                    (None, MiddlewareBodyDisposition::Standalone) => Some(
                        MiddlewareFrame::new(
                            Bytes::from(wire_frame.payload),
                            self.framing,
                            wire_frame.terminal,
                        )
                        .with_transformed(true),
                    ),
                    (Some(_), MiddlewareBodyDisposition::Standalone) | (None, _) => {
                        return Err(invalid());
                    }
                    (Some(downstream), MiddlewareBodyDisposition::Only)
                    | (Some(downstream), MiddlewareBodyDisposition::Drop) => {
                        if self.active_source.is_some()
                            || source_id != self.next_source_id.saturating_add(1)
                            || wire_frame.payload.len() > MAXIMUM_MAPPED_BYTES_PER_SOURCE
                        {
                            return Err(invalid());
                        }
                        self.next_source_id = source_id;
                        downstream
                            .resolve_source_output(
                                source_id,
                                disposition,
                                Bytes::from(wire_frame.payload),
                            )
                            .await
                            .map_err(|_| invalid())?
                    }
                    (Some(downstream), MiddlewareBodyDisposition::First) => {
                        if self.active_source.is_some()
                            || source_id != self.next_source_id.saturating_add(1)
                            || wire_frame.payload.len() > MAXIMUM_MAPPED_BYTES_PER_SOURCE
                        {
                            return Err(invalid());
                        }
                        self.active_source = Some(source_id);
                        self.active_output_count = 1;
                        self.active_output_bytes = wire_frame.payload.len();
                        downstream
                            .resolve_source_output(
                                source_id,
                                disposition,
                                Bytes::from(wire_frame.payload),
                            )
                            .await
                            .map_err(|_| invalid())?
                    }
                    (Some(downstream), MiddlewareBodyDisposition::More) => {
                        self.active_output_count = self
                            .active_output_count
                            .checked_add(1)
                            .ok_or_else(invalid)?;
                        self.active_output_bytes = self
                            .active_output_bytes
                            .checked_add(wire_frame.payload.len())
                            .ok_or_else(invalid)?;
                        if self.active_source != Some(source_id)
                            || self.active_output_count > MAXIMUM_MAPPED_FRAMES_PER_SOURCE
                            || self.active_output_bytes > MAXIMUM_MAPPED_BYTES_PER_SOURCE
                        {
                            return Err(invalid());
                        }
                        downstream
                            .resolve_source_output(
                                source_id,
                                disposition,
                                Bytes::from(wire_frame.payload),
                            )
                            .await
                            .map_err(|_| invalid())?
                    }
                    (Some(downstream), MiddlewareBodyDisposition::Last) => {
                        self.active_output_count = self
                            .active_output_count
                            .checked_add(1)
                            .ok_or_else(invalid)?;
                        self.active_output_bytes = self
                            .active_output_bytes
                            .checked_add(wire_frame.payload.len())
                            .ok_or_else(invalid)?;
                        if self.active_source != Some(source_id)
                            || self.active_output_count > MAXIMUM_MAPPED_FRAMES_PER_SOURCE
                            || self.active_output_bytes > MAXIMUM_MAPPED_BYTES_PER_SOURCE
                        {
                            return Err(invalid());
                        }
                        self.active_source = None;
                        self.active_output_count = 0;
                        self.active_output_bytes = 0;
                        self.next_source_id = source_id;
                        downstream
                            .resolve_source_output(
                                source_id,
                                disposition,
                                Bytes::from(wire_frame.payload),
                            )
                            .await
                            .map_err(|_| invalid())?
                    }
                };
                if disposition == MiddlewareBodyDisposition::Drop {
                    self.pending_transformed = true;
                    if let Some(frame) = frame {
                        return Ok(Some(frame.with_transformed(true)));
                    }
                    continue;
                }
                let mut frame = frame.ok_or_else(invalid)?;
                // 只有宿主来源事件本就没有 wire 时，空 payload 才是事实帧
                let facts_only = frame.bytes().is_empty()
                    && frame
                        .event()
                        .is_some_and(|event| event.wire_event().is_none());
                if !facts_only && !valid_plugin_frame(self.framing, frame.bytes()) {
                    return Err(invalid());
                }
                if self.pending_transformed {
                    frame = frame.with_transformed(true);
                    self.pending_transformed = false;
                }
                if frame.terminal() {
                    if self.active_source.is_some()
                        || match &self.downstream {
                            Some(downstream) => downstream.has_pending_sources().await,
                            None => false,
                        }
                    {
                        self.stream.take();
                        return Err(invalid());
                    }
                    self.terminal_seen = true;
                    let terminal = tokio::select! {
                        biased;
                        () = self.cancellation.cancelled() => Err(RpcError::Cancelled),
                        result = stream.next() => result,
                    };
                    self.stream.take();
                    if matches!(&terminal, Ok(Some(_))) {
                        self.session.invalid_response(self.stage);
                    }
                    validate_terminal_end(terminal, &self.downstream_error)?;
                }
                return Ok(Some(frame));
            }
        })
    }

    fn commit_downstream(
        &mut self,
        client_status_code: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            match &self.downstream {
                Some(downstream) => downstream.commit_downstream(client_status_code).await,
                None => Ok(()),
            }
        })
    }

    fn record_client_status(
        &mut self,
        client_status_code: u16,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            match &self.downstream {
                Some(downstream) => downstream.record_client_status(client_status_code).await,
                None => Ok(()),
            }
        })
    }

    fn is_finalized(&self) -> bool {
        self.downstream
            .as_ref()
            .is_none_or(MiddlewareBodyAuthority::is_finalized)
    }

    fn close(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        self.stream.take();
        Box::pin(async move {
            if let Some(downstream) = self.downstream.take() {
                downstream.close().await;
            }
        })
    }
}

fn take_error(slot: &Mutex<Option<MiddlewareError>>) -> Option<MiddlewareError> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

fn validate_terminal_end(
    terminal: Result<Option<Vec<u8>>, RpcError>,
    downstream_error: &Mutex<Option<MiddlewareError>>,
) -> Result<(), MiddlewareError> {
    // 只有插件在终帧后继续输出才属于状态无效；RPC 传输失败保持为调用故障，
    // 两条失败路径都不能覆盖已经记录的下游域错误
    match terminal {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(take_error(downstream_error).unwrap_or(MiddlewareError::InvalidState)),
        Err(error) => Err(take_error(downstream_error).unwrap_or_else(|| rpc_error(error))),
    }
}

const fn wire_transport(
    transport: gateway_core::engine::execution::ClientTransport,
) -> WireMiddlewareTransport {
    match transport {
        gateway_core::engine::execution::ClientTransport::HttpJson => {
            WireMiddlewareTransport::HttpJson
        }
        gateway_core::engine::execution::ClientTransport::HttpSse => {
            WireMiddlewareTransport::HttpSse
        }
        gateway_core::engine::execution::ClientTransport::WebSocket => {
            WireMiddlewareTransport::WebSocket
        }
        gateway_core::engine::execution::ClientTransport::InternalProbe => {
            WireMiddlewareTransport::Internal
        }
    }
}

fn valid_plugin_frame(framing: MiddlewareFraming, bytes: &[u8]) -> bool {
    match framing {
        MiddlewareFraming::JsonDocument => {
            serde_json::from_slice::<serde::de::IgnoredAny>(bytes).is_ok()
        }
        MiddlewareFraming::SseEvent => {
            (bytes.ends_with(b"\n\n") || bytes.ends_with(b"\r\n\r\n"))
                && std::str::from_utf8(bytes).is_ok_and(|event| parse_sse_events(event).is_ok())
        }
        MiddlewareFraming::RawBytes => true,
    }
}
