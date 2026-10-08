//! 插件中间件回调的调用状态、后续处理委托与资源回收

mod body;
mod execution;
mod facts;
mod headers;
mod registry;
pub use registry::MiddlewareCallbacks;

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use gateway_core::engine::{
    execution::ClientTransport,
    middleware::{
        MiddlewareBody, MiddlewareContext, MiddlewareError, MiddlewareFraming, MiddlewareHeader,
        MiddlewareNext, MiddlewareRequest, MiddlewareResponseEnvelope,
    },
};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::middleware::{
        BODY_CLOSE_METHOD, BODY_FACTS_METHOD, BODY_READ_METHOD, MiddlewareBodyClose,
        MiddlewareBodyCloseResult, MiddlewareBodyFacts, MiddlewareBodyFactsResult,
        MiddlewareBodyFraming, MiddlewareBodyHandle, MiddlewareBodyRead, MiddlewareBodyReadResult,
        MiddlewareHeader as WireHeader, MiddlewareNextRequest, MiddlewareNextResponse,
        MiddlewareRequestBody, MiddlewareResponseBody, MiddlewareResponseHead, NEXT_METHOD,
    },
};

use crate::RpcReply;

use body::BodyResource;
pub(crate) use body::MiddlewareBodyAuthority;
use headers::{apply_header_mutations, wire_headers};

pub(crate) struct MiddlewareInvocation {
    transport: ClientTransport,
    state: Mutex<InvocationState>,
    downstream_error: Arc<Mutex<Option<MiddlewareError>>>,
}

struct InvocationState {
    closed: bool,
    original_request: MiddlewareRequest,
    next: Option<MiddlewareNext>,
    next_consumed: bool,
    response: Option<DownstreamResponse>,
}

struct DownstreamResponse {
    id: String,
    protocol: String,
    status: u16,
    headers: Vec<MiddlewareHeader>,
    body_handle: String,
    body: Arc<BodyResource>,
    envelope: Option<MiddlewareResponseEnvelope>,
}

pub(crate) struct MiddlewareCompletion {
    pub(crate) protocol: String,
    pub(crate) status: u16,
    pub(crate) headers: Vec<MiddlewareHeader>,
    pub(crate) body: MiddlewareCompletionBody,
    pub(crate) envelope: Option<MiddlewareResponseEnvelope>,
}

pub(crate) enum MiddlewareCompletionBody {
    Empty,
    PassThrough(Box<dyn MiddlewareBody>),
    Stream {
        framing: MiddlewareFraming,
        downstream: Option<MiddlewareBodyAuthority>,
    },
}

impl MiddlewareInvocation {
    pub(crate) fn new(
        context: &MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport: context.transport,
            state: Mutex::new(InvocationState {
                closed: false,
                original_request: request,
                next: Some(next),
                next_consumed: false,
                response: None,
            }),
            downstream_error: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) fn request_parts(&self) -> Result<(String, Vec<WireHeader>, Vec<u8>), PluginFault> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok((
            state.original_request.protocol().to_owned(),
            wire_headers(state.original_request.headers())?,
            state.original_request.body().to_vec(),
        ))
    }

    pub(crate) fn take_downstream_error(&self) -> Option<MiddlewareError> {
        self.downstream_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn close(&self) {
        let body = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.response.take().map(|response| response.body)
        };
        if let Some(body) = body {
            body.close_detached();
        }
    }

    pub(crate) async fn call(
        self: &Arc<Self>,
        method: &str,
        params: serde_json::Value,
        payload: Vec<u8>,
        maximum_payload: usize,
    ) -> Result<RpcReply, PluginFault> {
        if payload.len() > maximum_payload {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "middleware payload exceeds its limit",
            ));
        }
        match method {
            NEXT_METHOD => self.call_next(params, payload).await,
            BODY_READ_METHOD => self.call_body_read(params, payload, maximum_payload).await,
            BODY_FACTS_METHOD => self.call_body_facts(params, payload, maximum_payload).await,
            BODY_CLOSE_METHOD => self.call_body_close(params, payload).await,
            _ => Err(PluginFault::new(
                ErrorCode::Unsupported,
                "middleware method is unsupported",
            )),
        }
    }

    async fn call_next(
        self: &Arc<Self>,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        let request: MiddlewareNextRequest =
            serde_json::from_value(params).map_err(|_| invalid())?;
        let (next, original) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed || state.next_consumed {
                return Err(conflict());
            }
            state.next_consumed = true;
            let next = state.next.take().ok_or_else(conflict)?;
            (next, state.original_request.clone())
        };
        let request = self.apply_next_request(original, request, payload)?;
        let response = match next.run(request).await {
            Ok(response) => response,
            Err(error) => {
                let fault = PluginFault::new(ErrorCode::Fault, "middleware terminal failed");
                self.record_downstream_error(error);
                return Err(fault);
            }
        };
        let (protocol, status, headers, body, envelope) = response.into_parts();
        validate_protocol(&protocol)?;
        validate_status(status)?;
        let response_headers = wire_headers(&headers)?;
        let metadata = envelope
            .as_ref()
            .map(|envelope| Box::new(facts::metadata(envelope.metadata())));
        let expected_framing = framing_for_response(self.transport, status);
        let response_id = uuid::Uuid::new_v4().to_string();
        let body_handle = uuid::Uuid::new_v4().to_string();
        let body = BodyResource::new(expected_framing, Arc::clone(&self.downstream_error), body);
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed || state.response.is_some() {
                drop(state);
                body.close_detached();
                return Err(resource_unavailable());
            }
            state.response = Some(DownstreamResponse {
                id: response_id.clone(),
                protocol: protocol.clone(),
                status,
                headers,
                body_handle: body_handle.clone(),
                body,
                envelope,
            });
        }
        let result = MiddlewareNextResponse {
            metadata,
            response: response_id,
            protocol,
            status,
            headers: response_headers,
            body: Some(MiddlewareBodyHandle {
                handle: body_handle,
                framing: wire_framing(expected_framing),
            }),
        };
        Ok(RpcReply {
            result: serde_json::to_value(result).map_err(|_| invalid())?,
            payload: Vec::new(),
        })
    }

    fn apply_next_request(
        &self,
        original: MiddlewareRequest,
        request: MiddlewareNextRequest,
        payload: Vec<u8>,
    ) -> Result<MiddlewareRequest, PluginFault> {
        if request.settings.is_some() || request.capabilities.is_some() {
            return Err(PluginFault::new(
                ErrorCode::Unsupported,
                "execution setting and capability rewrites are not wired at this boundary",
            ));
        }
        let (mut protocol, mut headers, original_body) = original.into_parts();
        if let Some(replacement) = request.protocol {
            validate_protocol(&replacement)?;
            protocol = replacement;
        }
        if !request.header_mutations.is_empty() {
            apply_header_mutations(&mut headers, &request.header_mutations)?;
        }
        let body = match request.body {
            MiddlewareRequestBody::Preserve if payload.is_empty() => original_body,
            MiddlewareRequestBody::Preserve => return Err(invalid()),
            MiddlewareRequestBody::Replace => Bytes::from(payload),
        };
        Ok(MiddlewareRequest::new(protocol, headers, body))
    }

    async fn call_body_facts(
        &self,
        params: serde_json::Value,
        payload: Vec<u8>,
        maximum_payload: usize,
    ) -> Result<RpcReply, PluginFault> {
        if !payload.is_empty() {
            return Err(invalid());
        }
        let request: MiddlewareBodyFacts = serde_json::from_value(params).map_err(|_| invalid())?;
        let facts = self.body(&request.handle)?.facts(request.source_id).await?;
        let present = facts.is_some();
        let payload = facts.unwrap_or_default();
        if payload.len() > maximum_payload {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "middleware facts exceed the payload limit",
            ));
        }
        Ok(RpcReply {
            result: serde_json::to_value(MiddlewareBodyFactsResult { present })
                .map_err(|_| invalid())?,
            payload,
        })
    }

    async fn call_body_read(
        &self,
        params: serde_json::Value,
        payload: Vec<u8>,
        maximum_payload: usize,
    ) -> Result<RpcReply, PluginFault> {
        if !payload.is_empty() {
            return Err(invalid());
        }
        let request: MiddlewareBodyRead = serde_json::from_value(params).map_err(|_| invalid())?;
        let maximum = usize::try_from(request.maximum_bytes).map_err(|_| invalid())?;
        if maximum == 0 || maximum > maximum_payload {
            return Err(invalid());
        }
        let body = self.body(&request.handle)?;
        let frame = body.read(maximum).await?;
        let (result, payload) = match frame {
            Some(frame) => (
                MiddlewareBodyReadResult {
                    framing: wire_framing(frame.framing),
                    source_id: frame.source_id,
                    eof: false,
                    terminal: frame.terminal,
                },
                frame.bytes.to_vec(),
            ),
            None => (
                MiddlewareBodyReadResult {
                    framing: wire_framing(body.expected_framing()),
                    source_id: 0,
                    eof: true,
                    terminal: false,
                },
                Vec::new(),
            ),
        };
        Ok(RpcReply {
            result: serde_json::to_value(result).map_err(|_| invalid())?,
            payload,
        })
    }

    async fn call_body_close(
        &self,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        if !payload.is_empty() {
            return Err(invalid());
        }
        let request: MiddlewareBodyClose = serde_json::from_value(params).map_err(|_| invalid())?;
        let body = self.body(&request.handle)?;
        body.close().await;
        Ok(RpcReply {
            result: serde_json::to_value(MiddlewareBodyCloseResult {}).map_err(|_| invalid())?,
            payload: Vec::new(),
        })
    }

    fn body(&self, handle: &str) -> Result<Arc<BodyResource>, PluginFault> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let response = state.response.as_ref().ok_or_else(resource_unavailable)?;
        if state.closed || response.body_handle != handle {
            return Err(resource_unavailable());
        }
        Ok(Arc::clone(&response.body))
    }

    pub(crate) async fn resolve_response(
        &self,
        response: MiddlewareResponseHead,
    ) -> Result<MiddlewareCompletion, MiddlewareError> {
        let downstream = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match response.response.as_deref() {
                Some(id) => {
                    let stored = state
                        .response
                        .as_mut()
                        .ok_or(MiddlewareError::InvalidState)?;
                    if stored.id != id {
                        return Err(MiddlewareError::InvalidState);
                    }
                    Some((
                        stored.protocol.clone(),
                        stored.status,
                        stored.headers.clone(),
                        stored.body_handle.clone(),
                        Arc::clone(&stored.body),
                        stored.envelope.take(),
                    ))
                }
                None => {
                    if state.response.is_some() {
                        return Err(MiddlewareError::InvalidState);
                    }
                    None
                }
            }
        };
        let (mut protocol, mut status, mut headers) = match &downstream {
            Some((protocol, status, headers, ..)) => (protocol.clone(), *status, headers.clone()),
            None => (
                response
                    .protocol
                    .clone()
                    .ok_or(MiddlewareError::InvalidState)?,
                response.status.ok_or(MiddlewareError::InvalidState)?,
                Vec::new(),
            ),
        };
        if let Some(replacement) = response.protocol {
            validate_protocol(&replacement).map_err(|_| MiddlewareError::InvalidState)?;
            protocol = replacement;
        }
        if let Some(replacement) = response.status {
            validate_status(replacement).map_err(|_| MiddlewareError::InvalidState)?;
            status = replacement;
        }
        if !response.header_mutations.is_empty() {
            apply_header_mutations(&mut headers, &response.header_mutations)
                .map_err(|_| MiddlewareError::InvalidState)?;
        }
        let body = match response.body {
            MiddlewareResponseBody::PassThrough { body } => {
                let (_, _, _, body_handle, stored_body, _) =
                    downstream.as_ref().ok_or(MiddlewareError::InvalidState)?;
                if body_handle != &body.handle
                    || wire_framing(stored_body.expected_framing()) != body.framing
                {
                    return Err(MiddlewareError::InvalidState);
                }
                MiddlewareCompletionBody::PassThrough(
                    stored_body
                        .take_unread()
                        .await
                        .ok_or(MiddlewareError::InvalidState)?,
                )
            }
            MiddlewareResponseBody::Empty => {
                // 调用过 next 后不能靠关闭下游流伪装成已完成；需要丢弃正文时也必须
                // 通过 preserving mapper 拉取到底，才能保留计量、终态和取消合同
                if downstream.is_some() {
                    return Err(MiddlewareError::InvalidState);
                }
                MiddlewareCompletionBody::Empty
            }
            MiddlewareResponseBody::Stream { framing } => {
                let framing = core_framing(framing);
                let expected = downstream.as_ref().map_or_else(
                    || framing_for_response(self.transport, status),
                    |(_, _, _, _, body, _)| body.expected_framing(),
                );
                if framing != expected {
                    return Err(MiddlewareError::InvalidState);
                }
                MiddlewareCompletionBody::Stream {
                    framing,
                    downstream: downstream.as_ref().map(|(_, _, _, _, body, _)| {
                        MiddlewareBodyAuthority::new(Arc::clone(body))
                    }),
                }
            }
        };
        Ok(MiddlewareCompletion {
            protocol,
            status,
            headers,
            body,
            envelope: downstream.and_then(|(_, _, _, _, _, envelope)| envelope),
        })
    }

    fn record_downstream_error(&self, error: MiddlewareError) {
        let mut stored = self
            .downstream_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stored.is_none() {
            *stored = Some(error);
        }
    }

    pub(crate) fn downstream_error_slot(&self) -> Arc<Mutex<Option<MiddlewareError>>> {
        Arc::clone(&self.downstream_error)
    }
}

impl Drop for MiddlewareInvocation {
    fn drop(&mut self) {
        self.close();
    }
}

fn validate_protocol(protocol: &str) -> Result<(), PluginFault> {
    if protocol.is_empty() || protocol.len() > 64 || protocol.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(())
}

fn validate_status(status: u16) -> Result<(), PluginFault> {
    if !(100..=599).contains(&status) {
        return Err(invalid());
    }
    Ok(())
}

const fn framing_for_response(transport: ClientTransport, status: u16) -> MiddlewareFraming {
    match transport {
        // WebSocket 的失败也作为 JSON 消息交付，不使用 HTTP 错误正文的字节流边界
        ClientTransport::WebSocket => MiddlewareFraming::JsonDocument,
        _ if status < 200 || status >= 300 => MiddlewareFraming::RawBytes,
        ClientTransport::HttpSse => MiddlewareFraming::SseEvent,
        ClientTransport::HttpJson | ClientTransport::InternalProbe => {
            MiddlewareFraming::JsonDocument
        }
    }
}

const fn wire_framing(framing: MiddlewareFraming) -> MiddlewareBodyFraming {
    match framing {
        MiddlewareFraming::JsonDocument => MiddlewareBodyFraming::JsonDocument,
        MiddlewareFraming::SseEvent => MiddlewareBodyFraming::SseEvent,
        MiddlewareFraming::RawBytes => MiddlewareBodyFraming::RawBytes,
    }
}

pub(crate) const fn core_framing(framing: MiddlewareBodyFraming) -> MiddlewareFraming {
    match framing {
        MiddlewareBodyFraming::JsonDocument => MiddlewareFraming::JsonDocument,
        MiddlewareBodyFraming::SseEvent => MiddlewareFraming::SseEvent,
        MiddlewareBodyFraming::RawBytes => MiddlewareFraming::RawBytes,
    }
}

fn resource_unavailable() -> PluginFault {
    PluginFault::new(ErrorCode::Conflict, "middleware resource is unavailable")
}

fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "middleware input is invalid")
}

fn conflict() -> PluginFault {
    PluginFault::new(ErrorCode::Conflict, "middleware next was already consumed")
}
