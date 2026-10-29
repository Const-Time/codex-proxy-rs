//! Only the outer API response may acknowledge Core's atomic delivery batch.
use crate::openai::error::{engine_error_response, gateway_error_response, openai_error_response};
use axum::response::IntoResponse;
use axum::{
    body::{Body, Bytes},
    http::{HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use futures::{StreamExt, stream};
use gateway_core::{
    engine::{CommitRequirement, middleware::*},
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseHeader},
    lifecycle::ConnectionGuard,
    operation::ProviderSessionState,
};
use gateway_protocol::openai::sse::{
    DONE_SSE_FRAME, parse_sse_events, response_failed_sse_event_with_id,
};
use std::{collections::VecDeque, convert::Infallible, time::Duration};

const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATCH_FRAMES: usize = 1024;
struct Batch {
    bytes: VecDeque<Bytes>,
    requirement: Option<CommitRequirement>,
    terminal: bool,
    facts: Vec<ProviderEvent>,
}
pub(super) struct Delivery {
    body: Option<Box<dyn MiddlewareBody>>,
    chat: Option<crate::openai::chat_completions::ChatProjection>,
    status: u16,
    valid: bool,
    streaming: bool,
    websocket: bool,
    original_response_id: Option<String>,
    provider_state: Option<ProviderSessionState>,
    committed: bool,
    finished: bool,
    pending: VecDeque<Bytes>,
    response_id: Option<String>,
    wire_terminal: bool,
    _connection: Option<Box<dyn ConnectionGuard>>,
}
impl Delivery {
    pub(super) fn new(
        response: MiddlewareResponse,
        format: super::WireFormat,
        connection: Option<Box<dyn ConnectionGuard>>,
    ) -> (Self, Vec<MiddlewareHeader>) {
        let (protocol, status, headers, body, _) = response.into_parts();
        let delivery = Self {
            body: Some(body),
            chat: None,
            status,
            valid: protocol == "openai" && (200..=599).contains(&status),
            streaming: format.streaming() && (200..300).contains(&status),
            websocket: format.websocket(),
            original_response_id: None,
            provider_state: None,
            committed: false,
            finished: false,
            pending: VecDeque::new(),
            response_id: None,
            wire_terminal: false,
            _connection: connection,
        };
        (delivery, headers)
    }
    pub(super) fn validate(&self) -> Result<(), MiddlewareError> {
        if self.valid {
            Ok(())
        } else {
            Err(MiddlewareError::InvalidState)
        }
    }
    pub(super) async fn record_client_status(&mut self, status: u16) {
        if let Ok(body) = self.body() {
            let _ = body.record_client_status(status).await;
        }
    }
    pub(super) fn status(&self) -> u16 {
        self.status
    }
    pub(super) fn take_output(&mut self) -> (Vec<Bytes>, bool) {
        (self.pending.drain(..).collect(), self.finished)
    }
    pub(super) fn replay(&self) -> Option<(String, Option<ProviderSessionState>)> {
        self.original_response_id
            .as_ref()
            .map(|id| (id.clone(), self.provider_state.clone()))
    }
    pub(super) async fn document(&mut self) -> Result<Vec<u8>, MiddlewareError> {
        let mut bytes = Vec::new();
        let mut batches = Vec::new();
        while let Some(batch) = self.batch(false).await? {
            for chunk in &batch.bytes {
                if bytes.len().saturating_add(chunk.len()) > MAX_BATCH_BYTES {
                    return Err(MiddlewareError::InvalidState);
                }
                bytes.extend_from_slice(chunk);
            }
            let terminal = batch.terminal;
            batches.push(batch);
            if terminal {
                // JSON's Core terminal is exhausted but deliberately uncommitted.
                if self.body()?.next_frame().await?.is_some() {
                    return Err(MiddlewareError::InvalidState);
                }
                break;
            }
        }
        if ((200..300).contains(&self.status) || self.websocket)
            && serde_json::from_slice::<serde_json::Value>(&bytes).is_err()
        {
            return Err(MiddlewareError::InvalidState);
        }
        if self.websocket
            && !serde_json::from_slice::<serde_json::Value>(&bytes)
                .map_err(|_| MiddlewareError::InvalidState)?
                .is_object()
        {
            return Err(MiddlewareError::InvalidState);
        }
        if (200..300).contains(&self.status)
            && let Some(chat) = self.chat.as_ref()
        {
            bytes = chat.document(&bytes)?;
        }
        for batch in &batches {
            self.acknowledge(batch).await?;
        }
        if !self.is_finalized() {
            return Err(MiddlewareError::InvalidState);
        }
        Ok(bytes)
    }
    /// Return validated protocol projection and original read-only facts without
    /// acknowledging the original Core barrier. The native RPC encoder is outermost.
    pub(super) async fn model_batch(
        &mut self,
        raw_json: bool,
    ) -> Result<Option<(gateway_core::engine::CoordinatedEvent, bool)>, MiddlewareError> {
        self.validate()?;
        if self.finished {
            return Ok(None);
        }
        let Some(mut batch) = self.batch(true).await? else {
            if !self.is_finalized() || (self.streaming && !self.wire_terminal) {
                return Err(MiddlewareError::InvalidState);
            }
            self.finished = true;
            return Ok(None);
        };
        let requirement = batch
            .requirement
            .unwrap_or(CommitRequirement::AlreadyCommitted);
        let mut wires = vec![];
        if self.streaming {
            if (batch.terminal && !self.wire_terminal)
                || (requirement == CommitRequirement::CommitBeforeDelivery
                    && batch.bytes.is_empty())
            {
                return Err(MiddlewareError::InvalidState);
            }
            for bytes in batch.bytes {
                let (wire, _) = super::attempt::output::sse(bytes)?;
                wires.push(wire);
            }
        } else {
            let mut bytes = vec![];
            loop {
                for chunk in &batch.bytes {
                    if bytes.len().saturating_add(chunk.len()) > MAX_BATCH_BYTES {
                        return Err(MiddlewareError::InvalidState);
                    }
                    bytes.extend_from_slice(chunk);
                }
                if batch.terminal {
                    break;
                }
                let next = self
                    .batch(true)
                    .await?
                    .ok_or(MiddlewareError::InvalidState)?;
                batch.facts.extend(next.facts);
                batch.bytes = next.bytes;
                batch.terminal = next.terminal;
                if next.requirement != batch.requirement {
                    return Err(MiddlewareError::InvalidState);
                }
            }
            let wire = if raw_json || !(200..300).contains(&self.status) {
                ProtocolWireEvent::raw_json("openai", Bytes::from(bytes))
                    .map_err(|_| MiddlewareError::InvalidState)?
            } else {
                let response: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|_| MiddlewareError::InvalidState)?;
                ProtocolWireEvent::json(
                    "openai",
                    Some("response.completed".into()),
                    serde_json::json!({"type":"response.completed","response":response}),
                )
                .map_err(|_| MiddlewareError::InvalidState)?
            };
            wires.push(wire);
        }
        let last_wire = wires.pop();
        let last_fact = last_wire.as_ref().and_then(|_| batch.facts.pop());
        let mut events = batch
            .facts
            .into_iter()
            .map(|event| event.with_wire_projection(None))
            .collect::<Vec<_>>();
        events.extend(wires.into_iter().map(ProviderEvent::wire));
        if let Some(wire) = last_wire {
            events.push(match last_fact {
                Some(event) => event.with_wire_projection(Some(wire)),
                None => ProviderEvent::wire(wire),
            });
        }
        let event = gateway_core::engine::CoordinatedEvent::try_batch(events, requirement)?;
        let terminal = batch.terminal;
        if terminal && requirement == CommitRequirement::AlreadyCommitted {
            self.model_commit(terminal, requirement).await?;
        }
        Ok(Some((event, terminal)))
    }
    pub(super) async fn model_commit(
        &mut self,
        terminal: bool,
        requirement: CommitRequirement,
    ) -> Result<(), MiddlewareError> {
        match (self.committed, requirement) {
            (false, CommitRequirement::CommitBeforeDelivery) => {
                let status = self.status;
                self.body()?.commit_downstream(Some(status)).await?;
                self.committed = true;
            }
            (true, CommitRequirement::AlreadyCommitted) => {}
            (false, CommitRequirement::AlreadyCommitted) if self.is_finalized() => {}
            _ => return Err(MiddlewareError::InvalidState),
        }
        if terminal {
            if self.body()?.next_frame().await?.is_some() || !self.is_finalized() {
                return Err(MiddlewareError::InvalidState);
            }
            self.finished = true;
        }
        Ok(())
    }
    fn observe_message(&mut self, payload: &[u8]) -> Result<(), MiddlewareError> {
        if self.wire_terminal {
            return Err(MiddlewareError::InvalidState);
        }
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| MiddlewareError::InvalidState)?;
        let kind = value
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(MiddlewareError::InvalidState)?;
        self.wire_terminal = matches!(
            kind,
            "response.completed" | "response.incomplete" | "response.failed" | "error"
        );
        Ok(())
    }
    fn validate_message_identity(&self, payloads: &VecDeque<Bytes>) -> Result<(), MiddlewareError> {
        if !self.websocket {
            return Ok(());
        }
        let Some(expected) = self.original_response_id.as_deref() else {
            return Ok(());
        };
        for payload in payloads {
            let value: serde_json::Value =
                serde_json::from_slice(payload).map_err(|_| MiddlewareError::InvalidState)?;
            for id in [value.pointer("/response/id"), value.get("response_id")] {
                if id.is_some_and(|v| v.as_str() != Some(expected)) {
                    return Err(MiddlewareError::InvalidState);
                }
            }
        }
        Ok(())
    }
    fn body(&mut self) -> Result<&mut (dyn MiddlewareBody + 'static), MiddlewareError> {
        self.body
            .as_deref_mut()
            .ok_or(MiddlewareError::InvalidState)
    }
    pub(super) fn is_finalized(&self) -> bool {
        self.body.as_ref().is_none_or(|body| body.is_finalized())
    }
    pub(super) fn model_finalized(&self) -> bool {
        self.finished && self.is_finalized()
    }
    pub(super) async fn close(&mut self) {
        if let Some(body) = self.body.take() {
            body.close().await;
        }
    }

    async fn batch(&mut self, capture_events: bool) -> Result<Option<Batch>, MiddlewareError> {
        let mut bytes = VecDeque::new();
        let mut facts = vec![];
        let mut length = 0usize;
        let mut host_events = false;
        for _ in 0..MAX_BATCH_FRAMES {
            let Some(frame) = self.body()?.next_frame().await? else {
                if host_events || !self.is_finalized() {
                    return Err(MiddlewareError::InvalidState);
                }
                return Ok((!bytes.is_empty()).then_some(Batch {
                    bytes,
                    requirement: None,
                    terminal: true,
                    facts,
                }));
            };
            let (payload, framing, terminal, envelope) = frame.into_parts();
            host_events |= envelope.is_some();
            if capture_events && let Some(envelope) = envelope.as_ref() {
                facts.extend(envelope.events().cloned());
            }
            length = length
                .checked_add(payload.len())
                .ok_or(MiddlewareError::InvalidState)?;
            if length > MAX_BATCH_BYTES {
                return Err(MiddlewareError::InvalidState);
            }
            if !payload.is_empty() {
                if self.streaming {
                    if self.websocket {
                        if framing != MiddlewareFraming::JsonDocument {
                            return Err(MiddlewareError::InvalidState);
                        }
                        self.observe_message(&payload)?;
                    } else {
                        if framing != MiddlewareFraming::SseEvent {
                            return Err(MiddlewareError::InvalidState);
                        }
                        self.observe_sse(&payload)?;
                    }
                }
                bytes.push_back(payload);
            }
            if self.websocket
                && let Some(envelope) = envelope.as_ref()
            {
                for event in envelope.events() {
                    if let Some(state) = event.session_update() {
                        self.provider_state = Some(state.clone());
                    }
                    for fact in event.canonical_facts() {
                        if let GatewayEvent::Started(meta) | GatewayEvent::Completed(meta) = fact {
                            self.original_response_id = Some(meta.response_id().to_owned());
                        }
                    }
                    // Match the original WS encoder's wire-first response identity.
                    if let Some(id) = event
                        .wire_event()
                        .and_then(|wire| {
                            wire.data()
                                .pointer("/response/id")
                                .or_else(|| wire.data().get("response_id"))
                        })
                        .and_then(|id| id.as_str())
                    {
                        self.original_response_id = Some(id.to_owned());
                    }
                }
            }
            let requirement = envelope
                .as_ref()
                .and_then(MiddlewareFrameEnvelope::batch_end);
            if requirement.is_some() || terminal {
                if host_events && requirement.is_none() {
                    return Err(MiddlewareError::InvalidState);
                }
                self.validate_message_identity(&bytes)?;
                return Ok(Some(Batch {
                    bytes,
                    requirement,
                    terminal,
                    facts,
                }));
            }
        }
        Err(MiddlewareError::InvalidState)
    }
    fn observe_sse(&mut self, payload: &[u8]) -> Result<(), MiddlewareError> {
        let events = if let Some(chat) = self.chat.as_mut() {
            chat.observe(payload)?
        } else {
            if !(payload.ends_with(b"\n\n") || payload.ends_with(b"\r\n\r\n")) {
                return Err(MiddlewareError::InvalidState);
            }
            let text = std::str::from_utf8(payload).map_err(|_| MiddlewareError::InvalidState)?;
            parse_sse_events(text).map_err(|_| MiddlewareError::InvalidState)?
        };
        for event in events {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data) {
                if let Some(id) = value.pointer("/response/id").and_then(|v| v.as_str()) {
                    self.response_id = Some(id.to_owned());
                }
                let kind = event
                    .event
                    .as_deref()
                    .or_else(|| value.get("type").and_then(|v| v.as_str()));
                self.wire_terminal |= matches!(
                    kind,
                    Some(
                        "response.completed" | "response.incomplete" | "response.failed" | "error"
                    )
                );
            }
        }
        Ok(())
    }
    async fn acknowledge(&mut self, batch: &Batch) -> Result<(), MiddlewareError> {
        match (self.committed, batch.requirement) {
            (false, Some(CommitRequirement::CommitBeforeDelivery)) => {
                let status = self.status;
                self.body()?.commit_downstream(Some(status)).await?;
                self.committed = true;
            }
            (true, Some(CommitRequirement::AlreadyCommitted)) => {}
            (_, None) if self.is_finalized() => {}
            _ => return Err(MiddlewareError::InvalidState),
        }
        Ok(())
    }
    async fn finish_terminal(&mut self) -> Result<(), MiddlewareError> {
        if self.body()?.next_frame().await?.is_some()
            || !self.is_finalized()
            || (self.streaming && !self.wire_terminal)
        {
            return Err(MiddlewareError::InvalidState);
        }
        self.finished = true;
        Ok(())
    }
    pub(super) async fn advance(&mut self) -> Result<(), MiddlewareError> {
        match self.batch(false).await? {
            Some(batch) => {
                if batch.terminal && !self.wire_terminal {
                    return Err(MiddlewareError::InvalidState);
                }
                if !self.committed
                    && batch.requirement == Some(CommitRequirement::CommitBeforeDelivery)
                    && batch.bytes.is_empty()
                {
                    return Err(MiddlewareError::InvalidState);
                }
                let bytes = if let Some(chat) = self.chat.as_mut() {
                    chat.stream(&batch.bytes)?
                } else {
                    batch.bytes.clone()
                };
                self.acknowledge(&batch).await?;
                if batch.terminal {
                    self.finish_terminal().await?;
                }
                self.pending.extend(bytes);
            }
            None => {
                if !self.is_finalized() || !self.wire_terminal {
                    return Err(MiddlewareError::InvalidState);
                }
                self.finished = true;
            }
        }
        if self.finished
            && let Some(chat) = self.chat.as_mut()
        {
            chat.finish()?;
        }
        if self.finished && !self.websocket {
            self.pending
                .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
        }
        Ok(())
    }
}

/// Validate the outer plugin result before acknowledging delivery. All accounting
/// and cleanup remain delegated to the response's original Core body owner.
pub async fn deliver_middleware_response(
    response: MiddlewareResponse,
    streaming: bool,
    connection: Option<Box<dyn ConnectionGuard>>,
) -> Response {
    deliver_with_chat(response, streaming, connection, None).await
}

pub(in crate::openai) async fn deliver_with_chat(
    response: MiddlewareResponse,
    streaming: bool,
    connection: Option<Box<dyn ConnectionGuard>>,
    chat: Option<crate::openai::chat_completions::ChatProjection>,
) -> Response {
    let (mut delivery, headers) = Delivery::new(
        response,
        super::WireFormat::Responses { streaming },
        connection,
    );
    delivery.chat = chat;
    let status = delivery.status;
    let stream = delivery.streaming;
    let result = async {
        delivery.validate()?;
        if stream {
            delivery.advance().await?;
            while delivery.chat.is_some() && delivery.pending.is_empty() && !delivery.finished {
                delivery.advance().await?;
            }
            if delivery.pending.is_empty() {
                return Err(MiddlewareError::InvalidState);
            }
            return Ok(None);
        }
        delivery.document().await.map(Some)
    }
    .await;
    match result {
        Ok(Some(bytes)) => http_response(
            status,
            headers,
            Body::from(bytes),
            if (200..300).contains(&status) {
                "application/json"
            } else {
                "application/octet-stream"
            },
        ),
        Ok(None) => streaming_response(delivery, headers),
        Err(error) => {
            let response = error_response(&error);
            if let Ok(body) = delivery.body() {
                let _ = body.record_client_status(response.status().as_u16()).await;
            }
            delivery.close().await;
            response
        }
    }
}

pub(in crate::openai) fn error_response(error: &MiddlewareError) -> Response {
    match error {
        MiddlewareError::Gateway(error) => gateway_error_response(error),
        MiddlewareError::Engine(error) => engine_error_response(error),
        _ => openai_error_response(
            StatusCode::BAD_GATEWAY,
            "Plugin middleware failed",
            "server_error",
            "middleware_failed",
        )
        .into_response(),
    }
}
fn http_response(
    status: u16,
    headers: Vec<MiddlewareHeader>,
    body: Body,
    content_type: &'static str,
) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    let headers: Vec<_> = headers
        .into_iter()
        .map(|h| {
            let (name, value) = h.into_parts();
            ProviderResponseHeader::new(name, value)
        })
        .collect();
    let options = crate::openai::responses::response_connection_options(&headers);
    for header in headers {
        if crate::openai::responses::response_header_is_forwardable(header.name(), &options)
            && let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(header.name().as_bytes()),
                HeaderValue::from_bytes(header.value()),
            )
        {
            response.headers_mut().append(name, value);
        }
    }
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));
    if content_type == "text/event-stream" {
        response.headers_mut().insert(
            "cache-control",
            HeaderValue::from_static("no-cache, no-transform"),
        );
        response
            .headers_mut()
            .insert("x-accel-buffering", HeaderValue::from_static("no"));
    }
    response
}

fn streaming_response(delivery: Delivery, headers: Vec<MiddlewareHeader>) -> Response {
    let status = delivery.status;
    let output = Box::pin(stream::unfold(delivery, |mut state| async move {
        loop {
            if let Some(chunk) = state.pending.pop_front() {
                return Some((Ok::<Bytes, Infallible>(chunk), state));
            }
            if state.finished {
                return None;
            }
            if state.advance().await.is_err() {
                state.close().await;
                if state.chat.is_some() {
                    state.pending.push_back(Bytes::from_static(b"data: {\"error\":{\"type\":\"server_error\",\"code\":\"invalid_upstream_response\",\"message\":\"Chat response could not be delivered\"}}\n\n"));
                } else {
                    state
                        .pending
                        .push_back(Bytes::from(response_failed_sse_event_with_id(
                            state.response_id.as_deref(),
                            "server_error",
                            "middleware_failed",
                            "Plugin response could not be delivered",
                        )));
                }
                state
                    .pending
                    .push_back(Bytes::from_static(DONE_SSE_FRAME.as_bytes()));
                state.finished = true;
            }
        }
    }));
    let body = Body::from_stream(stream::unfold(output, |mut output| async move {
        let next = tokio::select! { biased;
            next = output.next() => next?,
            () = tokio::time::sleep(Duration::from_secs(15)) => Ok(Bytes::from_static(b": keep-alive\n\n")),
        };
        Some((next, output))
    }));
    http_response(status, headers, body, "text/event-stream")
}
