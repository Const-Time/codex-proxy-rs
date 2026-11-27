//! Validate projected bytes before Core can acknowledge downstream delivery.
use super::super::messages_sse::{self, MessagesSse};
use bytes::{Bytes, BytesMut};
use gateway_core::{
    engine::{
        attempt_middleware::{AttemptOutput, AttemptResponseHead},
        middleware::*,
    },
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseHeader},
};
use gateway_protocol::{anthropic::ANTHROPIC_PROTOCOL, openai::sse::parse_sse_events};
use std::collections::VecDeque;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_FRAMES: usize = 1024;
struct Output {
    body: Option<Box<dyn MiddlewareBody>>,
    pending: VecDeque<ProviderEvent>,
    streaming: bool,
    raw_error: bool,
    raw_json: bool,
    websocket: bool,
    status: u16,
    original_response_id: Option<String>,
    wire_terminal: bool,
    response_chain: bool,
    finished: bool,
    messages: Option<MessagesSse>,
    protocol: String,
}
pub(super) fn decode(
    response: MiddlewareResponse,
    format: super::super::WireFormat,
) -> Result<AttemptOutput, MiddlewareError> {
    let (protocol, status, headers, body, _) = response.into_parts();
    if protocol != format.protocol() || !(200..=599).contains(&status) {
        return Err(MiddlewareError::InvalidState);
    }
    let output = Output {
        body: Some(body),
        pending: VecDeque::new(),
        streaming: format.streaming() && (200..300).contains(&status),
        raw_json: format.raw_json() || protocol != "openai",
        websocket: format.websocket(),
        status,
        original_response_id: None,
        raw_error: !(200..300).contains(&status),
        wire_terminal: false,
        response_chain: false,
        finished: false,
        messages: (protocol == ANTHROPIC_PROTOCOL).then(MessagesSse::default),
        protocol,
    };
    let head = AttemptResponseHead {
        status_code: status,
        headers: headers
            .into_iter()
            .map(|h| {
                let (name, value) = h.into_parts();
                ProviderResponseHeader::new(name, value)
            })
            .collect(),
    };
    Ok(AttemptOutput {
        head: Some(head),
        events: Box::pin(futures::stream::unfold(output, |mut state| async move {
            match state.next().await {
                Ok(Some(event)) => Some((Ok(event), state)),
                Ok(None) => None,
                Err(error) => {
                    state.finished = true;
                    state.close().await;
                    Some((Err(error), state))
                }
            }
        })),
    })
}
impl Output {
    fn body(&mut self) -> Result<&mut (dyn MiddlewareBody + 'static), MiddlewareError> {
        self.body
            .as_deref_mut()
            .ok_or(MiddlewareError::InvalidState)
    }
    async fn close(&mut self) {
        if let Some(body) = self.body.take() {
            body.close().await;
        }
    }
    async fn next(&mut self) -> Result<Option<ProviderEvent>, MiddlewareError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
        if self.finished {
            return Ok(None);
        }
        if !self.streaming {
            return self.document().await.map(Some);
        }
        let mut bytes = 0usize;
        let mut batch = VecDeque::new();
        let mut facts = vec![];
        let mut response_boundary = false;
        let messages_opening = self.messages.as_ref().is_some_and(|sse| !sse.started);
        let mut opening_payloads = Vec::new();
        let mut opening_start = None;
        for _ in 0..MAX_FRAMES {
            let Some(frame) = self.body()?.next_frame().await? else {
                if !self.wire_terminal || !batch.is_empty() || !opening_payloads.is_empty() {
                    return Err(MiddlewareError::InvalidState);
                }
                self.finished = true;
                self.close().await;
                return Ok(None);
            };
            let transformed = frame.transformed();
            let (payload, framing, terminal, envelope) = frame.into_parts();
            bytes = bytes.saturating_add(payload.len());
            if bytes > MAX_BYTES {
                return Err(MiddlewareError::InvalidState);
            }
            let boundary = envelope.is_some() || !transformed;
            let mut source_wire = false;
            if let Some(envelope) = envelope {
                source_wire =
                    messages_opening && envelope.events().any(|event| event.wire_event().is_some());
                response_boundary |= envelope.events().any(ProviderEvent::is_response_boundary);
                self.response_chain |= response_boundary;
                if self.websocket {
                    for fact in envelope.events().flat_map(|event| event.canonical_facts()) {
                        if let GatewayEvent::Started(meta) | GatewayEvent::Completed(meta) = fact {
                            self.original_response_id = Some(meta.response_id().to_owned());
                        }
                    }
                }
                facts.extend(
                    envelope
                        .events()
                        .flat_map(|e| e.canonical_facts().iter().cloned()),
                );
            }
            let wire = if messages_opening {
                if !payload.is_empty() {
                    if framing != MiddlewareFraming::SseEvent {
                        return Err(MiddlewareError::InvalidState);
                    }
                    let start = self
                        .messages
                        .as_mut()
                        .ok_or(MiddlewareError::InvalidState)?
                        .observe(&payload)?
                        .into_iter()
                        .find(messages_sse::Event::is_start);
                    opening_start = start.or(opening_start);
                    opening_payloads.push(payload);
                }
                // Observations may precede the source's first wire frame. Keep
                // their preludes until the opening can commit with its source.
                if source_wire || terminal || (boundary && opening_start.is_some()) {
                    let start = opening_start.take().ok_or(MiddlewareError::InvalidState)?;
                    let payload = if let [only] = opening_payloads.as_slice() {
                        only.clone()
                    } else {
                        let mut merged = BytesMut::with_capacity(bytes);
                        for chunk in &opening_payloads {
                            merged.extend_from_slice(chunk);
                        }
                        merged.freeze()
                    };
                    Some((start.into_wire(payload)?, false))
                } else {
                    None
                }
            } else if payload.is_empty() {
                None
            } else {
                let projected = if self.websocket {
                    if framing != MiddlewareFraming::JsonDocument {
                        return Err(MiddlewareError::InvalidState);
                    }
                    websocket_message(payload)?
                } else {
                    if framing != MiddlewareFraming::SseEvent {
                        return Err(MiddlewareError::InvalidState);
                    }
                    match self.messages.as_mut() {
                        Some(messages) => (messages_wire(messages, payload)?, false),
                        None => sse_for_protocol(payload, &self.protocol)?,
                    }
                };
                Some(projected)
            };
            if let Some((wire, is_terminal)) = wire {
                if self.wire_terminal && !self.response_chain {
                    return Err(MiddlewareError::InvalidState);
                }
                if self.response_chain && wire.data()["type"] == "response.created" {
                    self.wire_terminal = false;
                }
                self.wire_terminal |= is_terminal;
                let mut event =
                    ProviderEvent::canonical_with_wire(std::mem::take(&mut facts), wire);
                if std::mem::take(&mut response_boundary) {
                    event = event.with_response_boundary();
                }
                batch.push_back(event);
            } else if boundary && !messages_opening {
                batch.extend(
                    std::mem::take(&mut facts)
                        .into_iter()
                        .map(ProviderEvent::canonical),
                );
            }
            if terminal {
                if self.protocol != "openai" {
                    // The source holds the provider's terminal canonical event and receipts.
                    let completed = facts
                        .iter()
                        .any(|event| matches!(event, GatewayEvent::Completed(_)))
                        || batch.iter().any(|event| {
                            event
                                .canonical_facts()
                                .iter()
                                .any(|fact| matches!(fact, GatewayEvent::Completed(_)))
                        });
                    self.wire_terminal |= completed;
                }
                if !self.wire_terminal {
                    return Err(MiddlewareError::InvalidState);
                }
                // Exhaust the actual source before releasing a rewritten terminal.
                let mut exhausted = false;
                for _ in 0..MAX_FRAMES {
                    match self.body()?.next_frame().await? {
                        Some(tail) if tail.bytes().is_empty() => {}
                        Some(_) => return Err(MiddlewareError::InvalidState),
                        None => {
                            exhausted = true;
                            break;
                        }
                    }
                }
                if !exhausted {
                    return Err(MiddlewareError::InvalidState);
                }
                self.finished = true;
                self.close().await;
            }
            if (boundary || terminal) && !batch.is_empty() {
                if self.websocket
                    && let Some(expected) = self.original_response_id.as_deref()
                {
                    for event in &batch {
                        let Some(data) = event.wire_event().map(|wire| wire.data()) else {
                            continue;
                        };
                        for id in [data.pointer("/response/id"), data.get("response_id")] {
                            if id.is_some_and(|id| id.as_str() != Some(expected)) {
                                return Err(MiddlewareError::InvalidState);
                            }
                        }
                    }
                }
                self.pending = batch;
                return Ok(self.pending.pop_front());
            }
            if self.finished {
                return Ok(None);
            }
        }
        Err(MiddlewareError::InvalidState)
    }
    async fn document(&mut self) -> Result<ProviderEvent, MiddlewareError> {
        let mut bytes = vec![];
        let mut facts: Vec<GatewayEvent> = vec![];
        for _ in 0..MAX_FRAMES {
            let Some(frame) = self.body()?.next_frame().await? else {
                if self.raw_json
                    && !self.raw_error
                    && serde_json::from_slice::<serde_json::Value>(&bytes).is_err()
                {
                    return Err(MiddlewareError::InvalidState);
                }
                let wire = if self.websocket {
                    let mut value: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_slice(&bytes)
                            .map_err(|_| MiddlewareError::InvalidState)?;
                    value.insert("type".into(), "error".into());
                    value.insert("status".into(), self.status.into());
                    ProtocolWireEvent::json("openai", Some("error".into()), value.into())
                        .map_err(|_| MiddlewareError::InvalidState)?
                } else if self.raw_error || self.raw_json {
                    ProtocolWireEvent::raw_json(
                        if self.raw_error {
                            "openai"
                        } else {
                            self.protocol.as_str()
                        },
                        Bytes::from(bytes),
                    )
                    .map_err(|_| MiddlewareError::InvalidState)?
                } else {
                    let value: serde_json::Value = serde_json::from_slice(&bytes)
                        .map_err(|_| MiddlewareError::InvalidState)?;
                    ProtocolWireEvent::json(
                        "openai",
                        Some("response.completed".into()),
                        serde_json::json!({"type":"response.completed","response":value}),
                    )
                    .map_err(|_| MiddlewareError::InvalidState)?
                };
                self.finished = true;
                self.close().await;
                return Ok(ProviderEvent::canonical_with_wire(facts, wire));
            };
            let (payload, framing, _, envelope) = frame.into_parts();
            if !matches!(
                framing,
                MiddlewareFraming::JsonDocument | MiddlewareFraming::RawBytes
            ) || bytes.len().saturating_add(payload.len()) > MAX_BYTES
            {
                return Err(MiddlewareError::InvalidState);
            }
            bytes.extend_from_slice(&payload);
            if let Some(envelope) = envelope {
                facts.extend(
                    envelope
                        .events()
                        .flat_map(|e| e.canonical_facts().iter().cloned()),
                );
            }
        }
        Err(MiddlewareError::InvalidState)
    }
}
fn websocket_message(payload: Bytes) -> Result<(ProtocolWireEvent, bool), MiddlewareError> {
    let value: serde_json::Value =
        serde_json::from_slice(&payload).map_err(|_| MiddlewareError::InvalidState)?;
    let kind = value
        .get("type")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .ok_or(MiddlewareError::InvalidState)?;
    let terminal = matches!(
        kind,
        "response.completed" | "response.incomplete" | "response.failed" | "error"
    );
    let wire = ProtocolWireEvent::json("openai", Some(kind.to_owned()), value)
        .map_err(|_| MiddlewareError::InvalidState)?;
    Ok((wire, terminal))
}

pub(in crate::openai::middleware) fn sse(
    payload: Bytes,
) -> Result<(ProtocolWireEvent, bool), MiddlewareError> {
    sse_for_protocol(payload, "openai")
}

fn messages_wire(
    messages: &mut MessagesSse,
    payload: Bytes,
) -> Result<ProtocolWireEvent, MiddlewareError> {
    let mut events = messages.observe(&payload)?;
    match events.len() {
        0 => ProtocolWireEvent::raw_sse(ANTHROPIC_PROTOCOL, payload)
            .map_err(|_| MiddlewareError::InvalidState),
        1 => events
            .pop()
            .ok_or(MiddlewareError::InvalidState)?
            .into_wire(payload),
        _ => Err(MiddlewareError::InvalidState),
    }
}

fn sse_for_protocol(
    payload: Bytes,
    protocol: &str,
) -> Result<(ProtocolWireEvent, bool), MiddlewareError> {
    if !(payload.ends_with(b"\n\n") || payload.ends_with(b"\r\n\r\n")) {
        return Err(MiddlewareError::InvalidState);
    }
    let text = std::str::from_utf8(&payload).map_err(|_| MiddlewareError::InvalidState)?;
    let mut events = parse_sse_events(text).map_err(|_| MiddlewareError::InvalidState)?;
    if events.is_empty() {
        return Ok((
            ProtocolWireEvent::raw_sse(protocol, payload)
                .map_err(|_| MiddlewareError::InvalidState)?,
            false,
        ));
    }
    if events.len() != 1 {
        return Err(MiddlewareError::InvalidState);
    }
    let event = events.pop().ok_or(MiddlewareError::InvalidState)?;
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data) else {
        if protocol != "openai" {
            return Err(MiddlewareError::InvalidState);
        }
        return Ok((
            ProtocolWireEvent::raw_sse(protocol, payload)
                .map_err(|_| MiddlewareError::InvalidState)?,
            false,
        ));
    };
    let kind = event
        .event
        .as_deref()
        .or_else(|| value.get("type").and_then(|v| v.as_str()));
    let terminal = protocol == "openai"
        && matches!(
            kind,
            Some("response.completed" | "response.incomplete" | "response.failed" | "error")
        );
    let wire = ProtocolWireEvent::json_with_raw_sse_metadata(
        protocol,
        event.event,
        value,
        payload,
        event.id,
        event.retry,
    )
    .map_err(|_| MiddlewareError::InvalidState)?;
    Ok((wire, terminal))
}
impl Drop for Output {
    fn drop(&mut self) {
        if let Some(body) = self.body.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(body.close());
        }
    }
}
