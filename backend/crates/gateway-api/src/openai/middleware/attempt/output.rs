//! Validate projected bytes before Core can acknowledge downstream delivery.
use bytes::Bytes;
use gateway_core::{
    engine::{
        attempt_middleware::{AttemptOutput, AttemptResponseHead},
        middleware::*,
    },
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseHeader},
};
use gateway_protocol::openai::sse::parse_sse_events;
use std::collections::VecDeque;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_FRAMES: usize = 1024;
struct Output {
    body: Option<Box<dyn MiddlewareBody>>,
    pending: VecDeque<ProviderEvent>,
    streaming: bool,
    raw_error: bool,
    raw_json: bool,
    wire_terminal: bool,
    finished: bool,
}
pub(super) fn decode(
    response: MiddlewareResponse,
    format: super::super::WireFormat,
) -> Result<AttemptOutput, MiddlewareError> {
    let (protocol, status, headers, body, _) = response.into_parts();
    let output = Output {
        body: Some(body),
        pending: VecDeque::new(),
        streaming: format.streaming() && (200..300).contains(&status),
        raw_json: format.raw_json(),
        raw_error: !(200..300).contains(&status),
        wire_terminal: false,
        finished: false,
    };
    if protocol != "openai" || !(200..=599).contains(&status) {
        return Err(MiddlewareError::InvalidState);
    }
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
        for _ in 0..MAX_FRAMES {
            let Some(frame) = self.body()?.next_frame().await? else {
                if !self.wire_terminal || !batch.is_empty() {
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
            if let Some(envelope) = envelope {
                facts.extend(
                    envelope
                        .events()
                        .flat_map(|e| e.canonical_facts().iter().cloned()),
                );
            }
            if !payload.is_empty() {
                if framing != MiddlewareFraming::SseEvent {
                    return Err(MiddlewareError::InvalidState);
                }
                let (wire, is_terminal) = sse(payload)?;
                if self.wire_terminal {
                    return Err(MiddlewareError::InvalidState);
                }
                self.wire_terminal |= is_terminal;
                batch.push_back(ProviderEvent::canonical_with_wire(
                    std::mem::take(&mut facts),
                    wire,
                ));
            }
            if terminal {
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
                let wire = if self.raw_error || self.raw_json {
                    ProtocolWireEvent::raw_json("openai", Bytes::from(bytes))
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
fn sse(payload: Bytes) -> Result<(ProtocolWireEvent, bool), MiddlewareError> {
    if !(payload.ends_with(b"\n\n") || payload.ends_with(b"\r\n\r\n")) {
        return Err(MiddlewareError::InvalidState);
    }
    let text = std::str::from_utf8(&payload).map_err(|_| MiddlewareError::InvalidState)?;
    let mut events = parse_sse_events(text).map_err(|_| MiddlewareError::InvalidState)?;
    if events.is_empty() {
        return Ok((
            ProtocolWireEvent::raw_sse("openai", payload)
                .map_err(|_| MiddlewareError::InvalidState)?,
            false,
        ));
    }
    if events.len() != 1 {
        return Err(MiddlewareError::InvalidState);
    }
    let event = events.pop().ok_or(MiddlewareError::InvalidState)?;
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data) else {
        return Ok((
            ProtocolWireEvent::raw_sse("openai", payload)
                .map_err(|_| MiddlewareError::InvalidState)?,
            false,
        ));
    };
    let kind = event
        .event
        .as_deref()
        .or_else(|| value.get("type").and_then(|v| v.as_str()));
    let terminal = matches!(
        kind,
        Some("response.completed" | "response.incomplete" | "response.failed" | "error")
    );
    let wire = ProtocolWireEvent::json_with_raw_sse_metadata(
        "openai",
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
