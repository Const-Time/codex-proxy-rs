use bytes::Bytes;
use gateway_core::{engine::middleware::MiddlewareError, event::ProtocolWireEvent};
use gateway_protocol::{
    anthropic::ANTHROPIC_PROTOCOL,
    openai::sse::{SseEvent, parse_sse_events_strict, strict_sse_frame_is_complete},
};
use serde_json::Value;

pub(super) struct Event {
    sse: SseEvent,
    data: Value,
}

impl Event {
    pub(super) fn is_start(&self) -> bool {
        self.data["type"] == "message_start"
    }

    pub(super) fn into_wire(self, payload: Bytes) -> Result<ProtocolWireEvent, MiddlewareError> {
        ProtocolWireEvent::json_with_raw_sse_metadata(
            ANTHROPIC_PROTOCOL,
            self.sse.event,
            self.data,
            payload,
            self.sse.id,
            self.sse.retry,
        )
        .map_err(|_| MiddlewareError::InvalidState)
    }
}

#[derive(Default)]
pub(super) struct MessagesSse {
    seen_bytes: bool,
    pub(super) started: bool,
    pub(super) terminal: bool,
}

impl MessagesSse {
    pub(super) fn observe(&mut self, payload: &[u8]) -> Result<Vec<Event>, MiddlewareError> {
        if !strict_sse_frame_is_complete(payload) {
            return Err(MiddlewareError::InvalidState);
        }
        let text = std::str::from_utf8(payload).map_err(|_| MiddlewareError::InvalidState)?;
        let stream_start = !std::mem::replace(&mut self.seen_bytes, true);
        parse_sse_events_strict(text, stream_start)
            .map_err(|_| MiddlewareError::InvalidState)?
            .into_iter()
            .map(|sse| {
                let data = serde_json::from_str::<Value>(&sse.data)
                    .map_err(|_| MiddlewareError::InvalidState)?;
                let kind = data["type"]
                    .as_str()
                    .filter(|kind| !kind.trim().is_empty())
                    .ok_or(MiddlewareError::InvalidState)?;
                if self.terminal || sse.event.as_deref().is_some_and(|name| name != kind) {
                    return Err(MiddlewareError::InvalidState);
                }
                match kind {
                    "ping" => {}
                    "message_start" if !self.started => {
                        if ["/message/id", "/message/model"].iter().any(|field| {
                            !data
                                .pointer(field)
                                .and_then(Value::as_str)
                                .is_some_and(|value| !value.trim().is_empty())
                        }) {
                            return Err(MiddlewareError::InvalidState);
                        }
                        self.started = true;
                    }
                    "message_start" => return Err(MiddlewareError::InvalidState),
                    _ if !self.started => return Err(MiddlewareError::InvalidState),
                    "message_stop" => self.terminal = true,
                    "error" => {
                        if ["/error/type", "/error/message"]
                            .iter()
                            .any(|field| !data.pointer(field).is_some_and(Value::is_string))
                        {
                            return Err(MiddlewareError::InvalidState);
                        }
                        self.terminal = true;
                    }
                    _ => {}
                }
                Ok(Event { sse, data })
            })
            .collect()
    }
}
