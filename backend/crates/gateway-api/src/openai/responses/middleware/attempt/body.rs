//! Encode only events already observed by the owning Core attempt.
use super::super::super::OpenAiResponsesEncoder;
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    engine::{attempt_middleware::AttemptSource, middleware::*},
    event::ProviderEvent,
};
use std::collections::VecDeque;
const MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_DOCUMENT_EVENTS: usize = 16384;

pub(super) struct SourceBody {
    source: AttemptSource,
    encoder: OpenAiResponsesEncoder,
    pending: VecDeque<MiddlewareFrame>,
    exhausted: bool,
    streaming: bool,
}
impl SourceBody {
    pub(super) async fn response(
        source: AttemptSource,
        streaming: bool,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        let metadata = source.metadata().clone();
        let mut body = Self {
            source,
            encoder: OpenAiResponsesEncoder::new(),
            pending: VecDeque::new(),
            exhausted: false,
            streaming,
        };
        let mut headers = vec![];
        let mut events = vec![];
        let mut size = 0usize;
        loop {
            let Some(event) = body.source.next_event().await? else {
                body.exhausted = true;
                break;
            };
            if let Some(observation) = event.response_observation() {
                headers = observation
                    .client_headers()
                    .iter()
                    .map(|h| MiddlewareHeader::new(h.name(), h.value().clone()))
                    .collect();
            }
            if streaming {
                let visible = event.wire_event().is_some();
                body.encode(event)?;
                if visible {
                    break;
                }
                if body.pending.len() > MAX_DOCUMENT_EVENTS {
                    return Err(MiddlewareError::InvalidState);
                }
            } else {
                for frame in body.encoder.push_sse(&event) {
                    size = size.saturating_add(frame.len());
                }
                if size > MAX_DOCUMENT_BYTES || events.len() >= MAX_DOCUMENT_EVENTS {
                    return Err(MiddlewareError::InvalidState);
                }
                events.push(event);
            }
        }
        if !streaming {
            let encoded = serde_json::to_vec(
                &std::mem::take(&mut body.encoder)
                    .finish()
                    .map_err(|_| MiddlewareError::InvalidState)?,
            )
            .map_err(|_| MiddlewareError::InvalidState)?;
            if encoded.len() > MAX_DOCUMENT_BYTES {
                return Err(MiddlewareError::InvalidState);
            }
            let last = events.pop().ok_or(MiddlewareError::InvalidState)?;
            let frame = MiddlewareFrame::from_provider_event(
                Bytes::from(encoded),
                MiddlewareFraming::JsonDocument,
                true,
                last,
            );
            let (bytes, framing, terminal, envelope) = frame.into_parts();
            body.pending.push_back(
                MiddlewareFrame::new(bytes, framing, terminal).with_envelope(
                    envelope
                        .ok_or(MiddlewareError::InvalidState)?
                        .with_preceding_events(events),
                ),
            );
        }
        Ok(
            MiddlewareResponse::new("openai".into(), 200, headers, Box::new(body))
                .with_provider_metadata(metadata),
        )
    }
    fn encode(&mut self, event: ProviderEvent) -> Result<(), MiddlewareError> {
        let mut frames = self.encoder.push_sse(&event);
        if frames.iter().map(Bytes::len).sum::<usize>() > MAX_DOCUMENT_BYTES {
            return Err(MiddlewareError::InvalidState);
        }
        let terminal = self.encoder.is_completed() || self.encoder.has_wire_failure();
        let last = frames.pop().unwrap_or_default();
        for frame in frames {
            self.pending.push_back(
                MiddlewareFrame::new(frame, MiddlewareFraming::SseEvent, false)
                    .with_transformed(true),
            );
        }
        let framing = if last.is_empty() {
            MiddlewareFraming::RawBytes
        } else {
            MiddlewareFraming::SseEvent
        };
        self.pending.push_back(MiddlewareFrame::from_provider_event(
            last, framing, terminal, event,
        ));
        Ok(())
    }
}
impl MiddlewareBody for SourceBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            if let Some(frame) = self.pending.pop_front() {
                return Ok(Some(frame));
            }
            if self.exhausted {
                return Ok(None);
            }
            if !self.streaming {
                return Err(MiddlewareError::InvalidState);
            }
            match self.source.next_event().await? {
                Some(event) => {
                    self.encode(event)?;
                    Ok(self.pending.pop_front())
                }
                None => {
                    self.exhausted = true;
                    Ok(None)
                }
            }
        })
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            drop(self);
        })
    }
}
