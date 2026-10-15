//! Encode a Core session without acknowledging downstream delivery.
use crate::openai::responses::{OpenAiResponsesEncoder, PendingExecution};
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    engine::{CommitRequirement, CoordinatedEvent, execution::ExecutionSession, middleware::*},
    event::ProviderEvent,
};
use std::collections::VecDeque;

pub(super) struct ExecutionBody {
    execution: PendingExecution,
    encoder: OpenAiResponsesEncoder,
    pending: VecDeque<MiddlewareFrame>,
    streaming: bool,
    format: super::WireFormat,
    exhausted: bool,
    committed: bool,
    terminal: bool,
}

impl ExecutionBody {
    pub(super) async fn response(
        session: Box<dyn ExecutionSession>,
        format: super::WireFormat,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        let streaming = format.streaming();
        let mut body = Self {
            execution: PendingExecution::new(session),
            encoder: OpenAiResponsesEncoder::new(),
            pending: VecDeque::new(),
            streaming,
            format,
            exhausted: false,
            committed: false,
            terminal: false,
        };
        if streaming {
            let batch = body
                .session()?
                .next_event()
                .await?
                .ok_or(MiddlewareError::InvalidState)?;
            if batch.commit_requirement() != CommitRequirement::CommitBeforeDelivery {
                return Err(MiddlewareError::InvalidState);
            }
            let status = body.response_status()?;
            if (200..300).contains(&status) || format.websocket() {
                body.encode_batch(batch)?;
            } else {
                // An attempt may project a JSON error even for an SSE request.
                // Its one complete document remains behind the original barrier.
                let mut events = batch.into_provider_events();
                let encoded = encode_document(&events, format)?;
                let last = events.pop().ok_or(MiddlewareError::InvalidState)?;
                let (bytes, framing, terminal, envelope) = MiddlewareFrame::from_provider_event(
                    encoded,
                    if (200..300).contains(&status) {
                        MiddlewareFraming::JsonDocument
                    } else {
                        MiddlewareFraming::RawBytes
                    },
                    true,
                    last,
                )
                .into_parts();
                body.pending.push_back(
                    MiddlewareFrame::new(bytes, framing, terminal).with_envelope(
                        envelope
                            .ok_or(MiddlewareError::InvalidState)?
                            .with_preceding_events(events)
                            .with_batch_end(CommitRequirement::CommitBeforeDelivery),
                    ),
                );
                body.streaming = false;
                body.terminal = true;
            }
        } else {
            let mut events = body.session()?.collect_uncommitted().await?;
            let status = body.response_status()?;
            let encoded = encode_document(&events, format)?;
            let last = events.pop().ok_or(MiddlewareError::InvalidState)?;
            let frame = MiddlewareFrame::from_provider_event(
                encoded,
                if (200..300).contains(&status) {
                    MiddlewareFraming::JsonDocument
                } else {
                    MiddlewareFraming::RawBytes
                },
                true,
                last,
            );
            let (bytes, framing, terminal, envelope) = frame.into_parts();
            let envelope = envelope
                .ok_or(MiddlewareError::InvalidState)?
                .with_preceding_events(events)
                .with_batch_end(CommitRequirement::CommitBeforeDelivery);
            body.pending
                .push_back(MiddlewareFrame::new(bytes, framing, terminal).with_envelope(envelope));
            body.exhausted = true;
        }
        let headers = body
            .session()?
            .response_headers()
            .iter()
            .map(|h| MiddlewareHeader::new(h.name(), h.value().clone()))
            .collect();
        let status = body.response_status()?;
        Ok(MiddlewareResponse::new(
            "openai".into(),
            status,
            headers,
            Box::new(body),
        ))
    }
    fn session(&mut self) -> Result<&mut (dyn ExecutionSession + 'static), MiddlewareError> {
        self.execution
            .session_mut()
            .ok_or(MiddlewareError::InvalidState)
    }
    fn response_status(&mut self) -> Result<u16, MiddlewareError> {
        let session = self.session()?;
        Ok(session
            .middleware_response_head()
            .map(|head| head.status_code)
            .or_else(|| {
                session
                    .response_status_code()
                    .filter(|status| (200..300).contains(status))
            })
            .unwrap_or(200))
    }
    async fn finish_terminal(&mut self) -> Result<(), MiddlewareError> {
        let result = self.session()?.next_event().await;
        match result {
            Ok(None) if self.is_finalized() => self.exhausted = true,
            Err(_) if self.encoder.has_wire_failure() && self.is_finalized() => {
                self.exhausted = true
            }
            Err(error) => return Err(error.into()),
            _ => return Err(MiddlewareError::InvalidState),
        }
        Ok(())
    }
    fn encode_batch(&mut self, batch: CoordinatedEvent) -> Result<(), MiddlewareError> {
        let requirement = batch.commit_requirement();
        let events = batch.into_provider_events();
        let count = events.len();
        for (index, event) in events.into_iter().enumerate() {
            let frames = if self.format.websocket() {
                self.encoder
                    .push_websocket(&event)
                    .into_iter()
                    .map(Bytes::from)
                    .collect()
            } else {
                self.encoder.push_sse(&event)
            };
            self.terminal |= self.encoder.is_completed() || self.encoder.has_wire_failure();
            self.encode_event(event, frames, (index + 1 == count).then_some(requirement));
        }
        if count == 0 {
            return Err(MiddlewareError::InvalidState);
        }
        Ok(())
    }
    fn encode_event(
        &mut self,
        event: ProviderEvent,
        mut frames: Vec<Bytes>,
        boundary: Option<CommitRequirement>,
    ) {
        let message_framing = if self.format.websocket() {
            MiddlewareFraming::JsonDocument
        } else {
            MiddlewareFraming::SseEvent
        };
        let last = frames.pop().unwrap_or_default();
        for bytes in frames {
            self.pending
                .push_back(MiddlewareFrame::new(bytes, message_framing, false));
        }
        let framing = if last.is_empty() {
            MiddlewareFraming::RawBytes
        } else {
            message_framing
        };
        let frame = MiddlewareFrame::from_provider_event(
            last,
            framing,
            self.terminal && boundary.is_some(),
            event,
        );
        if let Some(requirement) = boundary {
            let (bytes, framing, terminal, envelope) = frame.into_parts();
            if let Some(envelope) = envelope {
                self.pending.push_back(
                    MiddlewareFrame::new(bytes, framing, terminal)
                        .with_envelope(envelope.with_batch_end(requirement)),
                );
            }
        } else {
            self.pending.push_back(frame);
        }
    }
}
impl MiddlewareBody for ExecutionBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            if let Some(frame) = self.pending.pop_front() {
                if frame.terminal() && self.committed {
                    self.finish_terminal().await?;
                }
                return Ok(Some(frame));
            }
            if self.exhausted {
                return Ok(None);
            }
            // A terminal may be read by RPC before its outer batch is committed.
            // Do not ask Core to advance through that still-unacknowledged barrier.
            if self.terminal && !self.committed {
                return Ok(None);
            }
            match self.session()?.next_event().await {
                Ok(Some(batch)) => {
                    if self.terminal
                        || batch.commit_requirement() != CommitRequirement::AlreadyCommitted
                    {
                        return Err(MiddlewareError::InvalidState);
                    }
                    self.encode_batch(batch)?;
                    let frame = self.pending.pop_front();
                    if frame.as_ref().is_some_and(MiddlewareFrame::terminal) {
                        self.finish_terminal().await?;
                    }
                    Ok(frame)
                }
                Ok(None) => {
                    if !self.is_finalized() {
                        return Err(MiddlewareError::InvalidState);
                    }
                    self.exhausted = true;
                    Ok(None)
                }
                Err(error) => Err(error.into()),
            }
        })
    }
    fn commit_downstream(
        &mut self,
        status: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            self.session()?.commit_downstream(status).await?;
            self.committed = true;
            if self.terminal && !self.exhausted {
                // Complete the original terminal, including settlement, before exposing it.
                self.finish_terminal().await?;
            }
            if !self.streaming && !self.is_finalized() {
                return Err(MiddlewareError::InvalidState);
            }
            Ok(())
        })
    }
    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            self.session()?
                .record_client_status(status)
                .await
                .map_err(Into::into)
        })
    }
    fn is_finalized(&self) -> bool {
        self.execution.is_finalized()
    }
    fn close(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.execution.cancel_and_finalize().await;
        })
    }
}

fn encode_document(
    events: &[ProviderEvent],
    format: super::WireFormat,
) -> Result<Bytes, MiddlewareError> {
    let raw = crate::openai::endpoint::raw_json_body(events)
        .map_err(|_| MiddlewareError::InvalidState)?;
    if let Some(raw) = raw {
        return Ok(raw);
    }
    if format.raw_json() {
        return Err(MiddlewareError::InvalidState);
    }
    let mut encoder = OpenAiResponsesEncoder::new();
    for event in events {
        encoder.push_sse(event);
    }
    serde_json::to_vec(
        &encoder
            .finish()
            .map_err(|_| MiddlewareError::InvalidState)?,
    )
    .map(Bytes::from)
    .map_err(|_| MiddlewareError::InvalidState)
}
