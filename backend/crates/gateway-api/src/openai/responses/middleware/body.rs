//! Encode a Core session without acknowledging downstream delivery.
use super::super::{OpenAiResponsesEncoder, PendingExecution};
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
    exhausted: bool,
    committed: bool,
    terminal: bool,
}

impl ExecutionBody {
    pub(super) async fn response(
        session: Box<dyn ExecutionSession>,
        streaming: bool,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        let mut body = Self {
            execution: PendingExecution::new(session),
            encoder: OpenAiResponsesEncoder::new(),
            pending: VecDeque::new(),
            streaming,
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
            body.encode_batch(batch)?;
        } else {
            let mut events = body.session()?.collect_uncommitted().await?;
            for event in &events {
                body.encoder.push_sse(event);
            }
            let encoded = serde_json::to_vec(
                &body
                    .encoder
                    .finish()
                    .map_err(|_| MiddlewareError::InvalidState)?,
            )
            .map_err(|_| MiddlewareError::InvalidState)?;
            body.encoder = OpenAiResponsesEncoder::new();
            let last = events.pop().ok_or(MiddlewareError::InvalidState)?;
            let frame = MiddlewareFrame::from_provider_event(
                Bytes::from(encoded),
                MiddlewareFraming::JsonDocument,
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
        Ok(MiddlewareResponse::new(
            "openai".into(),
            200,
            headers,
            Box::new(body),
        ))
    }
    fn session(&mut self) -> Result<&mut (dyn ExecutionSession + 'static), MiddlewareError> {
        self.execution
            .session_mut()
            .ok_or(MiddlewareError::InvalidState)
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
            let frames = self.encoder.push_sse(&event);
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
        let last = frames.pop().unwrap_or_default();
        for bytes in frames {
            self.pending.push_back(MiddlewareFrame::new(
                bytes,
                MiddlewareFraming::SseEvent,
                false,
            ));
        }
        let framing = if last.is_empty() {
            MiddlewareFraming::RawBytes
        } else {
            MiddlewareFraming::SseEvent
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
            if !self.streaming && !self.is_finalized() {
                return Err(MiddlewareError::InvalidState);
            }
            if self.streaming && self.terminal {
                // Complete the original terminal, including settlement, before exposing it.
                self.finish_terminal().await?;
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
