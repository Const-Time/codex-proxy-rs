//! Project validated Responses batches before the existing delivery acknowledgement.
use super::response::{ChatIdentity, ChatStreamEncoder, project_response};
use bytes::Bytes;
use gateway_core::engine::middleware::MiddlewareError;
use gateway_protocol::openai::sse::SseEventDecoder;
use std::collections::VecDeque;

pub(in crate::openai) struct ChatProjection {
    identity: ChatIdentity,
    encoder: ChatStreamEncoder,
    decoder: SseEventDecoder,
    validator: SseEventDecoder,
}
impl ChatProjection {
    pub(super) fn new(model: String, include_usage: bool) -> Self {
        let identity = ChatIdentity {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            model,
        };
        Self {
            encoder: ChatStreamEncoder::new(identity.clone(), include_usage),
            identity,
            decoder: SseEventDecoder::default(),
            validator: SseEventDecoder::default(),
        }
    }
    pub(in crate::openai) fn observe(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<gateway_protocol::openai::sse::SseEvent>, MiddlewareError> {
        self.validator
            .push(bytes)
            .map_err(|_| MiddlewareError::InvalidState)
    }
    pub(in crate::openai) fn document(&self, bytes: &[u8]) -> Result<Vec<u8>, MiddlewareError> {
        let value = serde_json::from_slice(bytes).map_err(|_| MiddlewareError::InvalidState)?;
        let projected =
            project_response(&value, &self.identity).map_err(|_| MiddlewareError::InvalidState)?;
        serde_json::to_vec(&projected).map_err(|_| MiddlewareError::InvalidState)
    }
    pub(in crate::openai) fn stream(
        &mut self,
        chunks: &VecDeque<Bytes>,
    ) -> Result<VecDeque<Bytes>, MiddlewareError> {
        let mut output = VecDeque::new();
        for chunk in chunks {
            for event in self
                .decoder
                .push(chunk)
                .map_err(|_| MiddlewareError::InvalidState)?
            {
                if event.data.trim() == "[DONE]" {
                    continue;
                }
                let value =
                    serde_json::from_str(&event.data).map_err(|_| MiddlewareError::InvalidState)?;
                output.extend(
                    self.encoder
                        .push_event(event.event.as_deref(), &value)
                        .map_err(|_| MiddlewareError::InvalidState)?,
                );
            }
        }
        Ok(output)
    }
    pub(in crate::openai) fn finish(&mut self) -> Result<(), MiddlewareError> {
        if !self
            .decoder
            .finish()
            .map_err(|_| MiddlewareError::InvalidState)?
            .is_empty()
            || !(self.encoder.is_completed() || self.encoder.has_wire_failure())
        {
            return Err(MiddlewareError::InvalidState);
        }
        Ok(())
    }
}
