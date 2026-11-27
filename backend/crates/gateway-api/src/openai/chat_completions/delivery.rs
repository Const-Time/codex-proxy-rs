//! Project validated Responses batches before the existing delivery acknowledgement.
use bytes::Bytes;
use gateway_core::engine::middleware::MiddlewareError;
use gateway_core::operation::Operation;
use gateway_protocol::{
    chat_completions::{
        ChatIdentity,
        responses::{ChatStreamEncoder, project_response},
    },
    openai::sse::{SseEventDecoder, encode_sse_event},
};

pub(in crate::openai) struct ChatProjection {
    identity: ChatIdentity,
    encoder: ChatStreamEncoder,
    decoder: SseEventDecoder,
}
impl ChatProjection {
    pub(in crate::openai) fn for_request(
        request: &crate::openai::responses::DecodedResponsesRequest,
    ) -> Option<Self> {
        let Operation::Generate(generate) = request.operation() else {
            return None;
        };
        let payload = generate.protocol_payload();
        if payload.protocol() != gateway_protocol::chat_completions::CHAT_PROTOCOL {
            return None;
        }
        let include_usage = payload
            .body()
            .get("stream_options")
            .and_then(|options| options.get("include_usage"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let identity = ChatIdentity {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            model: request.metadata().requested_model().to_owned(),
        };
        Some(Self {
            encoder: ChatStreamEncoder::new(identity.clone(), include_usage),
            identity,
            decoder: SseEventDecoder::default(),
        })
    }
    pub(in crate::openai) fn document(&self, bytes: &[u8]) -> Result<Bytes, MiddlewareError> {
        let value = serde_json::from_slice(bytes).map_err(|_| MiddlewareError::InvalidState)?;
        let projected =
            project_response(&value, &self.identity).map_err(|_| MiddlewareError::InvalidState)?;
        serde_json::to_vec(&projected)
            .map(Bytes::from)
            .map_err(|_| MiddlewareError::InvalidState)
    }

    pub(in crate::openai) fn start(&mut self) -> Result<Vec<Bytes>, MiddlewareError> {
        self.encoder
            .push_event(Some("response.created"), &serde_json::Value::Null)
            .map(|chunks| chunks.into_iter().map(chat_frame).collect())
            .map_err(|_| MiddlewareError::InvalidState)
    }
    pub(in crate::openai) fn stream(
        &mut self,
        chunks: Vec<Bytes>,
    ) -> Result<Vec<Bytes>, MiddlewareError> {
        let mut output = Vec::new();
        for chunk in chunks {
            for event in self
                .decoder
                .push(&chunk)
                .map_err(|_| MiddlewareError::InvalidState)?
            {
                if event.data.trim() == "[DONE]" {
                    continue;
                }
                let value =
                    serde_json::from_str(&event.data).map_err(|_| MiddlewareError::InvalidState)?;
                let chunks = self
                    .encoder
                    .push_event(event.event.as_deref(), &value)
                    .map_err(|_| MiddlewareError::InvalidState)?;
                output.extend(chunks.into_iter().map(chat_frame));
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

fn chat_frame(chunk: serde_json::Value) -> Bytes {
    Bytes::from(encode_sse_event("", &chunk.to_string()))
}
