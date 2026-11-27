use std::time::{SystemTime, UNIX_EPOCH};

use gateway_core::{
    engine::AttemptContext,
    error::{ProviderError, ProviderErrorKind as Kind},
    event::ProtocolWireEvent,
};
use gateway_protocol::{
    chat_completions::{CHAT_PROTOCOL, ChatIdentity, completion_chunks, genai, validate_request},
    genai::GENAI_PROTOCOL,
};
use serde_json::{Map, Value};

use crate::transport::{sent, unsent};

/// Projects OpenAI Chat requests and Cloud Code responses between their wire formats.
pub struct ChatProjection {
    identity: ChatIdentity,
    request_options: Map<String, Value>,
    encoder: genai::ChatStreamEncoder,
    stream: bool,
    include_usage: bool,
    complete_json: bool,
}

impl ChatProjection {
    pub fn prepare(
        body: &Map<String, Value>,
        context: &AttemptContext,
    ) -> Result<(Map<String, Value>, Self), ProviderError> {
        let converted = genai::convert_request(body).map_err(|error| {
            let kind = if error.is_target_unsupported() && validate_request(body).is_ok() {
                Kind::Unsupported
            } else {
                Kind::InvalidRequest
            };
            unsent(kind)
        })?;
        let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
        let include_usage = !stream
            || body
                .get("stream_options")
                .and_then(|options| options.get("include_usage"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
        let identity = ChatIdentity {
            id: format!("chatcmpl-{}", context.request_id().as_str()),
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            model: context
                .requested_model()
                .map(|id| id.as_str())
                .or_else(|| body.get("model").and_then(Value::as_str))
                .unwrap_or_default()
                .to_owned(),
        };
        let request_options = ["tools", "web_search_options"]
            .into_iter()
            .filter_map(|key| body.get(key).cloned().map(|value| (key.to_owned(), value)))
            .collect();
        Ok((
            converted,
            Self {
                encoder: genai::ChatStreamEncoder::with_request(
                    identity.clone(),
                    include_usage,
                    body,
                ),
                identity,
                request_options,
                stream,
                include_usage,
                complete_json: false,
            },
        ))
    }

    pub fn translate(
        &mut self,
        event: &ProtocolWireEvent,
    ) -> Result<Vec<ProtocolWireEvent>, ProviderError> {
        if self.complete_json || event.protocol() != GENAI_PROTOCOL {
            return Err(sent(Kind::Protocol));
        }
        if let Some(raw) = event.raw_json_body() {
            let body = serde_json::from_slice(raw).map_err(|_| sent(Kind::Protocol))?;
            let value =
                genai::project_response_with_request(&body, &self.identity, &self.request_options)
                    .map_err(|_| sent(Kind::Protocol))?;
            self.complete_json = true;
            if self.stream {
                let chunks = completion_chunks(&value, self.include_usage)
                    .map_err(|_| sent(Kind::Protocol))?;
                return wire_events(chunks);
            }
            let body = serde_json::to_vec(&value).map_err(|_| sent(Kind::Protocol))?;
            return ProtocolWireEvent::raw_json(CHAT_PROTOCOL, body.into())
                .map(|event| vec![event])
                .map_err(|_| sent(Kind::Protocol));
        }
        if !event.has_json_data() {
            return Ok(Vec::new());
        }
        let chunks = self
            .encoder
            .push_event(event.event_type(), event.data())
            .map_err(|_| sent(Kind::Protocol))?;
        wire_events(chunks)
    }

    pub fn finish(&mut self) -> Result<Vec<ProtocolWireEvent>, ProviderError> {
        if self.complete_json {
            return Ok(Vec::new());
        }
        wire_events(self.encoder.finish().map_err(|_| sent(Kind::Protocol))?)
    }
}

fn wire_events(chunks: Vec<Value>) -> Result<Vec<ProtocolWireEvent>, ProviderError> {
    chunks
        .into_iter()
        .map(|chunk| {
            ProtocolWireEvent::json(CHAT_PROTOCOL, None, chunk).map_err(|_| sent(Kind::Protocol))
        })
        .collect()
}
