//! 校验插件流式事件，生成领域事件并保持序列约束

use super::failure::{failure_error, invalid};

use bytes::Bytes;
use gateway_core::{
    engine::upstream_adapter::UpstreamAccountConnection,
    error::ProviderError,
    event::{
        ContentItem, ContentKind, EventSequenceValidator, FinishReason, GatewayEvent,
        ProtocolWireEvent, ProviderEvent, ProviderResponseObservation, ReasoningDelta,
        ResponseMeta, TextDelta, ToolCallDelta,
    },
    metering::Usage,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use gateway_plugin_sdk::call::{
    model::{CanonicalEvent, WireEvent, WirePayload},
    upstream_adapter::{UpstreamAdapterEvent, UpstreamContinuation},
};
use gateway_protocol::openai::sse::SseEventDecoder;

#[derive(Default)]
pub(super) struct EventDecoder {
    sequence: EventSequenceValidator,
    response_id: Option<String>,
    observation: Option<ProviderResponseObservation>,
    usage: Usage,
    contents: usize,
    body_bytes: usize,
    body_chunks: usize,
    other_wire: bool,
    pub(super) completed: bool,
    pub(super) replay_blocked: bool,
}

pub(super) struct DecodedEvent {
    pub(super) event: ProviderEvent,
    pub(super) continuation: Option<UpstreamContinuation>,
}

pub(super) struct DecodeContext<'a> {
    pub protocol: &'a str,
    pub transport: &'a str,
    pub client_transport: Option<&'a str>,
    pub account: &'a dyn UpstreamAccountConnection,
    pub sent: UpstreamSendState,
    pub started_at: std::time::Instant,
}

pub(super) enum EventDecodeError {
    Invalid(ProviderError),
    Upstream(ProviderError),
}

impl From<ProviderError> for EventDecodeError {
    fn from(error: ProviderError) -> Self {
        Self::Invalid(error)
    }
}

impl EventDecoder {
    pub(super) fn decode(
        &mut self,
        payload: &[u8],
        context: DecodeContext<'_>,
    ) -> Result<DecodedEvent, EventDecodeError> {
        let DecodeContext {
            protocol,
            transport,
            client_transport,
            account,
            sent,
            started_at,
        } = context;
        let message = UpstreamAdapterEvent::decode(payload).map_err(|source| {
            invalid(sent).with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                source.to_string(),
            ))
        })?;
        self.replay_blocked |= !message.event.facts.is_empty()
            || message.event.wire.is_some()
            || message.continuation.is_some();
        if self.completed || message.event.facts.len() > 64 || message.event.host.is_some() {
            return Err(invalid(sent).into());
        }
        if let Some(wire) = &message.event.wire {
            if let WirePayload::RawBody { body } = &wire.payload {
                self.body_bytes = self.body_bytes.saturating_add(body.len());
                self.body_chunks += 1;
                if !matches!(client_transport, Some("http_json" | "internal"))
                    || self.other_wire
                    || self.body_bytes > 8 * 1024 * 1024
                    || self.body_chunks > 16384
                {
                    return Err(invalid(sent).into());
                }
            } else {
                if self.body_chunks != 0 {
                    return Err(invalid(sent).into());
                }
                self.other_wire = true;
            }
        }
        let wire = message
            .event
            .wire
            .map(|wire| decode_wire(wire, protocol, sent))
            .transpose()?;
        let current = self
            .observation
            .take()
            .unwrap_or(ProviderResponseObservation::new(
                UpstreamTransport::new(transport).map_err(|_| invalid(sent))?,
            ));
        self.observation = Some(match message.observation {
            Some(update) => super::observation::merge(current, update, sent)?,
            None => current,
        });
        if let Some(failure) = message.failure {
            if !message.event.facts.is_empty() || message.continuation.is_some() {
                return Err(invalid(sent).into());
            }
            let mut events = Vec::new();
            if let Some(observation) = self.observation.clone() {
                events.push(ProviderEvent::observation(observation));
            }
            if let Some(wire) = wire {
                events.push(ProviderEvent::wire(wire));
            }
            return Err(EventDecodeError::Upstream(
                failure_error(failure, sent)?.with_atomic_client_events(events),
            ));
        }
        let mut facts = Vec::new();
        for fact in message.event.facts {
            let fact = match fact {
                CanonicalEvent::Started { id, model } => {
                    validate_id(&id, sent)?;
                    validate_model(model.as_deref(), sent)?;
                    self.response_id = Some(id.clone());
                    GatewayEvent::Started(meta(id, model))
                }
                CanonicalEvent::ContentAdded { index, kind } => {
                    self.contents += 1;
                    if self.contents > 4096 {
                        return Err(invalid(sent).into());
                    }
                    GatewayEvent::ContentAdded(ContentItem::new(
                        index,
                        match kind {
                            gateway_plugin_sdk::call::model::ContentKind::Text => ContentKind::Text,
                            gateway_plugin_sdk::call::model::ContentKind::Reasoning => {
                                ContentKind::Reasoning
                            }
                            gateway_plugin_sdk::call::model::ContentKind::ToolCall => {
                                ContentKind::ToolCall
                            }
                            gateway_plugin_sdk::call::model::ContentKind::Image => {
                                ContentKind::Image
                            }
                            gateway_plugin_sdk::call::model::ContentKind::Audio => {
                                ContentKind::Audio
                            }
                        },
                    ))
                }
                CanonicalEvent::TextDelta { index, text } => GatewayEvent::TextDelta(TextDelta {
                    content_index: index,
                    text,
                }),
                CanonicalEvent::ReasoningDelta { index, text } => {
                    GatewayEvent::ReasoningDelta(ReasoningDelta {
                        content_index: index,
                        text,
                    })
                }
                CanonicalEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments,
                } => {
                    validate_id(&id, sent)?;
                    if name
                        .as_ref()
                        .is_some_and(|name| name.len() > 512 || name.chars().any(char::is_control))
                    {
                        return Err(invalid(sent).into());
                    }
                    GatewayEvent::ToolCallDelta(ToolCallDelta {
                        content_index: index,
                        call_id: id,
                        name,
                        arguments_delta: arguments,
                    })
                }
                CanonicalEvent::Usage { usage } => {
                    if [
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cached_tokens,
                        usage.cache_write_tokens,
                        usage.reasoning_tokens,
                        usage.image_input_tokens,
                        usage.image_output_tokens,
                        usage.total_tokens,
                    ]
                    .into_iter()
                    .flatten()
                    .any(|count| count > i64::MAX as u64)
                    {
                        return Err(invalid(sent).into());
                    }
                    let usage = Usage {
                        input_tokens: usage.input_tokens,
                        output_tokens: usage.output_tokens,
                        cached_tokens: usage.cached_tokens,
                        cache_write_tokens: usage.cache_write_tokens,
                        reasoning_tokens: usage.reasoning_tokens,
                        image_input_tokens: usage.image_input_tokens,
                        image_output_tokens: usage.image_output_tokens,
                        total_tokens: usage.total_tokens,
                    };
                    self.usage.merge(&usage);
                    GatewayEvent::Usage(usage)
                }
                CanonicalEvent::Completed { id, model, reason } => {
                    validate_model(model.as_deref(), sent)?;
                    if self.response_id.as_deref() != Some(id.as_str()) {
                        return Err(invalid(sent).into());
                    }
                    if let Some(cost) = account.calculate_cost(
                        self.observation.as_ref().and_then(|o| o.service_tier()),
                        &self.usage,
                    ) {
                        let cost = GatewayEvent::CalculatedCost(cost);
                        self.sequence.observe(&cost).map_err(|source| {
                            invalid(sent).with_raw_upstream_error(
                                gateway_core::error::RawUpstreamError::new(source.to_string()),
                            )
                        })?;
                        facts.push(cost);
                    }
                    self.completed = true;
                    GatewayEvent::Completed(meta(id, model).with_finish_reason(match reason {
                        gateway_plugin_sdk::call::model::FinishReason::Stop => FinishReason::Stop,
                        gateway_plugin_sdk::call::model::FinishReason::Length => {
                            FinishReason::Length
                        }
                        gateway_plugin_sdk::call::model::FinishReason::ToolCall => {
                            FinishReason::ToolCall
                        }
                        gateway_plugin_sdk::call::model::FinishReason::ContentFilter => {
                            FinishReason::ContentFilter
                        }
                        gateway_plugin_sdk::call::model::FinishReason::Other => FinishReason::Other,
                    }))
                }
            };
            self.sequence.observe(&fact).map_err(|source| {
                invalid(sent).with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                    source.to_string(),
                ))
            })?;
            facts.push(fact);
        }
        if message.continuation.is_some() && !self.completed {
            return Err(invalid(sent).into());
        }
        let mut event = if let Some(wire) = wire {
            ProviderEvent::canonical_with_wire(facts, wire)
        } else {
            let mut facts = facts.into_iter();
            let Some(first) = facts.next() else {
                return Ok(DecodedEvent {
                    event: ProviderEvent::observation(
                        self.observation.clone().ok_or_else(|| invalid(sent))?,
                    ),
                    continuation: message.continuation,
                });
            };
            facts.fold(ProviderEvent::canonical(first), ProviderEvent::with_fact)
        };
        let observation = super::observation::observe_output(
            self.observation.take().ok_or_else(|| invalid(sent))?,
            event.canonical_facts(),
            event.wire_event().is_some(),
            started_at,
        );
        event.attach_observation(observation.clone());
        self.observation = Some(observation);
        Ok(DecodedEvent {
            event,
            continuation: message.continuation,
        })
    }

    pub(super) fn finish(&self, sent: UpstreamSendState) -> Result<(), ProviderError> {
        self.sequence.finish().map_err(|source| {
            invalid(sent).with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                source.to_string(),
            ))
        })
    }
}

fn meta(id: String, model: Option<String>) -> ResponseMeta {
    model.map_or_else(
        || ResponseMeta::for_provider_endpoint(id.clone()),
        |model| ResponseMeta::new(id.clone(), model),
    )
}

fn validate_id(id: &str, sent: UpstreamSendState) -> Result<(), ProviderError> {
    if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
        return Err(invalid(sent));
    }
    Ok(())
}

fn validate_model(model: Option<&str>, sent: UpstreamSendState) -> Result<(), ProviderError> {
    if model.is_some_and(|model| {
        model.is_empty() || model.len() > 256 || model.chars().any(char::is_control)
    }) {
        return Err(invalid(sent));
    }
    Ok(())
}

fn decode_wire(
    wire: WireEvent,
    protocol: &str,
    sent: UpstreamSendState,
) -> Result<ProtocolWireEvent, ProviderError> {
    if wire.protocol != protocol {
        return Err(invalid(sent));
    }
    let result = match wire.payload {
        WirePayload::Json {
            event,
            data,
            id,
            retry,
            raw_sse,
        } => {
            if let Some(raw) = raw_sse {
                let frame = single_sse(&raw, sent)?;
                let [parsed] = frame.events() else {
                    return Err(invalid(sent));
                };
                if parsed.event != event
                    || parsed.id != id
                    || parsed.retry != retry
                    || serde_json::from_str::<serde_json::Value>(&parsed.data)
                        .ok()
                        .as_ref()
                        != Some(&data)
                {
                    return Err(invalid(sent));
                }
                ProtocolWireEvent::json_with_raw_sse_metadata(
                    protocol,
                    event,
                    data,
                    Bytes::from(raw),
                    id,
                    retry,
                )
            } else {
                ProtocolWireEvent::json_with_sse_metadata(protocol, event, data, id, retry)
            }
        }
        WirePayload::WebSocket {
            event,
            data,
            message,
        } => {
            let message = String::from_utf8(message).map_err(|_| invalid(sent))?;
            if let Some(data) = data {
                ProtocolWireEvent::json(protocol, event, data)
                    .map(|wire| wire.with_raw_websocket_message(message))
            } else {
                ProtocolWireEvent::raw_websocket(protocol, message)
            }
        }
        WirePayload::RawSse { frame } => {
            single_sse(&frame, sent)?;
            ProtocolWireEvent::raw_sse(protocol, Bytes::from(frame))
        }
        WirePayload::RawJson { body } => {
            serde_json::from_slice::<serde_json::Value>(&body).map_err(|source| {
                invalid(sent).with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                    source.to_string(),
                ))
            })?;
            ProtocolWireEvent::raw_json(protocol, Bytes::from(body))
        }
        WirePayload::RawBody { body } => ProtocolWireEvent::raw_body(protocol, Bytes::from(body)),
    };
    result.map_err(|source| {
        invalid(sent).with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
            source.to_string(),
        ))
    })
}

fn single_sse(
    bytes: &[u8],
    sent: UpstreamSendState,
) -> Result<gateway_protocol::openai::sse::SseFrame, ProviderError> {
    let mut frames = SseEventDecoder::default().push_frames(bytes);
    if frames.len() != 1 || frames[0].raw() != bytes || std::str::from_utf8(bytes).is_err() {
        return Err(invalid(sent));
    }
    frames.pop().ok_or_else(|| invalid(sent))
}
