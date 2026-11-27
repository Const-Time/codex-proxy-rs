//! 去除资源服务响应外层，协议正文与候选完成、用量观测独立处理

use crate::transport::{sent, unwrap_frame};
use bytes::Bytes;
use futures::Stream;
use gateway_core::{
    error::{ProviderError, ProviderErrorKind as Kind},
    event::{
        ContentItem, ContentKind, FinishReason, GatewayEvent, ProtocolWireEvent, ProviderEvent,
        ProviderResponseObservation, ProviderResponseTimings, ResponseMeta, TextDelta,
        UpstreamHttpVersion,
    },
    lifecycle::{CancellationToken, Deadline},
    metering::{ModelPriceOverride, Usage},
    routing::UpstreamModelId,
    upstream::UpstreamTransport,
};
use gateway_protocol::{
    genai::{GENAI_PROTOCOL, GenerateContentObservation, GenerationFinish, primary_text_parts},
    openai::sse::{
        MAX_SSE_EVENT_BUFFER_BYTES, SseError, SseEvent, SseEventDecoder,
        encode_sse_event_with_metadata,
    },
};
use reqwest::Response;
use serde_json::Value;
use std::time::Instant;

/// Request metadata used to decode and meter a Cloud Code response.
pub struct CloudCodeResponseContext {
    pub stream: bool,
    pub model: UpstreamModelId,
    pub deadline: Deadline,
    pub cancel: CancellationToken,
    pub token: String,
    pub pricing: Option<ModelPriceOverride>,
    pub started_at: Instant,
}

/// Decodes Cloud Code JSON or SSE into native wire events and canonical usage facts.
pub fn decode_cloud_code_response(
    mut response: Response,
    context: CloudCodeResponseContext,
) -> impl Stream<Item = Result<ProviderEvent, ProviderError>> + Send + 'static {
    let CloudCodeResponseContext {
        stream,
        model,
        deadline,
        cancel,
        token,
        pricing,
        started_at,
    } = context;
    let headers_ms = elapsed_ms(started_at);
    async_stream::try_stream! {
        let mut timings = ProviderResponseTimings { headers_ms: Some(headers_ms), ..Default::default() };
        let mut observation = ProviderResponseObservation::new(
            UpstreamTransport::new("http").map_err(|_| sent(Kind::ProviderInfrastructureUnavailable))?,
        ).with_status_code(response.status().as_u16());
        if let Some(version) = UpstreamHttpVersion::parse(&format!("{:?}", response.version())) {
            observation = observation.with_http_version(version);
        }
        let mut decoder = SseEventDecoder::default();
        let mut json_body = Vec::new();
        let mut observed = GenerateContentObservation::default();
        let mut started = None;
        let mut text_started = false;
        let mut calculated_cost = None;
        let mut priceable = pricing.is_some();
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(sent(Kind::Cancelled)),
                _ = deadline.wait() => Err(sent(Kind::Timeout)),
                result = response.chunk() => result.map_err(|_| sent(Kind::Transport)),
            }?;
            let eof = chunk.is_none();
            let frames = if stream {
                match chunk {
                    Some(bytes) => decoder.push(&bytes),
                    None => decoder.finish(),
                }.map_err(|error| match error {
                    SseError::BufferExceeded { .. } => sent(Kind::MessageTooBig),
                    _ => sent(Kind::Protocol),
                })?
            } else if let Some(bytes) = chunk {
                if json_body.len().saturating_add(bytes.len()) > MAX_SSE_EVENT_BUFFER_BYTES {
                    Err(sent(Kind::MessageTooBig))?;
                }
                json_body.extend_from_slice(&bytes);
                Vec::new()
            } else {
                let data = String::from_utf8(std::mem::take(&mut json_body))
                    .map_err(|_| sent(Kind::Protocol))?;
                vec![SseEvent { event: None, data, id: None, retry: None }]
            };
            for frame in frames {
                let inner = unwrap_frame(&frame.data)?;
                let value: Value = serde_json::from_slice(&inner).map_err(|_| sent(Kind::Protocol))?;
                if value.get("error").is_some_and(Value::is_object) {
                    let status=value.get("error").and_then(|e|e.get("code")).and_then(Value::as_u64).and_then(|code|u16::try_from(code).ok());
                    let error = super::failure::body_error(status, &frame.data, &token);
                    Err(if started.is_none() && value.as_object().is_some_and(|body| body.len() == 1) {
                        super::failure::explicit_generation_rejection(error)
                    } else {
                        error
                    })?;
                }
                priceable &= crate::billing::response_is_priceable(&value);
                let usage = observed.observe(&value).map(usage);
                let cost_update = match &usage {
                    _ if !priceable => Some(None),
                    Some(usage) => Some(crate::billing::calculate(model.as_str(), usage, pricing.as_ref())
                        .map(|breakdown| breakdown.calculated_cost())),
                    None => None,
                };
                let cost_fact = if let Some(cost) = cost_update && cost != calculated_cost {
                    calculated_cost = cost;
                    Some(match &calculated_cost {
                        Some(cost) => GatewayEvent::CalculatedCost(cost.clone()),
                        None => GatewayEvent::CalculatedCostWithdrawn,
                    })
                } else {
                    None
                };
                let mut metering = usage.into_iter().map(GatewayEvent::Usage).chain(cost_fact);
                if !stream && observed.finish().is_none() {
                    Err(sent(Kind::Protocol).with_atomic_client_events(
                        metering.by_ref().map(ProviderEvent::canonical).collect(),
                    ))?;
                }
                let first_frame = started.is_none();
                let meta = started.get_or_insert_with(|| {
                    let id = value.get("responseId").and_then(Value::as_str)
                        .map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned);
                    let model = value.get("modelVersion").and_then(Value::as_str).unwrap_or(model.as_str());
                    ResponseMeta::new(id, model)
                });
                let text = primary_text_parts(&value).collect::<String>();
                let previous_timings = timings;
                observe_timings(&mut timings, &value, elapsed_ms(started_at));
                let wire = if stream {
                    let data = std::str::from_utf8(&inner).map_err(|_| sent(Kind::Protocol))?;
                    let raw = encode_sse_event_with_metadata(
                        frame.event.as_deref().unwrap_or(""), data, frame.id.as_deref(), frame.retry,
                    );
                    ProtocolWireEvent::json_with_raw_sse_metadata(
                        GENAI_PROTOCOL, frame.event, value, Bytes::from(raw), frame.id, frame.retry,
                    )
                } else {
                    ProtocolWireEvent::raw_json(GENAI_PROTOCOL, inner)
                }.map_err(|_| sent(Kind::Protocol))?;
                let mut event = ProviderEvent::wire(wire);
                if timings != previous_timings {
                    event.attach_observation(observation.clone().with_timings(timings));
                }
                // 首批必须含可交付的协议帧，开始事实随首帧一起提交
                if first_frame {
                    event = event.with_fact(GatewayEvent::Started(meta.clone()));
                }
                // 探测与首输出观测使用通用文本事实，协议正文仍由原生帧完整交付
                if !text.is_empty() {
                    if !text_started {
                        event = event.with_fact(GatewayEvent::ContentAdded(ContentItem::new(
                            0, ContentKind::Text,
                        )));
                        text_started = true;
                    }
                    event = event.with_fact(GatewayEvent::TextDelta(TextDelta {
                        content_index: 0,
                        text,
                    }));
                }
                for fact in metering {
                    event = event.with_fact(fact);
                }
                yield event;
            }
            if eof {
                break;
            }
        }
        let ending = observed.finish().ok_or_else(|| sent(Kind::Protocol))?;
        let meta = started.ok_or_else(|| sent(Kind::Protocol))?;
        yield ProviderEvent::canonical(GatewayEvent::Completed(meta.with_finish_reason(finish_reason(ending))));
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn observe_timings(timings: &mut ProviderResponseTimings, frame: &Value, elapsed: u64) {
    timings.first_event_ms.get_or_insert(elapsed);
    for part in frame
        .get("candidates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|candidate| {
            candidate
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                == 0
        })
        .filter_map(|candidate| {
            candidate
                .pointer("/content/parts")
                .and_then(Value::as_array)
        })
        .flatten()
    {
        // 空占位、签名及用量帧不算首字，工具调用本身属于可交付输出
        if part
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
        {
            let first = if part.get("thought").and_then(Value::as_bool) == Some(true) {
                &mut timings.first_reasoning_ms
            } else {
                &mut timings.first_text_ms
            };
            first.get_or_insert(elapsed);
            timings.first_token_ms.get_or_insert(elapsed);
        } else if part.get("functionCall").is_some_and(Value::is_object) {
            timings.first_token_ms.get_or_insert(elapsed);
        }
    }
}

fn usage(facts: &gateway_protocol::genai::UsageMetadata) -> Usage {
    let mut usage = Usage::new();
    usage.input_tokens = facts.input_tokens();
    usage.cached_tokens = facts.cached;
    usage.output_tokens = facts.output_tokens();
    usage.reasoning_tokens = facts.thoughts;
    usage.total_tokens = facts.total;
    usage.image_input_tokens = facts.image_input_tokens();
    usage.image_output_tokens = facts.candidate_image;
    usage
}

fn finish_reason(ending: GenerationFinish<'_>) -> FinishReason {
    match ending {
        GenerationFinish::Blocked => FinishReason::ContentFilter,
        GenerationFinish::Candidate {
            reason: "MAX_TOKENS",
            ..
        } => FinishReason::Length,
        GenerationFinish::Candidate {
            reason: "STOP" | "OTHER",
            tool_call: true,
        } => FinishReason::ToolCall,
        GenerationFinish::Candidate { reason: "STOP", .. } => FinishReason::Stop,
        GenerationFinish::Candidate {
            reason: "SAFETY" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" | "RECITATION",
            ..
        } => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}
