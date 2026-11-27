//! Messages 原生响应保留 wire，旁路产生生命周期、用量及计费事实

use std::collections::BTreeMap;

use gateway_core::{
    event::{ContentItem, ContentKind, ReasoningDelta, TextDelta, ToolCallDelta},
    metering::{CalculatedCost, Usage},
};
use gateway_protocol::{
    anthropic::MessageUsage,
    openai::sse::{
        MAX_SSE_EVENT_BUFFER_BYTES, SseEventDecoder, SseFrame, strict_sse_frame_is_complete,
    },
};

use super::*;
use crate::provider::observation::insert_first_timing;
use crate::transport::usage::{OpenAiBillingUsage, configured_token_billing_breakdown};

// 非流式上游需要完整解码后才能交付；流式交付不受此总长限制
const MAX_MESSAGES_JSON_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct ColdMessages {
    pub client: CodexBackendClient,
    pub body: Bytes,
    pub passthrough_headers: reqwest::header::HeaderMap,
    pub context: AttemptContext,
    pub lease: Arc<CodexCredentialLease>,
    pub selector: Arc<CodexCredentialSelector>,
    pub quota: Arc<CodexCredentialQuotaService>,
    pub model: UpstreamModelId,
    pub response_origin: Url,
    pub endpoint: &'static str,
    pub count: bool,
    pub streaming: bool,
    pub fast_requested: bool,
}

pub(super) fn cold_messages_stream(mut request: ColdMessages) -> EventStream {
    Box::pin(async_stream::try_stream! {
        let failure_context = OpenAiFailureContext {
            client: &request.client, selector: &request.selector, quota: &request.quota,
            response_origin: &request.response_origin, cyber_policy_scope: None,
            allows_account_state_mutation: request.lease.allows_account_state_mutation(),
            image_endpoint: false,
        };
        let authorization = request.lease.authentication().authorization_header().map_err(|_| invalid_request())?;
        let trace = request.context.trace().clone();
        let upstream_context = CodexRequestContext::auxiliary(authorization.expose_secret(), None,
            request.context.request_id().as_str(), None).with_trace(&trace);
        let response = tokio::select! {
            biased;
            _ = request.context.cancellation().cancelled() => Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::NotSent)),
            _ = request.context.deadline().wait() => Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::NotSent)),
            response = request.client.post_messages(request.endpoint, std::mem::take(&mut request.body), &request.passthrough_headers, upstream_context) => {
                match response {
                    Ok(response) => Ok(response),
                    Err(error) => {
                        let failure = map_client_error(error, UpstreamSendState::Sent, false);
                        apply_failure(&failure_context, request.lease.account(), &failure).await;
                        Err(failure.error)
                    }
                }
            }
        }?;
        if response.streaming != request.streaming { Err(protocol_error())?; }
        let transport = if response.streaming { CodexBackendTransport::HttpSse } else { CodexBackendTransport::HttpJson };
        let observation = |timings| codex_response_observation(transport, &response.diagnostics,
            &response.response_metadata, &response.transport_metrics, None, timings);
        let mut timings = openai_response_timings(&response.transport_metrics, &response.response_metadata);
        if let Some(snapshot) = observation(timings) {
            yield ProviderEvent::observation(snapshot);
        }
        let streaming = response.streaming;
        let mut body = response.body;
        let mut decoder = SseEventDecoder::strict();
        let mut json = Vec::new();
        let mut state = MessageState::default();
        loop {
            let next = tokio::select! {
                biased;
                _ = request.context.cancellation().cancelled() => Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::Sent)),
                _ = request.context.deadline().wait() => Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::Sent)),
                chunk = body.next() => match chunk {
                    Some(Ok(bytes)) => Ok(Some(bytes)),
                    Some(Err(error)) => Err(map_stream_error(error).error),
                    None => Ok(None),
                },
            }?;
            let Some(bytes) = next else { break; };
            trace.capture("upstream.response.body", &bytes);
            if !bytes.is_empty() && insert_first_timing(&mut timings.first_event_ms, request.context.timing_started_at())
                && let Some(snapshot) = observation(timings) {
                yield ProviderEvent::observation(snapshot);
            }
            if streaming {
                for frame in decoder.push_frames(&bytes) {
                    let Some(event) = observed_frame(&mut state, frame, &request).await? else { continue; };
                    if observe_content_timings(&event, &mut timings, request.context.timing_started_at())
                        && let Some(snapshot) = observation(timings) {
                        yield ProviderEvent::observation(snapshot);
                    }
                    yield event;
                    if state.completed { break; }
                }
                if state.completed { break; }
            } else {
                if bytes.len() > MAX_MESSAGES_JSON_BYTES - json.len() { Err(protocol_error())?; }
                json.extend_from_slice(&bytes);
            }
        }
        if streaming {
            if !state.completed {
                for frame in decoder.finish_frames() {
                    let Some(event) = observed_frame(&mut state, frame, &request).await? else { continue; };
                    if observe_content_timings(&event, &mut timings, request.context.timing_started_at())
                        && let Some(snapshot) = observation(timings) {
                        yield ProviderEvent::observation(snapshot);
                    }
                    yield event;
                    if state.completed { break; }
                }
            }
            if !state.completed { Err(protocol_error())?; }
        } else {
            let value: Value = serde_json::from_slice(&json).map_err(|_| protocol_error())?;
            if value.get("type").and_then(Value::as_str) == Some("error") {
                let error = message_error(&value);
                record_message_failure(&request, &error).await;
                Err(error)?;
            }
            let meta = if request.count {
                if value.get("input_tokens").and_then(Value::as_u64).is_none() { Err(protocol_error())?; }
                ResponseMeta::new(request.context.request_id().as_str(), request.model.as_str())
            } else {
                message_meta(&value)?
            };
            let wire = ProtocolWireEvent::raw_json(
                ANTHROPIC_PROTOCOL, json.into(),
            ).map_err(|_| protocol_error())?;
            // 原生出口只交付 wire，开始事实必须附在可交付正文上，避免提前提交空批次
            let mut event = ProviderEvent::wire(wire).with_fact(GatewayEvent::Started(meta.clone()));
            if !request.count {
                state.usage.merge(&value["usage"]);
                event = with_usage(event, state.usage);
                state.unpriced_usage = unpriced_usage(&value["usage"]) || value.get("content").and_then(Value::as_array).is_some_and(|blocks| {
                    blocks.iter().any(|block| block.get("type").and_then(Value::as_str) == Some("server_tool_use"))
                });
                state.final_output_usage = true;
                event = state.with_cost(event, &request);
                if let Some(blocks) = value.get("content").and_then(Value::as_array) {
                    for block in blocks {
                        let target = match block.get("type").and_then(Value::as_str) {
                            Some("text") if block.get("text").and_then(Value::as_str).is_some_and(|text| !text.is_empty()) => Some(&mut timings.first_text_ms),
                            Some("thinking" | "redacted_thinking") => Some(&mut timings.first_reasoning_ms),
                            Some("tool_use" | "server_tool_use") => Some(&mut timings.first_token_ms),
                            _ => None,
                        };
                        if let Some(target) = target {
                            insert_first_timing(target, request.context.timing_started_at());
                            insert_first_timing(&mut timings.first_token_ms, request.context.timing_started_at());
                        }
                    }
                }
                if let Some(snapshot) = observation(timings) {
                    event.attach_observation(snapshot);
                }
            }
            yield event.with_fact(GatewayEvent::Completed(meta.with_finish_reason(finish_reason(value.get("stop_reason").and_then(Value::as_str)))));
        }
    })
}

// Messages 的首字从可消费内容开始，ping、消息元数据和空增量不计入输出延迟
fn observe_content_timings(
    event: &ProviderEvent,
    timings: &mut ProviderResponseTimings,
    started: Instant,
) -> bool {
    let mut changed = false;
    for fact in event.canonical_facts() {
        let target = match fact {
            GatewayEvent::TextDelta(delta) if !delta.text.is_empty() => {
                Some(&mut timings.first_text_ms)
            }
            GatewayEvent::ReasoningDelta(delta) if !delta.text.is_empty() => {
                Some(&mut timings.first_reasoning_ms)
            }
            GatewayEvent::ToolCallDelta(delta)
                if delta.name.is_some() || !delta.arguments_delta.is_empty() =>
            {
                Some(&mut timings.first_token_ms)
            }
            _ => None,
        };
        if let Some(target) = target {
            changed |= insert_first_timing(target, started);
            changed |= insert_first_timing(&mut timings.first_token_ms, started);
        }
    }
    changed
}

#[derive(Default)]
struct MessageState {
    meta: Option<ResponseMeta>,
    prelude: Vec<u8>,
    usage: MessageUsage,
    reason: Option<String>,
    tools: BTreeMap<u32, String>,
    completed: bool,
    unpriced_usage: bool,
    final_output_usage: bool,
    calculated_cost: Option<CalculatedCost>,
}

impl MessageState {
    fn frame(
        &mut self,
        frame: SseFrame,
        request: &ColdMessages,
    ) -> Result<Option<ProviderEvent>, ProviderError> {
        let (mut raw, events) = frame.into_parts();
        if raw.len() > MAX_SSE_EVENT_BUFFER_BYTES || !strict_sse_frame_is_complete(&raw) {
            return Err(protocol_error());
        }
        std::str::from_utf8(&raw).map_err(|_| protocol_error())?;
        let Some(sse) = events.into_iter().next() else {
            return if self.meta.is_some() {
                ProtocolWireEvent::raw_sse(ANTHROPIC_PROTOCOL, raw.into())
                    .map(|wire| Some(ProviderEvent::wire(wire)))
                    .map_err(|_| protocol_error())
            } else {
                self.buffer_prelude(&raw)?;
                Ok(None)
            };
        };
        let value: Value = serde_json::from_str(&sse.data).map_err(|_| protocol_error())?;
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .filter(|kind| !kind.trim().is_empty())
            .ok_or_else(protocol_error)?;
        if sse.event.as_deref().is_some_and(|name| name != kind) {
            return Err(protocol_error());
        }
        if kind == "error" {
            return Err(message_error(&value));
        }
        if self.meta.is_none() && kind != "message_start" {
            return if kind == "ping" {
                self.buffer_prelude(&raw)?;
                Ok(None)
            } else {
                Err(protocol_error())
            };
        }
        if !self.prelude.is_empty() {
            // The first wire event carries the original stream prefix without
            // letting comments or pings become a separate commit boundary.
            self.buffer_prelude(&raw)?;
            raw = std::mem::take(&mut self.prelude);
        }
        let wire = ProtocolWireEvent::json_with_raw_sse_metadata(
            ANTHROPIC_PROTOCOL,
            sse.event,
            value.clone(),
            raw.into(),
            sse.id,
            sse.retry,
        )
        .map_err(|_| protocol_error())?;
        let mut event = ProviderEvent::wire(wire);
        match kind {
            "message_start" => {
                if self.meta.is_some() {
                    return Err(protocol_error());
                }
                let meta = message_meta(&value["message"])?;
                self.meta = Some(meta.clone());
                self.usage.merge(&value["message"]["usage"]);
                self.unpriced_usage |= unpriced_usage(&value["message"]["usage"]);
                event = with_usage(event.with_fact(GatewayEvent::Started(meta)), self.usage);
            }
            "content_block_start" => {
                let index = block_index(&value)?;
                let block = &value["content_block"];
                let block_type = block["type"].as_str().ok_or_else(protocol_error)?;
                self.unpriced_usage |= block_type == "server_tool_use";
                let kind = match block_type {
                    "thinking" | "redacted_thinking" => ContentKind::Reasoning,
                    "tool_use" | "server_tool_use" => ContentKind::ToolCall,
                    _ => ContentKind::Text,
                };
                event = event.with_fact(GatewayEvent::ContentAdded(ContentItem::new(index, kind)));
                if kind == ContentKind::ToolCall {
                    let id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    self.tools.insert(index, id.clone());
                    event = event.with_fact(GatewayEvent::ToolCallDelta(ToolCallDelta {
                        content_index: index,
                        call_id: id,
                        name: block.get("name").and_then(Value::as_str).map(str::to_owned),
                        arguments_delta: String::new(),
                    }));
                }
                event = self.with_cost(event, request);
            }
            "content_block_delta" => {
                let index = block_index(&value)?;
                let delta = &value["delta"];
                match delta["type"].as_str().ok_or_else(protocol_error)? {
                    "text_delta" => {
                        event = event.with_fact(GatewayEvent::TextDelta(TextDelta {
                            content_index: index,
                            text: delta["text"]
                                .as_str()
                                .ok_or_else(protocol_error)?
                                .to_owned(),
                        }))
                    }
                    "thinking_delta" => {
                        event = event.with_fact(GatewayEvent::ReasoningDelta(ReasoningDelta {
                            content_index: index,
                            text: delta["thinking"]
                                .as_str()
                                .ok_or_else(protocol_error)?
                                .to_owned(),
                        }))
                    }
                    "input_json_delta" => {
                        event = event.with_fact(GatewayEvent::ToolCallDelta(ToolCallDelta {
                            content_index: index,
                            call_id: self.tools.get(&index).cloned().unwrap_or_default(),
                            name: None,
                            arguments_delta: delta["partial_json"]
                                .as_str()
                                .ok_or_else(protocol_error)?
                                .to_owned(),
                        }))
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                block_index(&value)?;
            }
            "message_delta" => {
                self.usage.merge(&value["usage"]);
                self.unpriced_usage |= unpriced_usage(&value["usage"]);
                self.final_output_usage |= value["usage"]["output_tokens"].as_u64().is_some();
                self.reason = value
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or(self.reason.take());
                // 结束用量先于 message_stop 到达，费用必须同时记录，避免随后截断或取消丢账
                event = self.with_cost(with_usage(event, self.usage), request);
            }
            "message_stop" => {
                let meta = self.meta.clone().ok_or_else(protocol_error)?;
                self.completed = true;
                event = event.with_fact(GatewayEvent::Completed(
                    meta.with_finish_reason(finish_reason(self.reason.as_deref())),
                ));
            }
            _ => {}
        }
        Ok(Some(event))
    }

    fn buffer_prelude(&mut self, raw: &[u8]) -> Result<(), ProviderError> {
        if raw.len() > MAX_SSE_EVENT_BUFFER_BYTES - self.prelude.len() {
            return Err(protocol_error());
        }
        self.prelude.extend_from_slice(raw);
        Ok(())
    }

    fn with_cost(&mut self, event: ProviderEvent, request: &ColdMessages) -> ProviderEvent {
        let cost = if self.unpriced_usage || !self.final_output_usage || request.fast_requested {
            None
        } else {
            messages_cost(self.usage, request)
        };
        if self.calculated_cost == cost {
            return event;
        }
        self.calculated_cost = cost;
        event.with_fact(match &self.calculated_cost {
            Some(cost) => GatewayEvent::CalculatedCost(cost.clone()),
            None => GatewayEvent::CalculatedCostWithdrawn,
        })
    }
}

fn block_index(value: &Value) -> Result<u32, ProviderError> {
    value["index"]
        .as_u64()
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(protocol_error)
}

fn message_meta(value: &Value) -> Result<ResponseMeta, ProviderError> {
    Ok(ResponseMeta::new(
        value
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(protocol_error)?,
        value
            .get("model")
            .and_then(Value::as_str)
            .ok_or_else(protocol_error)?,
    ))
}

fn finish_reason(value: Option<&str>) -> FinishReason {
    match value {
        Some("max_tokens" | "model_context_window_exceeded") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCall,
        Some("refusal") => FinishReason::ContentFilter,
        _ => FinishReason::Stop,
    }
}

fn with_usage(event: ProviderEvent, usage: MessageUsage) -> ProviderEvent {
    event.with_fact(GatewayEvent::Usage(Usage {
        input_tokens: usage.total_input_tokens(),
        output_tokens: usage.output_tokens,
        cached_tokens: usage.cache_read_input_tokens,
        cache_write_tokens: usage.cache_creation_input_tokens,
        ..Usage::new()
    }))
}

fn messages_cost(usage: MessageUsage, request: &ColdMessages) -> Option<CalculatedCost> {
    // Messages 的计价只能来自显式覆盖；模型别名不能隐式借用 OpenAI 内置价格
    let pricing = request
        .context
        .pricing()
        .get(PROVIDER_NAME)
        .and_then(|models| models.get(request.model.as_str()))?;
    let cache_read = usage.cache_read_input_tokens.unwrap_or_default();
    let cache_write = usage.cache_creation_input_tokens.unwrap_or_default();
    let input_tokens = usage
        .input_tokens?
        .checked_add(cache_read)?
        .checked_add(cache_write)?;
    let output_tokens = usage.output_tokens?;
    let usage = OpenAiBillingUsage::new(input_tokens, output_tokens, cache_read, cache_write);
    configured_token_billing_breakdown(usage, pricing).map(|cost| cost.calculated_cost())
}

fn unpriced_usage(usage: &Value) -> bool {
    usage.get("speed").and_then(Value::as_str) == Some("fast")
        || usage
            .pointer("/cache_creation/ephemeral_1h_input_tokens")
            .and_then(Value::as_u64)
            .is_some_and(|tokens| tokens > 0)
        || usage
            .get("server_tool_use")
            .and_then(Value::as_object)
            .is_some_and(|tools| {
                tools
                    .values()
                    .any(|value| value.as_u64().is_some_and(|count| count > 0))
            })
}

fn message_error(value: &Value) -> ProviderError {
    let error = &value["error"];
    let (Some(error_type), Some(message)) = (
        error["type"]
            .as_str()
            .filter(|kind| !kind.trim().is_empty()),
        error["message"].as_str(),
    ) else {
        return protocol_error();
    };
    let kind = match error_type {
        "authentication_error" => ProviderErrorKind::Unauthorized,
        "rate_limit_error" => ProviderErrorKind::RateLimited,
        "invalid_request_error" => ProviderErrorKind::InvalidRequest,
        "overloaded_error" => ProviderErrorKind::UpstreamCapacityUnavailable,
        "api_error" => ProviderErrorKind::Unavailable,
        _ => ProviderErrorKind::Protocol,
    };
    provider_error(kind, UpstreamSendState::Sent).with_client_visible_upstream_error(
        ClientVisibleUpstreamError::new(message, None, Some(error_type.to_owned())),
    )
}

async fn observed_frame(
    state: &mut MessageState,
    frame: SseFrame,
    request: &ColdMessages,
) -> Result<Option<ProviderEvent>, ProviderError> {
    match state.frame(frame, request) {
        Ok(event) => Ok(event),
        Err(error) => {
            record_message_failure(request, &error).await;
            Err(error)
        }
    }
}

async fn record_message_failure(request: &ColdMessages, error: &ProviderError) {
    if !request.lease.allows_account_state_mutation() {
        return;
    }
    let failure = match error.kind() {
        ProviderErrorKind::Unauthorized => Some(CodexAccountFailure::CredentialExpired),
        ProviderErrorKind::RateLimited => Some(CodexAccountFailure::RateLimited {
            retry_after: error.retry_after(),
        }),
        _ => None,
    };
    if let Some(failure) = failure {
        // 账号写回复用 selector 的凭据 revision 校验，流内错误不能污染已轮换的凭据
        if let Err(error) = request
            .selector
            .record_failure(request.lease.account(), failure, None)
            .await
        {
            tracing::warn!(account_id = %request.lease.account_id(), error = %error, "Failed to record Messages account failure");
        }
    }
}
