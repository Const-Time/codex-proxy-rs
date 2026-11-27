mod stream;

use gateway_core::operation::TokenCountRequest;
use gateway_protocol::anthropic::ANTHROPIC_PROTOCOL;
use reqwest::header::HeaderMap;

use super::*;
use crate::credential::CredentialProtocol;

struct MessagesInput {
    body: Map<String, Value>,
    passthrough_headers: HeaderMap,
    count: bool,
}

impl CodexProvider {
    pub(super) async fn execute_messages(
        &self,
        request: &GenerateRequest,
        candidate: &ProviderCandidate,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let payload = request.protocol_payload();
        gateway_protocol::anthropic::validate_request(payload.body())
            .map_err(|_| invalid_request())?;
        let affinity = crate::credential::derive_messages_session_affinity(
            payload,
            context.client_api_key_ref(),
        );
        self.messages_stream(
            payload.body().clone(),
            payload.context(),
            candidate,
            context,
            CredentialProtocol::Messages,
            affinity.as_ref().map(CodexSessionAffinity::key),
        )
        .await
    }

    pub(super) async fn execute_messages_count(
        &self,
        request: &TokenCountRequest,
        candidate: &ProviderCandidate,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let payload = request.payload();
        if payload.protocol() != ANTHROPIC_PROTOCOL {
            return Err(provider_error(
                ProviderErrorKind::Unsupported,
                UpstreamSendState::NotSent,
            ));
        }
        let body = serde_json::from_slice(payload.body()).map_err(|_| invalid_request())?;
        gateway_protocol::anthropic::validate_count_request(&body)
            .map_err(|_| invalid_request())?;
        self.messages_stream(
            body,
            payload.context(),
            candidate,
            context,
            CredentialProtocol::MessagesCount,
            None,
        )
        .await
    }

    async fn messages_stream(
        &self,
        body: Map<String, Value>,
        payload_context: &Map<String, Value>,
        candidate: &ProviderCandidate,
        context: AttemptContext,
        protocol: CredentialProtocol,
        session_affinity_key: Option<&ProviderSessionAffinityKey>,
    ) -> Result<ProviderStream, ProviderError> {
        let model = candidate.upstream_model().ok_or_else(invalid_request)?;
        let started = Instant::now();
        let request = SelectCodexCredential {
            upstream_model: model.as_str(),
            request_url: &self.responses_url,
            attempt: &context,
            session_affinity_key,
        };
        let selection = self.selector.select_for_protocol(&request, protocol);
        let lease = if let Some(remaining) = context.connection_budget().startup_remaining() {
            tokio::time::timeout(remaining, selection)
                .await
                .map_err(|_| {
                    provider_error(ProviderErrorKind::Timeout, UpstreamSendState::NotSent)
                })?
        } else {
            selection.await
        }
        .map_err(map_selection_error)?;
        let account_selection_wait_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.messages_stream_with_lease(
            MessagesInput {
                body,
                passthrough_headers: crate::transport::request::decode_passthrough_headers(
                    payload_context,
                ),
                count: protocol == CredentialProtocol::MessagesCount,
            },
            candidate,
            context,
            lease,
            account_selection_wait_ms,
        )
    }

    pub(super) fn execute_messages_probe(
        &self,
        generate: &GenerateRequest,
        candidate: &ProviderCandidate,
        context: AttemptContext,
        lease: CodexCredentialLease,
        account_selection_wait_ms: u64,
    ) -> Result<ProviderStream, ProviderError> {
        let probe = generate.protocol_payload().body();
        let messages = match probe.get("input") {
            Some(Value::String(text)) => vec![json!({"role": "user", "content": text})],
            Some(Value::Array(items)) if !items.is_empty() => items
                .iter()
                .map(probe_message)
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(invalid_request()),
        };
        let mut body = Map::from_iter([
            ("messages".to_owned(), Value::Array(messages)),
            ("stream".to_owned(), Value::Bool(true)),
            (
                "max_tokens".to_owned(),
                json!(
                    probe
                        .get("max_output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(256)
                ),
            ),
        ]);
        if let Some(system) = probe.get("instructions").and_then(Value::as_str) {
            body.insert("system".to_owned(), json!(system));
        }
        self.messages_stream_with_lease(
            MessagesInput {
                body,
                passthrough_headers: HeaderMap::new(),
                count: false,
            },
            candidate,
            context,
            lease,
            account_selection_wait_ms,
        )
    }

    fn messages_stream_with_lease(
        &self,
        input: MessagesInput,
        candidate: &ProviderCandidate,
        context: AttemptContext,
        lease: CodexCredentialLease,
        account_selection_wait_ms: u64,
    ) -> Result<ProviderStream, ProviderError> {
        let MessagesInput {
            mut body,
            passthrough_headers,
            count,
        } = input;
        let model = candidate
            .upstream_model()
            .cloned()
            .ok_or_else(invalid_request)?;
        body.insert("model".to_owned(), json!(model.as_str()));
        let streaming = !count && body.get("stream").and_then(Value::as_bool) == Some(true);
        let endpoint = if count {
            "/messages/count_tokens"
        } else {
            "/messages"
        };
        let crate::credential::CodexRuntimeAuthentication::ApiKey(authentication) =
            lease.authentication()
        else {
            return Err(invalid_request());
        };
        let transport = if streaming {
            HTTP_SSE_TRANSPORT
        } else {
            HTTP_JSON_TRANSPORT
        };
        let metadata = ProviderCallMetadata::new(
            candidate.provider().clone(),
            model.clone(),
            lease.account_id().clone(),
            UpstreamTransport::new(transport).map_err(|_| invalid_request())?,
        )
        .with_selection_observation(ProviderSelectionObservation::new(
            account_selection_wait_ms,
            lease.capacity_snapshot(),
        ));
        let client = self
            .client
            .for_messages_account(lease.account(), lease.authentication())
            .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?
            .with_connection_budget(context.connection_budget().clone());
        let response_origin = Url::parse(&endpoint_url(
            &authentication.configuration.base_url,
            endpoint,
        ))
        .map_err(|_| invalid_request())?;
        let allows_account_state_mutation = lease.allows_account_state_mutation();
        let lease = Arc::new(lease);
        let events = stream::cold_messages_stream(stream::ColdMessages {
            client,
            fast_requested: body.get("speed").and_then(Value::as_str) == Some("fast"),
            body: serde_json::to_vec(&body)
                .map_err(|_| invalid_request())?
                .into(),
            passthrough_headers,
            context,
            lease: Arc::clone(&lease),
            selector: Arc::clone(&self.selector),
            quota: Arc::clone(&self.quota),
            model,
            response_origin,
            endpoint,
            count,
            streaming,
        });
        let stream = ProviderStream::new(metadata, events, lease);
        Ok(if allows_account_state_mutation && !count {
            stream.with_filtered_account_feedback(
                Arc::clone(&self.account_feedback),
                openai_failure_affects_account_score,
            )
        } else {
            stream
        })
    }
}

fn probe_message(item: &Value) -> Result<Value, ProviderError> {
    let role = item["role"]
        .as_str()
        .filter(|role| matches!(*role, "user" | "assistant"))
        .ok_or_else(invalid_request)?;
    let content = match &item["content"] {
        Value::String(text) => json!(text),
        Value::Array(blocks) if !blocks.is_empty() => Value::Array(
            blocks
                .iter()
                .map(|block| {
                    let text = block["text"]
                        .as_str()
                        .filter(|_| {
                            matches!(block["type"].as_str(), Some("input_text" | "output_text"))
                        })
                        .ok_or_else(invalid_request)?;
                    Ok(json!({"type": "text", "text": text}))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err(invalid_request()),
    };
    Ok(json!({"role": role, "content": content}))
}

fn invalid_request() -> ProviderError {
    provider_error(
        ProviderErrorKind::InvalidRequest,
        UpstreamSendState::NotSent,
    )
}

fn protocol_error() -> ProviderError {
    provider_error(ProviderErrorKind::Protocol, UpstreamSendState::Sent)
}
