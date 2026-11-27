use gateway_core::event::ProviderEvent;
use gateway_protocol::anthropic::ANTHROPIC_PROTOCOL;
use provider_openai::credential::{ApiKeyProtocol, ApiKeyTransport};
use wiremock::matchers::{body_partial_json, query_param};

use super::*;

const MODEL: &str = "gpt-5.4";
const RESPONSES_ACCOUNT: &str = "acct_provider_contract";
const MESSAGES_ACCOUNT: &str = "acct_messages_contract";
const JSON_RESPONSE: &str = r#"{ "type":"message", "id":"msg_native", "model":"gpt-5.4", "content":[{"type":"text","text":"hello"}], "stop_reason":"max_tokens", "usage":{"input_tokens":10,"cache_read_input_tokens":3,"cache_creation_input_tokens":2,"output_tokens":4} }"#;
const MESSAGE_START: &str = "event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_native\",\"model\":\"gpt-5.4\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":2,\"output_tokens\":0}}}\r\n\r\n";
const SSE_RESPONSE: &str = concat!(
    "event: message_start\rdata: {\"type\":\"message_start\",\rx-extension: ignored\r",
    "data: \"message\":{\"id\":\"msg_native\",\"model\":\"gpt-5.4\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":2,\"output_tokens\":0}}}\r\r",
    ": native frame\r\n\r\n",
    "event: ping\ndata: {\"type\":\"ping\"}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":4}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\r\r",
);
const STREAM_PRELUDE: &str = ": upstream keepalive\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n";

fn messages_operation(stream: bool) -> Operation {
    let headers = json!([
        ["anthropic-version", STANDARD.encode("2023-06-01")],
        ["anthropic-beta", STANDARD.encode("fixture-beta")],
        ["x-api-key", STANDARD.encode("caller-key")],
        ["authorization", STANDARD.encode("Bearer caller-key")],
        ["cookie", STANDARD.encode("caller-session=value")],
    ]);
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            ANTHROPIC_PROTOCOL,
            json!({"model":"gateway-alias", "stream":stream, "max_tokens":10,
                "messages":[{"role":"user","content":"test"}], "system":"native-system"})
            .as_object()
            .unwrap()
            .clone(),
        )
        .unwrap()
        .with_context(Map::from_iter([(
            "opaque_request_headers".to_owned(),
            headers,
        )])),
    ))
}

fn count_operation() -> Operation {
    Operation::CountTokens(gateway_core::operation::TokenCountRequest::from_raw_json(
        RawJsonPayload::new(
            ANTHROPIC_PROTOCOL,
            Bytes::from_static(
                br#"{"model":"gpt-5.4","messages":[{"role":"user","content":"test"}]}"#,
            ),
        )
        .unwrap(),
    ))
}

async fn snapshot_from_catalog(provider: &CodexProvider) -> RuntimeSnapshot {
    let kind = ProviderKind::new("openai").unwrap();
    let models = provider
        .query_model_capabilities()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| {
            ProviderModel::new(
                kind.clone(),
                entry.upstream_model().clone(),
                entry.capabilities().clone(),
            )
        })
        .collect();
    RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        account_policy(),
        vec![kind],
        models,
        Vec::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn messages_account_preserves_native_json_sse_and_metering_with_overlapping_models() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/responses/v1/models"))
        .and(header("authorization", "Bearer sk-api-test-only"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":[{"id":MODEL}]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/anthropic/v1/models"))
        .and(query_param("limit", "1000"))
        .and(header("x-api-key", "sk-api-test-only"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(|request: &wiremock::Request| {
            if request
                .url
                .query_pairs()
                .any(|(key, value)| key == "after_id" && value == MODEL)
            {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data":[{"id":"claude-other"}], "has_more":false}))
            } else {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data":[{"id":MODEL}], "has_more":true, "last_id":MODEL}))
            }
        })
        .mount(&server)
        .await;
    let native_sse_response = format!(
        "{STREAM_PRELUDE}{}",
        SSE_RESPONSE.replacen(
            "event: message_start",
            "\u{feff}event: upstream_extension",
            1
        )
    );
    for (stream, content_type, body) in [
        (false, "application/json", JSON_RESPONSE),
        (true, "text/event-stream", native_sse_response.as_str()),
    ] {
        Mock::given(method("POST"))
            .and(path("/anthropic/v1/messages"))
            .and(header("x-api-key", "sk-api-test-only"))
            .and(header("anthropic-beta", "fixture-beta"))
            .and(body_partial_json(
                json!({"model":MODEL,"stream":stream, "system":"native-system"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body.to_owned(), content_type))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/responses/v1/responses"))
        .and(header("authorization", "Bearer sk-api-test-only"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(CAPTURE_COMPLETED_SSE, "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let store = Arc::new(MemoryAccountStore::default());
    store
        .seed_api_key(
            RESPONSES_ACCOUNT,
            format!("{}/responses/v1", server.uri()),
            ApiKeyTransport::Http,
        )
        .await;
    store
        .seed_api_key_with_protocol(
            MESSAGES_ACCOUNT,
            "http://messages.invalid/anthropic/v1".to_owned(),
            ApiKeyTransport::Http,
            ApiKeyProtocol::Messages,
        )
        .await;
    store.set_egress(
        MESSAGES_ACCOUNT,
        Some(gateway_core::account::OutboundProxy::parse(&server.uri()).unwrap()),
    );
    prime_catalog(&store, server.uri()).await;
    let provider = provider_with_base_url(&store, server.uri());
    assert_eq!(provider.query_model_capabilities().await.unwrap().len(), 2);
    let snapshot = snapshot_from_catalog(&provider).await;
    let plan = |model, operation: &Operation| {
        snapshot.plan(
            &PublicModelId::new(model).unwrap(),
            operation,
            contract_account_scope(),
            &RoutingContext::default(),
        )
    };
    assert!(plan("claude-other", &generate_operation()).is_err());
    assert!(plan(MODEL, &count_operation()).is_err());

    for (stream, expected_wire) in [(false, JSON_RESPONSE), (true, native_sse_response.as_str())] {
        let operation = messages_operation(stream);
        let planned = plan(MODEL, &operation).unwrap();
        let pricing = if stream {
            Default::default()
        } else {
            serde_json::from_value(json!({"openai": {(MODEL): {
                "multiplierBps": 10000,
                "bands": {"standard": {"input":"1", "output":"2", "cacheRead":"0.5", "cacheWrite":"1.25"}}
            }}})).unwrap()
        };
        let output = provider
            .execute(
                ProviderRequest::new(operation, planned.candidates()[0].clone()),
                context_with_pricing("req_messages_json_sse", CancellationToken::new(), pricing),
            )
            .await
            .unwrap();
        assert_eq!(
            output.metadata().provider_account_id().as_str(),
            MESSAGES_ACCOUNT
        );
        let events: Vec<_> = output.map(Result::unwrap).collect().await;
        let mut usage = None;
        let mut completed = false;
        let mut ticks = None;
        let mut wire_body = Vec::new();
        for event in events {
            if let Some(wire) = event.wire_event() {
                assert_eq!(wire.protocol(), ANTHROPIC_PROTOCOL);
                wire_body.extend_from_slice(wire.raw_json_body().or(wire.raw_sse_frame()).unwrap());
            }
            for fact in event.canonical_facts() {
                match fact {
                    GatewayEvent::Usage(value) => usage = Some(value.clone()),
                    GatewayEvent::Completed(_) => completed = true,
                    GatewayEvent::CalculatedCost(cost) => {
                        ticks = Some(cost.total().amount().scaled())
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(wire_body, expected_wire.as_bytes());
        assert!(completed);
        assert_eq!(ticks, (!stream).then_some(220_000));
        let usage = usage.unwrap();
        assert_eq!(usage.input_tokens, Some(15));
        assert_eq!(usage.output_tokens, Some(4));
        assert_eq!(usage.cached_tokens, Some(3));
        assert_eq!(usage.cache_write_tokens, Some(2));
    }
    let operation = generate_operation();
    let planned = plan(MODEL, &operation).unwrap();
    let mut output = provider
        .execute(
            ProviderRequest::new(operation, planned.candidates()[0].clone()),
            context("req_responses_collision", CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        output.metadata().provider_account_id().as_str(),
        RESPONSES_ACCOUNT
    );
    while let Some(event) = output.next().await {
        event.unwrap();
    }

    for request in server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/anthropic/v1/messages")
    {
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("cookie"));
    }
}

async fn messages_fixture() -> (
    MockServer,
    Arc<MemoryAccountStore>,
    CodexProvider,
    ProviderRequest,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/anthropic/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":[{"id":MODEL}]})))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    store
        .seed_api_key_with_protocol(
            MESSAGES_ACCOUNT,
            format!("{}/anthropic/v1", server.uri()),
            ApiKeyTransport::Http,
            ApiKeyProtocol::Messages,
        )
        .await;
    prime_catalog(&store, server.uri()).await;
    let provider = provider_with_base_url(&store, server.uri());
    let operation = messages_operation(true);
    let candidate = snapshot_from_catalog(&provider)
        .await
        .plan(
            &PublicModelId::new(MODEL).unwrap(),
            &operation,
            contract_account_scope(),
            &RoutingContext::default(),
        )
        .unwrap()
        .candidates()[0]
        .clone();
    (
        server,
        store,
        provider,
        ProviderRequest::new(operation, candidate),
    )
}

#[tokio::test]
async fn invalid_messages_sse_never_creates_a_deliverable_prelude() {
    let (server, _, provider, request) = messages_fixture().await;
    for invalid in [
        &b"event: message_start\ndata: {unterminated\n\n"[..],
        b"data: \xff\n\n",
        b"data: [DONE]\n\n",
        b"data: 123\n\n",
        b"event: ping\ndata: {}\n\n",
        b"event: message_start\ndata: {\"type\":\"ping\"}\n\n",
        b"data: {\"type\":\"message_start\",\r\"message\":{\"id\":\"msg_native\",\"model\":\"gpt-5.4\"}}\n\n",
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\"}}\n\n",
    ] {
        let upstream = Mock::given(method("POST"))
            .and(path("/anthropic/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                [STREAM_PRELUDE.as_bytes(), invalid, MESSAGE_START.as_bytes()].concat(),
                "text/event-stream",
            ))
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        let mut stream = provider
            .execute(
                request.clone(),
                context("req_invalid_messages", CancellationToken::new()),
            )
            .await
            .unwrap();
        let failure = loop {
            match stream.next().await.expect("upstream protocol failure") {
                Ok(event) => {
                    assert!(event.wire_event().is_none());
                    assert!(event.canonical_facts().is_empty());
                }
                Err(failure) => break failure,
            }
        };
        assert_eq!(failure.kind(), ProviderErrorKind::Protocol);
        assert!(failure.client_visible_upstream_error().is_none());
        assert!(stream.next().await.is_none());
        drop(upstream);
    }
}

#[tokio::test]
async fn messages_redirect_retains_only_upstream_status() {
    let (server, _, provider, request) = messages_fixture().await;
    Mock::given(method("POST"))
        .and(path("/anthropic/v1/messages"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/redirect-target", server.uri()))
                .set_body_json(json!({"error":{"type":"api_error","message":"redirect body"}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let mut stream = provider
        .execute(
            request,
            context("req_messages_redirect", CancellationToken::new()),
        )
        .await
        .unwrap();
    let failure = stream.next().await.unwrap().unwrap_err();
    assert_eq!(failure.upstream_status(), Some(302));
    assert!(failure.client_visible_upstream_error().is_none());
    assert!(failure.client_visible_upstream_response().is_none());
    assert!(stream.next().await.is_none());
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/redirect-target")
    );
}

#[tokio::test]
async fn messages_sse_withdraws_known_cost_and_retains_usage_on_native_failure() {
    let (server, _, provider, request) = messages_fixture().await;
    let stream_body = format!(
        "{MESSAGE_START}{}",
        concat!(
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":4}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":7,\"server_tool_use\":{\"web_search_requests\":1}}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":8}}\n\n",
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Fixture overload after partial output\"},\"extra\":\"not an error field\"}\n\n",
        )
    );
    Mock::given(method("POST"))
        .and(path("/anthropic/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(stream_body, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;
    let pricing = serde_json::from_value(json!({"openai": {(MODEL): {
        "multiplierBps": 10000,
        "bands": {"standard": {"input":"1", "output":"2", "cacheRead":"0.5", "cacheWrite":"1.25"}}
    }}}))
    .unwrap();
    let mut stream = provider
        .execute(
            request,
            context_with_pricing(
                "req_messages_cost_withdrawn",
                CancellationToken::new(),
                pricing,
            ),
        )
        .await
        .unwrap();
    let mut cost_updates = Vec::new();
    let mut usage = None;
    let failure = loop {
        match stream.next().await.expect("native Messages error") {
            Ok(event) => {
                for fact in event.canonical_facts() {
                    match fact {
                        GatewayEvent::CalculatedCost(cost) => {
                            cost_updates.push(Some(cost.total().amount().scaled()))
                        }
                        GatewayEvent::CalculatedCostWithdrawn => cost_updates.push(None),
                        GatewayEvent::Usage(value) => usage = Some(value.clone()),
                        GatewayEvent::Completed(_) => panic!("failed Messages stream completed"),
                        _ => {}
                    }
                }
            }
            Err(failure) => break failure,
        }
    };
    assert_eq!(cost_updates, [Some(220_000), None]);
    let usage = usage.unwrap();
    assert_eq!(usage.input_tokens, Some(15));
    assert_eq!(usage.output_tokens, Some(8));
    assert_eq!(usage.cached_tokens, Some(3));
    assert_eq!(usage.cache_write_tokens, Some(2));
    assert_eq!(
        failure.kind(),
        ProviderErrorKind::UpstreamCapacityUnavailable
    );
    let public = failure.client_visible_upstream_error().unwrap();
    assert_eq!(public.error_type(), Some("overloaded_error"));
    assert_eq!(public.message(), "Fixture overload after partial output");
    assert!(!failure.has_atomic_client_events());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn connection_probe_uses_the_pinned_messages_credential_with_one_revision_checked_load() {
    let (server, store, provider, request) = messages_fixture().await;
    let account = store.account(MESSAGES_ACCOUNT).unwrap();
    store.set_enabled(account.id(), false).await.unwrap();
    store
        .seed_api_key(
            RESPONSES_ACCOUNT,
            format!("{}/responses/v1", server.uri()),
            ApiKeyTransport::Http,
        )
        .await;
    Mock::given(method("POST"))
        .and(path("/anthropic/v1/messages"))
        .and(header("x-api-key", "sk-api-test-only"))
        .and(header("anthropic-version", "2023-06-01"))
        .and(body_partial_json(json!({"model":MODEL,"messages":[{"role":"user","content":[{"type":"text","text":"Reply with exactly OK."}]}],"stream":true,"max_tokens":256})))
        .respond_with(ResponseTemplate::new(200).set_body_raw(SSE_RESPONSE, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;
    let payload = ProtocolPayload::json_object("openai", json!({"model":MODEL,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Reply with exactly OK."}]}],"stream":true,"store":false}).as_object().unwrap().clone()).unwrap();
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(payload));
    let credential_loads = store.credential_loads();
    assert!(provider.diagnostic_selects_generation_protocol());
    let stream = provider
        .execute(
            ProviderRequest::new(operation, request.candidate().clone()),
            diagnostic_context("req_messages_probe", MESSAGES_ACCOUNT),
        )
        .await
        .unwrap();
    assert_eq!(store.credential_loads() - credential_loads, 1);
    assert_eq!(stream.metadata().provider_account_id(), account.id());
    let events: Vec<_> = stream.map(Result::unwrap).collect().await;
    assert!(
        events
            .iter()
            .flat_map(ProviderEvent::canonical_facts)
            .any(|fact| matches!(fact, GatewayEvent::TextDelta(delta) if delta.text == "hello"))
    );
    assert!(
        events
            .iter()
            .flat_map(ProviderEvent::canonical_facts)
            .any(|fact| matches!(fact, GatewayEvent::Completed(_)))
    );
    assert!(!store.account(MESSAGES_ACCOUNT).unwrap().enabled());
    assert!(!store.has_quota(MESSAGES_ACCOUNT));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn messages_count_requires_official_base_in_checked_credential() {
    use gateway_core::account::AccountCatalogObservation;

    let store = Arc::new(MemoryAccountStore::default());
    for (id, base) in [
        (MESSAGES_ACCOUNT, "https://api.anthropic.com:443/v1/"),
        (RESPONSES_ACCOUNT, "https://api.anthropic.com:8443/v1"),
    ] {
        store
            .seed_api_key_with_protocol(
                id,
                base.to_owned(),
                ApiKeyTransport::Http,
                ApiKeyProtocol::Messages,
            )
            .await;
        let account = store.account(id).unwrap();
        let observation = AccountCatalogObservation {
            account_id: account.id().clone(),
            revision: account.revision(),
            plan: None,
            etag: None,
            observed_at: SystemTime::now(),
        };
        assert!(
            store
                .save_account_catalog(&observation, b"\x03{\"data\":[{\"id\":\"gpt-5.4\"}]}")
                .await
                .unwrap()
        );
    }
    let provider = provider(&store);
    let snapshot = snapshot_from_catalog(&provider).await;
    let operation = count_operation();
    let plan = snapshot
        .plan(
            &PublicModelId::new(MODEL).unwrap(),
            &operation,
            contract_account_scope(),
            &RoutingContext::default(),
        )
        .unwrap();
    let request = ProviderRequest::new(operation, plan.candidates()[0].clone());
    let result = provider
        .execute(
            request.clone(),
            diagnostic_context("req_count_wrong_origin", RESPONSES_ACCOUNT),
        )
        .await;
    assert!(result.is_err());
    let stream = provider
        .execute(
            request,
            diagnostic_context("req_count_official_origin", MESSAGES_ACCOUNT),
        )
        .await
        .unwrap();
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        MESSAGES_ACCOUNT
    );
    // Dropping the cold stream never opens a connection to Anthropic.
    drop(stream);
}
