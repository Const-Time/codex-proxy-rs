use super::*;
use gateway_core::account::FastMode;
use gateway_core::routing::{ModelPresentation, ModelServiceTier};

fn planned(operation: Operation, supports_priority: bool) -> ProviderRequest {
    let provider = ProviderKind::new("openai").unwrap();
    let mut model = ProviderModel::new(
        provider.clone(),
        UpstreamModelId::new("gpt-5.4").unwrap(),
        ModelCapabilities::new(BTreeSet::from([operation.kind()]), Some(32_000))
            .with_upstream_feature_validation(),
    );
    if supports_priority {
        model = model.with_presentation(
            ModelPresentation::default()
                .with_service_tiers(vec![ModelServiceTier::new("priority", "Fast", "Priority")]),
        );
    }
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        account_policy(),
        vec![provider],
        vec![model],
        vec![],
    )
    .unwrap();
    let plan = snapshot
        .plan(
            &PublicModelId::new("gpt-5.4").unwrap(),
            &operation,
            contract_account_scope(),
            &RoutingContext::default(),
        )
        .unwrap();
    ProviderRequest::new(operation, plan.candidates()[0].clone())
}
fn prepare(provider: &CodexProvider, request: ProviderRequest, mode: FastMode) -> ProviderRequest {
    let operation =
        provider.prepare_operation(request.operation().clone(), request.candidate(), mode);
    ProviderRequest::new(operation, request.candidate().clone())
}

#[tokio::test]
async fn fast_policy_uses_frozen_catalog_evidence_preserves_explicit_tiers_and_host_state() {
    let provider = provider(&Arc::new(MemoryAccountStore::default()));
    let cases = [
        (None, None, Some(json!("priority")), None),
        (
            Some(Value::Null),
            Some(Value::Null),
            Some(json!("priority")),
            Some(Value::Null),
        ),
        (
            Some(json!("default")),
            Some(json!("default")),
            Some(json!("priority")),
            Some(json!("default")),
        ),
        (
            Some(json!("priority")),
            Some(json!("priority")),
            Some(json!("priority")),
            Some(json!("default")),
        ),
        (
            Some(json!("fast")),
            Some(json!("fast")),
            Some(json!("fast")),
            Some(json!("default")),
        ),
        (
            Some(json!("flex")),
            Some(json!("flex")),
            Some(json!("flex")),
            Some(json!("flex")),
        ),
        (
            Some(json!("auto")),
            Some(json!("auto")),
            Some(json!("auto")),
            Some(json!("auto")),
        ),
        (
            Some(json!(123)),
            Some(json!(123)),
            Some(json!(123)),
            Some(json!(123)),
        ),
        (
            Some(json!(" Priority ")),
            Some(json!(" Priority ")),
            Some(json!(" Priority ")),
            Some(json!("default")),
        ),
    ];
    for supports in [false, true] {
        for (input, default, enabled, disabled) in &cases {
            for (mode, expected) in [
                (FastMode::Default, default),
                (FastMode::Enabled, if supports { enabled } else { input }),
                (FastMode::Disabled, disabled),
            ] {
                let mut body=json!({"model":"gpt-5.4","input":"hello","reasoning":{},"unknown":{"keep":true}}).as_object().unwrap().clone();
                if let Some(input) = input {
                    body.insert("service_tier".into(), input.clone());
                }
                let state = ProviderSessionState::new(
                    "openai",
                    json!({"private":"fixture"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let context = json!({"use_websocket":false,"connection_id":"fixture"})
                    .as_object()
                    .unwrap()
                    .clone();
                let operation = Operation::Generate(
                    GenerateRequest::from_protocol_payload(
                        ProtocolPayload::json_object("openai", body.clone())
                            .unwrap()
                            .with_context(context.clone()),
                    )
                    .with_provider_session_state(state.clone()),
                );
                let wire = Bytes::from(serde_json::to_vec(&body).unwrap());
                let carrier = gateway_core::engine::middleware::MiddlewareRequest::new(
                    "openai",
                    vec![],
                    wire.clone(),
                )
                .replace_parts(
                    "openai".into(),
                    vec![],
                    wire,
                    Some(
                        gateway_core::engine::middleware::MiddlewareCapabilityDeclaration {
                            handled: BTreeSet::new(),
                            required: BTreeSet::from([
                                gateway_core::operation::Feature::JsonSchema,
                            ]),
                        },
                    ),
                )
                .unwrap();
                let operation = carrier.apply_capabilities(operation).unwrap();
                let requirements = operation.capability_requirements();
                let original = planned(operation, supports);
                let prepared = prepare(&provider, original.clone(), mode);
                let Operation::Generate(request) = prepared.operation() else {
                    panic!("generate");
                };
                assert_eq!(
                    request.protocol_payload().body().get("service_tier"),
                    expected.as_ref(),
                    "{mode:?} supports={supports} input={input:?}"
                );
                assert_eq!(request.protocol_payload().context(), &context);
                assert_eq!(request.provider_session_state("openai"), Some(&state));
                assert_eq!(prepared.operation().capability_requirements(), requirements);
                let mut actual = request.protocol_payload().body().clone();
                actual.remove("service_tier");
                body.remove("service_tier");
                assert_eq!(actual, body);
                let Operation::Generate(original) = original.operation() else {
                    panic!("generate");
                };
                assert_eq!(
                    original.protocol_payload().body().get("service_tier"),
                    input.as_ref()
                );
            }
        }
    }
}

#[tokio::test]
async fn fast_http_wire_and_pricing_follow_the_outbound_tier_preserving_response_diagnostics() {
    let pricing: gateway_core::metering::PricingOverrides =
        serde_json::from_value(json!({"openai":{"gpt-5.4":{"multiplierBps":10000,"bands":{
            "standard":{"input":"1","output":"1","cacheRead":"0","cacheWrite":"0"},
            "fast":{"input":"2","output":"2","cacheRead":"0","cacheWrite":"0"}
        }}}}))
        .unwrap();
    for (mode, input, expected, response_tier, cost) in [
        (FastMode::Enabled, None, "priority", None, 40000),
        (FastMode::Disabled, Some("priority"), "default", None, 20000),
        (FastMode::Default, Some("priority"), "priority", None, 40000),
        (FastMode::Enabled, None, "priority", Some("default"), 40000),
        (
            FastMode::Disabled,
            Some("priority"),
            "default",
            Some("priority"),
            20000,
        ),
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let server = MockServer::start().await;
        let created =
            json!({"type":"response.created","response":{"id":"resp_fast_http","model":"gpt-5.4"}});
        let mut completed = json!({"type":"response.completed","response":{"id":"resp_fast_http","model":"gpt-5.4","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"input_tokens_details":{"cached_tokens":0},"total_tokens":2}}});
        if let Some(tier) = response_tier {
            completed["response"]["service_tier"] = json!(tier);
        }
        let response = [created, completed]
            .into_iter()
            .map(|value| format!("data: {value}\n\n"))
            .collect::<String>();
        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(response),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = provider_with_base_url(&store, server.uri());
        let Operation::Generate(request) = http_generate_operation() else {
            panic!("generate");
        };
        let mut body = request.protocol_payload().body().clone();
        if let Some(input) = input {
            body.insert("service_tier".into(), json!(input));
        }
        let request = planned(Operation::Generate(request.with_protocol_body(body)), true);
        let request = prepare(&provider, request, mode);
        let mut stream = provider
            .execute(
                request,
                context_with_pricing("req_fast_http", CancellationToken::new(), pricing.clone()),
            )
            .await
            .unwrap();
        let mut costs = vec![];
        let mut observation = None;
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            if let Some(value) = event.response_observation() {
                assert_eq!(value.service_tier(), Some(expected));
                observation = Some(value.clone());
            }
            for fact in event.canonical_facts() {
                if let GatewayEvent::CalculatedCost(value) = fact {
                    costs.push(value.total().amount().scaled());
                }
            }
        }
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            captured_request_body(&requests[0])["service_tier"],
            expected
        );
        assert_eq!(costs, vec![cost]);
        let observation = observation.expect("final provider observation");
        let metadata: Value =
            serde_json::from_str(observation.provider_metadata().unwrap().as_json()).unwrap();
        assert_eq!(
            metadata.get("requestedServiceTier").and_then(Value::as_str),
            Some(expected)
        );
        assert_eq!(
            metadata.get("upstreamServiceTier").and_then(Value::as_str),
            response_tier
        );
    }
}

#[tokio::test]
async fn fast_websocket_wire_uses_the_same_prepared_native_body() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_codex_test_websocket(socket).await;
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let create: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(create["type"], "response.create");
                    assert_eq!(create["service_tier"], "priority");
                    break;
                }
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                other => panic!("unexpected frame {other:?}"),
            }
        }
        socket.send(Message::Text(json!({"type":"response.completed","response":{"id":"resp_fast_ws","model":"gpt-5.4","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}).to_string().into())).await.unwrap();
    });
    let provider = provider_with_base_url(&store, base_url);
    let request = prepare(
        &provider,
        planned(generate_operation(), true),
        FastMode::Enabled,
    );
    let mut stream = provider
        .execute(request, context("req_fast_ws", CancellationToken::new()))
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            event.unwrap();
        }
    })
    .await
    .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn fast_defaults_leave_other_protocols_and_raw_endpoints_unchanged() {
    let provider = provider(&Arc::new(MemoryAccountStore::default()));
    let body = json!({"model":"gpt-5.4","input":"hello","service_tier":"priority"})
        .as_object()
        .unwrap()
        .clone();
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("other", body.clone()).unwrap(),
    ));
    let request = prepare(&provider, planned(operation, true), FastMode::Disabled);
    let Operation::Generate(request) = request.operation() else {
        panic!("generate");
    };
    assert_eq!(request.protocol_payload().protocol(), "other");
    assert_eq!(request.protocol_payload().body(), &body);
    let wire = Bytes::from_static(br#"{ "service_tier": "priority", "unknown":true }"#);
    let operation = Operation::GenerateImage(ImageRequest::from_raw_json(
        ImageRequestKind::Generation,
        RawJsonPayload::new("openai", wire.clone()).unwrap(),
    ));
    let request = prepare(
        &provider,
        planned_provider_endpoint_request("openai", operation),
        FastMode::Disabled,
    );
    let Operation::GenerateImage(request) = request.operation() else {
        panic!("image");
    };
    assert_eq!(request.payload().body(), &wire);
    let operation = Operation::Search(StandaloneSearchRequest::from_raw_json(
        RawJsonPayload::new("openai", wire.clone()).unwrap(),
    ));
    let request = prepare(
        &provider,
        planned_provider_endpoint_request("openai", operation),
        FastMode::Disabled,
    );
    let Operation::Search(request) = request.operation() else {
        panic!("search");
    };
    assert_eq!(request.payload().body(), &wire);
}
