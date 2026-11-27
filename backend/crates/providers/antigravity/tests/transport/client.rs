use futures::{StreamExt, TryStreamExt, pin_mut};
use gateway_core::{
    error::{PreDeliveryRetry, ProviderErrorKind},
    event::{GatewayEvent, ProviderEvent},
    lifecycle::{CancellationToken, Deadline},
    routing::UpstreamModelId,
    upstream::UpstreamSendState,
};
use gateway_protocol::genai::GENAI_PROTOCOL;
use provider_antigravity::{
    CloudCodeClient, CloudCodeResponseContext, GenerateMode, decode_cloud_code_response,
};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime};
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path, query_param},
};

fn response_context(stream: bool) -> CloudCodeResponseContext {
    CloudCodeResponseContext {
        stream,
        model: UpstreamModelId::new("gemini-test").unwrap(),
        deadline: Deadline::from(SystemTime::now() + Duration::from_secs(5)),
        cancel: CancellationToken::new(),
        token: "stub-access".to_owned(),
        pricing: None,
        started_at: Instant::now(),
    }
}

fn priced_response_context(stream: bool) -> CloudCodeResponseContext {
    CloudCodeResponseContext {
        pricing: Some(
            serde_json::from_value(json!({
                "multiplierBps": 10_000,
                "bands": {
                    "standard": {"input": "1", "output": "2", "cacheRead": "1", "cacheWrite": "1"},
                },
            }))
            .unwrap(),
        ),
        ..response_context(stream)
    }
}

#[tokio::test]
async fn native_envelopes_keep_system_instructions_and_project_json_and_sse() {
    let stub = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
    let request = json!({
        "systemInstruction": {"parts":[{"text":"System instruction"}]},
        "contents":[{"role":"user","parts":[{"text":"Test"}]}],
    });
    let envelope = json!({"project":"stub-project","model":"gemini-test","request":request});
    let response = json!({
        "candidates":[{"content":{"role":"model","parts":[{"text":"Result"}]},"finishReason":"STOP"}],
        "usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":3,"thoughtsTokenCount":2,"totalTokenCount":12},
    });
    Mock::given(method("POST"))
        .and(path("/v1internal:generateContent"))
        .and(header("authorization", "Bearer stub-access"))
        .and(body_json(&envelope))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"response": response})))
        .expect(1)
        .mount(&stub)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1internal:streamGenerateContent"))
        .and(query_param("alt", "sse"))
        .and(header("authorization", "Bearer stub-access"))
        .and(body_json(&envelope))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!(
                "id: stub-frame\ndata: {}\n\n",
                json!({"response": response})
            ),
            "text/event-stream",
        ))
        .expect(1)
        .mount(&stub)
        .await;
    let body = serde_json::to_vec(&request).unwrap();
    for streaming in [false, true] {
        let upstream = client
            .generate(
                "stub-project",
                "gemini-test",
                "stub-access",
                &body,
                GenerateMode::from(streaming),
            )
            .await
            .unwrap();
        let stream = decode_cloud_code_response(upstream, response_context(streaming));
        pin_mut!(stream);
        let result: Vec<ProviderEvent> = stream.try_collect().await.unwrap();
        let wire = result[0].wire_event().unwrap();
        assert_eq!(wire.protocol(), GENAI_PROTOCOL);
        if streaming {
            assert_eq!(wire.data(), &response);
            assert_eq!(wire.sse_id(), Some("stub-frame"));
        } else {
            let json: serde_json::Value =
                serde_json::from_slice(wire.raw_json_body().unwrap()).unwrap();
            assert_eq!(json, response);
        }
        let facts: Vec<_> = result
            .iter()
            .flat_map(ProviderEvent::canonical_facts)
            .collect();
        assert!(facts.iter().any(|fact| match fact {
            GatewayEvent::Usage(usage) =>
                usage.input_tokens == Some(7)
                    && usage.output_tokens == Some(5)
                    && usage.reasoning_tokens == Some(2)
                    && usage.total_tokens == Some(12),
            _ => false,
        }));
        assert!(matches!(facts.last(), Some(GatewayEvent::Completed(_))));
    }
}

fn text_completion() -> Value {
    json!({
        "candidates": [{
            "content": {"parts": [{"text": "Answer"}, {
                "functionCall": {"name": "tag", "args": {
                    "inlineData": {"mimeType": "audio/wav"},
                    "modality": "AUDIO"
                }}
            }]},
            "finishReason": "STOP",
        }],
        "usageMetadata": {
            "promptTokenCount": 7,
            "candidatesTokenCount": 3,
            "totalTokenCount": 10,
        },
        "businessMetadata": {"mime_type": "video/mp4", "modality": "VIDEO"},
    })
}

#[tokio::test]
async fn dedicated_search_uses_the_resolved_model_and_marks_its_envelope() {
    let stub = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
    let body =
        json!({"contents":[{"parts":[{"text":"Find sources"}]}],"tools":[{"googleSearch":{}}]});
    let expected = json!({
        "project":"stub-project",
        "model":"search-only",
        "requestType":"web_search",
        "request":{
            "contents":body["contents"],
            "tools":body["tools"],
            "systemInstruction":{"parts":[{"text":"Search the web for the query and return a concise answer grounded in the sources."}]}
        },
    });
    Mock::given(method("POST"))
        .and(path("/v1internal:generateContent"))
        .and(body_json(expected))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&stub)
        .await;
    client
        .generate(
            "stub-project",
            "search-only",
            "stub-access",
            body.to_string().as_bytes(),
            GenerateMode::WebSearch,
        )
        .await
        .unwrap();
}

fn frames(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| format!("data: {value}\n\n"))
        .collect()
}

#[tokio::test]
async fn incomplete_json_preserves_metering_only_on_the_protocol_error() {
    let stub = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
    let mut incomplete = text_completion();
    incomplete["candidates"][0]
        .as_object_mut()
        .unwrap()
        .remove("finishReason");
    Mock::given(method("POST"))
        .and(path("/v1internal:generateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"response": incomplete})))
        .expect(1)
        .mount(&stub)
        .await;
    let upstream = client
        .generate(
            "stub-project",
            "gemini-test",
            "stub-access",
            b"{}",
            GenerateMode::Json,
        )
        .await
        .unwrap();
    let events = decode_cloud_code_response(upstream, priced_response_context(false));
    pin_mut!(events);

    let mut error = events.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::Protocol);
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    let metering = error.take_atomic_client_events();
    assert!(metering.iter().all(|event| event.wire_event().is_none()));
    let facts: Vec<_> = metering
        .iter()
        .flat_map(ProviderEvent::canonical_facts)
        .collect();
    assert!(matches!(
        facts.as_slice(),
        [GatewayEvent::Usage(usage), GatewayEvent::CalculatedCost(cost)]
            if usage.input_tokens == Some(7)
                && usage.output_tokens == Some(3)
                && usage.total_tokens == Some(10)
                && cost.total().amount().scaled() == 130_000
    ));
    assert!(events.next().await.is_none());
}

#[tokio::test]
async fn pricing_checks_candidate_media_and_all_usage_frames_in_json_and_sse() {
    for stream in [false, true] {
        for (unpriced, with_media) in [(false, false), (true, false), (true, true)] {
            let stub = MockServer::start().await;
            let client =
                CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
            let mut completion = text_completion();
            let mut extra_frame = None;
            if with_media {
                let media = json!({"file_data": {"mime_type": "video/mp4", "file_uri": "https://example.invalid/video"}});
                if stream {
                    completion["candidates"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("finishReason");
                    extra_frame = Some(json!({
                        "response": {"candidates": [{"content": {"parts": [media]}, "finishReason": "STOP"}]}
                    }));
                } else {
                    completion["candidates"][0]["content"]["parts"][1] = media;
                }
            } else if unpriced {
                let details = json!([{"modality": "AUDIO", "tokenCount": 2}]);
                if stream {
                    // A later frame can invalidate the cost computed from an earlier usage frame.
                    extra_frame = Some(json!({
                        "response": {"usageMetadata": {"toolUsePromptTokensDetails": details}}
                    }));
                } else {
                    completion["usageMetadata"]["cacheTokensDetails"] = details;
                }
            }
            let template = ResponseTemplate::new(200);
            let mut response = vec![json!({"response": completion})];
            response.extend(extra_frame);
            if stream {
                response.push(json!({"response": {"usageMetadata": {"candidatesTokenCount": 4}}}));
            }
            let (action, template) = if stream {
                (
                    "/v1internal:streamGenerateContent",
                    template.set_body_raw(frames(&response), "text/event-stream"),
                )
            } else {
                (
                    "/v1internal:generateContent",
                    template.set_body_json(&response[0]),
                )
            };
            Mock::given(method("POST"))
                .and(path(action))
                .respond_with(template)
                .expect(1)
                .mount(&stub)
                .await;
            let upstream = client
                .generate(
                    "stub-project",
                    "gemini-test",
                    "stub-access",
                    b"{}",
                    GenerateMode::from(stream),
                )
                .await
                .unwrap();
            let events: Vec<_> =
                decode_cloud_code_response(upstream, priced_response_context(stream))
                    .try_collect()
                    .await
                    .unwrap();
            let facts: Vec<_> = events
                .iter()
                .flat_map(ProviderEvent::canonical_facts)
                .collect();
            assert!(
                facts
                    .iter()
                    .any(|fact| matches!(fact, GatewayEvent::Usage(_)))
            );
            let cost_updates: Vec<_> = facts
                .iter()
                .filter_map(|fact| match fact {
                    GatewayEvent::CalculatedCost(cost) => {
                        Some(Some(cost.total().amount().scaled()))
                    }
                    GatewayEvent::CalculatedCostWithdrawn => Some(None),
                    _ => None,
                })
                .collect();
            let expected: &[_] = match (stream, unpriced) {
                (false, false) => &[Some(130_000)],
                (false, true) => &[],
                (true, false) => &[Some(130_000), Some(150_000)],
                (true, true) => &[Some(130_000), None],
            };
            assert_eq!(cost_updates, expected);
            assert!(matches!(facts.last(), Some(GatewayEvent::Completed(_))));
        }
    }
}

#[tokio::test]
async fn a_rejection_after_generation_output_cannot_rotate_the_account() {
    let rejection = json!({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED"}});
    for emitted_output in [false, true] {
        let stub = MockServer::start().await;
        let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
        let mut response_frames = Vec::new();
        if emitted_output {
            response_frames.push(json!({"response": text_completion()}));
        }
        response_frames.push(rejection.clone());
        Mock::given(method("POST"))
            .and(path("/v1internal:streamGenerateContent"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(frames(&response_frames), "text/event-stream"),
            )
            .expect(1)
            .mount(&stub)
            .await;
        let upstream = client
            .generate(
                "stub-project",
                "gemini-test",
                "stub-access",
                b"{}",
                GenerateMode::Stream,
            )
            .await
            .unwrap();
        let events = decode_cloud_code_response(upstream, priced_response_context(true));
        pin_mut!(events);
        if emitted_output {
            let event = events.next().await.unwrap().unwrap();
            assert!(event.wire_event().is_some());
            assert!(event.canonical_facts().iter().any(|fact| matches!(
                fact, GatewayEvent::CalculatedCost(cost) if cost.total().amount().scaled() == 130_000
            )));
        }
        let error = events.next().await.unwrap().err().unwrap();
        assert_eq!(error.kind(), ProviderErrorKind::RateLimited);
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        assert_eq!(error.replay_is_safe(), !emitted_output);
        assert_eq!(
            error.pre_delivery_retry(),
            (!emitted_output).then_some(PreDeliveryRetry::AccountRotation)
        );
        assert!(events.next().await.is_none());
    }
}

#[tokio::test]
async fn usage_frame_publishes_its_cost_before_cancellation() {
    let stub = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
    Mock::given(method("POST"))
        .and(path("/v1internal:streamGenerateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            frames(&[json!({"response": text_completion()})]),
            "text/event-stream",
        ))
        .expect(1)
        .mount(&stub)
        .await;
    let upstream = client
        .generate(
            "stub-project",
            "gemini-test",
            "stub-access",
            b"{}",
            GenerateMode::Stream,
        )
        .await
        .unwrap();
    let context = priced_response_context(true);
    let cancel = context.cancel.clone();
    let events = decode_cloud_code_response(upstream, context);
    pin_mut!(events);
    let frame = events.next().await.unwrap().unwrap();
    assert!(frame.canonical_facts().iter().any(|fact| matches!(
        fact, GatewayEvent::Usage(usage) if usage.total_tokens == Some(10)
    )));
    assert!(frame.canonical_facts().iter().any(|fact| matches!(
        fact, GatewayEvent::CalculatedCost(cost) if cost.total().amount().scaled() == 130_000
    )));

    cancel.cancel();
    let error = events.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
}

#[tokio::test]
async fn rejected_credential_leaves_one_successful_second_attempt() {
    for (status, kind) in [
        (401, ProviderErrorKind::Unauthorized),
        (429, ProviderErrorKind::RateLimited),
    ] {
        let stub = MockServer::start().await;
        let client = CloudCodeClient::new(&Url::parse(&stub.uri()).unwrap(), None, false).unwrap();
        Mock::given(method("POST"))
            .and(path("/v1internal:generateContent"))
            .and(header("authorization", "Bearer first-account"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(json!({"error": {"code": status}})),
            )
            .expect(1)
            .mount(&stub)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:generateContent"))
            .and(header("authorization", "Bearer second-account"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"response": text_completion()})),
            )
            .expect(1)
            .mount(&stub)
            .await;

        let error = client
            .generate(
                "first-project",
                "gemini-test",
                "first-account",
                b"{}",
                GenerateMode::Json,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        assert!(error.replay_is_safe());
        assert_eq!(
            error.pre_delivery_retry(),
            Some(PreDeliveryRetry::AccountRotation)
        );

        let response = client
            .generate(
                "second-project",
                "gemini-test",
                "second-account",
                b"{}",
                GenerateMode::Json,
            )
            .await
            .unwrap();
        let events: Vec<_> = decode_cloud_code_response(response, priced_response_context(false))
            .try_collect()
            .await
            .unwrap();
        let facts = || events.iter().flat_map(ProviderEvent::canonical_facts);
        assert_eq!(
            facts()
                .filter(|fact| matches!(fact, GatewayEvent::Completed(_)))
                .count(),
            1
        );
        assert_eq!(
            facts()
                .filter(|fact| matches!(fact, GatewayEvent::CalculatedCost(_)))
                .count(),
            1
        );
    }
}
