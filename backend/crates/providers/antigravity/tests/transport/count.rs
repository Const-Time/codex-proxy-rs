use futures::{Stream, TryStreamExt};
use gateway_core::{
    error::{ProviderError, ProviderErrorKind},
    event::{GatewayEvent, ProviderEvent},
    lifecycle::{CancellationToken, Deadline},
    routing::UpstreamModelId,
    upstream::UpstreamSendState,
};
use gateway_protocol::genai::GENAI_PROTOCOL;
use provider_antigravity::{CloudCodeClient, decode_count_response};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime};
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

#[tokio::test]
async fn count_sends_contents_or_nested_generation_input_in_the_count_envelope() {
    let upstream = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&upstream.uri()).unwrap(), None, true).unwrap();
    let contents = json!([{"role":"user","parts":[{"text":"Count me"}]}]);
    let tools = json!([{"functionDeclarations":[{"name":"lookup"}]}]);
    let instruction = json!({"parts":[{"text":"System instruction"}]});
    Mock::given(method("POST"))
        .and(path("/v1internal:countTokens"))
        .and(header("authorization", "Bearer stub-access"))
        .and(body_json(json!({"request":{
            "contents":contents,
            "tools":tools,
            "systemInstruction":instruction,
        }})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"totalTokens":23})))
        .expect(3)
        .mount(&upstream)
        .await;
    let generation = json!({
        "model":"models/ignored",
        "contents":contents,
        "tools":tools,
        "systemInstruction":instruction,
        "labels":{"client":"ignored"},
        "safetySettings":[],
        "tool_config":{},
        "sessionId":"ignored",
    });
    for body in [
        generation.clone(),
        json!({"contents":[], "generateContentRequest":generation}),
        json!({"generate_content_request":generation}),
    ] {
        let response = client
            .count_tokens("stub-access", body.to_string().as_bytes())
            .await
            .unwrap();
        assert_eq!(
            response.json::<Value>().await.unwrap(),
            json!({"totalTokens":23})
        );
    }
}

#[tokio::test]
async fn malformed_count_input_does_not_send_a_request() {
    let upstream = MockServer::start().await;
    let client = CloudCodeClient::new(&Url::parse(&upstream.uri()).unwrap(), None, false).unwrap();
    for body in ["[]", r#"{"generateContentRequest":null}"#] {
        let error = client
            .count_tokens("stub-access", body.as_bytes())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ProviderErrorKind::InvalidRequest);
        assert_eq!(error.send_state(), UpstreamSendState::NotSent);
    }
    assert!(upstream.received_requests().await.unwrap().is_empty());
}

async fn count_response(body: &str) -> (MockServer, reqwest::Response) {
    let upstream = MockServer::start().await;
    Mock::given(path("/v1internal:countTokens"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.as_bytes(), "application/json"))
        .mount(&upstream)
        .await;
    let client = CloudCodeClient::new(&upstream.uri().parse().unwrap(), None, false).unwrap();
    let response = client
        .count_tokens("stub-access", br#"{"contents":[]}"#)
        .await
        .unwrap();
    (upstream, response)
}

fn decode(
    response: reqwest::Response,
    cancellation: CancellationToken,
) -> impl Stream<Item = Result<ProviderEvent, ProviderError>> {
    decode_count_response(
        response,
        UpstreamModelId::new("gemini-high").unwrap(),
        Deadline::from(SystemTime::now() + Duration::from_secs(5)),
        cancellation,
        Instant::now(),
    )
}

#[tokio::test]
async fn count_response_preserves_raw_json_and_emits_only_lifecycle_facts() {
    for (body, expected) in [
        (
            r#"{ "totalTokens": 7, "extra": true }"#,
            r#"{ "totalTokens": 7, "extra": true }"#,
        ),
        ("{}", r#"{"totalTokens":0}"#),
    ] {
        let (_upstream, response) = count_response(body).await;
        let events: Vec<ProviderEvent> = decode(response, CancellationToken::new())
            .try_collect()
            .await
            .unwrap();
        let [event] = events.as_slice() else {
            panic!("expected a single count event");
        };
        let wire = event.wire_event().unwrap();
        assert_eq!(wire.protocol(), GENAI_PROTOCOL);
        assert_eq!(wire.raw_json_body().unwrap().as_ref(), expected.as_bytes());
        assert!(matches!(
            event.canonical_facts(),
            [GatewayEvent::Started(_), GatewayEvent::Completed(_)]
        ));
    }
}

#[tokio::test]
async fn malformed_count_response_and_cancelled_body_never_emit_wire() {
    for (cancelled, expected) in [
        (false, ProviderErrorKind::Protocol),
        (true, ProviderErrorKind::Cancelled),
    ] {
        let (_upstream, response) = count_response(r#"{"totalTokens":-1}"#).await;
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let error = decode(response, cancellation)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
    }
}
