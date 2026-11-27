use gateway_core::{
    account::{AccountSelectionPolicy, RotationStrategy},
    engine::{AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext},
    error::ProviderErrorKind,
    event::ProtocolWireEvent,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    upstream::UpstreamSendState,
};
use gateway_protocol::{chat_completions::CHAT_PROTOCOL, genai::GENAI_PROTOCOL};
use provider_antigravity::ChatProjection;
use serde_json::{Value, json};
use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

fn attempt() -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_test-request").unwrap(),
            ClientApiKeyId::new("test-client").unwrap(),
        ),
        NonZeroU32::MIN,
        SystemTime::now() + Duration::from_secs(1),
        AccountSelectionPolicy::new(
            RotationStrategy::RoundRobin,
            NonZeroU32::MIN,
            Duration::ZERO,
        ),
        AccountAttemptContext::default(),
        None,
        CancellationToken::new(),
    )
}

#[test]
fn chat_distinguishes_a_candidate_limitation_from_invalid_input() {
    for (value, expected) in [
        (json!(false), ProviderErrorKind::Unsupported),
        (json!("false"), ProviderErrorKind::InvalidRequest),
    ] {
        let request = json!({
            "model": "alias",
            "messages": [{"role": "user", "content": "Question"}],
            "tools": [{"type": "function", "function": {"name": "inspect"}}],
            "parallel_tool_calls": value,
        });
        let error = ChatProjection::prepare(request.as_object().unwrap(), &attempt())
            .err()
            .expect("request should be rejected during conversion");
        assert_eq!(error.kind(), expected);
        assert_eq!(error.send_state(), UpstreamSendState::NotSent);
        assert!(error.pre_delivery_retry().is_none());
    }
}

#[test]
fn chat_projects_genai_completion_json_and_sse() {
    let response = json!({
        "candidates":[{"content":{"role":"model","parts":[{"text":"Answer"}]},"finishReason":"STOP"}],
        "usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6},
    });
    for streaming in [false, true] {
        let request = json!({
            "model":"test-public-alias",
            "messages":[{"role":"system","content":"Instructions"},{"role":"user","content":"Question"}],
            "stream": streaming,
            "stream_options": if streaming { json!({"include_usage":true}) } else { Value::Null },
        });
        let (converted, mut projection) =
            ChatProjection::prepare(request.as_object().unwrap(), &attempt()).unwrap();
        assert_eq!(
            converted["systemInstruction"]["parts"][0]["text"],
            "Instructions"
        );
        assert_eq!(converted["contents"][0]["parts"][0]["text"], "Question");
        if streaming {
            let wire = ProtocolWireEvent::json(GENAI_PROTOCOL, None, response.clone()).unwrap();
            let mut chunks = projection.translate(&wire).unwrap();
            chunks.extend(projection.finish().unwrap());
            assert!(chunks.iter().all(|chunk| chunk.protocol() == CHAT_PROTOCOL));
            assert!(
                chunks
                    .iter()
                    .any(|chunk| chunk.data()["choices"][0]["delta"]["content"] == "Answer")
            );
            assert!(
                chunks
                    .iter()
                    .any(|chunk| chunk.data()["choices"][0]["finish_reason"] == "stop")
            );
            assert_eq!(chunks.last().unwrap().data()["usage"]["total_tokens"], 6);
        } else {
            let wire = ProtocolWireEvent::raw_json(
                GENAI_PROTOCOL,
                serde_json::to_vec(&response).unwrap().into(),
            )
            .unwrap();
            let projected = projection.translate(&wire).unwrap();
            assert_eq!(projected[0].protocol(), CHAT_PROTOCOL);
            let completion: Value =
                serde_json::from_slice(projected[0].raw_json_body().unwrap()).unwrap();
            assert_eq!(completion["id"], "chatcmpl-req_test-request");
            assert_eq!(completion["model"], "test-public-alias");
            assert_eq!(completion["choices"][0]["message"]["content"], "Answer");
            assert_eq!(completion["usage"]["total_tokens"], 6);
        }
    }
}
