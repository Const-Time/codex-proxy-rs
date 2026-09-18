use super::*;
use gateway_core::operation::{GenerateRequest, ProtocolPayload};
use provider_openai::{encode_generate_request, transport::fingerprint::converge_request};
use uuid::Uuid;

const SESSION: &str = "0191ff0c-0022-7000-8000-112233445566";
const TURN: &str = "0191ff0c-0123-7000-8111-112233445566";

fn request(cache: &str) -> CodexResponsesRequest {
    encode_generate_request(&GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({
            "model":"gpt-test", "input":[{"role":"user","content":"don't rewrite me","session_id":SESSION}],
            "prompt_cache_key":cache,
            "client_metadata":{"session_id":SESSION,"thread_id":SESSION,"turn_id":TURN,
                "window_id":format!("{SESSION}:00012"),"root_turn_id":TURN,
                "context_window_id":TURN, "future":{"turn_id":"untouched"},
                "x-codex-turn-metadata":json!({"session_id":SESSION,"thread_id":SESSION,"turn_id":TURN,"window_id":format!("{SESSION}:00012"),"cwd":"中文路径"}).to_string()}
        }).as_object().unwrap().clone()).unwrap().with_context(Map::from_iter([
            ("session_id".to_owned(), json!(SESSION)),
            ("thread_id".to_owned(), json!(SESSION)),
            ("turn_id".to_owned(), json!(TURN)),
            ("codex_window_id".to_owned(), json!(format!("{SESSION}:00012"))),
            ("turn_metadata".to_owned(), json!(json!({"session_id":SESSION,"thread_id":SESSION,
                "turn_id":TURN,"cwd":"中文路径","tool_namespaces_info":["keep in body only"]}).to_string())),
        ])),
    ), "gpt-test").unwrap()
}

#[test]
fn convergence_preserves_uuid7_time_and_window_and_cache_relationships() {
    let mut request = request(SESSION);
    let input = request.input().to_vec();
    request.local_conversation_id = Some("unchanged-local-affinity".to_owned());
    converge_request(&mut request, &[1; 32], "account-a", "client-a");
    let session = request.client_session_id.as_deref().unwrap();
    assert_ne!(session, SESSION);
    let derived = Uuid::parse_str(session).unwrap();
    assert_eq!(derived.get_version_num(), 7);
    assert_eq!(
        &derived.as_bytes()[..6],
        &Uuid::parse_str(SESSION).unwrap().as_bytes()[..6]
    );
    assert_eq!(request.client_thread_id.as_deref(), Some(session));
    assert_eq!(request.client_request_id.as_deref(), Some(session));
    assert_eq!(request.prompt_cache_key(), Some(session));
    assert_eq!(
        request.codex_window_id.as_deref(),
        Some(format!("{session}:00012").as_str())
    );
    assert_eq!(
        request.client_metadata().unwrap()["window_id"],
        format!("{session}:00012")
    );
    assert_eq!(
        request.client_metadata().unwrap()["turn_id"],
        request.client_metadata().unwrap()["root_turn_id"]
    );
    assert_ne!(
        request.client_metadata().unwrap()["turn_id"],
        request.client_metadata().unwrap()["context_window_id"]
    );
    let embedded: Value = serde_json::from_str(
        request.client_metadata().unwrap()["x-codex-turn-metadata"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(embedded["session_id"], session);
    assert_eq!(embedded["cwd"], "中文路径");
    let header = request.turn_metadata.as_deref().unwrap();
    assert!(header.is_ascii());
    let header: Value = serde_json::from_str(header).unwrap();
    assert_eq!(header["session_id"], session);
    assert!(header.get("tool_namespaces_info").is_none());
    assert_eq!(
        request.client_metadata().unwrap()["future"]["turn_id"],
        "untouched"
    );
    assert_eq!(request.input(), input.as_slice());
    assert_eq!(
        request.local_conversation_id.as_deref(),
        Some("unchanged-local-affinity")
    );
}

#[test]
fn convergence_is_stable_per_account_and_client_and_preserves_explicit_cache_shape() {
    let cache = format!("custom_namespace:{SESSION}");
    let mut a = request(&cache);
    let mut same = a.clone();
    let mut other_account = a.clone();
    let mut other_client = a.clone();
    converge_request(&mut a, &[1; 32], "account-a", "client-a");
    converge_request(&mut same, &[1; 32], "account-a", "client-a");
    converge_request(&mut other_account, &[1; 32], "account-b", "client-a");
    converge_request(&mut other_client, &[1; 32], "account-a", "client-b");
    assert_eq!(a.body(), same.body());
    assert_ne!(a.client_session_id, other_account.client_session_id);
    assert_ne!(a.client_session_id, other_client.client_session_id);
    let (prefix, id) = a.prompt_cache_key().unwrap().split_once(':').unwrap();
    assert_eq!(prefix, "custom_namespace");
    assert_eq!(Uuid::parse_str(id).unwrap().get_version_num(), 7);
    assert_ne!(Some(id), a.client_session_id.as_deref());
}

#[test]
fn convergence_does_not_invent_absent_identities_or_rewrite_unknown_shapes() {
    let mut request = CodexResponsesRequest::from_body(json!({
        "model":"gpt-test","input":"hello","client_metadata":{"session_id":42,"future":"untouched"}
    }).as_object().unwrap().clone());
    converge_request(&mut request, &[1; 32], "account-a", "client-a");
    assert!(request.client_session_id.is_none());
    assert!(request.client_thread_id.is_none());
    assert_eq!(request.client_metadata().unwrap()["session_id"], 42);
    assert!(request.prompt_cache_key().is_none());
}

#[test]
fn convergence_aligns_conflicting_carriers_without_touching_business_input() {
    let original = request(SESSION);
    let mut body = original.body().clone();
    body.get_mut("client_metadata")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("session_id".into(), json!("conflicting"));
    body.insert("thread_id".into(), json!("conflicting-thread"));
    body.get_mut("client_metadata")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(
            "x-codex-turn-metadata".into(),
            json!(
                json!({
                    "session_id":"conflicting-session", "thread_id":"conflicting-thread",
                    "turn_id":"conflicting-turn", "window_id":"conflicting-window",
                    "tool_namespaces_info":["keep"]
                })
                .to_string()
            ),
        );
    let mut request = CodexResponsesRequest::from_body(body);
    request.client_session_id = original.client_session_id;
    request.client_thread_id = original.client_thread_id;
    request.client_turn_id = original.client_turn_id;
    request.codex_window_id = original.codex_window_id;
    converge_request(&mut request, &[3; 32], "account", "client");
    let embedded: Value = serde_json::from_str(
        request.client_metadata().unwrap()["x-codex-turn-metadata"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        embedded["session_id"],
        request.client_session_id.as_deref().unwrap()
    );
    assert_eq!(
        embedded["thread_id"],
        request.client_thread_id.as_deref().unwrap()
    );
    assert_eq!(
        embedded["turn_id"],
        request.client_turn_id.as_deref().unwrap()
    );
    assert_eq!(
        embedded["window_id"],
        request.codex_window_id.as_deref().unwrap()
    );
    assert_eq!(embedded["tool_namespaces_info"], json!(["keep"]));
    assert_eq!(
        request.body()["thread_id"],
        request.client_thread_id.as_deref().unwrap()
    );
}
