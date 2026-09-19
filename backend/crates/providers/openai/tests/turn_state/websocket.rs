use super::*;

async fn ready() -> (
    provider_openai::ProviderBundle,
    TcpListener,
    String,
    Arc<MemoryAccountStore>,
) {
    let http = MockServer::start().await;
    let candidate = token(10, 73);
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", candidate.as_str())
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&http)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = http.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let first = bundle(&config.config, &accounts, &store).await;
    let service = first.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    set_account(service.as_ref(), false, true).await;
    execute(&first, operation(None, false), "acct_state").await;
    set_account(service.as_ref(), false, true).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.config.api.base_url = format!("http://{}", listener.local_addr().unwrap());
    let second = bundle(&config.config, &accounts, &store).await;
    (second, listener, candidate, accounts)
}

fn frame(
    state: Option<&str>,
    turn: &str,
    previous: Option<ProviderSessionState>,
    previous_response: Option<&str>,
    metadata: Value,
) -> Operation {
    let mut context = Map::from_iter([
        ("use_websocket".to_owned(), json!(true)),
        ("session_id".to_owned(), json!("ws-state-session")),
        ("thread_id".to_owned(), json!("ws-state-thread")),
        ("turn_id".to_owned(), json!(turn)),
    ]);
    if let Some(state) = state {
        context.insert("turn_state".to_owned(), json!(state));
    }
    let mut body = json!({
        "model": "gpt-5.4", "input": "unchanged user input",
        "store": true, "client_metadata": metadata,
    });
    if let Some(id) = previous_response {
        body["previous_response_id"] = json!(id);
    }
    let mut request = GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(context),
    );
    if let Some(previous) = previous {
        request = request.with_provider_session_state(previous);
    }
    Operation::Generate(request)
}

fn completed(index: usize) -> Message {
    Message::Text(
        json!({
            "type": "response.completed",
            "response": {
                "id": format!("resp_ws_{index}"), "status": "completed",
                "model": "gpt-5.4", "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        })
        .to_string()
        .into(),
    )
}

#[tokio::test]
async fn managed_ws_without_fingerprint_convergence_is_frame_only_and_preserves_metadata() {
    let (bundle, listener, candidate, _) = ready().await;
    let expected = candidate.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket_with(socket, |request, _| {
            assert!(request.headers().get("x-codex-turn-state").is_none());
        })
        .await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(body["type"], "response.create");
        assert_eq!(body["client_metadata"]["x-codex-turn-state"], expected);
        assert_eq!(body["client_metadata"]["unrelated"], "preserved");
        assert!(body["client_metadata"].get("turnState").is_none());
        assert!(body["client_metadata"].get("turn_state").is_none());
        assert_eq!(body["input"], "unchanged user input");
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(
            Some("client"),
            "turn-1",
            None,
            None,
            json!({
                "x-codex-turn-state": "old-frame", "turnState": "alias-a",
                "turn_state": "alias-b", "unrelated": "preserved",
            }),
        ),
        "acct_state",
    )
    .await;
    assert_eq!(result["turnStateSent"], candidate);
    assert_eq!(result["turnStateSentSource"], "managed");
    server.await.unwrap();
}

#[tokio::test]
async fn reused_ws_rechecks_each_turn_preserves_continuations_and_does_not_stick_after_disable() {
    let (bundle, listener, candidate, _) = ready().await;
    let returned = token(11, 29);
    let expected_returned = returned.clone();
    let expected_candidate = candidate.clone();
    let server = tokio::spawn(async move {
        // All five requests must use this one socket, without another handshake.
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket_with(socket, |request, _| {
            assert!(request.headers().get("x-codex-turn-state").is_none());
        })
        .await;
        let expected = [
            expected_candidate.as_str(),
            expected_returned.as_str(),
            "native-state",
            expected_candidate.as_str(),
            "client-disabled",
        ];
        for (index, state) in expected.iter().enumerate() {
            let body: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(
                body["client_metadata"]["x-codex-turn-state"], *state,
                "frame {index}"
            );
            assert_eq!(body["client_metadata"]["unrelated"], "preserved");
            if index == 2 {
                assert_eq!(body["previous_response_id"], "resp_ws_1");
            }
            if index == 0 {
                ws.send(Message::Text(
                    json!({
                        "type": "codex.response.metadata",
                        "headers": {"x-codex-turn-state": expected_returned},
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            }
            ws.send(completed(index)).await.unwrap();
        }
    });
    let metadata = || json!({"unrelated": "preserved"});
    let (first, first_session) = execute_with_session(
        &bundle,
        frame(Some("client-first"), "turn-1", None, None, metadata()),
        "acct_state",
    )
    .await;
    assert_eq!(first["turnStateSent"], candidate);
    assert_eq!(first["turnStateSentSource"], "managed");
    assert_eq!(first["turnState"], returned);
    assert_eq!(first["turnStateSource"], "response");
    let service = bundle.turn_state_service().unwrap();
    assert_eq!(service.view().await.unwrap().targets[0].candidate_count, 1);
    let (same_turn, second_session) = execute_with_session(
        &bundle,
        frame(
            None,
            "turn-1",
            Some(first_session.unwrap()),
            None,
            metadata(),
        ),
        "acct_state",
    )
    .await;
    assert_eq!(same_turn["turnStateSent"], returned);
    assert_eq!(same_turn["turnStateSentSource"], "request");
    assert_eq!(same_turn["turnStateSource"], "request");
    let (native, third_session) = execute_with_session(
        &bundle,
        frame(
            Some("native-state"),
            "turn-1",
            Some(second_session.unwrap()),
            Some("resp_ws_1"),
            metadata(),
        ),
        "acct_state",
    )
    .await;
    assert_eq!(native["turnStateSentSource"], "request");
    assert_eq!(native["turnStateSent"], "native-state");
    let (new_turn, _) = execute_with_session(
        &bundle,
        frame(
            Some("client-new"),
            "turn-2",
            Some(third_session.unwrap()),
            None,
            metadata(),
        ),
        "acct_state",
    )
    .await;
    assert_eq!(new_turn["turnStateSentSource"], "managed");
    assert_eq!(new_turn["turnStateSent"], candidate);
    set_account(service.as_ref(), false, false).await;
    let disabled = execute(
        &bundle,
        frame(Some("client-disabled"), "turn-3", None, None, metadata()),
        "acct_state",
    )
    .await;
    assert_eq!(disabled["turnStateSentSource"], "request");
    assert_eq!(disabled["turnStateSent"], "client-disabled");
    server.await.unwrap();
}

#[tokio::test]
async fn ws_with_non_object_metadata_skips_takeover_without_claiming_a_sent_candidate() {
    let (bundle, listener, _, _) = ready().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket(socket).await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(body["client_metadata"], "opaque-client-value");
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(
            Some("client-state"),
            "turn-1",
            None,
            None,
            json!("opaque-client-value"),
        ),
        "acct_state",
    )
    .await;
    assert!(result.get("turnStateSent").is_none());
    assert!(result.get("turnStateSentSource").is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn ws_never_replays_another_accounts_managed_candidate() {
    let (bundle, listener, candidate, _) = ready().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket_with(socket, |request, _| {
            assert!(request.headers().get("x-codex-turn-state").is_none());
        })
        .await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert!(body["client_metadata"].get("x-codex-turn-state").is_none());
        assert_eq!(body["client_metadata"]["unrelated"], "preserved");
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(
            Some(&candidate),
            "turn-1",
            None,
            None,
            json!({
                "x-codex-turn-state": candidate, "unrelated": "preserved",
            }),
        ),
        "acct_other",
    )
    .await;
    assert!(result.get("turnStateSent").is_none());
    assert!(result.get("turnStateSentSource").is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn ws_revoked_candidate_does_not_disable_business_or_override_client_state() {
    let (bundle, listener, _, _) = ready().await;
    bundle
        .turn_state_service()
        .unwrap()
        .action("acct_state", "gpt-5.4", "revoke", &mutation())
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket(socket).await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(
            body["client_metadata"]["x-codex-turn-state"],
            "client-state"
        );
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(Some("client-state"), "turn-1", None, None, json!({})),
        "acct_state",
    )
    .await;
    assert_eq!(result["turnStateSent"], "client-state");
    assert_eq!(result["turnStateSentSource"], "request");
    server.await.unwrap();
}

#[tokio::test]
async fn ws_unknown_turn_boundary_does_not_replace_state_from_the_pool() {
    let (bundle, listener, candidate, _) = ready().await;
    let previous = ProviderSessionState::new(
        "openai",
        json!({
            "account_id": "acct_state", "conversation_id": "ws-state-session",
            "turn_state": candidate, "client_turn_id": null,
            "continuation_scope": "persisted",
        })
        .as_object()
        .unwrap()
        .clone(),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket(socket).await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert!(body["client_metadata"].get("x-codex-turn-state").is_none());
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(None, "current-turn", Some(previous), None, json!({})),
        "acct_state",
    )
    .await;
    assert!(result.get("turnStateSentSource").is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn ws_auth_rotation_rejects_old_pool_and_client_echo_without_disabling_business() {
    let (bundle, listener, candidate, accounts) = ready().await;
    let account = accounts.account("acct_state").unwrap();
    let repository = accounts.repository();
    let mut data = repository.load_complete_data(&account).await.unwrap();
    data.oauth_mut().unwrap().access_token = "rotated-for-ws".to_owned();
    repository
        .compare_and_swap_data(&account, data)
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = crate::transport::accept_codex_test_websocket_with(socket, |request, _| {
            assert!(request.headers().get("x-codex-turn-state").is_none());
            assert_eq!(request.headers()["authorization"], "Bearer rotated-for-ws");
        })
        .await;
        let body: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert!(body["client_metadata"].get("x-codex-turn-state").is_none());
        ws.send(completed(0)).await.unwrap();
    });
    let result = execute(
        &bundle,
        frame(
            Some(&candidate),
            "new-turn",
            None,
            None,
            json!({"x-codex-turn-state": candidate}),
        ),
        "acct_state",
    )
    .await;
    assert!(result.get("turnStateSent").is_none());
    assert!(result.get("turnStateSentSource").is_none());
    server.await.unwrap();
}
