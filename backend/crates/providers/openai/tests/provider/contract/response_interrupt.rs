use super::*;
use gateway_core::engine::response_control::{ResponseControl, ResponseInterruptError};

async fn next_json(socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>) -> Value {
    loop {
        match socket.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            Message::Pong(_) => {}
            other => panic!("unexpected synthetic upstream frame: {other:?}"),
        }
    }
}

fn interrupt_context(control: ResponseControl, previous: Option<&str>) -> AttemptContext {
    let account = ProviderAccountId::new("acct_provider_contract").unwrap();
    let provider = ProviderKind::new("openai").unwrap();
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_interrupt").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        )
        .with_response_control(Some(control)),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(30),
        account_policy(),
        AccountAttemptContext::new(
            BTreeSet::new(),
            None,
            previous.map(|_| ProviderAccountStateOwner::new(provider.clone(), account.clone())),
        )
        .with_account_scope(contract_account_scope()),
        previous.map(|id| {
            ContinuationBinding::Pinned(NativeContinuationPin::new(
                PreviousResponseId::new(id),
                PreviousResponseId::new(id),
                ClientApiKeyId::new("key_openai_contract").unwrap(),
                provider,
                account,
            ))
        }),
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn response_interrupt_uses_the_active_socket_and_releases_control_before_reuse() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_codex_test_websocket(socket).await;
        for id in ["resp_interrupt_a", "resp_interrupt_b"] {
            let create = next_json(&mut socket).await;
            assert_eq!(create["type"], "response.create");
            if id == "resp_interrupt_b" {
                assert_eq!(create["previous_response_id"], "resp_interrupt_a");
            }
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.created",
                        "response": {"id":id, "model":"gpt-5.4", "status":"in_progress"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Text(json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}).to_string().into())).await.unwrap();
            let interrupt = next_json(&mut socket).await;
            assert_eq!(
                interrupt,
                json!({"type":"response.interrupt", "response_id":id,"mode":"discard_partial_items"})
            );
            socket
                .send(Message::Text(
                    json!({
                        "type":"response.incomplete",
                        "response":{"id":id,"model":"gpt-5.4","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[]}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
    });
    let provider = provider_with_base_url(&store, base_url);
    let mut session_state = None;
    for id in ["resp_interrupt_a", "resp_interrupt_b"] {
        let control = ResponseControl::default();
        let previous = (id == "resp_interrupt_b").then_some("resp_interrupt_a");
        let mut request = generate_with_persisted_session_context(
            "acct_provider_contract",
            "interrupt-conversation",
            "interrupt-session",
            "interrupt-thread",
        );
        if let Some(state) = session_state.take() {
            request = request.with_provider_session_state(state);
        }
        let mut stream = provider
            .execute(
                planned_request("openai", Operation::Generate(request)),
                interrupt_context(control.clone(), previous),
            )
            .await
            .unwrap();
        let mut interrupted = false;
        timeout(Duration::from_secs(5), async {
            while let Some(event) = stream.next().await {
                let event = event.unwrap();
                if let Some(state) = event.session_update() {
                    session_state = Some(state.clone());
                }
                if !interrupted && control.interrupt(id).is_ok() {
                    assert_eq!(
                        control.interrupt("resp_foreign"),
                        Err(ResponseInterruptError::ResponseMismatch)
                    );
                    control.interrupt(id).unwrap();
                    interrupted = true;
                }
            }
        })
        .await
        .expect("interrupt completes the active response");
        assert!(interrupted);
        assert!(session_state.is_some());
        assert_eq!(
            control.interrupt(id),
            Err(ResponseInterruptError::Unavailable)
        );
    }
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn steering_keeps_native_socket_until_successor_or_pending_and_accounts_each_response() {
    for outcome in [
        "steered",
        "completed",
        "rejected",
        "pending",
        "failed_after_accept",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_codex_test_websocket(socket).await;
            assert_eq!(next_json(&mut socket).await["type"], "response.create");
            send_json(&mut socket, created("resp_parent")).await;
            let steer = next_json(&mut socket).await;
            assert_eq!(
                steer,
                json!({"type":"response.steer","previous_response_id":"resp_parent","input":"keep Windows support"})
            );
            let accepted = json!({"type":"response.steer.accepted","sequence_number":1,"steer":{"id":"steer_test","previous_response_id":"resp_parent"}});
            if outcome == "rejected" {
                send_json(&mut socket, json!({"type":"response.steer.failed","sequence_number":1,"error":{"type":"invalid_request_error","code":"steering_not_supported","message":"unsupported"},"steer":{"previous_response_id":"resp_parent","input":"keep Windows support"}})).await;
            } else {
                send_json(&mut socket, accepted).await;
            }
            send_json(
                &mut socket,
                terminal(
                    "resp_parent",
                    matches!(outcome, "steered" | "failed_after_accept"),
                    10,
                ),
            )
            .await;
            let previous = match outcome {
                "steered" | "completed" => {
                    // No response.create is sent for the automatic successor.
                    send_json(&mut socket, created("resp_successor")).await;
                    send_json(&mut socket, terminal("resp_successor", false, 20)).await;
                    "resp_successor"
                }
                "pending" => {
                    send_json(&mut socket, json!({"type":"response.steer.pending","sequence_number":3,"steer":{"id":"steer_test","previous_response_id":"resp_parent"},"required_input":[{"type":"function_call_output","call_id":"call_test","output":null}]})).await;
                    "resp_parent"
                }
                "failed_after_accept" => {
                    send_json(&mut socket, json!({"type":"response.steer.failed","sequence_number":3,"error":{"type":"invalid_request_error","code":"successor_creation_failed","message":"failed"},"steer":{"id":"steer_test","previous_response_id":"resp_parent","input":"keep Windows support"}})).await;
                    "resp_parent"
                }
                _ => "resp_parent",
            };
            // A later explicit continuation must reuse this exact socket, including pending input.
            let next = next_json(&mut socket).await;
            assert_eq!(next["type"], "response.create");
            assert_eq!(next["previous_response_id"], previous);
            send_json(&mut socket, created("resp_next")).await;
            send_json(&mut socket, terminal("resp_next", false, 5)).await;
        });
        let provider = provider_with_base_url(&store, base_url);
        let control = ResponseControl::default();
        let request = generate_with_persisted_session_context(
            "acct_provider_contract",
            "steer-conversation",
            "steer-session",
            "steer-thread",
        );
        let mut stream = provider
            .execute(
                planned_request("openai", Operation::Generate(request)),
                interrupt_context(control.clone(), None),
            )
            .await
            .unwrap();
        let mut sent = false;
        let mut kinds = Vec::new();
        let mut usages = Vec::new();
        let mut costs = Vec::new();
        let mut completed = 0;
        let mut session_state = None;
        timeout(Duration::from_secs(10), async {
            while let Some(event) = stream.next().await {
                let event = event.unwrap();
                if let Some(state) = event.session_update() { session_state = Some(state.clone()); }
                if let Some(wire) = event.wire_event() {
                    kinds.push(wire.data()["type"].as_str().unwrap().to_owned());
                    if !sent && wire.data()["type"] == "response.created" {
                        control.steer("resp_parent", json!({"type":"response.steer","previous_response_id":"resp_parent","input":"keep Windows support"})).unwrap();
                        sent = true;
                    }
                }
                for fact in event.canonical_facts() {
                    match fact {
                        GatewayEvent::Usage(usage) => usages.push(usage.input_tokens.unwrap()),
                        GatewayEvent::CalculatedCost(cost) => costs.push(cost.total()),
                        GatewayEvent::Completed(meta) => {
                            completed += 1;
                            assert_eq!(meta.response_id(), if matches!(outcome,"steered"|"completed") {"resp_successor"} else {"resp_parent"});
                        }
                        _ => {}
                    }
                }
            }
        }).await.expect("chain ends without waiting for another client create");
        assert!(sent);
        assert_eq!(completed, 1, "only chain EOF finalizes canonical execution");
        assert_eq!(
            usages,
            if matches!(outcome, "steered" | "completed") {
                vec![10, 20]
            } else {
                vec![10]
            }
        );
        assert_eq!(costs.len(), usages.len());
        assert!(kinds.iter().any(|kind| kind
            == if outcome == "rejected" {
                "response.steer.failed"
            } else {
                "response.steer.accepted"
            }));
        if outcome == "pending" {
            assert_eq!(kinds.last().unwrap(), "response.steer.pending");
        }
        let previous = if matches!(outcome, "steered" | "completed") {
            "resp_successor"
        } else {
            "resp_parent"
        };
        let request = generate_with_persisted_session_context(
            "acct_provider_contract",
            "steer-conversation",
            "steer-session",
            "steer-thread",
        )
        .with_provider_session_state(session_state.unwrap());
        let mut next = provider
            .execute(
                planned_request("openai", Operation::Generate(request)),
                interrupt_context(ResponseControl::default(), Some(previous)),
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(10), async {
            while let Some(event) = next.next().await {
                event.unwrap();
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(10), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn steering_disconnect_discards_accepted_input_without_replay() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_codex_test_websocket(socket).await;
        assert_eq!(next_json(&mut socket).await["type"], "response.create");
        send_json(&mut socket, created("resp_disconnect")).await;
        assert_eq!(next_json(&mut socket).await["type"], "response.steer");
        send_json(&mut socket, json!({"type":"response.steer.accepted","sequence_number":1,"steer":{"id":"steer_disconnect","previous_response_id":"resp_disconnect"}})).await;
        timeout(Duration::from_secs(5), async {
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Ping(value)) => {
                        let _ = socket.send(Message::Pong(value)).await;
                    }
                    Ok(Message::Text(value)) => panic!("unexpected replay: {value}"),
                    _ => {}
                }
            }
        })
        .await
        .expect("downstream cancellation closes the owned upstream socket");
    });
    let provider = provider_with_base_url(&store, base_url);
    let cancellation = CancellationToken::new();
    let control = ResponseControl::default().with_connection_cancellation(cancellation.clone());
    let request = generate_with_persisted_session_context(
        "acct_provider_contract",
        "disconnect-conversation",
        "disconnect-session",
        "disconnect-thread",
    );
    let mut stream = provider
        .execute(
            planned_request("openai", Operation::Generate(request)),
            interrupt_context(control.clone(), None),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            match event.wire_event().and_then(|wire| wire.data()["type"].as_str()) {
                Some("response.created") => control.steer("resp_disconnect", json!({"type":"response.steer","previous_response_id":"resp_disconnect","input":"cancel this connection"})).unwrap(),
                Some("response.steer.accepted") => { cancellation.cancel(); break; }
                _ => {}
            }
        }
    }).await.unwrap();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        control.interrupt("resp_disconnect"),
        Err(ResponseInterruptError::Unavailable)
    );
    drop(stream);
}

async fn send_json(socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn created(id: &str) -> Value {
    json!({"type":"response.created","response":{"id":id,"model":"gpt-5.4","status":"in_progress","output":[]}})
}
fn terminal(id: &str, steered: bool, input: u64) -> Value {
    let mut value = json!({"type":if steered {"response.incomplete"} else {"response.completed"}, "response":{"id":id,"model":"gpt-5.4","status":if steered {"incomplete"} else {"completed"},"output":[],"usage":{"input_tokens":input,"output_tokens":2,"total_tokens":input+2,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}});
    if steered {
        value["response"]["incomplete_details"] = json!({"reason":"steered"});
    }
    value
}
