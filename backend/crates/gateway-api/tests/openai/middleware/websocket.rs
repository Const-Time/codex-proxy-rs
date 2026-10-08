use super::{fixture::*, *};
use futures::{SinkExt, StreamExt};
use gateway_admin::model::plugins::PluginMiddlewareMount;
use gateway_core::operation::{Operation, ProviderSessionState};
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn connect(f: &Fixture) -> (Server, Socket) {
    let app = f.app.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer sk_middleware".parse().unwrap());
    request
        .headers_mut()
        .insert("x-custom", "preserved".parse().unwrap());
    let (socket, _) = connect_async(request).await.unwrap();
    (server, socket)
}
async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn next(socket: &mut Socket) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("WS event deadline")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}
async fn event(socket: &mut Socket) -> Value {
    loop {
        let value = next(socket).await;
        if value["type"] != "response.metadata" {
            return value;
        }
    }
}
fn create() -> Value {
    json!({"type":"response.create","model":"model-a","input":"original input"})
}
async fn released(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.effects.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ws_layers_uses_json_messages_and_preserves_core_identity_and_cost() {
    for mounts in mount_combinations() {
        for mode in [Mode::Rewrite, Mode::MessageRewrite, Mode::Head] {
            let f = Fixture::with_mounts(mode, mounts.clone()).await;
            let (_server, mut socket) = connect(&f).await;
            let mut body = create();
            body["use_websocket"] = json!(false);
            send(&mut socket, body).await;
            let metadata = next(&mut socket).await;
            assert_eq!(metadata["type"], "response.metadata", "{metadata}");
            if matches!(mode, Mode::Head) {
                assert_eq!(metadata["headers"]["x-plugin-result"], "changed");
                assert!(metadata["headers"].get("set-cookie").is_none());
            }
            for kind in ["response.created", "response.completed"] {
                let value = next(&mut socket).await;
                assert_eq!(value["type"], kind, "{value}");
                if matches!(mode, Mode::MessageRewrite) {
                    assert_eq!(value["plugin"], true);
                }
            }
            f.settled().await;
            assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
            let seen = f.plugin.seen.lock().unwrap();
            assert_eq!(seen.len(), mounts.len());
            assert_eq!(
                seen[0].transport,
                gateway_core::engine::execution::ClientTransport::WebSocket
            );
            assert_eq!(seen[0].client_key_id.as_str(), "key_api_test");
            let ops = f.effects.operations.lock().unwrap();
            let Operation::Generate(generate) = &ops[0] else {
                panic!()
            };
            assert!(generate.protocol_payload().body().get("stream").is_none());
            assert_eq!(
                generate.protocol_payload().context()["use_websocket"],
                false
            );
            assert!(
                generate.protocol_payload().context()["downstream_websocket_connection_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("ws_")
            );
        }
    }
}
#[tokio::test]
async fn ws_layers_cannot_change_routing_transport_or_response_identity() {
    for mounts in mount_combinations() {
        for mode in [
            Mode::InvalidModel,
            Mode::Transport,
            Mode::MessageIdentity,
            Mode::BadResponse,
        ] {
            let f = Fixture::with_mounts(mode, mounts.clone()).await;
            let (_server, mut socket) = connect(&f).await;
            send(&mut socket, create()).await;
            let error = event(&mut socket).await;
            assert_eq!(error["type"], "error", "{error}");
            assert!(error["status"].as_u64().unwrap() >= 400);
            released(&f).await;
            assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
            assert_eq!(
                f.effects.calls.load(Ordering::SeqCst),
                usize::from(matches!(mode, Mode::MessageIdentity | Mode::BadResponse))
            );
        }
    }
}
#[tokio::test]
async fn ws_layers_short_circuit_returns_consumable_error_without_provider_call() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::ShortCircuit, mounts.clone()).await;
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        let error = event(&mut socket).await;
        assert_eq!(error["type"], "error", "{error}");
        assert_eq!(error["status"], 403);
        assert_eq!(error["error"]["message"], "blocked");
        released(&f).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        let charges = f.effects.charges.lock().unwrap();
        if mounts.contains(&PluginMiddlewareMount::Request) {
            assert!(
                charges.is_empty(),
                "request short circuit never starts Core"
            );
        } else {
            assert_eq!(charges.len(), 1);
            assert_eq!(charges[0].amount_usd.scaled(), 0);
        }
    }
}
#[tokio::test]
async fn ws_layers_keeps_control_future_alive_and_queues_creates_serially() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::Rewrite, mounts.clone()).await;
        f.effects.interrupt.store(true, Ordering::SeqCst);
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        assert_eq!(event(&mut socket).await["type"], "response.created");
        send(&mut socket, create()).await;
        send(
        &mut socket,
        json!({"type":"response.interrupt","mode":"discard_partial_items","response_id":"other"}),
    )
    .await;
        assert_eq!(event(&mut socket).await["status"], 400);
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.plugin.seen.lock().unwrap().len(), mounts.len());
        send(&mut socket, json!({"type":"response.interrupt","mode":"discard_partial_items","response_id":"resp_original"})).await;
        assert_eq!(event(&mut socket).await["type"], "response.completed");
        assert_eq!(event(&mut socket).await["type"], "response.created");
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 2);
        socket.close(None).await.unwrap();
        released(&f).await;
        assert_eq!(f.effects.finalizations.lock().unwrap().len(), 2);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 2);
    }
}
#[tokio::test]
async fn ws_layers_preserves_native_replay_state_and_connection_identity() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::MessageRewrite, mounts.clone()).await;
        let state = ProviderSessionState::new(
            "openai",
            json!({"private":"host-only"}).as_object().unwrap().clone(),
        )
        .unwrap();
        *f.effects.state.lock().unwrap() = Some(state.clone());
        let (_server, mut socket) = connect(&f).await;
        for previous in [None, Some("resp_original")] {
            let mut request = create();
            if let Some(id) = previous {
                request["previous_response_id"] = json!(id);
            }
            send(&mut socket, request).await;
            let created = event(&mut socket).await;
            assert_eq!(
                created["type"], "response.created",
                "previous={previous:?} {created}"
            );
            let completed = event(&mut socket).await;
            assert_eq!(completed["type"], "response.completed", "{completed}");
        }
        released(&f).await;
        let operations = f.effects.operations.lock().unwrap();
        assert!(operations[0].provider_session_state("openai").is_none());
        assert!(operations[1].provider_session_state("openai") == Some(&state));
        let ids: Vec<_> = operations
            .iter()
            .map(|op| {
                let Operation::Generate(g) = op else { panic!() };
                g.protocol_payload().context()["downstream_websocket_connection_id"].clone()
            })
            .collect();
        assert_eq!(ids[0], ids[1]);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 2);
    }
}
#[tokio::test]
async fn ws_layers_revocation_interrupts_stalled_source_and_fails_closed_next_turn() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::Rewrite, mounts.clone()).await;
        *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        assert_eq!(event(&mut socket).await["type"], "response.created");
        f.plugin.access.revoke();
        assert_eq!(event(&mut socket).await["type"], "error");
        released(&f).await;
        send(&mut socket, create()).await;
        assert_eq!(event(&mut socket).await["type"], "error");
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn ws_request_and_attempt_both_execute_in_one_core_request() {
    let f = Fixture::with_mounts(
        Mode::Rewrite,
        vec![
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ],
    )
    .await;
    let (_server, mut socket) = connect(&f).await;
    send(&mut socket, create()).await;
    assert_eq!(event(&mut socket).await["type"], "response.created");
    assert_eq!(event(&mut socket).await["type"], "response.completed");
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    let seen = f.plugin.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].mount, MiddlewareMount::Request);
    assert_eq!(seen[1].mount, MiddlewareMount::Attempt);
    assert_eq!(seen[0].request_id, seen[1].request_id);
}

#[tokio::test]
async fn ws_invalid_terminal_expansion_does_not_deliver_prefix_or_lose_receipt() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::ExpandInvalid, mounts.clone()).await;
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        assert_eq!(event(&mut socket).await["type"], "response.created");
        let value = event(&mut socket).await;
        assert_eq!(value["type"], "error", "{value}");
        assert!(!value.to_string().contains("must-not-leak"));
        f.settled().await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn ws_revocation_before_first_forward_poll_releases_started_execution() {
    for mounts in mount_combinations() {
        let f = Fixture::with_mounts(Mode::Rewrite, mounts.clone()).await;
        *f.effects.revoke_after_start.lock().unwrap() = Some(f.plugin.access.clone());
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        assert_eq!(event(&mut socket).await["type"], "error");
        released(&f).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    }
}

fn mount_combinations() -> Vec<Vec<PluginMiddlewareMount>> {
    vec![
        vec![PluginMiddlewareMount::Attempt],
        vec![PluginMiddlewareMount::Request],
        vec![
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ],
    ]
}

#[tokio::test]
async fn ws_disconnect_and_revoke_during_request_plugin_wait_release_the_owner() {
    for mode in [Mode::WaitBeforeNext, Mode::WaitAfterNext] {
        for revoke in [false, true] {
            let f = Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Request]).await;
            let (_server, mut socket) = connect(&f).await;
            send(&mut socket, create()).await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let entered = !f.plugin.seen.lock().unwrap().is_empty();
                    let started = f.effects.calls.load(Ordering::SeqCst) == 1;
                    if entered && (matches!(mode, Mode::WaitBeforeNext) || started) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if revoke {
                f.plugin.access.revoke();
                assert_eq!(event(&mut socket).await["type"], "error");
            } else {
                socket.close(None).await.unwrap();
            }
            released(&f).await;
            let expected = usize::from(matches!(mode, Mode::WaitAfterNext));
            assert_eq!(f.effects.calls.load(Ordering::SeqCst), expected);
            assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
            assert_eq!(f.effects.charges.lock().unwrap().len(), expected);
        }
    }
}

#[tokio::test]
async fn ws_request_changed_continuation_does_not_inherit_another_responses_private_state() {
    let f = Fixture::with_mounts(
        Mode::ChangeContinuation,
        vec![
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ],
    )
    .await;
    *f.effects.state.lock().unwrap() = Some(
        ProviderSessionState::new(
            "openai",
            json!({"private":"original-state"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap(),
    );
    let (_server, mut socket) = connect(&f).await;
    for previous in [None, Some("resp_original")] {
        let mut request = create();
        if let Some(id) = previous {
            request["previous_response_id"] = json!(id);
        }
        send(&mut socket, request).await;
        assert_eq!(event(&mut socket).await["type"], "response.created");
        assert_eq!(event(&mut socket).await["type"], "response.completed");
    }
    released(&f).await;
    let operations = f.effects.operations.lock().unwrap();
    assert_eq!(operations.len(), 2);
    assert!(operations[1].provider_session_state("openai").is_none());
    let Operation::Generate(g) = &operations[1] else {
        panic!()
    };
    assert_eq!(
        g.protocol_payload().body()["previous_response_id"],
        "resp_other"
    );
    assert_eq!(f.effects.charges.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn ws_request_preserves_capacity_and_continuation_error_projections() {
    use gateway_core::error::{
        ClientVisibleUpstreamError, ClientVisibleUpstreamResponse, ContinuationRecoveryDisposition,
        ProviderError, ProviderErrorKind,
    };
    use gateway_core::event::ProviderResponseHeader;
    use gateway_core::upstream::UpstreamSendState;
    let cases: [(fn() -> ProviderError, u64); 2] = [
        (
            || {
                ProviderError::new(
                    ProviderErrorKind::UpstreamCapacityUnavailable,
                    UpstreamSendState::Sent,
                )
                .with_status(400)
                .with_client_visible_upstream_error(
                    ClientVisibleUpstreamError::new(
                        "Model at capacity",
                        Some("slow_down".into()),
                        Some("server_error".into()),
                    ),
                )
            },
            503,
        ),
        (
            || {
                ProviderError::new(ProviderErrorKind::QuotaExhausted, UpstreamSendState::Sent)
                    .with_status(429)
                    .with_continuation_recovery_disposition(
                        ContinuationRecoveryDisposition::ClientReplayRequired,
                    )
                    .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                        "Previous response was not found",
                        Some("previous_response_not_found".into()),
                        Some("invalid_request_error".into()),
                    ))
                    .with_client_visible_upstream_response(
                        ClientVisibleUpstreamResponse::new(
                            400,
                            Some(b"application/json".to_vec()),
                            Bytes::from_static(b"{}"),
                        )
                        .with_headers(vec![
                            ProviderResponseHeader::new(
                                "x-request-id",
                                Bytes::from_static(b"req_original"),
                            ),
                            ProviderResponseHeader::new(
                                "authorization",
                                Bytes::from_static(b"must-not-leak"),
                            ),
                        ]),
                    )
            },
            400,
        ),
    ];
    for (failure, expected_status) in cases {
        let f = Fixture::with_mounts(Mode::Rewrite, vec![PluginMiddlewareMount::Request]).await;
        *f.effects.provider_failure.lock().unwrap() = Some(failure);
        let (_server, mut socket) = connect(&f).await;
        send(&mut socket, create()).await;
        let error = event(&mut socket).await;
        assert_eq!(error["type"], "error", "{error}");
        assert_eq!(error["status"], expected_status, "{error}");
        if expected_status == 400 {
            assert_eq!(error["error"]["code"], "previous_response_not_found");
            assert_eq!(error["headers"]["x-request-id"], "req_original");
            assert!(error["headers"].get("authorization").is_none());
        }
        released(&f).await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
        assert!(
            f.effects
                .charges
                .lock()
                .unwrap()
                .iter()
                .all(|charge| charge.amount_usd.scaled() == 0)
        );
    }
}
