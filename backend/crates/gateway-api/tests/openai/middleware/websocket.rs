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
async fn ws_attempt_uses_json_messages_and_preserves_core_identity_and_cost() {
    for mode in [Mode::Rewrite, Mode::MessageRewrite, Mode::Head] {
        let f = Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Attempt]).await;
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
        assert_eq!(seen.len(), 1);
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
#[tokio::test]
async fn ws_attempt_cannot_change_routing_transport_or_response_identity() {
    for mode in [
        Mode::InvalidModel,
        Mode::Transport,
        Mode::MessageIdentity,
        Mode::BadResponse,
    ] {
        let f = Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Attempt]).await;
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
#[tokio::test]
async fn ws_attempt_short_circuit_returns_consumable_error_without_provider_call() {
    let f = Fixture::with_mounts(Mode::ShortCircuit, vec![PluginMiddlewareMount::Attempt]).await;
    let (_server, mut socket) = connect(&f).await;
    send(&mut socket, create()).await;
    let error = event(&mut socket).await;
    assert_eq!(error["type"], "error", "{error}");
    assert_eq!(error["status"], 403);
    assert_eq!(error["error"]["message"], "blocked");
    released(&f).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
}
#[tokio::test]
async fn ws_attempt_keeps_control_future_alive_and_queues_creates_serially() {
    let f = Fixture::with_mounts(Mode::Rewrite, vec![PluginMiddlewareMount::Attempt]).await;
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
    assert_eq!(f.plugin.seen.lock().unwrap().len(), 1);
    send(&mut socket, json!({"type":"response.interrupt","mode":"discard_partial_items","response_id":"resp_original"})).await;
    assert_eq!(event(&mut socket).await["type"], "response.completed");
    assert_eq!(event(&mut socket).await["type"], "response.created");
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 2);
    socket.close(None).await.unwrap();
    released(&f).await;
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 2);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn ws_attempt_preserves_native_replay_state_and_connection_identity() {
    let f = Fixture::with_mounts(Mode::MessageRewrite, vec![PluginMiddlewareMount::Attempt]).await;
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
#[tokio::test]
async fn ws_attempt_revocation_interrupts_stalled_source_and_fails_closed_next_turn() {
    let f = Fixture::with_mounts(Mode::Rewrite, vec![PluginMiddlewareMount::Attempt]).await;
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
#[tokio::test]
async fn ws_required_request_layer_is_never_silently_bypassed() {
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
    assert_eq!(event(&mut socket).await["type"], "error");
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ws_invalid_terminal_expansion_does_not_deliver_prefix_or_lose_receipt() {
    let f = Fixture::with_mounts(Mode::ExpandInvalid, vec![PluginMiddlewareMount::Attempt]).await;
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

#[tokio::test]
async fn ws_revocation_before_first_forward_poll_releases_started_execution() {
    let f = Fixture::with_mounts(Mode::Rewrite, vec![PluginMiddlewareMount::Attempt]).await;
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
