use super::*;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
async fn standalone_websocket_layers_transform_native_input_output_and_send_without_recursive_entry_or_extra_charge()
 {
    let f = Fixture::with_mounts(
        Mode::WebSocketRewrite,
        vec![PluginMiddlewareMount::WebSocket],
    )
    .await;
    let (_server, mut socket) = connect(&f, "/v1/responses").await;
    socket
        .send(Message::Binary(vec![0, 255, 0].into()))
        .await
        .unwrap();
    let mut notice = false;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "plugin.notice" {
                notice = true;
                assert!(value.get("raw_plugin").is_none());
                continue;
            }
            assert_eq!(value["raw_plugin"], true);
            if value["type"] == "response.completed" {
                break;
            }
        }
    }
    assert!(notice);
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(f.effects.charges.lock().unwrap()[0].amount_usd.scaled(), 25);
    socket.send(Message::Ping(vec![1, 2].into())).await.unwrap();
    loop {
        if let Message::Pong(bytes) = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            assert_eq!(bytes.as_ref(), &[1, 2]);
            break;
        }
    }
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn plugin_owned_websocket_runs_both_directions_and_releases_connection_on_revocation() {
    let f = Fixture::with_mounts(
        Mode::HttpUpgrade,
        vec![
            PluginMiddlewareMount::Http,
            PluginMiddlewareMount::WebSocket,
        ],
    )
    .await;
    let (_server, mut socket) = connect(&f, "/plugin/socket").await;
    socket
        .send(Message::Binary(vec![0, 255, 0].into()))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, Message::Binary(vec![0, 255, 0, b'i', b'o'].into()));
    assert_eq!(f.plugin.websocket_seen.lock().unwrap().len(), 2);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn dropped_raw_message_does_not_start_a_model_and_next_message_can_complete() {
    let f = Fixture::with_mounts(Mode::WebSocketDrop, vec![PluginMiddlewareMount::WebSocket]).await;
    let (_server, mut socket) = connect(&f, "/v1/responses").await;
    socket.send(Message::Text("drop".into())).await.unwrap();
    socket
        .send(Message::Text(json_create().into()))
        .await
        .unwrap();
    loop {
        if let Message::Text(text) = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            && text.contains("response.completed")
        {
            break;
        }
    }
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next()).await;
    released(&f).await;
    f.manager.shutdown().await;
}
fn json_create() -> String {
    serde_json::json!({"type":"response.create","model":"model-a","input":"original input"})
        .to_string()
}

#[tokio::test]
async fn idle_and_pending_native_websocket_are_closed_by_message_plugin_revocation() {
    for pending in [false, true] {
        let f = Fixture::with_mounts(Mode::WaitBeforeNext, vec![PluginMiddlewareMount::WebSocket])
            .await;
        let (_server, mut socket) = connect(&f, "/v1/responses").await;
        if pending {
            socket
                .send(Message::Text(json_create().into()))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while f.plugin.websocket_seen.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        f.plugin.access.revoke();
        let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap();
        released(&f).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        f.manager.shutdown().await;
    }
}
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn serve(f: &Fixture) -> (Server, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = f.app.clone();
    (
        Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        })),
        address,
    )
}
async fn connect(f: &Fixture, path: &str) -> (Server, Socket) {
    let (server, address) = serve(f).await;
    let mut request = format!("ws://{address}{path}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer sk_middleware".parse().unwrap());
    request
        .headers_mut()
        .insert("x-custom", "preserved".parse().unwrap());
    if path == "/plugin/socket" {
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "fixture".parse().unwrap());
    }
    let (socket, response) = connect_async(request).await.unwrap();
    if path == "/plugin/socket" {
        assert_eq!(response.headers()["sec-websocket-protocol"], "fixture");
    }
    (server, socket)
}
async fn released(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.lifecycle.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn native_websocket_under_http_placement_keeps_original_request_scope_and_single_ledger() {
    let f = new(Mode::MessageIdentity).await;
    let (_server, mut socket) = connect(&f, "/v1/responses").await;
    assert_eq!(f.lifecycle.active.load(Ordering::SeqCst), 1);
    socket.send(Message::Text(serde_json::json!({"type":"response.create","model":"model-a","input":"original input"}).to_string().into())).await.unwrap();
    let mut completed = false;
    while !completed {
        let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            let event: serde_json::Value = serde_json::from_str(&text).unwrap();
            if event["type"] == "response.completed" {
                completed = true;
            }
        }
    }
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_placement_revocation_closes_an_idle_native_websocket_without_another_create() {
    let f = new(Mode::MessageIdentity).await;
    let (_server, mut socket) = connect(&f, "/v1/responses").await;
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn changed_or_discarded_native_handshake_never_starts_session_and_releases_connection_registration()
 {
    for (mode, status) in [(Mode::BadResponse, 502), (Mode::Head, 403)] {
        let f = new(mode).await;
        let (_server, address) = serve(&f).await;
        let mut request = format!("ws://{address}/v1/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", "Bearer sk_middleware".parse().unwrap());
        let error = connect_async(request).await.err().unwrap();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected HTTP refusal")
        };
        assert_eq!(response.status(), status);
        released(&f).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert!(f.effects.charges.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn plugin_owned_upgrade_preserves_binary_data_protocol_and_host_connection_lifetime() {
    let f = new(Mode::HttpUpgrade).await;
    let (_server, mut socket) = connect(&f, "/plugin/socket").await;
    assert_eq!(f.lifecycle.active.load(Ordering::SeqCst), 1);
    socket
        .send(Message::Binary(bytes::Bytes::from_static(b"raw\0socket")))
        .await
        .unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        echo,
        Message::Binary(bytes::Bytes::from_static(b"raw\0socket"))
    );
    f.plugin.access.revoke();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn invalid_plugin_handshake_does_not_allocate_a_connection_or_start_a_session() {
    let f = new(Mode::HttpUpgrade).await;
    let response = f
        .app
        .clone()
        .oneshot(Request::get("/plugin/socket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    body(response).await;
    assert_eq!(f.lifecycle.active.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn changed_or_discarded_plugin_handshake_releases_registration_without_waiting_for_transport_upgrade()
 {
    for (mode, status) in [
        (Mode::HttpUpgradeBadResponse, 502),
        (Mode::HttpUpgradeDiscard, 403),
    ] {
        let f = new(mode).await;
        let (_server, address) = serve(&f).await;
        let request = format!("ws://{address}/plugin/socket")
            .into_client_request()
            .unwrap();
        let error = connect_async(request).await.err().unwrap();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected HTTP refusal")
        };
        assert_eq!(response.status(), status);
        released(&f).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn peer_close_releases_plugin_owned_connection_and_does_not_start_model_execution() {
    let f = new(Mode::HttpUpgrade).await;
    let (_server, mut socket) = connect(&f, "/plugin/socket").await;
    socket.close(None).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn host_drain_closes_an_idle_plugin_owned_socket_and_releases_connection_registration() {
    let f = new(Mode::HttpUpgrade).await;
    let (_server, mut socket) = connect(&f, "/plugin/socket").await;
    gateway_core::lifecycle::ConnectionLifecycle::cancellation(f.lifecycle.as_ref()).cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    released(&f).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
