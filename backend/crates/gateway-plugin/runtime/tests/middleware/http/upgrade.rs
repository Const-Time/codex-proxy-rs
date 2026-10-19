use super::*;
use futures::FutureExt;
use gateway_core::engine::middleware::{http::upgrade::*, websocket as ws};
use std::sync::Mutex;

struct Socket {
    received: Mutex<Option<ws::Message>>,
    sent: Mutex<Vec<ws::Message>>,
    send_error: bool,
}
impl ws::Sender for Socket {
    fn send(&self, message: ws::Message) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            if self.send_error {
                return Err(MiddlewareError::Engine(
                    gateway_core::engine::EngineError::Deadline,
                ));
            }
            self.sent.lock().unwrap().push(message);
            Ok(())
        })
    }
}
impl ws::Session for Socket {
    fn receive(&self) -> BoxFuture<'_, Result<Option<ws::Message>, MiddlewareError>> {
        Box::pin(async { Ok(self.received.lock().unwrap().take()) })
    }
}
struct Transport {
    socket: Arc<Socket>,
    accepted: Arc<AtomicUsize>,
}
impl WebSocketUpgrade for Transport {
    fn accept(
        &self,
        parts: http::request::Parts,
        protocols: Vec<String>,
    ) -> BoxFuture<'static, Result<(core::Response, PendingSession), MiddlewareError>> {
        let socket = self.socket.clone();
        let accepted = self.accepted.clone();
        Box::pin(async move {
            accepted.fetch_add(1, Ordering::SeqCst);
            assert_eq!(protocols, vec!["fixture"]);
            assert_eq!(
                parts.headers["authorization"],
                "Bearer synthetic-http-fixture"
            );
            if parts.headers["x-fixture-mode"] == "upgrade-error" {
                return Err(MiddlewareError::Engine(
                    gateway_core::engine::EngineError::Deadline,
                ));
            }
            let mut response = core::Response::new(core::empty_body());
            *response.status_mut() = http::StatusCode::SWITCHING_PROTOCOLS;
            response.extensions_mut().insert(Marker);
            let pending: BoxFuture<'static, Result<Arc<dyn ws::Session>, Arc<MiddlewareError>>> =
                Box::pin(async move { Ok(socket as Arc<dyn ws::Session>) });
            Ok((response, pending.shared()))
        })
    }
}
async fn run(
    f: &Fixture,
    mode: &str,
) -> (
    Result<core::Response, MiddlewareError>,
    Arc<Socket>,
    Arc<AtomicUsize>,
) {
    let socket = Arc::new(Socket {
        received: Mutex::new(Some(ws::Message {
            kind: ws::Kind::Binary,
            payload: Bytes::from_static(b"raw\0socket"),
        })),
        sent: Mutex::new(vec![]),
        send_error: mode == "upgrade-send-error",
    });
    let accepted = Arc::new(AtomicUsize::new(0));
    let upgrade: Arc<dyn WebSocketUpgrade> = Arc::new(Transport {
        socket: socket.clone(),
        accepted: accepted.clone(),
    });
    let mut request = core::Request::new(core::empty_body());
    request
        .headers_mut()
        .insert("x-fixture-mode", mode.parse().unwrap());
    request.headers_mut().insert(
        "authorization",
        "Bearer synthetic-http-fixture".parse().unwrap(),
    );
    request.extensions_mut().insert(upgrade);
    let plan = f
        .manager
        .middleware_registry()
        .unwrap()
        .freeze_http(context())
        .unwrap();
    let next = compose(vec![], |_| {
        Box::pin(async { panic!("upgrade consumes the normal next") })
    });
    (plan.run(request, next).await, socket, accepted)
}
#[tokio::test]
async fn sdk_http_upgrade_task_starts_only_after_handshake_and_roundtrips_binary_message_once() {
    let f = Fixture::new().await;
    let (response, socket, accepted) = run(&f, "upgrade").await;
    let response = response.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(socket.sent.lock().unwrap().is_empty());
    assert!(response.extensions().get::<Marker>().is_some());
    let task = response
        .extensions()
        .get::<Upgraded>()
        .unwrap()
        .take_task(response.status())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    {
        let sent = socket.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, b"raw\0socket".as_slice());
        assert_eq!(sent[0].kind, ws::Kind::Binary);
    }
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_modified_or_discarded_handshake_never_starts_session_work() {
    let f = Fixture::new().await;
    let (response, socket, _) = run(&f, "changed-upgrade").await;
    assert!(response.is_err());
    assert!(socket.sent.lock().unwrap().is_empty());
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_ok()
    );
    let (response, socket, _) = run(&f, "discard-upgrade").await;
    let response = response.unwrap();
    assert_eq!(response.status(), 403);
    assert!(response.extensions().get::<Upgraded>().is_none());
    assert!(socket.sent.lock().unwrap().is_empty());
    drop(response);
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_upgrade_transport_errors_keep_the_original_engine_failure() {
    let f = Fixture::new().await;
    let (response, _, accepted) = run(&f, "upgrade-error").await;
    assert!(matches!(
        response,
        Err(MiddlewareError::Engine(
            gateway_core::engine::EngineError::Deadline
        ))
    ));
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_ok()
    );
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_upgrade_send_errors_keep_the_original_engine_failure() {
    let f = Fixture::new().await;
    let (response, socket, _) = run(&f, "upgrade-send-error").await;
    let response = response.unwrap();
    let task = response
        .extensions()
        .get::<Upgraded>()
        .unwrap()
        .take_task(response.status())
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(MiddlewareError::Engine(
            gateway_core::engine::EngineError::Deadline
        ))
    ));
    assert!(socket.sent.lock().unwrap().is_empty());
    f.stop().await;
}
