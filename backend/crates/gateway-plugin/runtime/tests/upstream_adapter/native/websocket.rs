use super::{continuation::assert_replay_required, fixture::Fixture};
use futures::{SinkExt, StreamExt};
use gateway_core::{
    engine::{
        ExecutionOutcome,
        continuation::{NativeContinuationPort, NativeContinuationScope, PreviousResponseId},
        execution::ClientTransport,
    },
    policy::ClientApiKeyId,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::tungstenite::{
    Message,
    handshake::server::{Callback, ErrorResponse, Request, Response},
};

#[tokio::test]
async fn real_core_concurrent_websocket_continuations_cannot_claim_the_same_connection_twice() {
    for provider in ["openai", "xai"] {
        let server = Socket::new(false, false).await;
        let Some(f) = Fixture::for_websocket(provider, &server.url).await else {
            return;
        };
        let f = Arc::new(f);
        complete(&f, "websocket", None, 1).await;
        let mut first = f
            .start_transport(
                "sk_native_fixture",
                "websocket",
                Some("adapter-response"),
                ClientTransport::WebSocket,
            )
            .await
            .unwrap();
        let (unblock, gate) = tokio::sync::oneshot::channel();
        *f.core.admit_gate.lock().unwrap() = Some(gate);
        let waiting = {
            let f = f.clone();
            tokio::spawn(async move {
                f.start_transport(
                    "sk_native_fixture",
                    "websocket",
                    Some("adapter-response"),
                    ClientTransport::WebSocket,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            while f.core.active.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second continuation must resolve its pin before entering admission");
        first.session.collect_uncommitted().await.unwrap();
        first.session.commit_downstream(Some(200)).await.unwrap();
        first.session.detach_finalize().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while f.in_flight().await != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first native account lease must release while the second admission waits");
        unblock.send(()).unwrap();
        let mut second = tokio::time::timeout(Duration::from_secs(3), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let error = second.session.collect_uncommitted().await.unwrap_err();
        assert_replay_required(&error);
        second.session.detach_finalize().await;
        f.clean(3).await;
        server.assert_authorization(&f, 1);
        assert_eq!(server.pings.load(Ordering::SeqCst), 2);
        assert_eq!(
            server.closed.load(Ordering::SeqCst),
            0,
            "the losing request must not close the winner's retained connection"
        );
        assert_eq!(f.core.charges.lock().unwrap()[2].amount_usd.scaled(), 0);
        assert_eq!(f.core.finalizations.lock().unwrap()[2].attempt_count, 1);
        complete(&f, "websocket", Some("adapter-response"), 4).await;
        server.assert_authorization(&f, 1);
        assert_eq!(server.pings.load(Ordering::SeqCst), 3);
        f.disable().await;
        server.wait_closed(1).await;
        assert!(f.server.requests.lock().unwrap().is_empty());
        Arc::try_unwrap(f)
            .unwrap_or_else(|_| panic!("all concurrent callers must release the fixture"))
            .close()
            .await;
    }
}

struct Recorder(Arc<Mutex<Vec<http::HeaderMap>>>);
impl Callback for Recorder {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        self.0.lock().unwrap().push(request.headers().clone());
        Ok(response)
    }
}
struct Socket {
    url: String,
    headers: Arc<Mutex<Vec<http::HeaderMap>>>,
    pings: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    changed: Arc<tokio::sync::Notify>,
    failures: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Socket {
    async fn new(resume: bool, pause: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let headers = Arc::new(Mutex::new(vec![]));
        let pings = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let changed = Arc::new(tokio::sync::Notify::new());
        let failures = Arc::new(Mutex::new(vec![]));
        let (saved, counts, ends, ready, changes, errors) = (
            headers.clone(),
            pings.clone(),
            closed.clone(),
            entered.clone(),
            changed.clone(),
            failures.clone(),
        );
        let task = tokio::spawn(async move {
            let mut sockets = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = sockets.join_next(), if !sockets.is_empty() => {
                        if let Some(Err(error)) = result { errors.lock().unwrap().push(error.to_string()); }
                    }
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let (saved, counts, ends, ready, changes) = (saved.clone(), counts.clone(), ends.clone(), ready.clone(), changes.clone());
                        sockets.spawn(async move {
                            let mut socket = tokio_tungstenite::accept_hdr_async(socket, Recorder(saved)).await.unwrap();
                            loop {
                                let next = socket.next().await;
                                let Some(Ok(Message::Text(text))) = next else {
                                    assert!(matches!(next, None | Some(Err(_)) | Some(Ok(Message::Close(_)))));
                                    break;
                                };
                                assert_eq!(text.as_str(), "fixture-ping");
                                counts.fetch_add(1, Ordering::SeqCst);
                                ready.notify_one();
                                if pause {
                                    assert!(matches!(socket.next().await, None | Some(Err(_)) | Some(Ok(Message::Close(_)))));
                                    break;
                                }
                                if resume {
                                    assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "fixture-resume");
                                }
                                socket.send(Message::Text("fixture-pong".into())).await.unwrap();
                            }
                            ends.fetch_add(1, Ordering::SeqCst);
                            changes.notify_waiters();
                        });
                    }
                }
            }
        });
        Self {
            url,
            headers,
            pings,
            closed,
            entered,
            changed,
            failures,
            task,
        }
    }
    async fn wait_closed(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let changed = self.changed.notified();
                if self.closed.load(Ordering::SeqCst) >= count {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("revocation must close the actual upstream WebSocket");
        assert!(self.failures.lock().unwrap().is_empty());
    }
    fn assert_authorization(&self, f: &Fixture, connections: usize) {
        let headers = self.headers.lock().unwrap();
        assert_eq!(headers.len(), connections);
        for h in headers.iter() {
            assert_eq!(h["authorization"], "Bearer fixture-provider-secret");
            if f.provider == "openai" {
                assert_eq!(h["chatgpt-account-id"], format!("chatgpt-{}", f.account));
                assert!(!h.contains_key("x-grok-user-id"));
            } else {
                assert_eq!(h["x-grok-user-id"], format!("subject-{}", f.account));
                assert_eq!(h["x-xai-token-auth"], "xai-grok-cli");
                assert!(!h.contains_key("chatgpt-account-id"));
            }
            assert!(!h.contains_key("cookie"));
        }
    }
}

async fn complete(f: &Fixture, mode: &str, previous: Option<&str>, charges: usize) {
    let mut started = f
        .start_transport(
            "sk_native_fixture",
            mode,
            previous,
            ClientTransport::WebSocket,
        )
        .await
        .unwrap();
    let events = started.session.collect_uncommitted().await.unwrap();
    assert!(events.iter().any(|event| event.session_update().is_some()));
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
    f.clean(charges).await;
}

#[tokio::test]
async fn real_core_websocket_adapters_reuse_original_connection_without_idle_account_lease() {
    for provider in ["openai", "xai"] {
        for mode in ["websocket", "websocket-read-cancel"] {
            let server = Socket::new(mode == "websocket-read-cancel", false).await;
            let Some(f) = Fixture::for_websocket(provider, &server.url).await else {
                return;
            };
            complete(&f, mode, None, 1).await;
            let pin = f
                .continuation
                .resolve(
                    &ClientApiKeyId::new("key_alice").unwrap(),
                    &PreviousResponseId::new("adapter-response"),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pin.scope(), NativeContinuationScope::ConnectionLocal);
            assert!(
                pin.session_state()
                    .unwrap()
                    .extension_owner()
                    .unwrap()
                    .connection_local
            );
            assert_eq!(pin.account(), &f.account);
            assert_eq!(f.in_flight().await, 0);
            complete(&f, mode, Some("adapter-response"), 2).await;
            server.assert_authorization(&f, 1);
            assert_eq!(server.pings.load(Ordering::SeqCst), 2);
            assert!(f.server.requests.lock().unwrap().is_empty());
            assert!(
                f.core
                    .charges
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|c| c.amount_usd.scaled() == 570_000)
            );
            assert!(
                f.core
                    .finalizations
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| r.outcome == ExecutionOutcome::Succeeded && r.attempt_count == 1)
            );
            f.disable().await;
            server.wait_closed(1).await;
            f.close().await;
        }
    }
}

#[tokio::test]
async fn real_core_websocket_adapter_restart_or_credential_change_requires_client_replay() {
    for provider in ["openai", "xai"] {
        for change in ["restart", "credential"] {
            let server = Socket::new(false, false).await;
            let Some(mut f) = Fixture::for_websocket(provider, &server.url).await else {
                return;
            };
            complete(&f, "websocket", None, 1).await;
            if change == "restart" {
                f.restart().await;
                server.wait_closed(1).await;
            } else {
                f.advance_credential().await;
            }
            let mut started = f
                .start_transport(
                    "sk_native_fixture",
                    "websocket",
                    Some("adapter-response"),
                    ClientTransport::WebSocket,
                )
                .await
                .unwrap();
            let error = started.session.collect_uncommitted().await.unwrap_err();
            assert_replay_required(&error);
            started.session.detach_finalize().await;
            f.clean(2).await;
            server.assert_authorization(&f, 1);
            assert_eq!(server.pings.load(Ordering::SeqCst), 1);
            assert_eq!(f.core.charges.lock().unwrap()[1].amount_usd.scaled(), 0);
            complete(&f, "websocket", None, 3).await;
            server.assert_authorization(&f, 2);
            f.disable().await;
            server.wait_closed(2).await;
            assert!(f.server.requests.lock().unwrap().is_empty());
            f.close().await;
        }
    }
}

#[tokio::test]
async fn real_core_websocket_adapter_cancel_key_revoke_and_plugin_disable_close_active_connection()
{
    for provider in ["openai", "xai"] {
        for revoke in ["cancel", "key", "plugin"] {
            let server = Socket::new(false, true).await;
            let Some(f) = Fixture::for_websocket(provider, &server.url).await else {
                return;
            };
            let mut started = f
                .start_transport(
                    "sk_native_fixture",
                    "websocket-wait",
                    None,
                    ClientTransport::WebSocket,
                )
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(200), started.session.next_event())
                    .await
                    .is_err()
            );
            tokio::time::timeout(Duration::from_secs(3), server.entered.notified())
                .await
                .unwrap();
            assert_eq!(f.in_flight().await, 1);
            match revoke {
                "key" => f.revoke_key(),
                "plugin" => f.disable().await,
                _ => started.session.cancel(),
            }
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(3),
                    started.session.collect_uncommitted()
                )
                .await
                .unwrap()
                .is_err()
            );
            started.session.detach_finalize().await;
            f.clean(1).await;
            server.wait_closed(1).await;
            server.assert_authorization(&f, 1);
            assert_eq!(
                f.core.finalizations.lock().unwrap()[0].outcome,
                ExecutionOutcome::Cancelled
            );
            assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
            assert!(f.server.requests.lock().unwrap().is_empty());
            f.close().await;
        }
    }
}
