//! Only Host transport observations can advance send state or parent effects.
use super::{
    sessions::{ConnectionOwner, ConnectionPool},
    target::UpstreamTarget,
};
use crate::RpcReply;
use crate::outbound::{
    HttpScope, MAX_PAYLOAD, SendGuard, denied, empty, headers, http_error, invalid,
};
use gateway_core::{
    engine::{nested::ExtensionCallScope, upstream_adapter::UpstreamAccountConnection},
    lifecycle::CancellationToken,
    upstream::UpstreamSendState,
};
use gateway_host::outbound::{
    HttpClient, HttpError, HttpRequest, ManagedWebSocket, NetworkPolicy, WebSocketMessage,
};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{host as wire, upstream_adapter as adapter},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex as AsyncMutex;
struct PendingRead(tokio::task::JoinHandle<Result<Option<WebSocketMessage>, HttpError>>);
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(super) struct ManagedCall {
    pub(super) account: Arc<dyn UpstreamAccountConnection>,
    pub(super) http: Arc<HttpScope>,
    pub(super) cancellation: CancellationToken,
    pub(super) closed: CancellationToken,
    target: Arc<UpstreamTarget>,
    socket: AsyncMutex<Option<(String, Arc<ManagedWebSocket>)>>,
    read: AsyncMutex<Option<PendingRead>>,
    pool: Arc<ConnectionPool>,
    owner: ConnectionOwner,
    websocket_allowed: bool,
}
pub(super) struct CallOptions {
    pub target: Arc<UpstreamTarget>,
    pub effects: Option<ExtensionCallScope>,
    pub cancellation: CancellationToken,
    pub timeout: Duration,
    pub pool: Arc<ConnectionPool>,
    pub owner: ConnectionOwner,
    pub connection: Option<String>,
    pub websocket_allowed: bool,
}
impl ManagedCall {
    pub(super) fn new(
        account: Arc<dyn UpstreamAccountConnection>,
        options: CallOptions,
    ) -> Result<Arc<Self>, PluginFault> {
        let socket = options
            .connection
            .as_deref()
            .map(|id| options.pool.take(id, &options.owner))
            .transpose()?
            .map(|(url, socket)| (url, Arc::new(socket)));
        Ok(Arc::new(Self {
            account: account.clone(),
            http: HttpScope::new(
                options.cancellation.clone(),
                options.timeout,
                options.effects,
                account.outbound_proxy().cloned(),
                format!(
                    "{}:{}:{}",
                    options.pool.identity,
                    options.owner.account_id,
                    options.owner.credential_revision
                ),
            ),
            cancellation: options.cancellation,
            closed: CancellationToken::new(),
            target: options.target,
            socket: AsyncMutex::new(socket),
            read: AsyncMutex::default(),
            pool: options.pool,
            owner: options.owner,
            websocket_allowed: options.websocket_allowed,
        }))
    }
    pub(super) fn finish(&self) {
        self.closed.cancel();
        self.http.finish();
        // The execution retains a completed socket until it validates RPC End and the continuation.
        if let Ok(mut read) = self.read.try_lock() {
            read.take();
        }
    }
    fn timeout(&self) -> Result<Duration, PluginFault> {
        self.http.timeout()
    }
    pub(super) fn fault(&self, code: ErrorCode, message: &'static str) -> PluginFault {
        self.http.fault(code, message)
    }
    fn start_send(&self, purpose: adapter::UpstreamPathPurpose) -> SendGuard<'_> {
        self.http.start_send(purpose)
    }
    fn request(
        &self,
        input: adapter::UpstreamHttpRequest,
        body: Vec<u8>,
    ) -> Result<(HttpRequest, adapter::UpstreamPathPurpose), PluginFault> {
        if input.headers.len() > 128 || body.len() > MAX_PAYLOAD {
            return Err(invalid());
        }
        let (url, purpose) = self.target.resolve(&input.path, &input.query)?;
        let auth = self.account.authorization().map_err(|_| {
            PluginFault::new(
                ErrorCode::Rejected,
                "selected account authentication is unavailable",
            )
        })?;
        let mut headers: Vec<_> = auth
            .into_iter()
            .filter(|h| {
                !input
                    .headers
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(h.name()))
            })
            .map(|h| {
                let (n, v) = h.into_parts();
                (n, v.to_vec())
            })
            .collect();
        headers.extend(input.headers.into_iter().map(|(n, v)| (n, v.into_bytes())));
        Ok((
            HttpRequest {
                method: input.method,
                url: url.into(),
                headers,
                body,
            },
            purpose,
        ))
    }
    pub(super) async fn retain_connection(&self) -> Result<String, PluginFault> {
        if self.read.lock().await.is_some() {
            return Err(denied());
        }
        let (url, socket) = self.socket.lock().await.take().ok_or_else(denied)?;
        let socket = match Arc::try_unwrap(socket) {
            Ok(s) if !s.is_closed() => s,
            Ok(_) => return Err(denied()),
            Err(s) => {
                s.close().await;
                return Err(denied());
            }
        };
        self.pool.put(self.owner.clone(), (url, socket))
    }
    async fn connection(&self) -> Result<Arc<ManagedWebSocket>, PluginFault> {
        self.socket
            .lock()
            .await
            .as_ref()
            .map(|(_, s)| s.clone())
            .ok_or_else(denied)
    }
    pub(super) async fn dispatch(
        self: &Arc<Self>,
        client: &HttpClient,
        method: &str,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        if payload.len() > MAX_PAYLOAD {
            return Err(invalid());
        }
        match method {
            "host.upstream.http.do" | "host.upstream.http.do_stream" => {
                let (request, purpose) = self.request(
                    serde_json::from_value(params).map_err(|_| invalid())?,
                    payload,
                )?;
                self.http
                    .send(client, request, method.ends_with("do_stream"), purpose)
                    .await
            }
            "host.upstream.http.stream_read" | "host.upstream.http.stream_close" => {
                self.http.dispatch(client, method, params, payload).await
            }
            "host.upstream.websocket.open" => {
                if !payload.is_empty() || !self.websocket_allowed {
                    return Err(denied());
                }
                let input: adapter::UpstreamWebSocketRequest =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                let (request, purpose) = self.request(
                    adapter::UpstreamHttpRequest {
                        method: "GET".into(),
                        path: input.path,
                        query: input.query,
                        headers: input.headers,
                    },
                    vec![],
                )?;
                if purpose != adapter::UpstreamPathPurpose::Inference {
                    return Err(denied());
                }
                let mut socket = self.socket.try_lock().map_err(|_| denied())?;
                if let Some((url, connection)) = socket.as_ref().filter(|(_, s)| !s.is_closed()) {
                    if *url != request.url {
                        return Err(denied());
                    }
                    let _ = connection;
                    return Ok(RpcReply {
                        result: serde_json::json!({"status":101,"headers":[],"stream":null}),
                        payload: vec![],
                    });
                }
                let timeout = self.timeout()?;
                let guard = self.start_send(purpose);
                let url = request.url.clone();
                let response = client
                    .open_websocket(
                        request,
                        self.account.outbound_proxy(),
                        &NetworkPolicy::unrestricted(),
                        timeout,
                    )
                    .await;
                guard.finish(
                    response
                        .as_ref()
                        .map_or_else(|e| e.send_state, |_| UpstreamSendState::Sent),
                );
                let mut response = response.map_err(http_error)?;
                let payload = if let Some(body) = &mut response.body {
                    self.http.collect(body).await?
                } else {
                    vec![]
                };
                if let Some(connection) = response.connection {
                    *socket = Some((url, Arc::new(connection)));
                }
                Ok(RpcReply {
                    result: serde_json::to_value(wire::HttpResponse {
                        status: response.status,
                        headers: headers(response.headers)?,
                        stream: None,
                    })
                    .map_err(|_| invalid())?,
                    payload,
                })
            }
            "host.upstream.websocket.send" => {
                let input: adapter::UpstreamWebSocketMessage =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                let message = match input.kind {
                    adapter::WebSocketMessageKind::Text => {
                        WebSocketMessage::Text(String::from_utf8(payload).map_err(|_| invalid())?)
                    }
                    adapter::WebSocketMessageKind::Binary => {
                        WebSocketMessage::Binary(payload.into())
                    }
                };
                let socket = self.connection().await?;
                let timeout = self.timeout()?;
                let guard = self.start_send(adapter::UpstreamPathPurpose::Inference);
                let result = socket.send(message, timeout).await;
                guard.finish(
                    result
                        .as_ref()
                        .map_or_else(|e| e.send_state, |()| UpstreamSendState::Sent),
                );
                result.map_err(http_error)?;
                Ok(empty())
            }
            "host.upstream.websocket.read" => {
                if params != serde_json::json!({}) || !payload.is_empty() {
                    return Err(invalid());
                }
                let mut pending = self.read.try_lock().map_err(|_| {
                    PluginFault::new(ErrorCode::Conflict, "upstream socket already has a reader")
                })?;
                if pending.is_none() {
                    let socket = self.connection().await?;
                    let timeout = self.timeout()?;
                    *pending = Some(PendingRead(tokio::spawn(async move {
                        socket.read(timeout).await
                    })));
                }
                // Cancelling one SDK callback drops only this waiter; the next read resumes this task.
                let result = (&mut pending.as_mut().ok_or_else(denied)?.0).await;
                pending.take();
                let result = result
                    .map_err(|_| self.fault(ErrorCode::Fault, "upstream read task ended"))?
                    .map_err(http_error)?;
                let (kind, payload) = match result {
                    Some(WebSocketMessage::Text(t)) => {
                        (Some(adapter::WebSocketMessageKind::Text), t.into_bytes())
                    }
                    Some(WebSocketMessage::Binary(b)) => {
                        (Some(adapter::WebSocketMessageKind::Binary), b.to_vec())
                    }
                    None => (None, vec![]),
                };
                Ok(RpcReply {
                    result: serde_json::to_value(adapter::UpstreamWebSocketRead {
                        eof: kind.is_none(),
                        kind,
                    })
                    .map_err(|_| invalid())?,
                    payload,
                })
            }
            "host.upstream.websocket.close" => {
                if params != serde_json::json!({}) || !payload.is_empty() {
                    return Err(invalid());
                }
                let socket = self.socket.lock().await.take();
                if let Some((_, socket)) = socket {
                    socket.close().await;
                }
                if let Ok(mut pending) = self.read.try_lock() {
                    pending.take();
                }
                Ok(empty())
            }
            _ => Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unknown managed upstream method",
            )),
        }
    }
}
