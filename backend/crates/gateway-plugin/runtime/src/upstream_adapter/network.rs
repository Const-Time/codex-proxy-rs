//! Only Host transport observations can advance send state or parent effects.
use super::{
    sessions::{ConnectionOwner, ConnectionPool},
    target::UpstreamTarget,
};
use crate::RpcReply;
use gateway_core::{
    engine::{nested::ExtensionCallScope, upstream_adapter::UpstreamAccountConnection},
    lifecycle::CancellationToken,
    upstream::UpstreamSendState,
};
use gateway_host::outbound::{
    HttpBody, HttpClient, HttpError, HttpErrorKind, HttpRequest, ManagedWebSocket, NetworkPolicy,
    WebSocketMessage,
};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault, SendState,
    call::{host as wire, upstream_adapter as adapter},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
#[derive(Default)]
pub(super) struct SendWatermark(AtomicU8, AtomicUsize);
impl SendWatermark {
    pub(super) fn get(&self) -> UpstreamSendState {
        if self.1.load(Ordering::Acquire) > 0 {
            return UpstreamSendState::Ambiguous;
        }
        match self.0.load(Ordering::Acquire) {
            0 => UpstreamSendState::NotSent,
            1 => UpstreamSendState::Sent,
            _ => UpstreamSendState::Ambiguous,
        }
    }
    fn observe(&self, state: UpstreamSendState) {
        self.0.fetch_max(
            match state {
                UpstreamSendState::NotSent => 0,
                UpstreamSendState::Sent => 1,
                UpstreamSendState::Ambiguous => 2,
            },
            Ordering::AcqRel,
        );
    }
}
struct SendGuard<'a> {
    call: &'a ManagedCall,
    purpose: adapter::UpstreamPathPurpose,
    finished: bool,
}
impl SendGuard<'_> {
    fn finish(mut self, state: UpstreamSendState) {
        self.observe(state);
        self.finished = true;
        self.call.send_state.1.fetch_sub(1, Ordering::AcqRel);
    }
    fn observe(&self, state: UpstreamSendState) {
        self.call.send_state.observe(state);
        if state != UpstreamSendState::NotSent
            && let Some(scope) = &self.call.effects
        {
            if self.purpose == adapter::UpstreamPathPurpose::Auxiliary {
                scope.observe_http_dispatch();
            } else {
                scope.observe_upstream_effect();
            }
        }
    }
}
impl Drop for SendGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.observe(UpstreamSendState::Ambiguous);
            self.call.send_state.1.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
struct HttpStream {
    body: AsyncMutex<Option<HttpBody>>,
    closed: CancellationToken,
}
impl HttpStream {
    fn close(&self) {
        self.closed.cancel();
        if let Ok(mut body) = self.body.try_lock() {
            body.take();
        }
    }
}
struct PendingRead(tokio::task::JoinHandle<Result<Option<WebSocketMessage>, HttpError>>);
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(super) struct ManagedCall {
    pub(super) account: Arc<dyn UpstreamAccountConnection>,
    pub(super) send_state: SendWatermark,
    pub(super) cancellation: CancellationToken,
    pub(super) closed: CancellationToken,
    target: Arc<UpstreamTarget>,
    effects: Option<ExtensionCallScope>,
    deadline: Instant,
    streams: Mutex<BTreeMap<String, Arc<HttpStream>>>,
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
            account,
            send_state: SendWatermark::default(),
            cancellation: options.cancellation,
            closed: CancellationToken::new(),
            target: options.target,
            effects: options.effects,
            deadline: Instant::now() + options.timeout,
            streams: Mutex::default(),
            socket: AsyncMutex::new(socket),
            read: AsyncMutex::default(),
            pool: options.pool,
            owner: options.owner,
            websocket_allowed: options.websocket_allowed,
        }))
    }
    pub(super) fn finish(&self) {
        self.closed.cancel();
        for (_, stream) in std::mem::take(
            &mut *self
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ) {
            stream.close();
        }
        // The execution retains a completed socket until it validates RPC End and the continuation.
        if let Ok(mut read) = self.read.try_lock() {
            read.take();
        }
    }
    fn timeout(&self) -> Result<Duration, PluginFault> {
        let left = self
            .deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(120));
        if left.is_zero() {
            Err(self.fault(ErrorCode::Timeout, "upstream deadline"))
        } else {
            Ok(left)
        }
    }
    pub(super) fn fault(&self, code: ErrorCode, message: &'static str) -> PluginFault {
        let mut fault = PluginFault::new(code, message);
        fault.send_state = wire_state(self.send_state.get());
        fault
    }
    fn start_send(&self, purpose: adapter::UpstreamPathPurpose) -> SendGuard<'_> {
        self.send_state.1.fetch_add(1, Ordering::AcqRel);
        SendGuard {
            call: self,
            purpose,
            finished: false,
        }
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
    async fn collect(&self, body: &mut HttpBody) -> Result<Vec<u8>, PluginFault> {
        let mut payload = Vec::new();
        while let Some(chunk) = body.read(64 * 1024).await.map_err(http_error)? {
            if payload.len() + chunk.len() > MAX_PAYLOAD {
                return Err(self.fault(ErrorCode::Capacity, "upstream body requires streaming"));
            }
            payload.extend_from_slice(&chunk);
        }
        Ok(payload)
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
                let timeout = self.timeout()?;
                let guard = self.start_send(purpose);
                // Pool scope binds the host account and credential revision without credential contents.
                let scope = format!(
                    "{}:{}:{}",
                    self.pool.identity, self.owner.account_id, self.owner.credential_revision
                );
                let response = client
                    .open_scoped(
                        request,
                        self.account.outbound_proxy(),
                        &NetworkPolicy::unrestricted(),
                        timeout,
                        Some(&scope),
                    )
                    .await;
                guard.finish(
                    response
                        .as_ref()
                        .map_or_else(|e| e.send_state, |_| UpstreamSendState::Sent),
                );
                let response = response.map_err(http_error)?;
                let mut body = response.body;
                let mut result = wire::HttpResponse {
                    status: response.status,
                    headers: headers(response.headers)?,
                    stream: None,
                };
                let payload = if method.ends_with("do_stream") {
                    let mut streams = self
                        .streams
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if self.closed.is_cancelled() {
                        return Err(denied());
                    }
                    if streams.len() >= 16 {
                        return Err(self.fault(ErrorCode::Capacity, "upstream stream capacity"));
                    }
                    let id = uuid::Uuid::new_v4().to_string();
                    streams.insert(
                        id.clone(),
                        Arc::new(HttpStream {
                            body: AsyncMutex::new(Some(body)),
                            closed: CancellationToken::new(),
                        }),
                    );
                    result.stream = Some(id);
                    vec![]
                } else {
                    self.collect(&mut body).await?
                };
                Ok(RpcReply {
                    result: serde_json::to_value(result).map_err(|_| invalid())?,
                    payload,
                })
            }
            "host.upstream.http.stream_read" => {
                if !payload.is_empty() {
                    return Err(invalid());
                }
                let request: wire::StreamRead =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                if request.maximum_bytes == 0 || request.maximum_bytes > 64 * 1024 {
                    return Err(invalid());
                }
                let stream = self
                    .streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&request.stream)
                    .cloned()
                    .ok_or_else(denied)?;
                let mut body = stream.body.try_lock().map_err(|_| {
                    PluginFault::new(ErrorCode::Conflict, "upstream stream already has a reader")
                })?;
                let result = tokio::select! {biased;
                    ()=stream.closed.cancelled()=>Err(denied()),
                    result=body.as_mut().ok_or_else(denied)?.read(request.maximum_bytes as usize)=>result.map_err(http_error),
                };
                if !matches!(result, Ok(Some(_))) {
                    body.take();
                    stream.closed.cancel();
                    self.streams
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&request.stream);
                }
                let chunk = result?;
                Ok(RpcReply {
                    result: serde_json::json!({"eof":chunk.is_none()}),
                    payload: chunk.map_or_else(Vec::new, |c| c.to_vec()),
                })
            }
            "host.upstream.http.stream_close" => {
                if !payload.is_empty() {
                    return Err(invalid());
                }
                let request: wire::StreamClose =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                let stream = self
                    .streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.stream)
                    .ok_or_else(denied)?;
                stream.close();
                Ok(empty())
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
                    self.collect(body).await?
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
fn empty() -> RpcReply {
    RpcReply {
        result: serde_json::json!({}),
        payload: vec![],
    }
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid managed upstream request")
}
fn denied() -> PluginFault {
    PluginFault::new(
        ErrorCode::PermissionDenied,
        "managed upstream resource unavailable",
    )
}
fn headers(headers: Vec<(String, Vec<u8>)>) -> Result<Vec<(String, String)>, PluginFault> {
    headers
        .into_iter()
        .map(|(n, v)| String::from_utf8(v).map(|v| (n, v)).map_err(|_| invalid()))
        .collect()
}
fn wire_state(state: UpstreamSendState) -> SendState {
    match state {
        UpstreamSendState::NotSent => SendState::NotSent,
        UpstreamSendState::Sent => SendState::Sent,
        UpstreamSendState::Ambiguous => SendState::Ambiguous,
    }
}
fn http_error(error: HttpError) -> PluginFault {
    let code = match error.kind() {
        HttpErrorKind::InvalidRequest => ErrorCode::InvalidInput,
        HttpErrorKind::AddressDenied => ErrorCode::PermissionDenied,
        HttpErrorKind::Capacity => ErrorCode::Capacity,
        HttpErrorKind::Timeout => ErrorCode::Timeout,
        _ => ErrorCode::Upstream,
    };
    let mut fault = PluginFault::new(code, "managed upstream transport failed");
    fault.send_state = wire_state(error.send_state);
    fault
}
