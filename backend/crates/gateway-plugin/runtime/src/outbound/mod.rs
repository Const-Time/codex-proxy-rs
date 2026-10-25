//! Host owns network dispatch and call-scoped bodies for both HTTP namespaces.
mod registry;
use crate::RpcReply;
use gateway_core::{
    account::OutboundProxy, engine::nested::ExtensionCallScope, lifecycle::CancellationToken,
    upstream::UpstreamSendState,
};
use gateway_host::outbound::{
    HttpBody, HttpClient, HttpError, HttpErrorKind, HttpRequest, NetworkPolicy,
};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault, SendState,
    call::{host as wire, upstream_adapter as adapter},
};
pub(crate) use registry::Registry;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
pub(crate) type SharedClient = Arc<OnceLock<Result<HttpClient, HttpError>>>;
pub(crate) const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
pub(crate) fn is_method(method: &str) -> bool {
    matches!(
        method,
        "host.http.do" | "host.http.do_stream" | "host.http.stream_read" | "host.http.stream_close"
    )
}
#[derive(Default)]
pub(crate) struct SendWatermark(AtomicU8, AtomicUsize);
impl SendWatermark {
    pub(crate) fn get(&self) -> UpstreamSendState {
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
pub(crate) struct SendGuard<'a> {
    call: &'a HttpScope,
    purpose: adapter::UpstreamPathPurpose,
    finished: bool,
}
impl SendGuard<'_> {
    pub(crate) fn finish(mut self, state: UpstreamSendState) {
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

pub(crate) struct HttpScope {
    pub(crate) send_state: SendWatermark,
    pub(crate) cancellation: CancellationToken,
    pub(crate) closed: CancellationToken,
    effects: Option<ExtensionCallScope>,
    proxy: Option<OutboundProxy>,
    pool_scope: Mutex<String>,
    deadline: crate::resources::InvocationDeadline,
    operation_timeout: Duration,
    streaming: AtomicBool,
    streams: Mutex<BTreeMap<String, Arc<HttpStream>>>,
}
impl Drop for HttpScope {
    fn drop(&mut self) {
        self.finish();
    }
}
impl HttpScope {
    pub(crate) fn new(
        cancellation: CancellationToken,
        timeout: Duration,
        effects: Option<ExtensionCallScope>,
        proxy: Option<OutboundProxy>,
        pool_scope: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            send_state: SendWatermark::default(),
            cancellation,
            closed: CancellationToken::new(),
            effects,
            proxy,
            pool_scope: Mutex::new(pool_scope),
            deadline: crate::resources::InvocationDeadline::new(Default::default(), timeout),
            operation_timeout: timeout.min(Duration::from_secs(120)),
            streaming: AtomicBool::new(false),
            streams: Mutex::default(),
        })
    }
    pub(crate) fn finish(&self) {
        self.closed.cancel();
        self.close_streams();
    }
    pub(crate) fn close_streams(&self) {
        for (_, stream) in std::mem::take(
            &mut *self
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ) {
            stream.close();
        }
    }
    pub(crate) fn rotate_pool_scope(&self, pool_scope: String) {
        self.close_streams();
        *self
            .pool_scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = pool_scope;
    }
    pub(crate) fn streaming(&self, streaming: bool) {
        self.streaming.store(streaming, Ordering::Release);
    }
    pub(crate) fn timeout(&self) -> Result<Duration, PluginFault> {
        if self.closed.is_cancelled() {
            return Err(denied());
        }
        // After RPC accepts streaming, the original resource owner controls cancellation.
        if self.streaming.load(Ordering::Acquire) {
            return Ok(self.operation_timeout);
        }
        let left = self.deadline.at().bounded(Duration::from_secs(120));
        if left.is_zero() {
            Err(self.fault(ErrorCode::Timeout, "network callback deadline"))
        } else {
            Ok(left)
        }
    }
    pub(crate) fn fault(&self, code: ErrorCode, message: &'static str) -> PluginFault {
        let mut fault = PluginFault::new(code, message);
        fault.send_state = wire_state(self.send_state.get());
        fault
    }
    pub(crate) fn start_send(&self, purpose: adapter::UpstreamPathPurpose) -> SendGuard<'_> {
        self.send_state.1.fetch_add(1, Ordering::AcqRel);
        SendGuard {
            call: self,
            purpose,
            finished: false,
        }
    }
    pub(crate) async fn collect(&self, body: &mut HttpBody) -> Result<Vec<u8>, PluginFault> {
        let mut payload = Vec::new();
        while let Some(chunk) = body.read(64 * 1024).await.map_err(http_error)? {
            if payload.len() + chunk.len() > MAX_PAYLOAD {
                return Err(self.fault(ErrorCode::Capacity, "HTTP body requires streaming"));
            }
            payload.extend_from_slice(&chunk);
        }
        Ok(payload)
    }

    pub(crate) async fn send(
        &self,
        client: &HttpClient,
        request: HttpRequest,
        streaming: bool,
        purpose: adapter::UpstreamPathPurpose,
    ) -> Result<RpcReply, PluginFault> {
        if request.body.len() > MAX_PAYLOAD || request.headers.len() > 128 {
            return Err(invalid());
        }
        let timeout = self.timeout()?;
        let guard = self.start_send(purpose);
        let pool_scope = self
            .pool_scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let response = client
            .open_scoped(
                request,
                self.proxy.as_ref(),
                &NetworkPolicy::unrestricted(),
                timeout,
                Some(&pool_scope),
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
            http_version: format!("{:?}", response.version),
            headers: response.headers,
            trailers: Vec::new(),
            stream: None,
        };
        let payload = if streaming {
            let mut streams = self
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.closed.is_cancelled() {
                return Err(denied());
            }
            if streams.len() >= 16 {
                return Err(self.fault(ErrorCode::Capacity, "HTTP stream capacity"));
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
            {
                let payload = self.collect(&mut body).await?;
                result.trailers = body.take_trailers();
                payload
            }
        };
        Ok(RpcReply {
            result: serde_json::to_value(result).map_err(|_| invalid())?,
            payload,
        })
    }

    pub(crate) async fn dispatch(
        &self,
        client: &HttpClient,
        method: &str,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        if payload.len() > MAX_PAYLOAD {
            return Err(invalid());
        }
        match method {
            "host.http.do" | "host.http.do_stream" => {
                let request: wire::HttpRequest =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                self.send(
                    client,
                    HttpRequest {
                        method: request.method,
                        url: request.url,
                        headers: request.headers,
                        body: payload,
                    },
                    method == "host.http.do_stream",
                    adapter::UpstreamPathPurpose::Auxiliary,
                )
                .await
            }
            "host.http.stream_read" | "host.upstream.http.stream_read" => {
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
                    PluginFault::new(ErrorCode::Conflict, "HTTP stream already has a reader")
                })?;
                let result = tokio::select! {biased;
                    ()=stream.closed.cancelled()=>Err(denied()),
                    result=body.as_mut().ok_or_else(denied)?.read_with_timeout(request.maximum_bytes as usize, self.timeout()?)=>result.map_err(http_error),
                };
                let trailers = body
                    .as_mut()
                    .map_or_else(Vec::new, |body| body.take_trailers());
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
                    result: serde_json::json!({"eof":chunk.is_none(), "trailers": trailers}),
                    payload: chunk.map_or_else(Vec::new, |c| c.to_vec()),
                })
            }
            "host.http.stream_close" | "host.upstream.http.stream_close" => {
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

            _ => Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unknown external HTTP method",
            )),
        }
    }
}
pub(crate) fn empty() -> RpcReply {
    RpcReply {
        result: serde_json::json!({}),
        payload: vec![],
    }
}
pub(crate) fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid managed HTTP request")
}
pub(crate) fn denied() -> PluginFault {
    PluginFault::new(
        ErrorCode::PermissionDenied,
        "managed HTTP resource unavailable",
    )
}
pub(crate) fn wire_state(state: UpstreamSendState) -> SendState {
    match state {
        UpstreamSendState::NotSent => SendState::NotSent,
        UpstreamSendState::Sent => SendState::Sent,
        UpstreamSendState::Ambiguous => SendState::Ambiguous,
    }
}
pub(crate) fn http_error(error: HttpError) -> PluginFault {
    let code = match error.kind() {
        HttpErrorKind::InvalidRequest => ErrorCode::InvalidInput,
        HttpErrorKind::AddressDenied => ErrorCode::PermissionDenied,
        HttpErrorKind::Capacity => ErrorCode::Capacity,
        HttpErrorKind::Timeout => ErrorCode::Timeout,
        _ => ErrorCode::Upstream,
    };
    let mut fault = PluginFault::new(code, "managed HTTP transport failed");
    fault.send_state = wire_state(error.send_state);
    fault
}
