//! HTTP 边界只搬运类型化请求、响应与正文资源；业务默认实现仍由 API 持有

mod dispatch;
mod execution;
pub(crate) mod resources;
mod upgrade;
pub(super) use execution::invoke;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use gateway_core::{engine::middleware::MiddlewareError, engine::middleware::http as core};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::middleware::{MiddlewareHeader, NEXT_METHOD, http as wire},
};
use http::{HeaderMap, HeaderName, HeaderValue, Version};

use crate::RpcReply;

pub(crate) struct Invocation {
    instance_id: String,
    state: Mutex<State>,
    resources: Arc<resources::Resources>,
    session: Mutex<Option<upgrade::Session>>,
    pub(super) cancellation: gateway_core::lifecycle::CancellationToken,
    downstream_error: Mutex<Option<MiddlewareError>>,
    pub(super) callbacks_closed: gateway_core::lifecycle::CancellationToken,
    context: core::Context,
    dispatcher: Option<Arc<dyn core::Dispatcher>>,
}

struct State {
    settings: Option<gateway_core::routing::RequestSettings>,
    request: Option<http::request::Parts>,
    next: Option<core::Next>,
}

pub(super) fn resolve_settings(
    settings: Option<gateway_core::routing::RequestSettings>,
    value: serde_json::Value,
    timeout_ms: Option<u64>,
    instance_id: &str,
) -> Result<core::Settings, PluginFault> {
    let settings = match (settings, value.is_null()) {
        (Some(settings), false) => Ok(Some(
            settings
                .replace(
                    serde_json::from_value(value).map_err(|_| invalid())?,
                    instance_id,
                )
                .map_err(|_| invalid())?,
        )),
        (settings, true) => Ok(settings),
        (None, false) => Err(invalid()),
    }?;
    let runtime = settings;
    Ok(core::Settings {
        runtime,
        timeout: timeout_ms.map(Duration::from_millis),
    })
}

impl Invocation {
    pub(crate) fn new(
        request: core::Request,
        next: core::Next,
        instance_id: String,
        context: core::Context,
    ) -> Result<(Arc<Self>, wire::Request), MiddlewareError> {
        let (parts, body) = request.into_parts();
        let dispatcher = parts.extensions.get::<Arc<dyn core::Dispatcher>>().cloned();
        let settings = parts
            .extensions
            .get::<core::Settings>()
            .and_then(|settings| settings.runtime.clone());
        let head = wire::Request {
            settings: serde_json::to_value(settings.as_ref().map(|settings| settings.values()))
                .map_err(|_| MiddlewareError::InvalidState)?,
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            version: wire_version(parts.version)?,
            headers: wire_headers(&parts.headers),
            timeout_ms: parts
                .extensions
                .get::<core::Settings>()
                .and_then(|settings| settings.timeout)
                .map(|timeout| {
                    let ms = u64::try_from(timeout.as_millis())
                        .map_err(|_| MiddlewareError::InvalidState)?;
                    if Duration::from_millis(ms) != timeout {
                        return Err(MiddlewareError::InvalidState);
                    }
                    Ok(ms)
                })
                .transpose()?,
            body: wire::Body::Empty,
        };
        let invocation = Arc::new(Self {
            instance_id,
            state: Mutex::new(State {
                settings,
                request: Some(parts),
                next: Some(next),
            }),
            resources: Arc::new(resources::Resources::default()),
            session: Mutex::new(None),
            cancellation: context.cancellation.clone(),
            context,
            dispatcher,
            downstream_error: Mutex::new(None),
            callbacks_closed: Default::default(),
        });
        let body = invocation
            .resources
            .insert_body(body)
            .map_err(|_| MiddlewareError::Fault)?;
        Ok((invocation, wire::Request { body, ..head }))
    }

    async fn next(
        &self,
        request: wire::Request,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        // 先校验完整 HTTP 值，再消费唯一续体；请求 extensions（包括连接升级）原样搬运
        let method = request.method.parse().map_err(|_| invalid())?;
        let uri = request.uri.parse().map_err(|_| invalid())?;
        let headers = headers(request.headers)?;
        let settings = resolve_settings(
            self.request_settings(),
            request.settings,
            request.timeout_ms,
            &self.instance_id,
        )?;
        let body = self.resources.body(request.body, payload).await?;
        let (mut parts, next) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = state.next.take().ok_or_else(unavailable)?;
            let parts = state.request.take().ok_or_else(unavailable)?;
            state.settings = settings.runtime.clone();
            (parts, next)
        };
        parts.method = method;
        parts.uri = uri;
        parts.version = version(request.version);
        parts.headers = headers;
        parts.extensions.insert(settings);
        let response = next
            .run(core::Request::from_parts(parts, body))
            .await
            .map_err(|error| self.failed(error))?;
        self.resources.response(response)
    }

    pub(crate) async fn complete(
        &self,
        response: wire::Response,
        payload: Vec<u8>,
        stream_body: Option<core::Body>,
    ) -> Result<core::Response, MiddlewareError> {
        let status = http::StatusCode::from_u16(response.status)
            .map_err(|_| MiddlewareError::InvalidState)?;
        let headers = headers(response.headers).map_err(|_| MiddlewareError::InvalidState)?;
        let body = if matches!(response.body, wire::Body::Stream) && payload.is_empty() {
            stream_body.ok_or(MiddlewareError::InvalidState)?
        } else {
            self.resources
                .body(response.body, payload)
                .await
                .map_err(|_| MiddlewareError::InvalidState)?
        };
        let mut result = match response.response {
            Some(source) => {
                core::Response::from_parts(self.resources.take_response(&source)?, body)
            }
            None => core::Response::new(body),
        };
        *result.status_mut() = status;
        *result.version_mut() = version(response.version);
        *result.headers_mut() = headers;
        Ok(result)
    }
}

impl Invocation {
    pub(super) fn request_settings(&self) -> Option<gateway_core::routing::RequestSettings> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settings
            .clone()
    }

    pub(super) fn call(
        self: Arc<Self>,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
        maximum_payload: usize,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        Box::pin(async move {
            if self.is_session_method(&method, &params) {
                return self
                    .session_call(method, params, payload, maximum_payload)
                    .await;
            }
            if is_resource_method(&method) {
                return self
                    .resources
                    .call(&method, params, payload, maximum_payload)
                    .await;
            }
            match method.as_str() {
                wire::DISPATCH_METHOD => {
                    self.dispatch(
                        serde_json::from_value(params).map_err(|_| invalid())?,
                        payload,
                    )
                    .await
                }
                wire::UPGRADE_METHOD if payload.is_empty() => {
                    self.upgrade(serde_json::from_value(params).map_err(|_| invalid())?)
                        .await
                }
                NEXT_METHOD => {
                    self.next(
                        serde_json::from_value(params).map_err(|_| invalid())?,
                        payload,
                    )
                    .await
                }
                _ => Err(invalid()),
            }
        })
    }
}

pub(crate) fn wire_headers(headers: &HeaderMap) -> Vec<MiddlewareHeader> {
    headers
        .iter()
        .map(|(name, value)| MiddlewareHeader {
            name: name.as_str().to_owned(),
            value: value.as_bytes().to_vec(),
        })
        .collect()
}

pub(crate) fn headers(headers: Vec<MiddlewareHeader>) -> Result<HeaderMap, PluginFault> {
    let checked: Vec<_> = headers
        .iter()
        .map(|h| {
            gateway_core::engine::middleware::MiddlewareHeader::new(
                h.name.clone(),
                bytes::Bytes::copy_from_slice(&h.value),
            )
        })
        .collect();
    super::headers::validate_headers(&checked)?;
    let mut result = HeaderMap::new();
    for header in headers {
        result
            .try_append(
                HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| invalid())?,
                HeaderValue::from_bytes(&header.value).map_err(|_| invalid())?,
            )
            .map_err(|_| invalid())?;
    }
    Ok(result)
}

fn wire_version(version: Version) -> Result<wire::Version, MiddlewareError> {
    Ok(match version {
        Version::HTTP_09 => wire::Version::Http09,
        Version::HTTP_10 => wire::Version::Http10,
        Version::HTTP_11 => wire::Version::Http11,
        Version::HTTP_2 => wire::Version::Http2,
        Version::HTTP_3 => wire::Version::Http3,
        _ => return Err(MiddlewareError::InvalidState),
    })
}

pub(super) fn version(version: wire::Version) -> Version {
    match version {
        wire::Version::Http09 => Version::HTTP_09,
        wire::Version::Http10 => Version::HTTP_10,
        wire::Version::Http11 => Version::HTTP_11,
        wire::Version::Http2 => Version::HTTP_2,
        wire::Version::Http3 => Version::HTTP_3,
    }
}

fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "HTTP middleware input is invalid")
}
fn unavailable() -> PluginFault {
    PluginFault::new(
        ErrorCode::Conflict,
        "HTTP middleware resource was consumed or closed",
    )
}

impl Invocation {
    pub(super) fn failed(&self, error: MiddlewareError) -> PluginFault {
        let failure = fault(&error);
        *self
            .downstream_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
        failure
    }
    pub(super) fn take_downstream_error(&self) -> Option<MiddlewareError> {
        self.downstream_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .or_else(|| {
                self.resources
                    .take_error()
                    .map(|error| match error.downcast::<MiddlewareError>() {
                        Ok(error) => *error,
                        Err(source) => MiddlewareError::Remote {
                            source,
                            rejected: false,
                        },
                    })
            })
            .or_else(|| {
                self.session
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .and_then(|session| session.take_error())
            })
    }
}
pub(crate) fn is_method(method: &str) -> bool {
    is_resource_method(method)
        || matches!(
            method,
            NEXT_METHOD
                | wire::DISPATCH_METHOD
                | wire::UPGRADE_METHOD
                | wire::RECEIVE_METHOD
                | gateway_plugin_sdk::call::middleware::websocket::SEND_METHOD
                | gateway_plugin_sdk::call::middleware::BODY_READ_METHOD
                | gateway_plugin_sdk::call::middleware::BODY_CLOSE_METHOD
        )
}
fn is_resource_method(method: &str) -> bool {
    matches!(
        method,
        wire::BODY_READ_METHOD
            | wire::BODY_CLOSE_METHOD
            | wire::BODY_CREATE_METHOD
            | wire::BODY_WRITE_METHOD
    )
}
pub(super) fn fault(error: &MiddlewareError) -> PluginFault {
    PluginFault::new(
        if error.is_rejected() {
            ErrorCode::Rejected
        } else {
            ErrorCode::Fault
        },
        "HTTP middleware terminal failed",
    )
}
impl Invocation {
    pub(super) fn close_callbacks(&self) {
        self.callbacks_closed.cancel();
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.resources.close();
    }
}
pub(super) fn rpc_error(error: crate::RpcError) -> MiddlewareError {
    let rejected =
        matches!(&error,crate::RpcError::Remote(fault) if fault.code==ErrorCode::Rejected);
    MiddlewareError::Remote {
        source: Box::new(error),
        rejected,
    }
}
