//! HTTP values and lazy frames; authentication, routing and transport stay with their owners.
pub mod upgrade;

use super::MiddlewareError;
use crate::{
    lifecycle::{CancellationToken, Deadline},
    routing::RequestSettings,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{BodyExt as _, Empty, combinators::UnsyncBoxBody};
use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

pub type Body = UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;
pub type Request = http::Request<Body>;
pub type Response = http::Response<Body>;
pub type Next = crate::middleware::Next<Request, Response, MiddlewareError>;

/// Pre-authentication scope contains no Key or administrative principal. IDs and
/// ancestry describe invocation placement; the terminal still authenticates normally.
#[derive(Clone, Debug)]
pub struct Context {
    pub request_id: String,
    pub call_id: String,
    pub parent_call_id: Option<String>,
    pub call_scope: crate::engine::nested::ExtensionCallScope,
    pub cancellation: CancellationToken,
    pub deadline_at: Deadline,
}

impl Context {
    /// The host issues ancestry and IDs; plugin wire values cannot substitute them.
    pub fn child(&self, instance: String) -> Result<Self, crate::error::GatewayError> {
        Ok(Self {
            call_scope: self.call_scope.fork_http(instance)?,
            call_id: uuid::Uuid::now_v7().to_string(),
            parent_call_id: Some(self.call_id.clone()),
            cancellation: CancellationToken::linked([self.cancellation.clone()]),
            ..self.clone()
        })
    }
}

/// Captures the host router and its publication snapshot, without network loopback.
pub trait Dispatcher: Send + Sync {
    fn request_settings(&self) -> Option<RequestSettings> {
        None
    }
    fn dispatch(
        &self,
        context: Context,
        request: Request,
    ) -> BoxFuture<'static, Result<Response, MiddlewareError>>;
}

/// Projection of the existing request settings and API timeout, carried in extensions.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub timeout: Option<Duration>,
    pub runtime: Option<RequestSettings>,
}

#[must_use]
pub fn empty_body() -> Body {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Revocation remains live after the initial middleware RPC or Core settlement.
/// Dropping the source on revocation releases its original resources; no task is spawned.
pub fn guard_body(body: Body, cancellation: CancellationToken, deadline: Deadline) -> Body {
    GuardedBody {
        body: Some(body),
        cancelled: Box::pin(async move { cancellation.cancelled().await }),
        expired: Box::pin(async move { deadline.wait().await }),
    }
    .boxed_unsync()
}
struct GuardedBody {
    body: Option<Body>,
    cancelled: BoxFuture<'static, ()>,
    expired: BoxFuture<'static, ()>,
}
impl HttpBody for GuardedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.body.is_none() {
            return Poll::Ready(None);
        }
        let error = if self.cancelled.as_mut().poll(cx).is_ready() {
            Some(MiddlewareError::Fault)
        } else if self.expired.as_mut().poll(cx).is_ready() {
            Some(MiddlewareError::Engine(
                crate::engine::EngineError::Deadline,
            ))
        } else {
            None
        };
        if let Some(error) = error {
            self.body.take();
            return Poll::Ready(Some(Err(Box::new(error))));
        }
        let result = Pin::new(self.body.as_mut().expect("live body checked above")).poll_frame(cx);
        if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.body.take();
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(HttpBody::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(SizeHint::default, HttpBody::size_hint)
    }
}
