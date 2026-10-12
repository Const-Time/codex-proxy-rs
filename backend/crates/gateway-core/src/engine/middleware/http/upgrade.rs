//! A validated transport handshake retains its session task until the response is committed.
use super::{MiddlewareError, Response};
use crate::engine::middleware::websocket::Session;
use futures::future::{BoxFuture, Shared};
use std::sync::{Arc, Mutex};

pub type PendingSession =
    Shared<BoxFuture<'static, Result<Arc<dyn Session>, Arc<MiddlewareError>>>>;
pub type SessionTask = BoxFuture<'static, Result<(), MiddlewareError>>;

/// The API owns handshake validation and the actual socket. Passing the original
/// request parts preserves its unforgeable transport extensions.
pub trait WebSocketUpgrade: Send + Sync {
    fn accept(
        &self,
        parts: http::request::Parts,
        protocols: Vec<String>,
    ) -> BoxFuture<'static, Result<(Response, PendingSession), MiddlewareError>>;
}

/// Cloned response extensions share a single task. Replacing or discarding the
/// response drops the task without starting detached work.
#[derive(Clone)]
pub struct Upgraded {
    status: http::StatusCode,
    task: Arc<Mutex<Option<SessionTask>>>,
}
impl Upgraded {
    pub fn new(status: http::StatusCode, task: SessionTask) -> Result<Self, MiddlewareError> {
        if !matches!(
            status,
            http::StatusCode::SWITCHING_PROTOCOLS | http::StatusCode::OK
        ) {
            return Err(MiddlewareError::InvalidState);
        }
        Ok(Self {
            status,
            task: Arc::new(Mutex::new(Some(task))),
        })
    }
    pub fn status(&self) -> http::StatusCode {
        self.status
    }
    /// Only the unchanged, successfully validated handshake may consume the task.
    pub fn take_task(&self, committed_status: http::StatusCode) -> Option<SessionTask> {
        if committed_status != self.status {
            return None;
        }
        self.task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}
