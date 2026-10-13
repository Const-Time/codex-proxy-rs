//! Duplex frame contracts; the API owns wire parsing, connection guards and writes.
use super::{MiddlewareError, MiddlewareHeader};
use crate::lifecycle::{CancellationToken, Deadline};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Incoming,
    Outgoing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Text,
    Binary,
    Ping,
    Pong,
    Close { code: Option<u16> },
}
#[derive(Clone, Debug)]
pub struct Message {
    pub kind: Kind,
    pub payload: Bytes,
}
/// Waiting for a read must not hold the transport writer.
pub trait Sender: Send + Sync {
    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), MiddlewareError>>;
}
pub trait Session: Sender {
    fn receive(&self) -> BoxFuture<'_, Result<Option<Message>, MiddlewareError>>;
}
#[derive(Clone)]
pub struct Context {
    pub origin: Option<super::service::Origin>,
    pub connection_id: String,
    pub direction: Direction,
    pub headers: Arc<[MiddlewareHeader]>,
    pub excluded_plugin_instances: BTreeSet<String>,
    pub cancellation: CancellationToken,
    pub deadline_at: Deadline,
    pub sender: Arc<dyn Sender>,
}
impl Context {
    pub fn service_origin(
        &self,
        factory: Option<Arc<dyn super::service::Factory>>,
    ) -> super::service::Origin {
        let mut origin = self
            .origin
            .clone()
            .unwrap_or_else(|| super::service::Origin {
                factory,
                context: super::service::Context {
                    operation: "host.services.call",
                    request_id: self.connection_id.clone(),
                    call_id: uuid::Uuid::now_v7().to_string(),
                    parent_call_id: None,
                    cancellation: self.cancellation.clone(),
                    deadline_at: self.deadline_at,
                    call_scope: crate::engine::nested::ExtensionCallScope::transport_root(
                        self.excluded_plugin_instances.clone(),
                    ),
                    dispatcher: None,
                },
            });
        origin.context.cancellation =
            CancellationToken::linked([origin.context.cancellation, self.cancellation.clone()]);
        origin.context.deadline_at = self
            .deadline_at
            .at()
            .map_or(origin.context.deadline_at, |at| {
                origin.context.deadline_at.cap(at)
            });
        origin
    }
}
pub type Next = crate::middleware::Next<Message, Option<Message>, MiddlewareError>;
