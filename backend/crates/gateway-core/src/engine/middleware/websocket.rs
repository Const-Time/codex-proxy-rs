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
    pub connection_id: String,
    pub direction: Direction,
    pub headers: Arc<[MiddlewareHeader]>,
    pub excluded_plugin_instances: BTreeSet<String>,
    pub cancellation: CancellationToken,
    pub deadline_at: Deadline,
    pub sender: Arc<dyn Sender>,
}
pub type Next = crate::middleware::Next<Message, Option<Message>, MiddlewareError>;
