//! A rendezvous with Core, not an independently pollable Provider stream.
//!
//! Plugins obtain an event only after Core has registered the cold attempt and
//! observed its original facts. The owner keeps driving this protocol while a
//! middleware call or its lazy output is pending; there is no second ledger.
use super::{
    AttemptContext,
    middleware::MiddlewareError,
    provider::{ProviderCallMetadata, ProviderRequest},
};
use crate::{event::ProviderEvent, operation::Operation};
use futures::{
    Stream,
    channel::{mpsc, oneshot},
    future::{BoxFuture, poll_fn},
    stream::BoxStream,
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub type AttemptEvents = BoxStream<'static, Result<ProviderEvent, MiddlewareError>>;
pub type AttemptNext = crate::middleware::Next<Operation, AttemptSource, MiddlewareError>;

/// One immutable root binding creates a fresh invocation for each real attempt.
/// Protocol adapters implement this port; they never receive Provider credentials
/// or the original cold ProviderStream.
pub trait AttemptMiddleware: Send + Sync {
    fn invoke(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
        next: AttemptNext,
    ) -> BoxFuture<'static, Result<AttemptEvents, MiddlewareError>>;
}

/// Host-authorized source. Its read future may be dropped and resumed without
/// requesting another upstream event or losing the already requested result.
pub struct AttemptSource {
    metadata: ProviderCallMetadata,
    commands: mpsc::UnboundedSender<Command>,
    pending: Option<oneshot::Receiver<Result<Option<ProviderEvent>, MiddlewareError>>>,
    ended: bool,
}
impl AttemptSource {
    pub fn metadata(&self) -> &ProviderCallMetadata {
        &self.metadata
    }
    pub async fn next_event(&mut self) -> Result<Option<ProviderEvent>, MiddlewareError> {
        if self.ended {
            return Ok(None);
        }
        if self.pending.is_none() {
            let (sender, receiver) = oneshot::channel();
            self.commands
                .unbounded_send(Command::Read(sender))
                .map_err(|_| MiddlewareError::InvalidState)?;
            self.pending = Some(receiver);
        }
        let result = self
            .pending
            .as_mut()
            .ok_or(MiddlewareError::InvalidState)?
            .await
            .map_err(|_| MiddlewareError::InvalidState);
        self.pending = None;
        match result {
            Ok(Ok(Some(event))) => Ok(Some(event)),
            Ok(Ok(None)) => {
                self.ended = true;
                Ok(None)
            }
            Ok(Err(error)) | Err(error) => {
                self.ended = true;
                Err(error)
            }
        }
    }
    pub fn into_events(self) -> AttemptEvents {
        Box::pin(futures::stream::unfold(self, |mut source| async move {
            match source.next_event().await {
                Ok(Some(event)) => Some((Ok(event), source)),
                Ok(None) => None,
                Err(error) => Some((Err(error), source)),
            }
        }))
    }
}

pub(super) enum Command {
    Next(NextRequest),
    Read(oneshot::Sender<Result<Option<ProviderEvent>, MiddlewareError>>),
}
pub(super) struct NextRequest {
    pub(super) operation: Operation,
    reply: oneshot::Sender<Result<AttemptSource, MiddlewareError>>,
    commands: mpsc::UnboundedSender<Command>,
}
impl NextRequest {
    pub(super) fn accept(self, metadata: ProviderCallMetadata) {
        let _ = self.reply.send(Ok(AttemptSource {
            metadata,
            commands: self.commands,
            pending: None,
            ended: false,
        }));
    }
}

enum State {
    Calling(BoxFuture<'static, Result<AttemptEvents, MiddlewareError>>),
    Output(AttemptEvents),
    Closed,
}
pub(super) enum Step {
    Command(Command),
    Output(ProviderEvent),
    Error(MiddlewareError),
    End,
}
pub(super) struct Invocation {
    state: State,
    commands: mpsc::UnboundedReceiver<Command>,
    pub(super) source_complete: bool,
    pub(super) original_error: Option<crate::error::ProviderError>,
}
impl Invocation {
    pub(super) fn new(
        middleware: Arc<dyn AttemptMiddleware>,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Self {
        let (commands, receiver) = mpsc::unbounded();
        let next = crate::middleware::compose(vec![], move |operation| {
            Box::pin(async move {
                let (sender, response) = oneshot::channel();
                commands
                    .unbounded_send(Command::Next(NextRequest {
                        operation,
                        reply: sender,
                        commands: commands.clone(),
                    }))
                    .map_err(|_| MiddlewareError::InvalidState)?;
                response.await.map_err(|_| MiddlewareError::InvalidState)?
            })
        });
        Self {
            state: State::Calling(middleware.invoke(request, context, next)),
            commands: receiver,
            source_complete: false,
            original_error: None,
        }
    }
    pub(super) async fn next_step(&mut self) -> Step {
        poll_fn(|cx| self.poll_step(cx)).await
    }
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Step> {
        loop {
            match &mut self.state {
                State::Calling(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(events)) => {
                        self.state = State::Output(events);
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = State::Closed;
                        return Poll::Ready(Step::Error(error));
                    }
                    Poll::Pending => {}
                },
                State::Output(events) => match events.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(event))) => return Poll::Ready(Step::Output(event)),
                    Poll::Ready(Some(Err(error))) => {
                        self.state = State::Closed;
                        return Poll::Ready(Step::Error(error));
                    }
                    Poll::Ready(None) => {
                        self.state = State::Closed;
                        return Poll::Ready(Step::End);
                    }
                    Poll::Pending => {}
                },
                State::Closed => return Poll::Ready(Step::End),
            }
            return match Pin::new(&mut self.commands).poll_next(cx) {
                Poll::Ready(Some(command)) => Poll::Ready(Step::Command(command)),
                // A standalone lazy body may no longer hold a next/source sender.
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
            };
        }
    }
}
