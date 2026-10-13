//! The API owns validation, the original upgrade extension and the duplex socket.
use axum::{
    body::Body,
    extract::{
        FromRequestParts,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
};
use futures::{
    FutureExt, SinkExt, StreamExt,
    future::BoxFuture,
    stream::{SplitSink, SplitStream},
};
use gateway_core::{
    engine::middleware::{MiddlewareError, http as core, websocket as ws},
    lifecycle::{CancellationToken, ConnectionGuard, ConnectionLifecycle},
};
use http_body_util::BodyExt;
use std::sync::{Arc, Mutex};

pub(super) struct Upgrade {
    pub(super) context: core::Context,
    pub(super) lifecycle: Arc<dyn ConnectionLifecycle>,
}
impl core::upgrade::WebSocketUpgrade for Upgrade {
    fn accept(
        &self,
        mut parts: http::request::Parts,
        protocols: Vec<String>,
    ) -> BoxFuture<'static, Result<(core::Response, core::upgrade::PendingSession), MiddlewareError>>
    {
        let context = self.context.clone();
        let lifecycle = self.lifecycle.clone();
        Box::pin(async move {
            let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &())
                .await
                .map_err(|_| MiddlewareError::InvalidState)?;
            let guard = lifecycle
                .try_register()
                .map_err(|_| MiddlewareError::Fault)?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let response = upgrade
                .protocols(protocols)
                .max_message_size(usize::MAX)
                .max_frame_size(usize::MAX)
                .on_upgrade(move |socket| async move {
                    let _ = sender.send(socket);
                });
            let pending=async move {
                let socket=tokio::select! {biased;()=context.cancellation.cancelled()=>return Err(Arc::new(MiddlewareError::Fault)),()=context.deadline_at.wait()=>return Err(Arc::new(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline))),socket=receiver=>socket.map_err(|_|Arc::new(MiddlewareError::Fault))?};
                let (writer,reader)=socket.split();
                let writer=Writer {socket:tokio::sync::Mutex::new(writer),context:context.clone()};
                let (incoming,received)=tokio::sync::mpsc::channel(1);
                let read=tokio::spawn(read_messages(reader,incoming,context.clone()));
                Ok(Arc::new(Session {received:tokio::sync::Mutex::new(received),writer,reader:read.abort_handle(),_guard:Mutex::new(guard),context}) as Arc<dyn ws::Session>)
            }.boxed().shared();
            Ok((
                response
                    .map(|body: Body| body.map_err(|error| Box::new(error) as _).boxed_unsync()),
                pending,
            ))
        })
    }
}
struct Session {
    received: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Result<ws::Message, MiddlewareError>>>,
    writer: Writer,
    reader: tokio::task::AbortHandle,
    _guard: Mutex<Box<dyn ConnectionGuard>>,
    context: core::Context,
}
impl ws::Session for Session {
    fn receive(&self) -> BoxFuture<'_, Result<Option<ws::Message>, MiddlewareError>> {
        Box::pin(async move {
            tokio::select! {biased;()=self.context.cancellation.cancelled()=>Err(MiddlewareError::Fault),()=self.context.deadline_at.wait()=>Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),message=async {self.received.lock().await.recv().await}=>message.transpose()}
        })
    }
}
impl ws::Sender for Session {
    fn send(&self, message: ws::Message) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        self.writer.send(message)
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
struct Writer {
    socket: tokio::sync::Mutex<SplitSink<WebSocket, Message>>,
    context: core::Context,
}
impl ws::Sender for Writer {
    fn send(&self, message: ws::Message) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            let message = from_core(message)?;
            tokio::select! {biased;()=self.context.cancellation.cancelled()=>Err(MiddlewareError::Fault),()=self.context.deadline_at.wait()=>Err(MiddlewareError::Engine(gateway_core::engine::EngineError::Deadline)),result=async {
                let mut socket=self.socket.lock().await;
                let mut guard=CancelOnDrop(Some(self.context.cancellation.clone()));
                socket.send(message).await.map_err(remote)?;
                guard.0.take();
                Ok(())
            }=>result}
        })
    }
}
struct CancelOnDrop(Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.0 {
            cancellation.cancel();
        }
    }
}
async fn read_messages(
    mut reader: SplitStream<WebSocket>,
    incoming: tokio::sync::mpsc::Sender<Result<ws::Message, MiddlewareError>>,
    context: core::Context,
) {
    loop {
        let result = tokio::select! {biased;()=context.cancellation.cancelled()=>return,()=context.deadline_at.wait()=>return,message=reader.next()=>message};
        let Some(result) = result else { return };
        let result = result.map(into_core).map_err(remote);
        let failed = result.is_err();
        tokio::select! {biased;()=context.cancellation.cancelled()=>return,()=context.deadline_at.wait()=>return,result=incoming.send(result)=>if result.is_err() {return}}
        if failed {
            return;
        }
    }
}
fn remote(error: axum::Error) -> MiddlewareError {
    MiddlewareError::Remote {
        source: Box::new(error),
        rejected: false,
    }
}
fn into_core(message: Message) -> ws::Message {
    let (kind, payload) = match message {
        Message::Text(data) => (ws::Kind::Text, data.into()),
        Message::Binary(data) => (ws::Kind::Binary, data),
        Message::Ping(data) => (ws::Kind::Ping, data),
        Message::Pong(data) => (ws::Kind::Pong, data),
        Message::Close(Some(frame)) => (
            ws::Kind::Close {
                code: Some(frame.code),
            },
            frame.reason.into(),
        ),
        Message::Close(None) => (ws::Kind::Close { code: None }, bytes::Bytes::new()),
    };
    ws::Message { kind, payload }
}
fn from_core(message: ws::Message) -> Result<Message, MiddlewareError> {
    Ok(match message.kind {
        ws::Kind::Text => Message::Text(
            message
                .payload
                .try_into()
                .map_err(|_| MiddlewareError::InvalidState)?,
        ),
        ws::Kind::Binary => Message::Binary(message.payload),
        ws::Kind::Ping if message.payload.len() <= 125 => Message::Ping(message.payload),
        ws::Kind::Pong if message.payload.len() <= 125 => Message::Pong(message.payload),
        ws::Kind::Close { code: Some(code) } if message.payload.len() <= 123 => {
            Message::Close(Some(CloseFrame {
                code,
                reason: message
                    .payload
                    .try_into()
                    .map_err(|_| MiddlewareError::InvalidState)?,
            }))
        }
        ws::Kind::Close { code: None } if message.payload.is_empty() => Message::Close(None),
        _ => return Err(MiddlewareError::InvalidState),
    })
}
