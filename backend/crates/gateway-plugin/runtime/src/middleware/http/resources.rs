//! HTTP 正文与响应部件共享同一调用资源池；next 与主动分派复用句柄和背压
use super::*;
use bytes::Bytes;
use futures::StreamExt as _;
use gateway_plugin_sdk::call::middleware::{MiddlewareBodyClose, MiddlewareBodyRead};
use http_body::Frame;
use http_body_util::{BodyExt as _, BodyStream, Full, StreamBody};
use std::{collections::BTreeMap, sync::Weak};

#[derive(Default)]
pub(crate) struct Resources {
    entries: Mutex<Entries>,
    error: Mutex<Option<Box<dyn std::error::Error + Send + Sync>>>,
}

#[derive(Default)]
struct Entries {
    bodies: BTreeMap<String, Arc<BodyResource>>,
    pipes: BTreeMap<String, Arc<BodyPipe>>,
    responses: BTreeMap<String, http::response::Parts>,
}
struct BodyResource {
    original: bool,
    cancellation: gateway_core::lifecycle::CancellationToken,
    state: tokio::sync::Mutex<BodyState>,
}

struct BodyPipe {
    sender: tokio::sync::Mutex<Option<tokio::sync::mpsc::Sender<Option<Frame<Bytes>>>>>,
    cancellation: gateway_core::lifecycle::CancellationToken,
}

impl Drop for BodyPipe {
    fn drop(&mut self) {
        if self.sender.get_mut().is_some() {
            self.cancellation.cancel();
        }
    }
}

struct PipeReader {
    receiver: tokio::sync::mpsc::Receiver<Option<Frame<Bytes>>>,
    cancellation: gateway_core::lifecycle::CancellationToken,
    resources: Weak<Resources>,
    handle: String,
}

impl Drop for PipeReader {
    fn drop(&mut self) {
        // 下游可以提前返回；消费端退出即归还管道名额，并唤醒仍在背压中等待的写入
        self.cancellation.cancel();
        if let Some(resources) = self.resources.upgrade() {
            resources
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pipes
                .remove(&self.handle);
        }
    }
}

struct BodyState {
    body: Option<core::Body>,
    pending: Option<Bytes>,
}

impl Resources {
    pub(super) fn take_error(&self) -> Option<Box<dyn std::error::Error + Send + Sync>> {
        self.error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
    pub(super) fn close(&self) {
        let entries = std::mem::take(
            &mut *self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        // Drop after releasing the registry lock: PipeReader may return its writer slot.
        for body in entries.bodies.values() {
            body.cancellation.cancel();
        }
        for pipe in entries.pipes.values() {
            pipe.cancellation.cancel();
        }
        drop(entries);
    }
    pub(super) fn insert_body(&self, body: core::Body) -> Result<wire::Body, PluginFault> {
        self.insert_scoped_body(body, true)
    }
    fn insert_scoped_body(
        &self,
        body: core::Body,
        original: bool,
    ) -> Result<wire::Body, PluginFault> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.bodies.len() >= 32 {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "HTTP body resource capacity reached",
            ));
        }
        let handle = uuid::Uuid::new_v4().to_string();
        entries.bodies.insert(
            handle.clone(),
            Arc::new(BodyResource {
                original,
                cancellation: Default::default(),
                state: tokio::sync::Mutex::new(BodyState {
                    body: Some(body),
                    pending: None,
                }),
            }),
        );
        Ok(wire::Body::Handle { handle })
    }

    pub(crate) async fn body(
        &self,
        source: wire::Body,
        payload: Vec<u8>,
    ) -> Result<core::Body, PluginFault> {
        match source {
            wire::Body::Empty if payload.is_empty() => Ok(core::empty_body()),
            wire::Body::Bytes => Ok(Full::new(Bytes::from(payload))
                .map_err(|never| match never {})
                .boxed_unsync()),
            wire::Body::Handle { handle } if payload.is_empty() => {
                let resource = self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .bodies
                    .get(&handle)
                    .cloned()
                    .ok_or_else(unavailable)?;
                let mut state = resource.state.lock().await;
                let body = state.body.take().ok_or_else(unavailable)?;
                self.entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .bodies
                    .remove(&handle);
                Ok(if let Some(pending) = state.pending.take() {
                    StreamBody::new(
                        futures::stream::once(async { Ok(Frame::data(pending)) })
                            .chain(BodyStream::new(body)),
                    )
                    .boxed_unsync()
                } else {
                    body
                })
            }
            _ => Err(invalid()),
        }
    }

    fn create_body(self: &Arc<Self>) -> Result<RpcReply, PluginFault> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.pipes.len() >= 16 || entries.bodies.len() >= 32 {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "HTTP upload capacity reached",
            ));
        }
        let handle = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let cancellation = gateway_core::lifecycle::CancellationToken::new();
        let reader = PipeReader {
            receiver,
            cancellation: cancellation.clone(),
            resources: Arc::downgrade(self),
            handle: handle.clone(),
        };
        let body = StreamBody::new(futures::stream::try_unfold(reader, |mut reader| async move {
            let message = tokio::select! {
                biased;
                () = reader.cancellation.cancelled() => return Err(Box::new(MiddlewareError::Fault) as Box<dyn std::error::Error + Send + Sync>),
                message = reader.receiver.recv() => message,
            };
            match message {
                Some(Some(frame)) => Ok(Some((frame, reader))),
                Some(None) => Ok(None),
                None => Err(Box::new(MiddlewareError::Fault) as _),
            }
        })).boxed_unsync();
        entries.pipes.insert(
            handle.clone(),
            Arc::new(BodyPipe {
                sender: tokio::sync::Mutex::new(Some(sender)),
                cancellation,
            }),
        );
        entries.bodies.insert(
            handle.clone(),
            Arc::new(BodyResource {
                original: true,
                cancellation: Default::default(),
                state: tokio::sync::Mutex::new(BodyState {
                    body: Some(body),
                    pending: None,
                }),
            }),
        );
        Ok(RpcReply {
            result: serde_json::to_value(wire::BodyPipe { handle }).map_err(|_| invalid())?,
            payload: Vec::new(),
        })
    }

    async fn write(
        &self,
        write: wire::BodyWrite,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        let frame = match (write.eof, write.trailers) {
            (true, None) if payload.is_empty() => None,
            (false, None) => Some(Frame::data(Bytes::from(payload))),
            (false, Some(trailers)) if payload.is_empty() => {
                Some(Frame::trailers(headers(trailers)?))
            }
            _ => return Err(invalid()),
        };
        let pipe = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pipes
            .get(&write.handle)
            .cloned()
            .ok_or_else(unavailable)?;
        let mut sender = pipe.sender.lock().await;
        let sender_ref = sender.as_ref().ok_or_else(unavailable)?;
        let result = tokio::select! {
            biased;
            () = pipe.cancellation.cancelled() => Err(unavailable()),
            result = sender_ref.send(frame) => result.map_err(|_| unavailable()),
        };
        if write.eof || result.is_err() {
            sender.take();
            self.entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pipes
                .remove(&write.handle);
        }
        result?;
        Ok(RpcReply {
            result: serde_json::json!({}),
            payload: Vec::new(),
        })
    }

    async fn read(
        &self,
        read: MiddlewareBodyRead,
        maximum_payload: usize,
    ) -> Result<RpcReply, PluginFault> {
        let maximum = usize::try_from(read.maximum_bytes)
            .map_err(|_| invalid())?
            .min(maximum_payload);
        if maximum == 0 {
            return Err(invalid());
        }
        let resource = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bodies
            .get(&read.handle)
            .cloned()
            .ok_or_else(unavailable)?;
        let read = async {
            let mut state = resource.state.lock().await;
            let frame = if let Some(bytes) = state.pending.take() {
                Some(Ok(Frame::data(bytes)))
            } else {
                state.body.as_mut().ok_or_else(unavailable)?.frame().await
            };
            if !matches!(&frame, Some(Ok(_))) {
                // EOF 和读取失败均结束读取所有权；原响应部件仍可随替换后的正文返回
                state.body.take();
                self.entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .bodies
                    .remove(&read.handle);
            }
            let frame = frame.transpose().map_err(|error| {
                let failure = match error.downcast_ref::<MiddlewareError>() {
                    Some(error) => fault(error),
                    None => PluginFault::new(ErrorCode::Fault, "HTTP body read failed"),
                };
                if resource.original {
                    *self
                        .error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                }
                failure
            })?;
            let mut result = wire::BodyRead::default();
            let payload = match frame {
                None => {
                    result.eof = true;
                    Vec::new()
                }
                Some(frame) => match frame.into_data() {
                    Ok(mut bytes) => {
                        let payload = bytes.split_to(bytes.len().min(maximum)).to_vec();
                        if !bytes.is_empty() {
                            state.pending = Some(bytes);
                        }
                        payload
                    }
                    Err(frame) => {
                        result.trailers =
                            Some(wire_headers(&frame.into_trailers().map_err(|_| invalid())?));
                        Vec::new()
                    }
                },
            };
            Ok(RpcReply {
                result: serde_json::to_value(result).map_err(|_| invalid())?,
                payload,
            })
        };
        tokio::select! {
            biased;
            () = resource.cancellation.cancelled() => Err(unavailable()),
            result = read => result,
        }
    }
    pub(crate) fn response(&self, response: core::Response) -> Result<RpcReply, PluginFault> {
        self.scoped_response(response, true)
    }
    pub(super) fn dispatched_response(
        &self,
        response: core::Response,
    ) -> Result<RpcReply, PluginFault> {
        self.scoped_response(response, false)
    }
    fn scoped_response(
        &self,
        response: core::Response,
        original: bool,
    ) -> Result<RpcReply, PluginFault> {
        let (parts, body) = response.into_parts();
        let body = self.insert_scoped_body(body, original)?;
        let wire::Body::Handle { handle: id } = &body else {
            return Err(invalid());
        };
        let id = id.clone();
        let response = wire::Response {
            status: parts.status.as_u16(),
            version: wire_version(parts.version).map_err(|_| invalid())?,
            headers: wire_headers(&parts.headers),
            body,
            response: Some(id.clone()),
            session: false,
        };
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .responses
            .insert(id, parts);
        Ok(RpcReply {
            result: serde_json::to_value(response).map_err(|_| invalid())?,
            payload: Vec::new(),
        })
    }

    pub(super) fn take_response(&self, id: &str) -> Result<http::response::Parts, MiddlewareError> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .responses
            .remove(id)
            .ok_or(MiddlewareError::InvalidState)
    }

    pub(crate) async fn call(
        self: &Arc<Self>,
        method: &str,
        params: serde_json::Value,
        payload: Vec<u8>,
        maximum_payload: usize,
    ) -> Result<RpcReply, PluginFault> {
        match method {
            wire::BODY_CREATE_METHOD if payload.is_empty() && params == serde_json::json!({}) => {
                self.create_body()
            }
            wire::BODY_WRITE_METHOD => {
                self.write(
                    serde_json::from_value(params).map_err(|_| invalid())?,
                    payload,
                )
                .await
            }
            wire::BODY_READ_METHOD if payload.is_empty() => {
                self.read(
                    serde_json::from_value(params).map_err(|_| invalid())?,
                    maximum_payload,
                )
                .await
            }
            wire::BODY_CLOSE_METHOD if payload.is_empty() => {
                let close: MiddlewareBodyClose =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                let (body, pipe) = {
                    let mut entries = self
                        .entries
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    (
                        entries.bodies.remove(&close.handle),
                        entries.pipes.remove(&close.handle),
                    )
                };
                if let Some(body) = body {
                    body.cancellation.cancel();
                }
                if let Some(pipe) = pipe {
                    pipe.cancellation.cancel();
                }
                Ok(RpcReply {
                    result: serde_json::json!({}),
                    payload: Vec::new(),
                })
            }
            _ => Err(invalid()),
        }
    }
}
