mod attempt;
mod body;
mod delivery;
mod fixture;
mod raw;
mod request;
mod websocket;

use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_api::openai::middleware::{deliver_middleware_response, execution_middleware_response};
use gateway_core::{
    engine::{
        CommitRequirement, CoordinatedEvent, EngineError, execution::ExecutionSession,
        middleware::*,
    },
    event::{ProtocolWireEvent, ProviderEvent},
};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct Trace {
    commits: Mutex<Vec<u16>>,
    cancelled: AtomicBool,
    finalized: AtomicBool,
    statuses: Mutex<Vec<u16>>,
}
struct Session {
    trace: Arc<Trace>,
    events: VecDeque<CoordinatedEvent>,
    collected: Option<Vec<ProviderEvent>>,
    fail_commit: bool,
}
impl Session {
    fn json(trace: Arc<Trace>) -> Self {
        Self {
            trace,
            events: VecDeque::new(),
            collected: Some(vec![event("response.created"), event("response.completed")]),
            fail_commit: false,
        }
    }
    fn sse(trace: Arc<Trace>, batches: Vec<Vec<ProviderEvent>>) -> Self {
        Self {
            trace,
            events: batches
                .into_iter()
                .enumerate()
                .map(|(i, events)| {
                    CoordinatedEvent::try_batch(
                        events,
                        if i == 0 {
                            CommitRequirement::CommitBeforeDelivery
                        } else {
                            CommitRequirement::AlreadyCommitted
                        },
                    )
                    .unwrap()
                })
                .collect(),
            collected: None,
            fail_commit: false,
        }
    }
}
impl ExecutionSession for Session {
    fn response_headers(&self) -> &[gateway_core::event::ProviderResponseHeader] {
        &[]
    }
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            if self.trace.cancelled.load(Ordering::SeqCst) {
                self.trace.finalized.store(true, Ordering::SeqCst);
                return Err(EngineError::Cancelled);
            }
            let next = self.events.pop_front();
            if next.is_none() {
                self.trace.finalized.store(true, Ordering::SeqCst);
            }
            Ok(next)
        })
    }
    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        Box::pin(async move { Ok(self.collected.take().unwrap()) })
    }
    fn commit_downstream(&mut self, status: Option<u16>) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            if self.fail_commit {
                return Err(EngineError::ProviderMetadataMismatch);
            }
            self.trace.commits.lock().unwrap().push(status.unwrap());
            if self.events.is_empty() {
                self.trace.finalized.store(true, Ordering::SeqCst);
            }
            Ok(())
        })
    }
    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            self.trace.statuses.lock().unwrap().push(status);
            Ok(())
        })
    }
    fn is_finalized(&self) -> bool {
        self.trace.finalized.load(Ordering::SeqCst)
    }
    fn cancel(&self) {
        self.trace.cancelled.store(true, Ordering::SeqCst);
    }
    fn detach_finalize(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.trace.finalized.store(true, Ordering::SeqCst);
        })
    }
}
fn event(kind: &str) -> ProviderEvent {
    ProviderEvent::wire(ProtocolWireEvent::json("openai", Some(kind.to_owned()), json!({"type":kind,"response":{"id":"resp_original","status":if kind == "response.completed" { "completed" } else { "in_progress" },"output":[]}})).unwrap())
}
#[derive(Clone, Copy)]
enum Rewrite {
    Json,
    Invalid,
    Empty,
    InvalidSecond,
    MissingTerminal,
    ExpandInvalidTerminal,
    Pass,
    MessageJson,
    MessageIdentity,
}
struct Rewriter {
    inner: Box<dyn MiddlewareBody>,
    mode: Rewrite,
    seen: usize,
    pending: Option<MiddlewareFrame>,
}
impl MiddlewareBody for Rewriter {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            if let Some(frame) = self.pending.take() {
                return Ok(Some(frame));
            }
            let Some(frame) = self.inner.next_frame().await? else {
                return Ok(None);
            };
            self.seen += 1;
            let (bytes, framing, terminal, envelope) = frame.into_parts();
            if terminal && matches!(self.mode, Rewrite::ExpandInvalidTerminal) {
                let mut last =
                    MiddlewareFrame::new(Bytes::from_static(b"invalid final event"), framing, true);
                if let Some(envelope) = envelope {
                    last = last.with_envelope(envelope);
                }
                self.pending = Some(last);
                let bytes = if framing == MiddlewareFraming::JsonDocument {
                    Bytes::from_static(
                        br#"{"type":"response.output_text.delta","delta":"must-not-leak"}"#,
                    )
                } else {
                    Bytes::from_static(b"event: response.output_text.delta\ndata: {\"delta\":\"must-not-leak\"}\n\n")
                };
                return Ok(Some(
                    MiddlewareFrame::new(bytes, framing, false).with_transformed(true),
                ));
            }
            let bytes = match self.mode {
                Rewrite::MessageJson | Rewrite::MessageIdentity if !bytes.is_empty() => {
                    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    if matches!(self.mode, Rewrite::MessageIdentity) {
                        value["response"]["id"] = json!("resp_forged");
                    } else {
                        value["plugin"] = json!(true);
                    }
                    Bytes::from(value.to_string())
                }
                Rewrite::Json => Bytes::from_static(br#"{"id":"resp_rewritten","output":[]}"#),
                Rewrite::Invalid => Bytes::from_static(b"not a document"),
                Rewrite::InvalidSecond if self.seen == 2 => Bytes::from_static(b"not an SSE event"),
                Rewrite::Empty => Bytes::new(),
                Rewrite::MissingTerminal if terminal => Bytes::from_static(
                    b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
                ),
                _ => bytes,
            };
            let mut frame = MiddlewareFrame::new(bytes, framing, terminal).with_transformed(true);
            if let Some(envelope) = envelope {
                frame = frame.with_envelope(envelope);
            }
            Ok(Some(frame))
        })
    }
    fn commit_downstream(
        &mut self,
        status: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        self.inner.commit_downstream(status)
    }
    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        self.inner.record_client_status(status)
    }
    fn is_finalized(&self) -> bool {
        self.inner.is_finalized()
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        self.inner.close()
    }
}
fn rewrite(response: MiddlewareResponse, mode: Rewrite) -> MiddlewareResponse {
    let (protocol, status, headers, body, _) = response.into_parts();
    MiddlewareResponse::new(
        protocol,
        status,
        headers,
        Box::new(Rewriter {
            inner: body,
            mode,
            seen: 0,
            pending: None,
        }),
    )
}
async fn bytes(response: axum::response::Response) -> Bytes {
    axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap()
}
