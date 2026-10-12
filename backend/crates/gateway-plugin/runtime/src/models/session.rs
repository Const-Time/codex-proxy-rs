//! A callback may disappear while Core commits. Retain the worker and settlement owner.
use super::{capacity, encode, engine_fault, gateway_fault, request::Request};
use crate::RpcReply;
use gateway_core::{
    engine::{
        CommitRequirement,
        execution::{ExecutionService, ExecutionSession, PreparedRootExecution},
    },
    lifecycle::CancellationToken,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::host::*};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, oneshot};

pub(super) struct CancelOnDrop(Option<CancellationToken>);
impl CancelOnDrop {
    pub(super) fn new(stop: CancellationToken) -> Self {
        Self(Some(stop))
    }
    pub(super) fn disarm(mut self) {
        self.0.take();
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            stop.cancel();
        }
    }
}
pub(super) struct Session {
    pub request_id: String,
    session: Option<Box<dyn ExecutionSession>>,
    stop: CancellationToken,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(session) = self.session.take()
            && !session.is_finalized()
        {
            session.cancel();
            drop(tokio::spawn(session.detach_finalize()));
        }
    }
}
pub(super) async fn open(
    service: Arc<dyn ExecutionService>,
    prepared: PreparedRootExecution,
    request: Request,
    stop: CancellationToken,
    permit: OwnedSemaphorePermit,
) -> Result<Session, PluginFault> {
    let (sender, receiver) = oneshot::channel();
    drop(tokio::spawn(async move {
        if sender.is_closed() {
            return;
        }
        let started = match request.endpoint_provider {
            Some(provider) => {
                service
                    .start_prepared_provider_endpoint(
                        prepared,
                        provider,
                        request.request.operation,
                        request.request.metadata,
                    )
                    .await
            }
            None => service.start_prepared(prepared, request.request).await,
        };
        let result = started
            .map(|started| Session {
                request_id: started.request_id.as_str().into(),
                session: Some(started.session),
                stop,
                _permit: permit,
            })
            .map_err(gateway_fault);
        let _ = sender.send(result);
    }));
    receiver
        .await
        .map_err(|_| PluginFault::new(ErrorCode::Fault, "model start worker stopped"))?
}
fn batch(events: Vec<gateway_core::event::ProviderEvent>) -> Result<(u32, Vec<u8>), PluginFault> {
    let count = u32::try_from(events.len()).map_err(|_| capacity())?;
    let events = events
        .iter()
        .map(crate::middleware::facts::snapshot)
        .collect::<Result<Vec<_>, _>>()?;
    let payload = ModelEventBatch { events }
        .encode()
        .map_err(|_| capacity())?;
    Ok((count, payload))
}
pub(super) async fn execute(mut owner: Session) -> Result<RpcReply, PluginFault> {
    // This worker owns the complete unary commit, even if its receiver is cancelled.
    let session = owner.session.as_mut().ok_or_else(super::invalid)?;
    let result = async {
        let events = session.collect_uncommitted().await.map_err(engine_fault)?;
        let (events, payload) = batch(events)?;
        session
            .commit_downstream(Some(200))
            .await
            .map_err(engine_fault)?;
        if !session.is_finalized() {
            return Err(PluginFault::new(
                ErrorCode::Fault,
                "model execution did not finalize",
            ));
        }
        let mut reply = encode(ModelExecuteResult {
            request_id: owner.request_id.clone(),
            events,
        })?;
        reply.payload = payload;
        Ok(reply)
    }
    .await;
    result.map_err(|fault| observed_fault(fault, session.send_state()))
}

struct Pending {
    events: u32,
    payload: Vec<u8>,
    commit: CommitRequirement,
}
struct ReadState {
    owner: Session,
    pending: Option<Pending>,
}
type ReadResult = (ReadState, Result<RpcReply, PluginFault>);
struct State {
    current: Option<ReadState>,
    worker: Option<tokio::task::JoinHandle<ReadResult>>,
}
pub(super) struct ModelStream {
    state: Arc<Mutex<State>>,
    stop: CancellationToken,
    closed: AtomicBool,
}
impl ModelStream {
    pub(super) fn new(owner: Session) -> Self {
        Self {
            stop: owner.stop.clone(),
            state: Arc::new(Mutex::new(State {
                current: Some(ReadState {
                    owner,
                    pending: None,
                }),
                worker: None,
            })),
            closed: AtomicBool::new(false),
        }
    }
    pub(super) async fn read(&self, maximum_bytes: u32) -> Result<RpcReply, PluginFault> {
        if maximum_bytes == 0 || maximum_bytes as usize > super::MAXIMUM_BYTES {
            return Err(super::invalid());
        }
        if self.closed.load(Ordering::Acquire) || self.stop.is_cancelled() {
            return Err(PluginFault::new(
                ErrorCode::Cancelled,
                "model stream is closed",
            ));
        }
        let mut state = self.state.try_lock().map_err(|_| capacity())?;
        if state.worker.is_none() {
            let current = state.current.take().ok_or_else(super::invalid)?;
            state.worker = Some(tokio::spawn(read(current, maximum_bytes as usize)));
        }
        // Dropping this await retains the JoinHandle and its in-flight Core commit.
        let result = state.worker.as_mut().ok_or_else(super::invalid)?.await;
        state.worker = None;
        let (current, result) = result
            .map_err(|_| PluginFault::new(ErrorCode::Fault, "model stream worker stopped"))?;
        state.current = Some(current);
        if self.stop.is_cancelled() {
            return Err(PluginFault::new(
                ErrorCode::Cancelled,
                "model stream is closed",
            ));
        }
        result
    }
    pub(super) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.stop.cancel();
        let state = self.state.clone();
        drop(tokio::spawn(async move {
            let mut state = state.lock().await;
            if let Some(worker) = state.worker.take()
                && let Ok((current, _)) = worker.await
            {
                state.current = Some(current);
            }
            // Session Drop transfers unfinished cleanup to the existing Core owner.
            state.current.take();
        }));
    }
}
impl Drop for ModelStream {
    fn drop(&mut self) {
        self.close();
    }
}
async fn read(mut state: ReadState, maximum: usize) -> ReadResult {
    let result = read_inner(&mut state, maximum).await;
    let result = result.map_err(|fault| {
        state
            .owner
            .session
            .as_ref()
            .map_or(fault.clone(), |session| {
                observed_fault(fault, session.send_state())
            })
    });
    (state, result)
}
fn observed_fault(
    mut fault: PluginFault,
    observed: gateway_core::upstream::UpstreamSendState,
) -> PluginFault {
    fault.send_state = match observed {
        gateway_core::upstream::UpstreamSendState::NotSent => fault.send_state,
        gateway_core::upstream::UpstreamSendState::Sent
            if fault.send_state != gateway_plugin_sdk::SendState::Ambiguous =>
        {
            gateway_plugin_sdk::SendState::Sent
        }
        _ => gateway_plugin_sdk::SendState::Ambiguous,
    };
    fault
}
async fn read_inner(state: &mut ReadState, maximum: usize) -> Result<RpcReply, PluginFault> {
    let session = state.owner.session.as_mut().ok_or_else(super::invalid)?;
    if state.pending.is_none() {
        let Some(event) = session.next_event().await.map_err(engine_fault)? else {
            if !session.is_finalized() {
                return Err(PluginFault::new(
                    ErrorCode::Fault,
                    "model stream did not finalize",
                ));
            }
            return encode(ModelStreamReadResult {
                events: 0,
                end: true,
            });
        };
        let commit = event.commit_requirement();
        let (events, payload) = batch(event.into_provider_events())?;
        state.pending = Some(Pending {
            events,
            payload,
            commit,
        });
    }
    let pending = state.pending.as_ref().ok_or_else(super::invalid)?;
    if pending.payload.len() > maximum {
        return Err(capacity());
    }
    if pending.commit == CommitRequirement::CommitBeforeDelivery {
        session
            .commit_downstream(Some(200))
            .await
            .map_err(engine_fault)?;
    }
    let pending = state.pending.take().ok_or_else(super::invalid)?;
    let mut result = encode(ModelStreamReadResult {
        events: pending.events,
        end: session.is_finalized(),
    })?;
    result.payload = pending.payload;
    Ok(result)
}
