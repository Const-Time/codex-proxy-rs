//! Bounded observation delivery uses the original publication and process leases.
use crate::{RpcLimits, RpcSession};
use gateway_admin::{
    model::{AdminError, MutationActor, MutationContext, plugins::PluginExecutionScope},
    ports::plugins::{PluginObservation, PluginObserverTarget, PluginProcess},
};
use gateway_core::{
    engine::{
        middleware::service as core,
        observation::{
            RequestObservation, RequestObservationOutcome, RequestObserverPlan,
            WebSocketResponseObservation,
        },
    },
    lifecycle::CancellationToken,
    metering::{CostEstimateStatus, CostSource},
    upstream::UpstreamSendState,
};
use gateway_plugin_sdk::{
    SendState, Stage,
    call::observation::{
        Event, RequestCompleted, RequestCost, RequestCostSource, RequestCostStatus, RequestFailure,
        RequestMoney, RequestOutcome, RequestTerminal, RequestTimings, RequestUsage,
        WebSocketResponse,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const TIMEOUT: Duration = Duration::from_secs(2);
const ENVELOPE_RESERVE: usize = 16 * 1024;
struct Entry {
    process: Arc<dyn PluginProcess>,
    plugin_id: String,
    instance_id: String,
    order: i32,
    completed: Option<PluginExecutionScope>,
    websocket: Option<PluginExecutionScope>,
}
type Queues = Mutex<BTreeMap<String, mpsc::Sender<Queued>>>;
struct Queued {
    observation: PluginObservation,
    entries: Vec<usize>,
    _bytes: OwnedSemaphorePermit,
}
pub(crate) struct Plan {
    entries: Arc<[Entry]>,
    services: Option<Arc<dyn core::Factory>>,
    cancellation: CancellationToken,
    completed_slots: Arc<Semaphore>,
    worker_slots: Arc<Semaphore>,
    payload_slots: Arc<Semaphore>,
    queues: Arc<Queues>,
    capacity: usize,
    maximum_payload: usize,
}
impl Plan {
    pub(crate) fn compile(
        targets: Vec<PluginObserverTarget>,
        services: Option<Arc<dyn core::Factory>>,
        cancellation: CancellationToken,
    ) -> Result<Option<Arc<dyn RequestObserverPlan>>, AdminError> {
        let mut entries = Vec::with_capacity(targets.len());
        let mut instances = BTreeSet::new();
        for target in targets {
            if !(-10_000..=10_000).contains(&target.order)
                || (target.request_completed.is_none() && target.websocket_response.is_none())
                || target
                    .request_completed
                    .iter()
                    .chain(target.websocket_response.iter())
                    .any(|s| !s.is_valid())
            {
                return Err(AdminError::invalid("观察订阅顺序或作用范围无效"));
            }
            let identity = target
                .process
                .observer_identity()
                .ok_or_else(|| AdminError::unavailable("观察插件代次不可用"))?;
            if !instances.insert(identity.instance_id.clone()) {
                return Err(AdminError::invalid("观察实例不能重复订阅"));
            }
            entries.push(Entry {
                process: target.process,
                plugin_id: identity.plugin_id,
                instance_id: identity.instance_id,
                order: target.order,
                completed: target.request_completed,
                websocket: target.websocket_response,
            });
        }
        if entries.is_empty() {
            return Ok(None);
        }
        entries.sort_by(|a, b| {
            (a.order, &a.plugin_id, &a.instance_id).cmp(&(b.order, &b.plugin_id, &b.instance_id))
        });
        // The engine prepares its sessions with these same existing RPC limits.
        let limits = RpcLimits::default();
        let maximum_payload = limits
            .maximum_buffered_body_bytes
            .saturating_sub(ENVELOPE_RESERVE);
        Ok(Some(Arc::new(Self {
            entries: entries.into(),
            services,
            cancellation,
            completed_slots: Arc::new(Semaphore::new(
                limits.maximum_calls.min(limits.maximum_executing_calls),
            )),
            worker_slots: Arc::new(Semaphore::new(
                limits.maximum_calls.min(limits.maximum_executing_calls),
            )),
            payload_slots: Arc::new(Semaphore::new(maximum_payload)),
            queues: Arc::new(Mutex::default()),
            capacity: limits
                .maximum_calls
                .min(limits.maximum_executing_calls)
                .clamp(1, 256),
            maximum_payload,
        })))
    }
    fn queue(
        &self,
        request_id: &str,
        indexes: &[usize],
        handle: &tokio::runtime::Handle,
    ) -> Option<mpsc::Sender<Queued>> {
        let mut queues = self
            .queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sender) = queues.get(request_id) {
            return Some(sender.clone());
        }
        let slot = self.worker_slots.clone().try_acquire_owned().ok()?;
        let (sender, receiver) = mpsc::channel(self.capacity);
        queues.insert(request_id.to_owned(), sender.clone());
        let worker = Worker {
            entries: self.entries.clone(),
            services: self.services.clone(),
            cancellation: self.cancellation.clone(),
            queues: Arc::downgrade(&self.queues),
            request_id: request_id.to_owned(),
            indexes: indexes.to_vec(),
            sender: sender.downgrade(),
            _slot: slot,
        };
        handle.spawn(worker.run(receiver));
        Some(sender)
    }
}
impl RequestObserverPlan for Plan {
    fn dispatch(&self, observation: RequestObservation) {
        let request_id = observation.request_id().as_str();
        // Completion closes the request queue even if terminal delivery is full.
        self.queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(request_id);
        if self.cancellation.is_cancelled() {
            return;
        }
        let indexes: Vec<_> = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                (!observation.suppresses_plugin(&e.instance_id)
                    && e.completed.as_ref().is_some_and(|s| {
                        s.matches(observation.client_key_id(), observation.account_group_ids())
                    }))
                .then_some(i)
            })
            .collect();
        if indexes.is_empty() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let Ok(slot) = self.completed_slots.clone().try_acquire_owned() else {
            tracing::warn!(request_id, "插件终态观察容量已满");
            return;
        };
        let event = Event::RequestCompleted(Box::new(wire_observation(&observation)));
        let Some(metadata) = metadata(&event) else {
            tracing::warn!(request_id, "插件观察元数据超过上限");
            return;
        };
        let wire = PluginObservation {
            request_id: request_id.to_owned(),
            metadata,
            payload: Arc::from([]),
            call_scope: observation.extension_scope().clone(),
        };
        let entries = self.entries.clone();
        let services = self.services.clone();
        let cancellation = self.cancellation.clone();
        handle.spawn(async move {
            let _slot = slot;
            tokio::select! { biased;
                () = cancellation.cancelled() => {},
                () = deliver(&entries, indexes, wire, services, cancellation.clone()) => {},
            }
        });
    }
    fn dispatch_websocket_response(&self, observation: WebSocketResponseObservation) {
        if self.cancellation.is_cancelled() {
            return;
        }
        let request_id = observation.request_id().as_str();
        let indexes: Vec<_> = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                (!observation.suppresses_plugin(&e.instance_id)
                    && e.websocket.as_ref().is_some_and(|s| {
                        s.matches(observation.client_key_id(), observation.account_group_ids())
                    }))
                .then_some(i)
            })
            .collect();
        if indexes.is_empty() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        // Count JSON before allocating or queuing a body; raw wire wins over re-encoding.
        let length = if let Some(raw) = observation.wire().raw_json_body() {
            raw.len()
        } else if observation.wire().has_json_data() {
            let Some(length) = bounded_length(observation.wire().data(), self.maximum_payload)
            else {
                tracing::warn!(request_id, "插件观察原始帧超过上限");
                return;
            };
            length
        } else {
            0
        };
        if length > self.maximum_payload {
            tracing::warn!(request_id, "插件观察原始帧超过上限");
            return;
        }
        let Ok(bytes) = self
            .payload_slots
            .clone()
            .try_acquire_many_owned(length as u32)
        else {
            tracing::warn!(request_id, "插件观察帧缓冲容量已满");
            return;
        };
        let mut payload = Vec::with_capacity(length);
        if let Some(raw) = observation.wire().raw_json_body() {
            payload.extend_from_slice(raw);
        } else if observation.wire().has_json_data()
            && serde_json::to_writer(&mut payload, observation.wire().data()).is_err()
        {
            return;
        }
        let included = !payload.is_empty();
        let event = Event::WebSocketResponse(Box::new(WebSocketResponse {
            event_id: observation.event_id().to_owned(),
            request_id: request_id.to_owned(),
            config_revision: observation.config_revision().get(),
            operation: observation.operation().as_str().to_owned(),
            protocol: observation.wire().protocol().to_owned(),
            provider: observation.provider().as_str().to_owned(),
            attempt_index: observation.attempt_index().get(),
            sequence: observation.sequence(),
            payload_included: included,
            requested_model: observation.requested_model().map(|m| m.as_str().to_owned()),
            account_id: Some(observation.account_id().as_str().to_owned()),
            event_type: included
                .then(|| observation.wire().event_type())
                .flatten()
                .map(str::to_owned),
        }));
        let Some(metadata) = metadata(&event) else {
            tracing::warn!(request_id, "插件观察元数据超过上限");
            return;
        };
        let Some(sender) = self.queue(request_id, &indexes, &handle) else {
            tracing::warn!(request_id, "插件观察请求队列容量已满");
            return;
        };
        let queued = Queued {
            observation: PluginObservation {
                request_id: request_id.to_owned(),
                metadata,
                payload: payload.into(),
                call_scope: observation.extension_scope().clone(),
            },
            entries: indexes,
            _bytes: bytes,
        };
        if sender.try_send(queued).is_err() {
            tracing::warn!(request_id, "插件观察帧队列容量已满或已关闭");
        }
    }
}
struct Worker {
    entries: Arc<[Entry]>,
    services: Option<Arc<dyn core::Factory>>,
    cancellation: CancellationToken,
    queues: Weak<Queues>,
    request_id: String,
    indexes: Vec<usize>,
    sender: mpsc::WeakSender<Queued>,
    _slot: OwnedSemaphorePermit,
}
impl Worker {
    async fn run(self, mut receiver: mpsc::Receiver<Queued>) {
        let stopped = futures::future::join_all(self.indexes.iter().map(|i| {
            let token = self.entries[*i].process.access().cancellation();
            async move { token.cancelled().await }
        }));
        tokio::select! { biased;
            () = self.cancellation.cancelled() => {},
            _ = stopped => {},
            () = async {
                while let Some(queued) = receiver.recv().await {
                    let _bytes = queued._bytes;
                    deliver(&self.entries, queued.entries, queued.observation, self.services.clone(), self.cancellation.clone()).await;
                }
            } => {},
        }
        if let Some(queues) = self.queues.upgrade() {
            let mut queues = queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.sender.upgrade().is_some_and(|sender| {
                queues
                    .get(&self.request_id)
                    .is_some_and(|stored| stored.same_channel(&sender))
            }) {
                queues.remove(&self.request_id);
            }
        }
    }
}
async fn deliver(
    entries: &[Entry],
    indexes: Vec<usize>,
    wire: PluginObservation,
    services: Option<Arc<dyn core::Factory>>,
    cancellation: CancellationToken,
) {
    for index in indexes {
        if cancellation.is_cancelled() {
            break;
        }
        let entry = &entries[index];
        let observation = PluginObservation {
            request_id: wire.request_id.clone(),
            metadata: wire.metadata.clone(),
            payload: wire.payload.clone(),
            call_scope: wire.call_scope.clone(),
        };
        let result = tokio::select! { biased;
            () = cancellation.cancelled() => break,
            result = tokio::time::timeout(TIMEOUT, entry.process.observe(observation, services.clone(), cancellation.clone())) => result,
        };
        if !matches!(result, Ok(Ok(()))) {
            tracing::warn!(
                plugin_id = entry.plugin_id,
                instance_id = entry.instance_id,
                request_id = wire.request_id,
                "插件观察失败或超时"
            );
        }
    }
}
struct Counter {
    length: usize,
    maximum: usize,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.length = self
            .length
            .checked_add(bytes.len())
            .filter(|n| *n <= self.maximum)
            .ok_or_else(|| io::Error::other("observation buffer limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded_length(value: &impl serde::Serialize, maximum: usize) -> Option<usize> {
    let mut counter = Counter { length: 0, maximum };
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.length)
}
fn metadata(event: &Event) -> Option<serde_json::Value> {
    bounded_length(event, ENVELOPE_RESERVE)?;
    serde_json::to_value(event).ok()
}
pub(crate) async fn invoke(
    session: &RpcSession,
    registry: &Arc<crate::services::Registry>,
    outbound: &Arc<crate::outbound::Registry>,
    calls: &Arc<crate::resources::InvocationCalls>,
    observation: PluginObservation,
    factory: Option<Arc<dyn core::Factory>>,
    cancellation: CancellationToken,
) -> Result<(), AdminError> {
    let event: Event = serde_json::from_value(observation.metadata.clone())
        .map_err(|_| AdminError::invalid("观察元数据无效"))?;
    let valid_payload = match &event {
        Event::RequestCompleted(_) => observation.payload.is_empty(),
        Event::WebSocketResponse(e) => e.payload_included != observation.payload.is_empty(),
    };
    if event.request_id() != observation.request_id || !valid_payload {
        return Err(AdminError::invalid("观察关联或载荷无效"));
    }
    let mut context = session.context(
        Stage::Observation,
        session.maximum_call_timeout().min(TIMEOUT),
    );
    context.request_id = Some(observation.request_id.clone());
    let caller = registry.caller();
    let cancellation = CancellationToken::linked([cancellation, caller.cancellation()]);
    let origin = core::Origin {
        factory,
        context: core::Context {
            operation: "observer.observe",
            request_id: observation.request_id.clone(),
            call_id: context.resource_scope_id.clone(),
            parent_call_id: None,
            cancellation: cancellation.clone(),
            deadline_at: Default::default(),
            call_scope: observation.call_scope.clone(),
            dispatcher: None,
        },
    };
    let _services = registry
        .bind_origin(&context, origin)
        .map_err(|_| AdminError::unavailable("观察服务作用域不可用"))?;
    outbound
        .bind(
            &context,
            cancellation.clone(),
            Some(observation.call_scope.clone()),
        )
        .map_err(|_| AdminError::unavailable("观察网络作用域不可用"))?;
    let _outbound = outbound.guard(&context);
    let _caller = calls
        .enter(
            context.clone(),
            MutationContext {
                actor: MutationActor::Plugin {
                    instance_id: caller.instance_id().into(),
                    revision: caller.revision(),
                },
                request_id: observation.request_id,
            },
            None,
            cancellation.clone(),
            Some(observation.call_scope),
        )
        .map_err(|_| AdminError::unavailable("观察调用容量已耗尽"))?;
    let reply = tokio::select! { biased;
        () = cancellation.cancelled() => return Err(AdminError::unavailable("插件观察已取消")),
        result = session.call("observer.observe", context, observation.metadata, observation.payload.to_vec()) =>
            result.map_err(|_| AdminError::bad_gateway("插件观察失败，结果可能已生效"))?,
    };
    if !reply.payload.is_empty()
        || !(reply.result.is_null()
            || reply
                .result
                .as_object()
                .is_some_and(serde_json::Map::is_empty))
    {
        session.invalid_response(Stage::Observation);
        return Err(AdminError::bad_gateway("插件观察响应无效"));
    }
    Ok(())
}

fn wire_observation(observation: &RequestObservation) -> RequestCompleted {
    RequestCompleted {
        event_id: observation.event_id().to_owned(),
        request_id: observation.request_id().as_str().to_owned(),
        config_revision: observation.config_revision().get(),
        operation: observation.operation().as_str().to_owned(),
        client_key_id: Some(observation.client_key_id().as_str().to_owned()),
        account_id: observation
            .account_id()
            .map(|account| account.as_str().to_owned()),
        upstream_model: observation
            .upstream_model()
            .map(|model| model.as_str().to_owned()),
        response_model: observation.response_model().map(str::to_owned),
        service_tier: observation.service_tier().map(str::to_owned),
        requested_model: observation
            .requested_model()
            .map(|model| model.as_str().to_owned()),
        provider: observation
            .provider()
            .map(|provider| provider.as_str().to_owned()),
        completed_at_ms: millis_since_epoch(observation.completed_at()),
        terminal: RequestTerminal {
            outcome: wire_outcome(observation.outcome()),
            send_state: wire_send_state(observation.send_state()),
            attempt_count: observation.attempt_count(),
            client_status_code: observation.client_status_code(),
            error_code: observation
                .error_kind()
                .map(|error| error.as_str().to_owned()),
        },
        usage: {
            let usage = observation.usage();
            RequestUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cached_tokens: usage.cached_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                image_input_tokens: usage.image_input_tokens,
                image_output_tokens: usage.image_output_tokens,
                total_tokens: usage.total_tokens,
                cost: Some(wire_cost(observation)),
                timings: Some(wire_timings(observation)),
                failure: wire_failure(observation),
            }
        },
    }
}

fn wire_outcome(outcome: RequestObservationOutcome) -> RequestOutcome {
    match outcome {
        RequestObservationOutcome::Succeeded => RequestOutcome::Succeeded,
        RequestObservationOutcome::Failed => RequestOutcome::Failed,
        RequestObservationOutcome::Rejected => RequestOutcome::Rejected,
        RequestObservationOutcome::Cancelled => RequestOutcome::Cancelled,
        RequestObservationOutcome::Incomplete => RequestOutcome::Incomplete,
    }
}

fn wire_send_state(send_state: UpstreamSendState) -> SendState {
    match send_state {
        UpstreamSendState::NotSent => SendState::NotSent,
        UpstreamSendState::Sent => SendState::Sent,
        UpstreamSendState::Ambiguous => SendState::Ambiguous,
    }
}

fn wire_cost(observation: &RequestObservation) -> RequestCost {
    let cost = observation.cost();
    RequestCost {
        status: match cost.status() {
            CostEstimateStatus::Known => RequestCostStatus::Known,
            CostEstimateStatus::Unknown => RequestCostStatus::Unknown,
        },
        source: match cost.source() {
            CostSource::ProviderReported => RequestCostSource::ProviderReported,
            CostSource::Calculated => RequestCostSource::Calculated,
            CostSource::Unavailable => RequestCostSource::Unavailable,
        },
        total: cost.total().map(|total| RequestMoney {
            amount: total.amount().canonical(),
            currency: total.currency().as_str().to_owned(),
        }),
    }
}

fn wire_timings(observation: &RequestObservation) -> RequestTimings {
    let timings = observation.timings();
    RequestTimings {
        transport_decision_wait_ms: timings.transport_decision_wait_ms,
        connect_ms: timings.connect_ms,
        headers_ms: timings.headers_ms,
        first_event_ms: timings.first_event_ms,
        first_reasoning_ms: timings.first_reasoning_ms,
        first_text_ms: timings.first_text_ms,
        first_token_ms: timings.first_token_ms,
        provider_processing_ms: timings.provider_processing_ms,
        latency_ms: timings.latency_ms,
    }
}

fn wire_failure(observation: &RequestObservation) -> Option<RequestFailure> {
    (observation.outcome() != RequestObservationOutcome::Succeeded).then(|| RequestFailure {
        outcome: wire_outcome(observation.outcome()),
        send_state: wire_send_state(observation.send_state()),
        attempt_count: observation.attempt_count(),
        client_status_code: observation.client_status_code(),
        upstream_status_code: observation.upstream_status_code(),
        error_code: observation
            .error_kind()
            .map(|error| error.as_str().to_owned()),
        retry_after_ms: observation.retry_after_ms(),
    })
}

fn millis_since_epoch(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
