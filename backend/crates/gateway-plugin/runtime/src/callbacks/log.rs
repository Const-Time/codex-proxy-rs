//! Bounded plugin diagnostics use the original redaction and tracing owners.
use crate::RpcReply;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use gateway_core::diagnostics::{body_fingerprint, diagnostic_json};
use gateway_plugin_sdk::{
    CallContext, ErrorCode, Manifest, PluginFault,
    call::host::{LogLevel, LogRequest, LogResult},
};
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};

const WINDOW: Duration = Duration::from_secs(1);
const MAXIMUM_EVENTS: u32 = 64;
const MAXIMUM_INPUT_BYTES: usize = 8 * 1024;
const MAXIMUM_FIELDS: usize = 32;
type Drain = Shared<BoxFuture<'static, ()>>;

/// Tracks only lifecycle ownership; the original tracing backend owns output.
pub(crate) struct LogRegistry {
    slots: Arc<Semaphore>,
    state: Mutex<RegistryState>,
}
#[derive(Default)]
struct RegistryState {
    live: Vec<Weak<PluginLog>>,
    draining: Option<Drain>,
}
impl Default for LogRegistry {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(8)),
            state: Mutex::default(),
        }
    }
}
impl LogRegistry {
    pub(crate) fn register(
        &self,
        manifest: &Manifest,
    ) -> Result<Arc<PluginLog>, gateway_admin::model::AdminError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.draining.is_some() {
            return Err(gateway_admin::model::AdminError::unavailable(
                "插件运行时已关闭",
            ));
        }
        let log = Arc::new(
            PluginLog::new(manifest, self.slots.clone())
                .map_err(|_| gateway_admin::model::AdminError::invalid("插件日志身份无效"))?,
        );
        state.live.retain(|log| log.strong_count() != 0);
        state.live.push(Arc::downgrade(&log));
        Ok(log)
    }
    pub(crate) fn drain(&self) -> Drain {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.draining.is_none() {
            self.slots.close();
            let drains: Vec<_> = std::mem::take(&mut state.live)
                .into_iter()
                .filter_map(|log| log.upgrade())
                .map(|log| log.drain())
                .collect();
            state.draining = Some(
                async move {
                    let _ = futures::future::join_all(drains).await;
                }
                .boxed()
                .shared(),
            );
        }
        state.draining.as_ref().unwrap().clone()
    }
}

#[derive(Default)]
struct Writes {
    tasks: Vec<JoinHandle<()>>,
    draining: Option<Drain>,
}

pub(crate) struct PluginLog {
    plugin_id: String,
    plugin_version: String,
    budget: Mutex<Budget>,
    slots: Arc<Semaphore>,
    writes: Mutex<Writes>,
}
struct Budget {
    started: Instant,
    recorded: u32,
    suppressed: u64,
}
impl PluginLog {
    fn new(
        manifest: &Manifest,
        slots: Arc<Semaphore>,
    ) -> Result<Self, gateway_plugin_sdk::ManifestError> {
        Ok(Self {
            plugin_id: manifest.plugin_id()?,
            plugin_version: manifest.version.to_string(),
            budget: Mutex::new(Budget {
                started: Instant::now(),
                recorded: 0,
                suppressed: 0,
            }),
            slots,
            writes: Mutex::default(),
        })
    }
    pub(crate) fn record(
        self: &Arc<Self>,
        context: &CallContext,
        params: serde_json::Value,
        payload: &[u8],
    ) -> Result<RpcReply, PluginFault> {
        if !payload.is_empty()
            || serde_json::to_vec(&params).map_err(|_| invalid())?.len() > MAXIMUM_INPUT_BYTES
        {
            return Err(invalid());
        }
        let request: LogRequest = serde_json::from_value(params).map_err(|_| invalid())?;
        if request.event.is_empty()
            || request.event.len() > 64
            || !request
                .event
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            || request.fields.len() > MAXIMUM_FIELDS
        {
            return Err(invalid());
        }
        // Stop holds this same lock while sealing admission and taking ownership
        // of every accepted write, including writes whose RPC waiter has exited.
        let mut writes = self
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if writes.draining.is_some() {
            return Err(PluginFault::new(
                ErrorCode::Cancelled,
                "plugin logging ended",
            ));
        }
        writes.tasks.retain(|task| !task.is_finished());
        let (suppressed, slot) = {
            let mut budget = self
                .budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if budget.started.elapsed() >= WINDOW {
                budget.started = Instant::now();
                budget.recorded = 0;
            }
            if budget.recorded == MAXIMUM_EVENTS {
                budget.suppressed = budget.suppressed.saturating_add(1);
                return reply(false);
            }
            let Ok(slot) = self.slots.clone().try_acquire_owned() else {
                budget.suppressed = budget.suppressed.saturating_add(1);
                return reply(false);
            };
            budget.recorded += 1;
            (std::mem::take(&mut budget.suppressed), slot)
        };
        let fields = diagnostic_json(&serde_json::to_value(request.fields).map_err(|_| invalid())?);
        let event = body_fingerprint(request.event.as_bytes());
        let plugin_id = self.plugin_id.clone();
        let plugin_version = self.plugin_version.clone();
        let context = context.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let owner = self.clone();
        writes.tasks.push(tokio::task::spawn_blocking(move || {
            // Failed or cancelled preparation has no LivePlugin. Keep its logger
            // discoverable by Runtime shutdown until the accepted write finishes.
            let _owner = owner;
            let _slot = slot;
            tracing::dispatcher::with_default(&dispatch, || {
                macro_rules! record {
                    ($level:expr) => {
                        tracing::event!(target: "gateway_plugin", $level,
                            plugin_id = %plugin_id,
                            plugin_version = %plugin_version,
                            instance_id = %context.instance_id,
                            generation = context.generation,
                            incarnation = %context.incarnation,
                            call_id = context.call_id,
                            request_id = context.request_id.as_deref(),
                            attempt_id = context.attempt_id.as_deref(),
                            stage = ?context.stage,
                            event = %event,
                            fields = %fields,
                            suppressed_logs = suppressed,
                            "插件事件")
                    };
                }
                match request.level {
                    LogLevel::Debug => record!(tracing::Level::DEBUG),
                    LogLevel::Info => record!(tracing::Level::INFO),
                    LogLevel::Warn => record!(tracing::Level::WARN),
                    LogLevel::Error => record!(tracing::Level::ERROR),
                }
            });
        }));
        reply(true)
    }
    pub(crate) fn drain(&self) -> Drain {
        let mut writes = self
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if writes.draining.is_none() {
            let tasks = std::mem::take(&mut writes.tasks);
            // Store the same wait in the lifecycle owner, so cancelled/repeated
            // callers cannot detach accepted writes or report completion early.
            writes.draining = Some(
                async move {
                    // Telemetry backend failure does not change business results.
                    let _ = futures::future::join_all(tasks).await;
                }
                .boxed()
                .shared(),
            );
        }
        writes.draining.as_ref().unwrap().clone()
    }
}
fn reply(recorded: bool) -> Result<RpcReply, PluginFault> {
    Ok(RpcReply {
        result: serde_json::to_value(LogResult { recorded }).map_err(|_| invalid())?,
        payload: vec![],
    })
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid plugin log input")
}
