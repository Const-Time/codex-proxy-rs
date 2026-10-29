//! 下游客户端 WebSocket 的单 owner pump 与有界收发边界。

use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::extract::ws::{CloseFrame, Message, close_code};
mod transport;
use futures::{Sink, SinkExt, Stream, StreamExt};
use gateway_core::lifecycle::CancellationToken;
use thiserror::Error;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
pub(crate) use transport::{Socket, Upgrade};

const OUTBOUND_COMMAND_BUFFER: usize = 32;
const INBOUND_EVENT_BUFFER: usize = 32;
const DOWNSTREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CONNECTION_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const DOWNSTREAM_PING_INTERVAL: Duration = Duration::from_secs(25);

/// WebSocket pump 的写入与生命周期预算。
#[derive(Clone, Copy)]
pub struct ConnectionConfig {
    write_timeout: Duration,
    max_age: Duration,
}

impl ConnectionConfig {
    /// 生产环境固定预算。
    pub const PRODUCTION: Self = Self {
        write_timeout: DOWNSTREAM_WRITE_TIMEOUT,
        max_age: CONNECTION_MAX_AGE,
    };
}

/// 业务层可观察的客户端输入；Ping/Pong 始终由 pump 消费。
pub enum ConnectionEvent {
    Text(String),
    Binary,
    Expired,
    Exited(PumpExitReason),
}

/// 接收队列与活动期间暂存的请求共用容量；移动帧不释放它占用的名额。
pub(super) struct PendingConnectionEvent {
    pub(super) event: ConnectionEvent,
    _permit: Option<OwnedSemaphorePermit>,
}

impl PendingConnectionEvent {
    fn exited(reason: PumpExitReason) -> Self {
        Self {
            event: ConnectionEvent::Exited(reason),
            _permit: None,
        }
    }
}

/// 下游写入阶段；名称刻意使用 write，而不是暗示客户端已消费的 delivery。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramePhase {
    Metadata,
    First,
    Data,
    Terminal,
    FirstAndTerminal,
    Error,
    ConnectionLimit,
    Close,
}

impl FramePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::First => "first",
            Self::Data => "data",
            Self::Terminal => "terminal",
            Self::FirstAndTerminal => "first_and_terminal",
            Self::Error => "error",
            Self::ConnectionLimit => "connection_limit",
            Self::Close => "close",
        }
    }

    const fn is_milestone(self) -> bool {
        matches!(
            self,
            Self::First
                | Self::Terminal
                | Self::FirstAndTerminal
                | Self::Error
                | Self::ConnectionLimit
                | Self::Close
        )
    }
}

/// 一次下游写入的请求归属与协议阶段。
#[derive(Clone)]
pub struct WriteContext {
    request_id: Option<Arc<str>>,
    phase: FramePhase,
}

impl WriteContext {
    /// 创建归属于某个请求的写入上下文。
    #[must_use]
    pub fn request(request_id: &Arc<str>, phase: FramePhase) -> Self {
        Self {
            request_id: Some(Arc::clone(request_id)),
            phase,
        }
    }

    /// 创建连接级写入上下文。
    #[must_use]
    pub const fn connection(phase: FramePhase) -> Self {
        Self {
            request_id: None,
            phase,
        }
    }
}

/// WebSocket pump 停止的稳定原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpExitReason {
    PongTimeout,
    ClientClose,
    PeerEof,
    ReadError,
    WriteError,
    WriteTimeout,
    LifecycleShutdown,
    ConnectionMaxAge,
    CoordinatorDropped,
    InboundOverload,
    ServerClose,
    PumpStopped,
}

impl PumpExitReason {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::PongTimeout => "pong_timeout",
            Self::ClientClose => "client_close",
            Self::PeerEof => "peer_eof",
            Self::ReadError => "read_error",
            Self::WriteError => "write_error",
            Self::WriteTimeout => "write_timeout",
            Self::LifecycleShutdown => "lifecycle_shutdown",
            Self::ConnectionMaxAge => "connection_max_age",
            Self::CoordinatorDropped => "coordinator_dropped",
            Self::InboundOverload => "inbound_overload",
            Self::ServerClose => "server_close",
            Self::PumpStopped => "pump_stopped",
        }
    }
}

/// 下游 WebSocket 写入失败。
#[derive(Debug, Error)]
pub enum ConnectionWriteError {
    #[error("downstream WebSocket pump is closed")]
    Closed,
    #[error("downstream WebSocket write timed out after {timeout:?}")]
    Timeout { timeout: Duration },
    #[error("downstream WebSocket transport write failed: {message}")]
    Transport { message: String },
}

struct ConnectionCommand {
    message: Message,
    context: WriteContext,
    acknowledged: oneshot::Sender<Result<(), ConnectionWriteError>>,
}

#[derive(Default)]
struct ConnectionStats {
    command_queue_high_water: AtomicUsize,
    command_backpressure_count: AtomicU64,
    ping_received_count: AtomicU64,
    pong_written_count: AtomicU64,
    ping_written_count: AtomicU64,
    pong_received_count: AtomicU64,
    last_read_ms: AtomicU64,
    last_write_ms: AtomicU64,
}

impl ConnectionStats {
    fn record_read(&self, opened_at: Instant) {
        self.last_read_ms.store(
            u64::try_from(opened_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn record_write(&self, opened_at: Instant) {
        self.last_write_ms.store(
            u64::try_from(opened_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn observe_command_queue(&self, sender: &mpsc::Sender<ConnectionCommand>) {
        let queued = OUTBOUND_COMMAND_BUFFER
            .saturating_sub(sender.capacity())
            .saturating_add(1)
            .min(OUTBOUND_COMMAND_BUFFER);
        self.command_queue_high_water
            .fetch_max(queued, Ordering::Relaxed);
        if sender.capacity() == 0 {
            self.command_backpressure_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 协调层持有的单 owner WebSocket 连接句柄。
pub struct ResponsesWebSocketConnection {
    cancellation: CancellationToken,
    connection_id: Arc<str>,
    opened_at: Instant,
    expired: Arc<AtomicBool>,
    commands: Option<mpsc::Sender<ConnectionCommand>>,
    incoming: mpsc::Receiver<PendingConnectionEvent>,
    deferred: VecDeque<PendingConnectionEvent>,
    exited: oneshot::Receiver<PumpExitReason>,
    pump_task: Option<JoinHandle<()>>,
    stats: Arc<ConnectionStats>,
    config: ConnectionConfig,
    exit_reason: Option<PumpExitReason>,
}

impl ResponsesWebSocketConnection {
    pub(super) fn new(
        socket: Socket,
        connection_id: String,
        cancellation: CancellationToken,
        headers: Arc<[gateway_core::engine::middleware::MiddlewareHeader]>,
    ) -> Result<Self, gateway_core::engine::middleware::MiddlewareError> {
        let activity = socket.activity.subscribe();
        if let Some(origin) = crate::middleware::current() {
            let plan = origin
                .registry
                .freeze_websocket(
                    origin.context.call_scope.excluded_instances(),
                    cancellation.clone(),
                )
                .map_err(|_| gateway_core::engine::middleware::MiddlewareError::Fault)?;
            if !plan.is_empty() {
                let cancellation = plan.cancellation();
                let transport = crate::middleware::websocket::Transport::new(
                    socket,
                    origin.context,
                    connection_id.clone(),
                    headers,
                    plan,
                );
                return Ok(spawn_observed_connection(
                    transport,
                    Arc::<str>::from(connection_id),
                    cancellation,
                    ConnectionConfig::PRODUCTION,
                    activity,
                ));
            }
        }
        Ok(spawn_observed_connection(
            socket,
            Arc::<str>::from(connection_id),
            cancellation,
            ConnectionConfig::PRODUCTION,
            activity,
        ))
    }

    pub(in crate::openai) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(in crate::openai) fn id(&self) -> &str {
        &self.connection_id
    }

    /// 返回连接是否已经达到生命周期上限。
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expired.load(Ordering::Acquire)
    }

    pub(super) fn age(&self) -> Duration {
        self.opened_at.elapsed()
    }

    /// 等待下一个需要业务层处理的客户端事件。
    pub async fn next_event(&mut self) -> Option<ConnectionEvent> {
        if self.exit_reason.is_none() {
            match self.exited.try_recv() {
                Ok(reason) => {
                    self.exit_reason = Some(reason);
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.exit_reason = Some(PumpExitReason::PumpStopped);
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }
        if let Some(reason) = self.exit_reason {
            return Some(ConnectionEvent::Exited(reason));
        }
        if let Some(event) = self.deferred.pop_front() {
            return Some(event.event);
        }
        self.next_active_event().await.map(|event| event.event)
    }

    /// 活动响应期间只消费新到的帧，已排队的下一轮请求继续保持串行。
    pub(super) async fn next_active_event(&mut self) -> Option<PendingConnectionEvent> {
        if let Some(reason) = self.exit_reason {
            return Some(PendingConnectionEvent::exited(reason));
        }
        let event = tokio::select! {
            biased;
            reason = &mut self.exited => {
                Some(PendingConnectionEvent::exited(reason.unwrap_or(PumpExitReason::PumpStopped)))
            }
            event = self.incoming.recv() => event,
        };
        if let Some(PendingConnectionEvent {
            event: ConnectionEvent::Exited(reason),
            ..
        }) = event.as_ref()
        {
            self.exit_reason.get_or_insert(*reason);
        } else if event.is_none() {
            self.exit_reason.get_or_insert(PumpExitReason::PumpStopped);
        }
        event
    }

    pub(super) fn defer(&mut self, event: PendingConnectionEvent) {
        self.deferred.push_back(event);
    }

    /// 等待连接退出，不消费留给后续串行请求的业务帧。
    pub async fn wait_for_exit(&mut self) -> PumpExitReason {
        if let Some(reason) = self.exit_reason {
            return reason;
        }
        let reason = (&mut self.exited)
            .await
            .unwrap_or(PumpExitReason::PumpStopped);
        self.exit_reason = Some(reason);
        reason
    }

    /// 串行写入文本帧，并等待 pump 确认 transport 写入结果。
    ///
    /// # Errors
    ///
    /// pump 已关闭、写入超时或 transport 失败时返回稳定错误。
    pub async fn send_text(
        &mut self,
        payload: impl Into<axum::extract::ws::Utf8Bytes>,
        context: WriteContext,
    ) -> Result<(), ConnectionWriteError> {
        self.send(Message::Text(payload.into()), context).await
    }

    pub(super) async fn close_policy(
        &mut self,
        reason: &'static str,
        request_id: Option<&Arc<str>>,
    ) {
        let context = request_id.map_or_else(
            || WriteContext::connection(FramePhase::Close),
            |request_id| WriteContext::request(request_id, FramePhase::Close),
        );
        self.close(
            CloseFrame {
                code: close_code::POLICY,
                reason: reason.into(),
            },
            context,
            PumpExitReason::ServerClose,
        )
        .await;
    }

    pub(super) async fn close_for_connection_limit(&mut self, reason: &'static str) {
        self.close(
            CloseFrame {
                code: close_code::NORMAL,
                reason: reason.into(),
            },
            WriteContext::connection(FramePhase::Close),
            PumpExitReason::ConnectionMaxAge,
        )
        .await;
    }

    async fn close(
        &mut self,
        frame: CloseFrame,
        context: WriteContext,
        exit_reason: PumpExitReason,
    ) {
        if self
            .send(Message::Close(Some(frame)), context)
            .await
            .is_ok()
        {
            self.exit_reason.get_or_insert(exit_reason);
        }
    }

    async fn send(
        &mut self,
        message: Message,
        context: WriteContext,
    ) -> Result<(), ConnectionWriteError> {
        let Some(commands) = self.commands.as_ref().cloned() else {
            return Err(ConnectionWriteError::Closed);
        };
        self.stats.observe_command_queue(&commands);
        let (acknowledged, acknowledgement) = oneshot::channel();
        let started_at = Instant::now();
        let request_id = context.request_id.clone();
        let phase = context.phase;
        let command = ConnectionCommand {
            message,
            context,
            acknowledged,
        };
        let write = async move {
            commands
                .send(command)
                .await
                .map_err(|_| ConnectionWriteError::Closed)?;
            acknowledgement
                .await
                .map_err(|_| ConnectionWriteError::Closed)?
        };
        let result = match timeout(self.config.write_timeout, write).await {
            Ok(result) => result,
            Err(_) => Err(ConnectionWriteError::Timeout {
                timeout: self.config.write_timeout,
            }),
        };
        let duration = started_at.elapsed();
        match &result {
            Ok(()) if phase.is_milestone() => tracing::info!(
                websocket_connection_id = %self.connection_id,
                request_id = request_id.as_deref().unwrap_or(""),
                frame_phase = phase.as_str(),
                write_duration_ms = duration.as_millis(),
                "Responses WebSocket frame write succeeded"
            ),
            Ok(()) => tracing::debug!(
                websocket_connection_id = %self.connection_id,
                request_id = request_id.as_deref().unwrap_or(""),
                frame_phase = phase.as_str(),
                write_duration_ms = duration.as_millis(),
                "Responses WebSocket frame write succeeded"
            ),
            Err(error) => tracing::info!(
                websocket_connection_id = %self.connection_id,
                request_id = request_id.as_deref().unwrap_or(""),
                frame_phase = phase.as_str(),
                write_duration_ms = duration.as_millis(),
                error = %error,
                "Responses WebSocket frame write failed"
            ),
        }
        if let Err(error) = &result {
            let reason = match error {
                ConnectionWriteError::Timeout { .. } => PumpExitReason::WriteTimeout,
                ConnectionWriteError::Transport { .. } => PumpExitReason::WriteError,
                ConnectionWriteError::Closed => PumpExitReason::PumpStopped,
            };
            self.terminate(reason);
        }
        result
    }

    fn terminate(&mut self, reason: PumpExitReason) {
        self.exit_reason.get_or_insert(reason);
        self.commands.take();
        if let Some(task) = self.pump_task.take() {
            task.abort();
        }
    }

    pub(super) fn log_summary(&self, request_count: u64) {
        let connection_age_ms = self.age().as_millis();
        let read_idle_ms = connection_age_ms
            .saturating_sub(u128::from(self.stats.last_read_ms.load(Ordering::Relaxed)));
        let write_idle_ms = connection_age_ms
            .saturating_sub(u128::from(self.stats.last_write_ms.load(Ordering::Relaxed)));
        tracing::info!(
            websocket_connection_id = %self.connection_id,
            request_count,
            connection_age_ms,
            pump_exit_reason = self
                .exit_reason
                .map_or("unknown", PumpExitReason::as_str),
            command_queue_high_water = self
                .stats
                .command_queue_high_water
                .load(Ordering::Relaxed),
            command_backpressure_count = self
                .stats
                .command_backpressure_count
                .load(Ordering::Relaxed),
            ping_received_count = self.stats.ping_received_count.load(Ordering::Relaxed),
            pong_written_count = self.stats.pong_written_count.load(Ordering::Relaxed),
            ping_written_count = self.stats.ping_written_count.load(Ordering::Relaxed),
            pong_received_count = self.stats.pong_received_count.load(Ordering::Relaxed),
            read_idle_ms,
            write_idle_ms,
            "Responses WebSocket disconnected"
        );
    }
}

impl Drop for ResponsesWebSocketConnection {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(task) = self.pump_task.take() {
            task.abort();
        }
    }
}

/// 为一个 socket 启动单 owner pump，并返回协调层句柄。
pub fn spawn_connection<S, E>(
    socket: S,
    connection_id: Arc<str>,
    cancellation: CancellationToken,
    config: ConnectionConfig,
) -> ResponsesWebSocketConnection
where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    spawn_observed_connection(
        socket,
        connection_id,
        cancellation,
        config,
        watch::channel(Instant::now()).1,
    )
}

fn spawn_observed_connection<S, E>(
    socket: S,
    connection_id: Arc<str>,
    cancellation: CancellationToken,
    config: ConnectionConfig,
    activity: watch::Receiver<Instant>,
) -> ResponsesWebSocketConnection
where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    let opened_at = Instant::now();
    let expired = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(ConnectionStats::default());
    let (command_tx, command_rx) = mpsc::channel(OUTBOUND_COMMAND_BUFFER);
    let (incoming_tx, incoming_rx) = mpsc::channel(INBOUND_EVENT_BUFFER);
    let (exit_tx, exit_rx) = oneshot::channel();
    let pump_task = tokio::spawn(run_pump(
        socket,
        command_rx,
        incoming_tx,
        exit_tx,
        Arc::clone(&connection_id),
        cancellation.clone(),
        opened_at,
        Arc::clone(&expired),
        Arc::clone(&stats),
        config,
        activity,
    ));
    ResponsesWebSocketConnection {
        cancellation,
        connection_id,
        opened_at,
        expired,
        commands: Some(command_tx),
        incoming: incoming_rx,
        deferred: VecDeque::new(),
        exited: exit_rx,
        pump_task: Some(pump_task),
        stats,
        config,
        exit_reason: None,
    }
}

#[expect(clippy::too_many_arguments)]
async fn run_pump<S, E>(
    socket: S,
    mut commands: mpsc::Receiver<ConnectionCommand>,
    incoming: mpsc::Sender<PendingConnectionEvent>,
    exited: oneshot::Sender<PumpExitReason>,
    connection_id: Arc<str>,
    cancellation: CancellationToken,
    opened_at: Instant,
    expired: Arc<AtomicBool>,
    stats: Arc<ConnectionStats>,
    config: ConnectionConfig,
    mut activity: watch::Receiver<Instant>,
) where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin,
    E: fmt::Display,
{
    // Dropping or aborting the pump must also release resources bound across turns.
    struct CancelOnDrop(CancellationToken);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    let _lifetime = CancelOnDrop(cancellation.clone());
    let event_slots = Arc::new(Semaphore::new(INBOUND_EVENT_BUFFER));
    let (writer, mut reader) = socket.split();
    let writer = Mutex::new(writer);
    let (received, mut received_frames) = watch::channel(Instant::now());
    let receive = async {
        while let Some(message) = reader.next().await {
            if message.is_ok() {
                stats.record_read(opened_at);
                received.send_replace(Instant::now());
            }
            let event = match message {
                Ok(Message::Text(payload)) => ConnectionEvent::Text(payload.to_string()),
                Ok(Message::Binary(_)) => ConnectionEvent::Binary,
                Ok(Message::Pong(_)) => {
                    stats.pong_received_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(Message::Ping(payload)) => {
                    stats.ping_received_count.fetch_add(1, Ordering::Relaxed);
                    match timeout(Duration::from_secs(5), async {
                        writer.lock().await.send(Message::Pong(payload)).await
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            stats.pong_written_count.fetch_add(1, Ordering::Relaxed);
                            stats.record_write(opened_at);
                            continue;
                        }
                        Ok(Err(_)) => return PumpExitReason::WriteError,
                        Err(_) => return PumpExitReason::WriteTimeout,
                    }
                }
                Ok(Message::Close(_)) => return PumpExitReason::ClientClose,
                Err(_) => return PumpExitReason::ReadError,
            };
            if let Err(reason) = emit_incoming(&incoming, &event_slots, event) {
                return reason;
            }
        }
        PumpExitReason::PeerEof
    };
    let send = async {
        while let Some(command) = commands.recv().await {
            let ConnectionCommand {
                message,
                context,
                acknowledged,
            } = command;
            let closing = matches!(message, Message::Close(_));
            let result = timeout(config.write_timeout, async {
                writer.lock().await.send(message).await
            })
            .await;
            let reason = match &result {
                Ok(Ok(())) => None,
                Ok(Err(_)) => Some(PumpExitReason::WriteError),
                Err(_) => Some(PumpExitReason::WriteTimeout),
            };
            let result = result
                .map_err(|_| ConnectionWriteError::Transport {
                    message: "WebSocket write timed out".into(),
                })
                .and_then(|r| {
                    r.map_err(|e| ConnectionWriteError::Transport {
                        message: e.to_string(),
                    })
                });
            if result.is_ok() {
                stats.record_write(opened_at);
            }
            if let Err(error) = &result {
                tracing::debug!(websocket_connection_id=%connection_id, frame_phase=context.phase.as_str(), error=%error, "Responses WebSocket write failed");
            }
            let _ = acknowledged.send(result);
            if let Some(reason) = reason {
                return reason;
            }
            if closing {
                return PumpExitReason::ServerClose;
            }
        }
        PumpExitReason::CoordinatorDropped
    };
    let heartbeat = async {
        let mut ticks = tokio::time::interval_at(
            opened_at + DOWNSTREAM_PING_INTERVAL,
            DOWNSTREAM_PING_INTERVAL,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut deadline = None;
        let mut sequence = 0_u64;
        let mut raw_activity_open = true;
        loop {
            tokio::select! {
                biased;
                changed = activity.changed(), if raw_activity_open => {
                    if changed.is_err() { raw_activity_open = false; } else { deadline = None; }
                }
                changed = received_frames.changed() => { if changed.is_err() { return PumpExitReason::PeerEof; } deadline = None; }
                () = async { match deadline { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => return PumpExitReason::PongTimeout,
                _ = ticks.tick() => {
                    sequence = sequence.wrapping_add(1);
                    let Ok(mut heartbeat_writer) = writer.try_lock() else { continue; };
                    match timeout(Duration::from_secs(5), heartbeat_writer.send(Message::Ping(sequence.to_be_bytes().to_vec().into()))).await {
                        Ok(Ok(())) => { stats.ping_written_count.fetch_add(1, Ordering::Relaxed); stats.record_write(opened_at); }
                        Ok(Err(_)) => return PumpExitReason::WriteError,
                        Err(_) => return PumpExitReason::WriteTimeout,
                    }
                    deadline.get_or_insert(Instant::now() + Duration::from_secs(30));
                }
            }
        }
    };
    let age = async {
        tokio::time::sleep_until(opened_at + config.max_age).await;
        expired.store(true, Ordering::Release);
        match emit_incoming(&incoming, &event_slots, ConnectionEvent::Expired) {
            Ok(()) | Err(PumpExitReason::InboundOverload) => std::future::pending().await,
            Err(reason) => reason,
        }
    };
    let reason = tokio::select! {
        biased;
        () = cancellation.cancelled() => PumpExitReason::LifecycleShutdown,
        reason = receive => reason,
        reason = send => reason,
        reason = heartbeat => reason,
        reason = age => reason,
    };
    let _ = exited.send(reason);
    tracing::debug!(websocket_connection_id=%connection_id, pump_exit_reason=reason.as_str(), "Responses WebSocket pump exited");
}

fn emit_incoming(
    incoming: &mpsc::Sender<PendingConnectionEvent>,
    slots: &Arc<Semaphore>,
    event: ConnectionEvent,
) -> Result<(), PumpExitReason> {
    let permit = Arc::clone(slots)
        .try_acquire_owned()
        .map_err(|_| PumpExitReason::InboundOverload)?;
    incoming
        .try_send(PendingConnectionEvent {
            event,
            _permit: Some(permit),
        })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PumpExitReason::InboundOverload,
            mpsc::error::TrySendError::Closed(_) => PumpExitReason::CoordinatorDropped,
        })
}
