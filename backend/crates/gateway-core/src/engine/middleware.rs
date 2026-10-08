//! Host-owned middleware composition and lazy delivery contracts.
use crate::{
    account::ProviderAccountId,
    engine::{
        CommitRequirement, EngineError, ModelRequestId, execution::ClientTransport,
        provider::ProviderCallMetadata,
    },
    error::{GatewayError, ProviderError},
    event::ProviderEvent,
    lifecycle::CancellationToken,
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, ProviderKind},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{fmt, time::SystemTime};
/// 中间件在一次逻辑请求中的挂载位置
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MiddlewareMount {
    /// 所有 HTTP 接口的认证、路由和正文解析之前
    Http,
    /// 下游 WebSocket 消息在默认解析和写入之前组合
    WebSocket,
    /// 类型化公开服务在执行默认业务逻辑前组合
    Service,
    /// 客户端请求外层；一次逻辑请求只执行一次
    Request,
    /// 受管 Provider attempt 内层；每次真实 retry 都重新建立
    Attempt,
}

/// 中间件正文块的宿主边界
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareFraming {
    /// 一个完整 JSON document
    JsonDocument,
    /// 一个完整 SSE event，包含终止该事件的空行
    SseEvent,
    /// 不承诺 UTF-8 或消息边界的原始字节
    RawBytes,
}

/// 允许进入中间件合同的多值 HTTP 头
#[derive(Clone, PartialEq, Eq)]
pub struct MiddlewareHeader {
    name: String,
    value: Bytes,
}

impl MiddlewareHeader {
    #[must_use]
    pub fn new(name: impl Into<String>, value: Bytes) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn value(&self) -> &Bytes {
        &self.value
    }

    #[must_use]
    pub fn into_parts(self) -> (String, Bytes) {
        (self.name, self.value)
    }
}

impl fmt::Debug for MiddlewareHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MiddlewareHeader")
            .field("name", &self.name)
            .field("value", &"<not included in Debug>")
            .finish()
    }
}

/// Rewritable protocol facts; authority stays in the host terminal closure.
#[derive(Clone)]
pub struct MiddlewareRequest {
    protocol: String,
    headers: Vec<MiddlewareHeader>,
    body: Bytes,
}
impl MiddlewareRequest {
    pub fn new(protocol: impl Into<String>, headers: Vec<MiddlewareHeader>, body: Bytes) -> Self {
        Self {
            protocol: protocol.into(),
            headers,
            body,
        }
    }
    pub fn protocol(&self) -> &str {
        &self.protocol
    }
    pub fn headers(&self) -> &[MiddlewareHeader] {
        &self.headers
    }
    pub fn body(&self) -> &Bytes {
        &self.body
    }
    pub fn into_parts(self) -> (String, Vec<MiddlewareHeader>, Bytes) {
        (self.protocol, self.headers, self.body)
    }
}
/// 中间件逐次拉取的完整正文边界
pub struct MiddlewareFrame {
    bytes: Bytes,
    framing: MiddlewareFraming,
    terminal: bool,
    transformed: bool,
    envelope: Option<MiddlewareFrameEnvelope>,
}

impl MiddlewareFrame {
    #[must_use]
    pub const fn new(bytes: Bytes, framing: MiddlewareFraming, terminal: bool) -> Self {
        Self {
            bytes,
            framing,
            terminal,
            transformed: false,
            envelope: None,
        }
    }

    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub const fn framing(&self) -> MiddlewareFraming {
        self.framing
    }

    #[must_use]
    pub const fn terminal(&self) -> bool {
        self.terminal
    }

    /// 返回正文是否经过至少一个中间件的替换、展开或丢弃映射
    ///
    /// 插件可读取该事实；对快照的修改不改变宿主确认的来源
    #[must_use]
    pub const fn transformed(&self) -> bool {
        self.transformed
    }

    /// 返回来源事件的只读视图；读取不转移原结算和会话状态的所有权
    #[must_use]
    pub fn event(&self) -> Option<&ProviderEvent> {
        self.envelope.as_ref().map(MiddlewareFrameEnvelope::event)
    }

    /// 继承宿主确认的改写事实；`false` 不能清除内层已经记录的改写
    #[must_use]
    pub fn with_transformed(mut self, transformed: bool) -> Self {
        self.transformed |= transformed;
        self
    }

    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// 拆出插件可见正文与只能由宿主原样搬运的事实信封
    ///
    /// Runtime 在一对多转换时必须把信封附到最后一个输出；这样 `Completed` 不会在
    /// 前置输出交付前提前终结
    /// 读取快照不会转移信封的计量所有权
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Bytes,
        MiddlewareFraming,
        bool,
        Option<MiddlewareFrameEnvelope>,
    ) {
        (self.bytes, self.framing, self.terminal, self.envelope)
    }

    /// 把宿主持有的事实信封附到改写后的最后一个输出
    #[must_use]
    pub fn with_envelope(mut self, envelope: MiddlewareFrameEnvelope) -> Self {
        self.envelope = Some(envelope);
        self
    }

    pub fn from_provider_event(
        bytes: Bytes,
        framing: MiddlewareFraming,
        terminal: bool,
        event: ProviderEvent,
    ) -> Self {
        let transformed = false;
        Self {
            bytes,
            framing,
            terminal,
            transformed,
            envelope: Some(MiddlewareFrameEnvelope {
                event,
                batch_end: None,
            }),
        }
    }
}

impl fmt::Debug for MiddlewareFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MiddlewareFrame")
            .field("bytes", &"<not included in Debug>")
            .field("framing", &self.framing)
            .field("terminal", &self.terminal)
            .field("transformed", &self.transformed)
            .field("has_host_envelope", &self.envelope.is_some())
            .finish()
    }
}

/// 可读取完整事件、只能随正文移动一次的 Core 事实信封
///
/// 该类型故意不实现 `Clone`，防止同一 usage/cost 在一对多响应中被复制
pub struct MiddlewareFrameEnvelope {
    event: ProviderEvent,
    batch_end: Option<CommitRequirement>,
}

impl MiddlewareFrameEnvelope {
    pub fn with_batch_end(mut self, requirement: CommitRequirement) -> Self {
        self.batch_end = Some(requirement);
        self
    }
    pub fn batch_end(&self) -> Option<CommitRequirement> {
        self.batch_end
    }

    #[must_use]
    pub const fn event(&self) -> &ProviderEvent {
        &self.event
    }
}

impl fmt::Debug for MiddlewareFrameEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MiddlewareFrameEnvelope")
            .finish_non_exhaustive()
    }
}

/// 洋葱返回路径上的惰性正文
///
/// 实现必须让 `Drop` 与 [`Self::close`] 都回收底层流
/// 请求 finalization 继续委托
/// 原有 [`crate::engine::execution::ExecutionSession`]，本端口不建立第二套清理器
pub trait MiddlewareBody: Send {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>>;

    /// 在最外层输出准备完成后提交原有 Core delivery barrier
    /// 中间件实现只能向内委托
    fn commit_downstream(
        &mut self,
        _client_status_code: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async { Ok(()) })
    }

    /// 首字节前失败时记录最终返回给客户端的状态
    /// 中间件实现只能向内委托
    fn record_client_status(
        &mut self,
        _client_status_code: u16,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async { Ok(()) })
    }

    /// 没有调用 `next` 的短路正文视为无需 Core 结算；调用过 `next` 的包装器必须委托
    fn is_finalized(&self) -> bool {
        true
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// `next` 或短路返回的响应头与惰性正文
pub struct MiddlewareResponse {
    protocol: String,
    status_code: u16,
    headers: Vec<MiddlewareHeader>,
    body: Box<dyn MiddlewareBody>,
    envelope: Option<MiddlewareResponseEnvelope>,
}

impl MiddlewareResponse {
    #[must_use]
    pub fn new(
        protocol: String,
        status_code: u16,
        headers: Vec<MiddlewareHeader>,
        body: Box<dyn MiddlewareBody>,
    ) -> Self {
        Self {
            protocol,
            status_code,
            headers,
            body,
            envelope: None,
        }
    }

    #[must_use]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    #[must_use]
    pub const fn status_code(&self) -> u16 {
        self.status_code
    }

    #[must_use]
    pub fn headers(&self) -> &[MiddlewareHeader] {
        &self.headers
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        String,
        u16,
        Vec<MiddlewareHeader>,
        Box<dyn MiddlewareBody>,
        Option<MiddlewareResponseEnvelope>,
    ) {
        (
            self.protocol,
            self.status_code,
            self.headers,
            self.body,
            self.envelope,
        )
    }

    /// 搬运宿主 terminal 的来源事实；读取快照不转移其所有权
    #[must_use]
    pub fn with_envelope(mut self, envelope: MiddlewareResponseEnvelope) -> Self {
        self.envelope = Some(envelope);
        self
    }

    pub fn with_provider_metadata(mut self, metadata: ProviderCallMetadata) -> Self {
        self.envelope = Some(MiddlewareResponseEnvelope { metadata });
        self
    }
}

impl fmt::Debug for MiddlewareResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MiddlewareResponse")
            .field("protocol", &self.protocol)
            .field("status_code", &self.status_code)
            .field("headers", &self.headers)
            .field("body", &"<stream>")
            .field("has_host_envelope", &self.envelope.is_some())
            .finish()
    }
}

/// Attempt terminal 的 Core 来源事实；中间件只能随响应原样搬运
pub struct MiddlewareResponseEnvelope {
    metadata: ProviderCallMetadata,
}

impl MiddlewareResponseEnvelope {
    #[must_use]
    pub const fn metadata(&self) -> &ProviderCallMetadata {
        &self.metadata
    }

    pub fn into_provider_metadata(self) -> ProviderCallMetadata {
        self.metadata
    }
}

impl fmt::Debug for MiddlewareResponseEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MiddlewareResponseEnvelope")
            .finish_non_exhaustive()
    }
}

/// Facts frozen by the host for one invocation; never decoded from plugin input.
#[derive(Clone)]
pub struct MiddlewareContext {
    pub request_id: ModelRequestId,
    pub client_key_id: ClientApiKeyId,
    pub account_group_ids: Vec<AccountGroupId>,
    pub mount: MiddlewareMount,
    pub attempt_index: Option<u32>,
    pub operation: OperationKind,
    pub endpoint: String,
    pub transport: ClientTransport,
    pub provider: Option<ProviderKind>,
    pub model: Option<String>,
    pub account_id: Option<ProviderAccountId>,
    pub cancellation: CancellationToken,
    pub deadline_at: SystemTime,
}
/// 中间件链调用错误
/// 原有域错误保持原类型，不能被插件故障改写为“未发送”
#[derive(Debug)]
pub enum MiddlewareError {
    Rejected,
    Fault,
    /// 保留跨进程失败，供外层插件读取；客户端响应由协议 owner 决定
    Remote {
        source: Box<dyn std::error::Error + Send + Sync>,
        rejected: bool,
    },
    InvalidState,
    Gateway(GatewayError),
    Engine(EngineError),
    Provider(ProviderError),
}

impl fmt::Display for MiddlewareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected => "middleware rejected the request",
            Self::Fault => "middleware call failed",
            Self::Remote { source, .. } => return fmt::Display::fmt(source, formatter),
            Self::InvalidState => "middleware state is invalid",
            Self::Gateway(_) => "gateway middleware terminal failed",
            Self::Engine(_) => "execution middleware terminal failed",
            Self::Provider(_) => "provider middleware terminal failed",
        })
    }
}

impl std::error::Error for MiddlewareError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Remote { source, .. } => Some(source.as_ref()),
            Self::Gateway(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Provider(error) => Some(error),
            _ => None,
        }
    }
}

impl MiddlewareError {
    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected | Self::Remote { rejected: true, .. })
    }
}

impl From<GatewayError> for MiddlewareError {
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl From<EngineError> for MiddlewareError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<ProviderError> for MiddlewareError {
    fn from(error: ProviderError) -> Self {
        Self::Provider(error)
    }
}

/// 绑定当前调用且只能消费一次的宿主续体
pub type MiddlewareNext =
    crate::middleware::Next<MiddlewareRequest, MiddlewareResponse, MiddlewareError>;
