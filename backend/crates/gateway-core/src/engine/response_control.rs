//! 当前执行的响应中断信号；只由持有活动响应的 Provider 注册，不查找其他执行或连接。

use futures::channel::mpsc;
use serde_json::Value;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use crate::lifecycle::CancellationToken;

/// 单次根执行的控制入口；重试不继承已结束响应的控制目标。
#[derive(Clone, Default)]
pub struct ResponseControl {
    active: Arc<Mutex<Weak<InterruptTarget>>>,
    connection_cancellation: Option<CancellationToken>,
}

impl fmt::Debug for ResponseControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResponseControl([REDACTED])")
    }
}

struct InterruptTarget {
    response_id: String,
    requested: CancellationToken,
    steering: Option<Mutex<mpsc::Sender<Value>>>,
}

/// Provider 持有的活动响应控制权；释放后客户端无法再向该响应发出中断。
pub struct ActiveResponseInterrupt(Arc<InterruptTarget>);

/// 控制帧不能作用于当前执行时的稳定原因，不携带其他响应身份。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseInterruptError {
    Unavailable,
    ResponseMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSteerError {
    Unavailable,
    ResponseMismatch,
    Unsupported,
    QueueFull,
}

impl ResponseControl {
    #[must_use]
    pub fn with_connection_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.connection_cancellation = Some(cancellation);
        self
    }
    pub fn connection_cancellation(&self) -> Option<&CancellationToken> {
        self.connection_cancellation.as_ref()
    }
    /// 在上游确认响应 ID 后注册；同一执行只允许一个存活的响应 owner。
    pub fn activate(&self, response_id: String) -> Option<ActiveResponseInterrupt> {
        self.activate_inner(response_id, None)
    }

    /// Only a native streaming WS owner may advertise steering support.
    pub fn activate_with_steering(
        &self,
        response_id: String,
    ) -> Option<(ActiveResponseInterrupt, mpsc::Receiver<Value>)> {
        let (send, receive) = mpsc::channel(16);
        self.activate_inner(response_id, Some(send))
            .map(|active| (active, receive))
    }

    fn activate_inner(
        &self,
        response_id: String,
        steering: Option<mpsc::Sender<Value>>,
    ) -> Option<ActiveResponseInterrupt> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.upgrade().is_some() || response_id.is_empty() {
            return None;
        }
        let target = Arc::new(InterruptTarget {
            response_id,
            requested: CancellationToken::new(),
            steering: steering.map(Mutex::new),
        });
        *active = Arc::downgrade(&target);
        Some(ActiveResponseInterrupt(target))
    }

    /// Queue on this execution's current socket. Success is NOT upstream acceptance.
    pub fn steer(&self, response_id: &str, frame: Value) -> Result<(), ResponseSteerError> {
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = active.upgrade().ok_or(ResponseSteerError::Unavailable)?;
        if target.response_id != response_id {
            return Err(ResponseSteerError::ResponseMismatch);
        }
        let sender = target
            .steering
            .as_ref()
            .ok_or(ResponseSteerError::Unsupported)?;
        sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_send(frame)
            .map_err(|error| {
                if error.is_full() {
                    ResponseSteerError::QueueFull
                } else {
                    ResponseSteerError::Unavailable
                }
            })
    }

    /// 重复中断合并为一次信号；ID 必须匹配本执行当前的上游响应。
    pub fn interrupt(&self, response_id: &str) -> Result<(), ResponseInterruptError> {
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = active
            .upgrade()
            .ok_or(ResponseInterruptError::Unavailable)?;
        if target.response_id != response_id {
            return Err(ResponseInterruptError::ResponseMismatch);
        }
        target.requested.cancel();
        Ok(())
    }
}

impl ActiveResponseInterrupt {
    pub fn response_id(&self) -> &str {
        &self.0.response_id
    }

    pub fn requested(&self) -> impl Future<Output = ()> + Send + 'static {
        // 等待者只持有信号，不能延长活动响应 owner 的生命周期。
        let requested = self.0.requested.clone();
        async move { requested.cancelled().await }
    }
}
