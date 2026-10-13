//! 按所属会话保留、复用与回收插件受管上游连接

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use gateway_host::outbound::ManagedWebSocket;
use gateway_plugin_sdk::{ErrorCode, PluginFault};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConnectionOwner {
    pub client_key_id: String,
    pub account_id: String,
    pub credential_revision: u64,
}

/// 每个适配器代次独立持有连接；取出即独占，失败或取消不会归还半完成会话
pub(crate) struct ConnectionPool {
    pub(crate) identity: String,
    cancellation: gateway_core::lifecycle::CancellationToken,
    revoker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    idle: Mutex<BTreeMap<String, IdleConnection>>,
}

struct IdleConnection {
    owner: ConnectionOwner,
    socket: Option<(String, ManagedWebSocket)>,
    expires: tokio::task::JoinHandle<()>,
}

impl Drop for IdleConnection {
    fn drop(&mut self) {
        self.expires.abort();
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        if let Some(task) = self
            .revoker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}
impl ConnectionPool {
    pub(crate) fn new(cancellation: gateway_core::lifecycle::CancellationToken) -> Arc<Self> {
        let pool = Arc::new(Self {
            identity: uuid::Uuid::new_v4().to_string(),
            cancellation: cancellation.clone(),
            revoker: Mutex::default(),
            idle: Mutex::default(),
        });
        let weak = Arc::downgrade(&pool);
        let task = tokio::spawn(async move {
            cancellation.cancelled().await;
            if let Some(pool) = weak.upgrade() {
                pool.idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
            }
        });
        *pool
            .revoker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        pool
    }
    pub(crate) fn take(
        &self,
        id: &str,
        owner: &ConnectionOwner,
    ) -> Result<(String, ManagedWebSocket), PluginFault> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancellation.is_cancelled()
            || !idle
                .get(id)
                .is_some_and(|connection| connection.owner == *owner)
        {
            return Err(PluginFault::new(
                ErrorCode::Rejected,
                "upstream continuation connection is unavailable",
            ));
        }
        idle.remove(id)
            .and_then(|mut connection| connection.socket.take())
            .ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::Rejected,
                    "upstream continuation connection is unavailable",
                )
            })
    }

    pub(crate) fn put(
        self: &Arc<Self>,
        owner: ConnectionOwner,
        socket: (String, ManagedWebSocket),
    ) -> Result<String, PluginFault> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancellation.is_cancelled() || idle.len() >= 64 {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "upstream continuation capacity exhausted",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let weak = Arc::downgrade(self);
        let expired_id = id.clone();
        let expires = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30 * 60)).await;
            if let Some(pool) = weak.upgrade() {
                pool.idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&expired_id);
            }
        });
        idle.insert(
            id.clone(),
            IdleConnection {
                owner,
                socket: Some(socket),
                expires,
            },
        );
        Ok(id)
    }
}
