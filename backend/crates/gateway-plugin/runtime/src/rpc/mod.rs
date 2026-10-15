//! 插件双向 RPC 会话、调用结果与生命周期的公共入口

mod admission;
mod dispatch;
mod session;

pub use session::{CallbackHandler, RpcError, RpcLimits, RpcReply, RpcSession};
pub(crate) use session::{RpcSessionDiagnostic, RpcSessionLifecycle, Shared};

pub(crate) fn managed_next_stage(stage: gateway_plugin_sdk::Stage) -> bool {
    use gateway_plugin_sdk::Stage;
    matches!(
        stage,
        Stage::Http | Stage::WebSocket | Stage::Service | Stage::Request | Stage::Attempt
    )
}

/// Keep sources for controlled diagnostics without formatting plugin-controlled details.
#[derive(Clone)]
pub struct ErrorSource(std::sync::Arc<dyn std::error::Error + Send + Sync>);
impl ErrorSource {
    pub fn new(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(std::sync::Arc::new(error))
    }
}
impl std::ops::Deref for ErrorSource {
    type Target = dyn std::error::Error + Send + Sync;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
impl std::fmt::Debug for ErrorSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PluginErrorSource([REDACTED])")
    }
}
