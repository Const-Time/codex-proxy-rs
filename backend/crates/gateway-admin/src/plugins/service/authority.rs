//! Host-issued process identity; plugin JSON never grants service authority.
use crate::model::{
    AdminError, MutationActor, MutationContext,
    plugins::{PluginAccess, PluginRecord},
};
use gateway_core::engine::middleware::service::Caller;
use gateway_core::lifecycle::CancellationToken;
use std::num::NonZeroU64;

#[derive(Clone)]
pub struct PluginServiceAuthority {
    caller: Caller,
}
impl PluginServiceAuthority {
    /// The runtime supplies its validated record and original generation lease.
    pub fn new(record: &PluginRecord, access: PluginAccess) -> Result<Self, AdminError> {
        let revision =
            NonZeroU64::new(record.revision).ok_or_else(|| AdminError::invalid("插件版本无效"))?;
        if !record.enabled
            || !crate::model::auth::valid_plugin_instance(&record.id)
            || access.is_revoked()
        {
            return Err(AdminError::new(
                crate::model::AdminErrorKind::Forbidden,
                "插件服务身份不可用",
            ));
        }
        Ok(Self {
            caller: Caller::new(record.id.clone(), revision, access.cancellation())
                .map_err(|_| AdminError::invalid("插件服务身份无效"))?,
        })
    }
    pub(super) fn from_caller(caller: Caller) -> Result<Self, AdminError> {
        if caller.cancellation().is_cancelled() {
            return Err(AdminError::new(
                crate::model::AdminErrorKind::Forbidden,
                "插件服务身份已撤销",
            ));
        }
        Ok(Self { caller })
    }
    pub(super) fn authorizes(&self, instance: &str) -> bool {
        self.caller.instance_id() == instance && !self.caller.cancellation().is_cancelled()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.caller.cancellation()
    }
    pub(super) fn actor(&self, request_id: String) -> MutationContext {
        MutationContext {
            actor: MutationActor::Plugin {
                instance_id: self.caller.instance_id().into(),
                revision: self.caller.revision(),
            },
            request_id,
        }
    }
}
