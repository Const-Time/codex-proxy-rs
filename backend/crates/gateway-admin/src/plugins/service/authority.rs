//! Host-issued process identity; plugin JSON never grants service authority.
use crate::model::{
    AdminError, MutationActor, MutationContext,
    plugins::{PluginAccess, PluginRecord},
};
use gateway_core::lifecycle::CancellationToken;
use std::num::NonZeroU64;

#[derive(Clone)]
pub struct PluginServiceAuthority {
    instance_id: String,
    revision: NonZeroU64,
    access: PluginAccess,
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
            instance_id: record.id.clone(),
            revision,
            access,
        })
    }
    pub(super) fn authorizes(&self, instance: &str) -> bool {
        self.instance_id == instance && !self.access.is_revoked()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.access.cancellation()
    }
    pub(super) fn actor(&self, request_id: String) -> MutationContext {
        MutationContext {
            actor: MutationActor::Plugin {
                instance_id: self.instance_id.clone(),
                revision: self.revision,
            },
            request_id,
        }
    }
}
