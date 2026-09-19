//! State 维护的管理端能力与不透明加密文档存储端口。

use super::store::AdminStoreResult;
use crate::model::{AdminError, MutationContext, turn_state::*};
use async_trait::async_trait;

#[async_trait]
pub trait TurnStateStore: Send + Sync {
    async fn records(
        &self,
        _query: &TurnStateRecordQuery,
    ) -> AdminStoreResult<TurnStateRecordPage> {
        Err(super::store::AdminStoreError::new(
            super::store::AdminStoreErrorKind::Unavailable,
            "turn state records",
            "探测记录不可用",
        ))
    }
    /// Must succeed before sending a maintenance request. Never debit an end user.
    async fn begin_probe(&self, probe: &TurnStateProbeStart) -> AdminStoreResult<()>;
    async fn finish_probe(&self, id: &str, result: &TurnStateProbeResult) -> AdminStoreResult<()>;
    async fn load(&self) -> AdminStoreResult<(u64, Option<Vec<u8>>)>;
    /// 所有写入 CAS；审计不包含密文、代理地址或 token。
    async fn save(
        &self,
        revision: u64,
        sealed: Vec<u8>,
        context: Option<&MutationContext>,
    ) -> AdminStoreResult<u64>;
}

#[async_trait]
pub trait TurnStateService: Send + Sync {
    async fn records(
        &self,
        _query: TurnStateRecordQuery,
    ) -> Result<TurnStateRecordPage, AdminError> {
        Err(AdminError::unavailable("探测记录不可用"))
    }
    async fn view(&self) -> Result<TurnStateView, AdminError>;
    async fn configure_account(
        &self,
        account_id: &str,
        input: TurnStateAccountUpdate,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError>;
    async fn configure(
        &self,
        input: TurnStateSettingsInput,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError>;
    async fn action(
        &self,
        account_id: &str,
        model: &str,
        action: &str,
        context: &MutationContext,
    ) -> Result<TurnStateView, AdminError>;
    async fn test_pool(&self, id: &str) -> Result<TurnStatePoolTest, AdminError>;
}
