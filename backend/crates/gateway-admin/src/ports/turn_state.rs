//! State 维护的管理端能力与不透明加密文档存储端口。

use super::store::AdminStoreResult;
use crate::model::{AdminError, MutationContext, turn_state::*};
use async_trait::async_trait;

#[async_trait]
pub trait TurnStateStore: Send + Sync {
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
