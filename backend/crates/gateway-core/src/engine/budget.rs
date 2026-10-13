//! 按 Key 持久化 USD 限额与已取得费用，不依赖尽力写入的请求观测。

use std::time::SystemTime;

use futures::future::BoxFuture;

use crate::{error::GatewayError, metering::Decimal, policy::ClientApiKeyId};

use super::ModelRequestId;

/// Frozen at admission; changing or deleting a key cannot move its charge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientBudgetScope {
    pub user_id: String,
    pub group_id: crate::routing::AccountGroupId,
    pub billing: ClientBudgetBilling,
}

/// Immutable admission-time rule, shared by settlement and request projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientBudgetBilling {
    pub multiplier: Decimal,
    pub scope: crate::metering::MultiplierScope,
}

impl Default for ClientBudgetBilling {
    fn default() -> Self {
        Self {
            multiplier: Decimal::from_scaled(10_000_000_000).expect("one USD tick scale"),
            scope: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientBudgetLimits {
    pub daily_usd: Decimal,
    pub weekly_usd: Decimal,
}

impl ClientBudgetLimits {
    #[must_use]
    pub fn is_limited(self) -> bool {
        self.daily_usd != Decimal::ZERO || self.weekly_usd != Decimal::ZERO
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientBudgetStatus {
    pub limits: ClientBudgetLimits,
    pub daily_used_usd: Decimal,
    pub weekly_used_usd: Decimal,
    pub daily_resets_at: Option<SystemTime>,
    pub weekly_resets_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct ClientBudgetCharge {
    pub scope: Option<ClientBudgetScope>,
    pub key_id: ClientApiKeyId,
    pub request_id: ModelRequestId,
    /// Execution-owned cost facts, including discarded attempts; never read from telemetry.
    pub costs: Vec<crate::metering::CostEstimate>,
    pub completed_at: SystemTime,
}

impl ClientBudgetCharge {
    pub fn amount_usd(&self) -> Decimal {
        self.costs
            .iter()
            .filter_map(|cost| cost.total())
            .filter(|money| money.currency().as_str() == "USD")
            .fold(Decimal::ZERO, |sum, money| {
                sum.checked_add(money.amount()).unwrap_or(Decimal::MAX)
            })
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("client budget store is unavailable")]
pub struct ClientBudgetError;

pub trait ClientBudgetPort: Send + Sync {
    /// 原子检查当前限额与已用金额，不创建预扣费或待结算记录。
    fn admit(
        &self,
        key_id: ClientApiKeyId,
        expected_group: Option<crate::routing::AccountGroupId>,
        requested_model: Option<String>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>>;

    /// 按网关请求 ID 幂等累计已取得费用；写入失败由 Store 重试。
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>>;
}
