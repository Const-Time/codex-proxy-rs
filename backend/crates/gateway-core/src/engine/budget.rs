//! 用户与分组的冻结计费合同，不依赖尽力写入的请求观测。

use std::time::SystemTime;

use futures::future::BoxFuture;

use crate::{
    error::GatewayError,
    metering::{CostEstimate, Decimal, MultiplierScope},
    policy::ClientApiKeyId,
    routing::PublicModelId,
};

use super::ModelRequestId;

/// Frozen at admission; changing or deleting a key cannot move its charge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientBudgetScope {
    pub user_id: String,
    pub group_id: crate::routing::AccountGroupId,
    pub billing: ClientBudgetBilling,
}

/// Captured by the existing budget owner at admission for this public model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientBudgetBilling {
    multiplier: Decimal,
    scope: MultiplierScope,
}
impl Default for ClientBudgetBilling {
    fn default() -> Self {
        Self {
            multiplier: "1".parse().expect("fixed billing multiplier"),
            scope: MultiplierScope::All,
        }
    }
}
impl ClientBudgetBilling {
    pub fn new(multiplier: Decimal, scope: MultiplierScope) -> Result<Self, &'static str> {
        scope.validate()?;
        if multiplier.scaled() > 10_000_000_000_000 {
            return Err("billing multiplier exceeds the account group limit");
        }
        Ok(Self { multiplier, scope })
    }
    #[must_use]
    pub const fn multiplier(&self) -> Decimal {
        self.multiplier
    }
    #[must_use]
    pub const fn scope(&self) -> &MultiplierScope {
        &self.scope
    }
}

/// Only observed itemized USD fees are eligible for component multipliers.
/// Unknown fees stay in the raw total without guessing their components.
#[must_use]
pub fn budget_components(cost: &CostEstimate) -> [Decimal; 4] {
    let Some(total) = cost
        .total()
        .filter(|total| total.currency().as_str() == "USD")
    else {
        return [Decimal::ZERO; 4];
    };
    let Some(breakdown) = cost.breakdown().filter(|b| b.total_amount() == total) else {
        return [Decimal::ZERO; 4];
    };
    let fees = [
        breakdown.input_amount(),
        breakdown.output_amount(),
        breakdown.cache_read_amount(),
        breakdown.cache_write_amount(),
    ];
    if fees.iter().any(|fee| fee.currency() != total.currency()) {
        return [Decimal::ZERO; 4];
    }
    let mut components = fees.map(|fee| fee.amount());
    if let Some(image) = breakdown.image() {
        if image.input_amount.currency() != total.currency()
            || image.cache_read_amount.currency() != total.currency()
        {
            return [Decimal::ZERO; 4];
        }
        let Some(input) = components[0].checked_add(image.input_amount.amount()) else {
            return [Decimal::ZERO; 4];
        };
        let Some(cache) = components[2].checked_add(image.cache_read_amount.amount()) else {
            return [Decimal::ZERO; 4];
        };
        components[0] = input;
        components[2] = cache;
    }
    components
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
    /// 已取得的 USD 费用，包含重试；缺少费用的尝试按零累计。
    pub amount_usd: Decimal,
    /// Input, output, cache read, cache write; accumulated across the same attempts.
    pub components_usd: [Decimal; 4],
    pub completed_at: SystemTime,
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
        requested_model: Option<PublicModelId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>>;

    /// 按网关请求 ID 幂等累计已取得费用；写入失败由 Store 重试。
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>>;
}
