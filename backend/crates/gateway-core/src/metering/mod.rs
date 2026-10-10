//! 跨 Provider 的标准化用量与单次请求总费用。

use std::fmt;
use std::str::FromStr;

use crate::validation::MeteringError;

mod pricing;
mod scope;
pub use pricing::{
    ModelPriceOverride, PricingOverrides, TokenPrice, TokenPriceOverride, merge_pricing,
};
pub use scope::{CostComponent, MultiplierScope};

const DECIMAL_SCALE: u128 = 10_000_000_000;
const MAX_SCALED_DECIMAL: u128 = 99_999_999_999_999_999_999;

/// 与 PostgreSQL `numeric(20, 10)` 对齐的非负十进制定点值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Decimal(u128);

impl Decimal {
    pub const ZERO: Self = Self(0);
    /// 数据库可表示的最大非负金额。
    pub const MAX: Self = Self(MAX_SCALED_DECIMAL);

    /// 从按十位小数缩放的整数创建。
    ///
    /// # Errors
    ///
    /// 超出数据库范围时返回错误。
    pub const fn from_scaled(value: u128) -> Result<Self, MeteringError> {
        if value > MAX_SCALED_DECIMAL {
            return Err(MeteringError::InvalidDecimal);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn scaled(self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0
            .checked_add(other.0)
            .filter(|value| *value <= MAX_SCALED_DECIMAL)
            .map(Self)
    }

    /// Multiply and round half up to ten decimal places, matching PostgreSQL.
    #[must_use]
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        self.0
            .checked_mul(other.0)?
            .checked_add(DECIMAL_SCALE / 2)
            .and_then(|value| Self::from_scaled(value / DECIMAL_SCALE).ok())
    }

    /// 除以非零整数，保留最多十位小数。
    #[must_use]
    pub fn checked_div_u64(self, divisor: u64) -> Option<Self> {
        let divisor = u128::from(divisor);
        (divisor != 0)
            .then(|| self.0.checked_div(divisor))
            .flatten()
            .and_then(|value| Self::from_scaled(value).ok())
    }

    /// 去尾零的 canonical 字符串，用于 wire 序列化。
    #[must_use]
    pub fn canonical(self) -> String {
        let integer = self.0 / DECIMAL_SCALE;
        let fraction = self.0 % DECIMAL_SCALE;
        if fraction == 0 {
            integer.to_string()
        } else {
            let fraction = format!("{fraction:010}");
            let trimmed = fraction.trim_end_matches('0');
            format!("{integer}.{trimmed}")
        }
    }
}

impl FromStr for Decimal {
    type Err = MeteringError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.starts_with(['-', '+']) {
            return Err(MeteringError::InvalidDecimal);
        }
        let mut parts = value.split('.');
        let integer = parts.next().ok_or(MeteringError::InvalidDecimal)?;
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some()
            || integer.is_empty()
            || integer.len() > 10
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || fraction.len() > 10
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(MeteringError::InvalidDecimal);
        }

        let integer = integer
            .parse::<u128>()
            .map_err(|_| MeteringError::InvalidDecimal)?;
        let fraction = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse::<u128>()
                .map_err(|_| MeteringError::InvalidDecimal)?
                * 10_u128.pow(10_u32.saturating_sub(fraction.len() as u32))
        };
        let scaled = integer
            .checked_mul(DECIMAL_SCALE)
            .and_then(|whole| whole.checked_add(fraction))
            .ok_or(MeteringError::InvalidDecimal)?;
        Self::from_scaled(scaled)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let integer = self.0 / DECIMAL_SCALE;
        let fraction = self.0 % DECIMAL_SCALE;
        write!(formatter, "{integer}.{fraction:010}")
    }
}

/// 三字符大写货币代码。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CurrencyCode([u8; 3]);

impl CurrencyCode {
    /// 校验货币代码。
    ///
    /// # Errors
    ///
    /// 输入不是三个大写 ASCII 字符时返回错误。
    pub fn new(value: &str) -> Result<Self, MeteringError> {
        let bytes = value.as_bytes();
        if bytes.len() != 3 || !bytes.iter().all(u8::is_ascii_uppercase) {
            return Err(MeteringError::InvalidCurrency);
        }
        Ok(Self([bytes[0], bytes[1], bytes[2]]))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl fmt::Display for CurrencyCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// 带货币的非负总金额。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Money {
    amount: Decimal,
    currency: CurrencyCode,
}

impl Money {
    #[must_use]
    pub const fn new(amount: Decimal, currency: CurrencyCode) -> Self {
        Self { amount, currency }
    }

    #[must_use]
    pub const fn amount(self) -> Decimal {
        self.amount
    }

    #[must_use]
    pub const fn currency(self) -> CurrencyCode {
        self.currency
    }

    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        if self.currency != other.currency {
            return None;
        }
        self.amount
            .checked_add(other.amount)
            .map(|amount| Self::new(amount, self.currency))
    }
}

/// `model_requests` 的公共 Token 事实。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub image_input_tokens: Option<u64>,
    pub image_output_tokens: Option<u64>,
    /// Provider/协议报告的独立事实，不从其他列相加推导。
    pub total_tokens: Option<u64>,
}

impl Usage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            cache_write_tokens: None,
            reasoning_tokens: None,
            image_input_tokens: None,
            image_output_tokens: None,
            total_tokens: None,
        }
    }

    /// 合并同一最终上游结果的增量观测；每个字段以较新的非空值为准。
    pub fn merge(&mut self, newer: &Self) {
        if newer.input_tokens.is_some() {
            self.input_tokens = newer.input_tokens;
        }
        if newer.output_tokens.is_some() {
            self.output_tokens = newer.output_tokens;
        }
        if newer.cached_tokens.is_some() {
            self.cached_tokens = newer.cached_tokens;
        }
        if newer.cache_write_tokens.is_some() {
            self.cache_write_tokens = newer.cache_write_tokens;
        }
        if newer.reasoning_tokens.is_some() {
            self.reasoning_tokens = newer.reasoning_tokens;
        }
        if newer.image_input_tokens.is_some() {
            self.image_input_tokens = newer.image_input_tokens;
        }
        if newer.image_output_tokens.is_some() {
            self.image_output_tokens = newer.image_output_tokens;
        }
        if newer.total_tokens.is_some() {
            self.total_tokens = newer.total_tokens;
        }
    }
}

/// 费用金额的来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CostSource {
    ProviderReported,
    Calculated,
    Unavailable,
}

impl CostSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
            Self::Calculated => "calculated",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Provider 或受控代码对当次请求总费用的可信程度。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CostEstimateStatus {
    Known,
    Unknown,
}

impl CostEstimateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Known => "known",
            Self::Unknown => "unknown",
        }
    }
}

/// 单次模型请求的总费用及当次确定的本地计算明细。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostEstimate {
    status: CostEstimateStatus,
    source: CostSource,
    total: Option<Money>,
    breakdown: Option<std::sync::Arc<CalculatedCostBreakdown>>,
}

/// Provider 在单次请求终态上报的实际已计费总额。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProviderReportedCost {
    total: Money,
}

/// Provider 域依据公开单价和实际用量算出的单次总额。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalculatedCost {
    total: Money,
    breakdown: Option<std::sync::Arc<CalculatedCostBreakdown>>,
}

/// Provider 受控价格规则计算出的运行时费用明细。
///
/// 随本地费用事件保存，以免后续改价改变历史明细。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalculatedCostBreakdown {
    image: Option<ImageCostBreakdown>,
    input_amount: Money,
    output_amount: Money,
    cache_read_amount: Money,
    cache_write_amount: Money,
    standard_amount: Money,
    total_amount: Money,
    input_price_per_million: Money,
    output_price_per_million: Money,
    cache_read_price_per_million: Money,
    cache_write_price_per_million: Money,
    service_tier: Option<String>,
    multiplier_percent: u32,
    custom_multiplier_bps: u32,
    custom_multiplier_scope: MultiplierScope,
    standard_components: Option<[Money; 4]>,
}

/// 图像输入是总输入的子集，单独保留其价格，不能伪装成文本平均单价。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageCostBreakdown {
    pub input_tokens: u64,
    pub cached_tokens: u64,
    pub input_amount: Money,
    pub cache_read_amount: Money,
    pub input_price_per_million: Money,
    pub cache_read_price_per_million: Money,
}

/// 一次请求的费用组成，全部使用同一币种。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalculatedCostAmounts {
    input: Money,
    output: Money,
    cache_read: Money,
    cache_write: Money,
    standard: Money,
    total: Money,
}

impl CalculatedCostAmounts {
    #[must_use]
    pub const fn new(
        input: Money,
        output: Money,
        cache_read: Money,
        cache_write: Money,
        standard: Money,
        total: Money,
    ) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write,
            standard,
            total,
        }
    }
}

/// 每百万 Token 的费率组成，全部使用同一币种。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalculatedCostRates {
    input: Money,
    output: Money,
    cache_read: Money,
    cache_write: Money,
}

impl CalculatedCostRates {
    #[must_use]
    pub const fn new(input: Money, output: Money, cache_read: Money, cache_write: Money) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write,
        }
    }
}

impl CalculatedCostBreakdown {
    #[must_use]
    pub const fn new(
        amounts: CalculatedCostAmounts,
        rates: CalculatedCostRates,
        service_tier: Option<String>,
        multiplier_percent: u32,
    ) -> Self {
        Self {
            input_amount: amounts.input,
            image: None,
            output_amount: amounts.output,
            cache_read_amount: amounts.cache_read,
            cache_write_amount: amounts.cache_write,
            standard_amount: amounts.standard,
            total_amount: amounts.total,
            input_price_per_million: rates.input,
            output_price_per_million: rates.output,
            cache_read_price_per_million: rates.cache_read,
            cache_write_price_per_million: rates.cache_write,
            service_tier,
            multiplier_percent,
            custom_multiplier_bps: 10_000,
            custom_multiplier_scope: MultiplierScope::All,
            standard_components: None,
        }
    }

    #[must_use]
    pub fn with_image(mut self, image: ImageCostBreakdown) -> Self {
        self.image = Some(image);
        self
    }

    #[must_use]
    pub const fn image(&self) -> Option<&ImageCostBreakdown> {
        self.image.as_ref()
    }

    #[must_use]
    pub const fn input_amount(&self) -> Money {
        self.input_amount
    }

    #[must_use]
    pub const fn output_amount(&self) -> Money {
        self.output_amount
    }

    #[must_use]
    pub const fn cache_read_amount(&self) -> Money {
        self.cache_read_amount
    }

    #[must_use]
    pub const fn cache_write_amount(&self) -> Money {
        self.cache_write_amount
    }

    #[must_use]
    pub const fn standard_amount(&self) -> Money {
        self.standard_amount
    }

    #[must_use]
    pub const fn total_amount(&self) -> Money {
        self.total_amount
    }

    #[must_use]
    pub const fn input_price_per_million(&self) -> Money {
        self.input_price_per_million
    }

    #[must_use]
    pub const fn output_price_per_million(&self) -> Money {
        self.output_price_per_million
    }

    #[must_use]
    pub const fn cache_read_price_per_million(&self) -> Money {
        self.cache_read_price_per_million
    }

    #[must_use]
    pub const fn cache_write_price_per_million(&self) -> Money {
        self.cache_write_price_per_million
    }

    #[must_use]
    pub fn service_tier(&self) -> Option<&str> {
        self.service_tier.as_deref()
    }

    #[must_use]
    pub const fn multiplier_percent(&self) -> u32 {
        self.multiplier_percent
    }

    #[must_use]
    pub fn calculated_cost(&self) -> CalculatedCost {
        CalculatedCost {
            total: self.total_amount,
            breakdown: Some(std::sync::Arc::new(self.clone())),
        }
    }

    #[must_use]
    pub const fn custom_multiplier_bps(&self) -> u32 {
        self.custom_multiplier_bps
    }

    #[must_use]
    pub const fn custom_multiplier_scope(&self) -> &MultiplierScope {
        &self.custom_multiplier_scope
    }

    #[must_use]
    pub fn with_standard_components(mut self, amounts: [Money; 4]) -> Self {
        self.standard_components = Some(amounts);
        self
    }
    #[must_use]
    pub const fn standard_components(&self) -> Option<&[Money; 4]> {
        self.standard_components.as_ref()
    }

    /// 统一调整金额和有效单价，保留服务档位倍率的独立含义。
    #[must_use]
    pub fn with_custom_multiplier(mut self, bps: u32, scope: MultiplierScope) -> Option<Self> {
        if bps > 1_000_000 || scope.validate().is_err() {
            return None;
        }
        let scale = |money: Money, selected: bool| {
            if !selected {
                return Some(money);
            }
            let ticks = money
                .amount()
                .scaled()
                .checked_mul(u128::from(bps))?
                .checked_add(5_000)?
                .checked_div(10_000)?;
            Some(Money::new(
                Decimal::from_scaled(ticks).ok()?,
                money.currency(),
            ))
        };
        let previous_total = self.total_amount;
        let same_standard = self.standard_amount == previous_total;
        if self.standard_components.is_none() && same_standard {
            let sum = |a: Money, b: Option<Money>| -> Option<Money> {
                Some(Money::new(
                    Decimal::from_scaled(
                        a.amount()
                            .scaled()
                            .checked_add(b.map_or(0, |v| v.amount().scaled()))?,
                    )
                    .ok()?,
                    a.currency(),
                ))
            };
            self.standard_components = Some([
                sum(
                    self.input_amount,
                    self.image.as_ref().map(|i| i.input_amount),
                )?,
                self.output_amount,
                sum(
                    self.cache_read_amount,
                    self.image.as_ref().map(|i| i.cache_read_amount),
                )?,
                self.cache_write_amount,
            ]);
        }
        let mut token_total = self
            .input_amount
            .amount()
            .scaled()
            .checked_add(self.output_amount.amount().scaled())?
            .checked_add(self.cache_read_amount.amount().scaled())?
            .checked_add(self.cache_write_amount.amount().scaled())?;
        if let Some(image) = &mut self.image {
            token_total = token_total
                .checked_add(image.input_amount.amount().scaled())?
                .checked_add(image.cache_read_amount.amount().scaled())?;
            image.input_amount = scale(image.input_amount, scope.includes(CostComponent::Input))?;
            image.cache_read_amount = scale(
                image.cache_read_amount,
                scope.includes(CostComponent::CacheRead),
            )?;
            image.input_price_per_million = scale(
                image.input_price_per_million,
                scope.includes(CostComponent::Input),
            )?;
            image.cache_read_price_per_million = scale(
                image.cache_read_price_per_million,
                scope.includes(CostComponent::CacheRead),
            )?;
        }
        let other = Money::new(
            Decimal::from_scaled(previous_total.amount().scaled().checked_sub(token_total)?)
                .ok()?,
            previous_total.currency(),
        );
        self.input_amount = scale(self.input_amount, scope.includes(CostComponent::Input))?;
        self.output_amount = scale(self.output_amount, scope.includes(CostComponent::Output))?;
        self.cache_read_amount = scale(
            self.cache_read_amount,
            scope.includes(CostComponent::CacheRead),
        )?;
        self.cache_write_amount = scale(
            self.cache_write_amount,
            scope.includes(CostComponent::CacheWrite),
        )?;
        if let Some(components) = &mut self.standard_components {
            let original_sum = components.iter().try_fold(0u128, |sum, amount| {
                sum.checked_add(amount.amount().scaled())
            })?;
            let other = Money::new(
                Decimal::from_scaled(
                    self.standard_amount
                        .amount()
                        .scaled()
                        .checked_sub(original_sum)?,
                )
                .ok()?,
                self.standard_amount.currency(),
            );
            for (amount, component) in components.iter_mut().zip([
                CostComponent::Input,
                CostComponent::Output,
                CostComponent::CacheRead,
                CostComponent::CacheWrite,
            ]) {
                *amount = scale(*amount, scope.includes(component))?;
            }
            let total = components.iter().try_fold(
                scale(other, scope.is_all())?.amount().scaled(),
                |sum, amount| sum.checked_add(amount.amount().scaled()),
            )?;
            self.standard_amount = Money::new(
                Decimal::from_scaled(total).ok()?,
                self.standard_amount.currency(),
            );
        } else if scope.is_all() {
            self.standard_amount = scale(self.standard_amount, true)?;
        } else {
            return None;
        }
        // 各费用项独立四舍五入后求和，防止明细合计与账本金额相差一个 tick。
        let mut total = self
            .input_amount
            .amount()
            .scaled()
            .checked_add(self.output_amount.amount().scaled())?
            .checked_add(self.cache_read_amount.amount().scaled())?
            .checked_add(self.cache_write_amount.amount().scaled())?
            .checked_add(scale(other, scope.is_all())?.amount().scaled())?;
        if let Some(image) = &self.image {
            total = total
                .checked_add(image.input_amount.amount().scaled())?
                .checked_add(image.cache_read_amount.amount().scaled())?;
        }
        self.total_amount =
            Money::new(Decimal::from_scaled(total).ok()?, previous_total.currency());
        if same_standard {
            self.standard_amount = self.total_amount;
        }
        self.input_price_per_million = scale(
            self.input_price_per_million,
            scope.includes(CostComponent::Input),
        )?;
        self.output_price_per_million = scale(
            self.output_price_per_million,
            scope.includes(CostComponent::Output),
        )?;
        self.cache_read_price_per_million = scale(
            self.cache_read_price_per_million,
            scope.includes(CostComponent::CacheRead),
        )?;
        self.cache_write_price_per_million = scale(
            self.cache_write_price_per_million,
            scope.includes(CostComponent::CacheWrite),
        )?;
        self.custom_multiplier_bps = bps;
        self.custom_multiplier_scope = scope;
        Some(self)
    }
}

impl ProviderReportedCost {
    /// xAI 等 Provider 的 USD ticks 可直接传入；1 USD = 10^10 ticks。
    ///
    /// # Errors
    ///
    /// ticks 超出数据库 `numeric(20, 10)` 范围时失败。
    pub fn from_usd_ticks(ticks: u128) -> Result<Self, MeteringError> {
        Ok(Self {
            total: Money::new(Decimal::from_scaled(ticks)?, CurrencyCode(*b"USD")),
        })
    }

    #[must_use]
    pub const fn total(self) -> Money {
        self.total
    }

    #[must_use]
    pub const fn into_estimate(self) -> CostEstimate {
        CostEstimate {
            status: CostEstimateStatus::Known,
            source: CostSource::ProviderReported,
            total: Some(self.total),
            breakdown: None,
        }
    }
}

impl CalculatedCost {
    /// 从精确 USD ticks 创建本地计算费用；1 USD = 10^10 ticks。
    ///
    /// # Errors
    ///
    /// ticks 超出数据库 `numeric(20, 10)` 范围时失败。
    pub fn from_usd_ticks(ticks: u128) -> Result<Self, MeteringError> {
        Ok(Self {
            total: Money::new(Decimal::from_scaled(ticks)?, CurrencyCode(*b"USD")),
            breakdown: None,
        })
    }

    #[must_use]
    pub const fn total(&self) -> Money {
        self.total
    }

    #[must_use]
    pub fn into_estimate(self) -> CostEstimate {
        CostEstimate {
            status: CostEstimateStatus::Known,
            source: CostSource::Calculated,
            total: Some(self.total),
            breakdown: self.breakdown,
        }
    }
}

impl CostEstimate {
    #[must_use]
    pub fn breakdown(&self) -> Option<&CalculatedCostBreakdown> {
        self.breakdown.as_deref()
    }

    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            status: CostEstimateStatus::Unknown,
            source: CostSource::Unavailable,
            total: None,
            breakdown: None,
        }
    }

    #[must_use]
    pub const fn status(&self) -> CostEstimateStatus {
        self.status
    }

    #[must_use]
    pub const fn source(&self) -> CostSource {
        self.source
    }

    #[must_use]
    pub const fn total(&self) -> Option<Money> {
        self.total
    }
}
