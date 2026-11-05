//! Sum independently metered native responses, never reprice their combined token count.
use super::{CalculatedCostBreakdown, CostEstimate, CostEstimateStatus, Usage};
use std::sync::Arc;

impl Usage {
    pub fn add_responses(&self, other: &Self) -> Self {
        fn add(a: Option<u64>, b: Option<u64>) -> Option<u64> {
            match (a, b) {
                (Some(a), Some(b)) => a.checked_add(b),
                (a, b) => a.or(b),
            }
        }
        Self {
            input_tokens: add(self.input_tokens, other.input_tokens),
            output_tokens: add(self.output_tokens, other.output_tokens),
            cached_tokens: add(self.cached_tokens, other.cached_tokens),
            cache_write_tokens: add(self.cache_write_tokens, other.cache_write_tokens),
            reasoning_tokens: add(self.reasoning_tokens, other.reasoning_tokens),
            image_input_tokens: add(self.image_input_tokens, other.image_input_tokens),
            image_output_tokens: add(self.image_output_tokens, other.image_output_tokens),
            total_tokens: add(self.total_tokens, other.total_tokens),
        }
    }
}

impl CostEstimate {
    pub fn add_responses(&self, other: &Self) -> Self {
        let Some(total) = self
            .total
            .zip(other.total)
            .and_then(|(a, b)| a.checked_add(b))
        else {
            return Self::unavailable();
        };
        if self.source != other.source {
            return Self::unavailable();
        }
        Self {
            status: CostEstimateStatus::Known,
            source: self.source,
            total: Some(total),
            breakdown: self
                .breakdown()
                .zip(other.breakdown())
                .and_then(|(a, b)| a.add_responses(b))
                .map(Arc::new),
        }
    }
}

impl CalculatedCostBreakdown {
    fn add_responses(&self, other: &Self) -> Option<Self> {
        // A single displayed rate is only meaningful when both responses used it.
        if self.input_price_per_million != other.input_price_per_million
            || self.output_price_per_million != other.output_price_per_million
            || self.cache_read_price_per_million != other.cache_read_price_per_million
            || self.cache_write_price_per_million != other.cache_write_price_per_million
            || self.service_tier != other.service_tier
            || self.multiplier_percent != other.multiplier_percent
            || self.custom_multiplier_bps != other.custom_multiplier_bps
            || self.custom_multiplier_scope != other.custom_multiplier_scope
        {
            return None;
        }
        let mut sum = self.clone();
        sum.input_amount = self.input_amount.checked_add(other.input_amount)?;
        sum.output_amount = self.output_amount.checked_add(other.output_amount)?;
        sum.cache_read_amount = self
            .cache_read_amount
            .checked_add(other.cache_read_amount)?;
        sum.cache_write_amount = self
            .cache_write_amount
            .checked_add(other.cache_write_amount)?;
        sum.standard_amount = self.standard_amount.checked_add(other.standard_amount)?;
        sum.total_amount = self.total_amount.checked_add(other.total_amount)?;
        sum.image = match (&self.image, &other.image) {
            (None, None) => None,
            (Some(image), None) | (None, Some(image)) => Some(image.clone()),
            (Some(a), Some(b)) => {
                if a.input_price_per_million != b.input_price_per_million
                    || a.cache_read_price_per_million != b.cache_read_price_per_million
                {
                    return None;
                }
                let mut image = a.clone();
                image.input_tokens = a.input_tokens.checked_add(b.input_tokens)?;
                image.cached_tokens = a.cached_tokens.checked_add(b.cached_tokens)?;
                image.input_amount = a.input_amount.checked_add(b.input_amount)?;
                image.cache_read_amount = a.cache_read_amount.checked_add(b.cache_read_amount)?;
                Some(image)
            }
        };
        sum.standard_components = match (self.standard_components, other.standard_components) {
            (None, None) => None,
            (Some(a), Some(b)) => Some([
                a[0].checked_add(b[0])?,
                a[1].checked_add(b[1])?,
                a[2].checked_add(b[2])?,
                a[3].checked_add(b[3])?,
            ]),
            _ => return None,
        };
        Some(sum)
    }
}
