//! Local scheduling limits; upstream quota facts remain unchanged.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "WireLimits")]
pub struct AccountQuotaUsageLimits {
    short_term_percent: u8,
    weekly_percent: u8,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireLimits {
    short_term_percent: u8,
    weekly_percent: u8,
}

impl TryFrom<WireLimits> for AccountQuotaUsageLimits {
    type Error = &'static str;

    fn try_from(value: WireLimits) -> Result<Self, Self::Error> {
        Self::new(value.short_term_percent, value.weekly_percent)
    }
}

impl Default for AccountQuotaUsageLimits {
    fn default() -> Self {
        Self {
            short_term_percent: 100,
            weekly_percent: 100,
        }
    }
}

impl AccountQuotaUsageLimits {
    pub const UNLIMITED: Self = Self {
        short_term_percent: 100,
        weekly_percent: 100,
    };
    /// Each limit is an integer percentage; 100 keeps upstream's normal policy.
    pub fn new(short_term_percent: u8, weekly_percent: u8) -> Result<Self, &'static str> {
        if !(1..=100).contains(&short_term_percent) || !(1..=100).contains(&weekly_percent) {
            return Err("quota usage limits must be integers between 1 and 100");
        }
        Ok(Self {
            short_term_percent,
            weekly_percent,
        })
    }

    #[must_use]
    pub const fn short_term_percent(self) -> u8 {
        self.short_term_percent
    }

    #[must_use]
    pub const fn weekly_percent(self) -> u8 {
        self.weekly_percent
    }
}
