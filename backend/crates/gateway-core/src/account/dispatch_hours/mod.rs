use std::time::SystemTime;

use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "HoursWire")]
pub struct AccountDispatchHours {
    enabled: bool,
    time_zone: Tz,
    windows: Vec<Window>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HoursWire {
    enabled: bool,
    time_zone: Tz,
    windows: Vec<Window>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Window {
    #[serde(with = "hhmm")]
    start: NaiveTime,
    #[serde(with = "hhmm")]
    end: NaiveTime,
}

mod hhmm;

impl TryFrom<HoursWire> for AccountDispatchHours {
    type Error = &'static str;

    fn try_from(wire: HoursWire) -> Result<Self, Self::Error> {
        if wire.windows.len() > 16 || (wire.enabled && wire.windows.is_empty()) {
            return Err("dispatch hours require 1 to 16 windows when enabled");
        }
        if wire.windows.iter().any(|window| window.start == window.end) {
            return Err("dispatch window start and end must differ");
        }
        Ok(Self {
            enabled: wire.enabled,
            time_zone: wire.time_zone,
            windows: wire.windows,
        })
    }
}

impl Default for AccountDispatchHours {
    fn default() -> Self {
        Self::DISABLED
    }
}

impl AccountDispatchHours {
    pub const DISABLED: Self = Self {
        enabled: false,
        time_zone: chrono_tz::Asia::Shanghai,
        windows: Vec::new(),
    };

    #[must_use]
    pub fn allows_at(&self, now: SystemTime) -> bool {
        if !self.enabled {
            return true;
        }
        let local = DateTime::<Utc>::from(now)
            .with_timezone(&self.time_zone)
            .time();
        self.windows.iter().any(|window| {
            if window.start < window.end {
                window.start <= local && local < window.end
            } else {
                window.start <= local || local < window.end
            }
        })
    }
}
