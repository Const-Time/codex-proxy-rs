//! 将耗尽窗口的恢复基准与最新用量分开持久化，允许各窗口分批恢复。

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use gateway_core::account::{QuotaAccessState, QuotaEvidence, QuotaState};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{CodexAccountQuotaSnapshot, CodexCredentialQuotaError, CodexQuotaWindow};

pub(super) const RECOVERY_FIELD: &str = "_quota_recovery";
const RESET_RECOVERY_MAX_USED_PERCENT: f64 = 10.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct QuotaRecovery {
    // 绑定本次耗尽事实，避免真实推理成功后再次耗尽时复用上次的恢复进度。
    exhausted_at_micros: i64,
    pending: BTreeMap<String, Option<DateTime<Utc>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unmatched: Option<UnmatchedQuotaReset>,
    // 已连续观测到"未触顶"的待解除窗口；第二次确认后解除。旧文档没有该
    // 字段时反序列化为空集；旧版 null 候选仍可读取，但需重新确认 reset。
    // 记录候选的 reset，防止不同窗口的两次观测被误当作连续确认。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    candidates: BTreeMap<String, Option<DateTime<Utc>>>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    allowed_candidates: BTreeSet<String>,
}

// A rejection supplied a review deadline but its window could not be identified.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UnmatchedQuotaReset {
    reset_at: DateTime<Utc>,
    window_keys: BTreeSet<String>,
}

impl UnmatchedQuotaReset {
    fn recovered(&self, refreshed: &CodexAccountQuotaSnapshot) -> bool {
        let windows = refreshed.windows().iter().filter(|w| w.is_account_wide());
        SystemTime::from(self.reset_at) <= refreshed.observed_at()
            && refreshed.quota().access() == QuotaAccessState::Allowed
            && windows.clone().next().is_some()
            && windows
                .clone()
                .all(|w| w.used_percent().is_some() && !w.limit_reached())
            && self
                .window_keys
                .iter()
                .all(|key| windows.clone().any(|w| w.key() == key))
    }
}

impl QuotaRecovery {
    fn new(state: QuotaState, baseline: &CodexAccountQuotaSnapshot) -> Self {
        let windows = || baseline.windows().iter().filter(|w| w.is_account_wide());
        let mut pending = BTreeMap::new();
        for window in windows() {
            if window.limit_reached() {
                pending.insert(window.key().to_owned(), window.reset_at());
            }
        }
        // 旧版从 primary 派生的账号 reset 不能覆盖明确的周窗口耗尽事实。
        // 真实 usage_limit_reached 错误则可以补充尚未反映到百分比的耗尽窗口。
        let error_deadline = state
            .reset_at()
            .filter(|at| {
                state.evidence() == Some(QuotaEvidence::UsageLimitReached)
                    && state.observed_at().is_some_and(|denied| *at > denied)
            })
            .map(DateTime::<Utc>::from);
        let hinted = baseline.denied_window(state);
        let conflicting_reset = hinted
            .and_then(CodexQuotaWindow::reset_at)
            .zip(error_deadline)
            .is_some_and(|(snapshot, error)| (snapshot.timestamp() - error.timestamp()).abs() > 1);
        if let Some(window) = hinted {
            if !conflicting_reset {
                pending.insert(
                    window.key().to_owned(),
                    window.reset_at().or(error_deadline),
                );
            }
        } else if pending.is_empty() || state.evidence() == Some(QuotaEvidence::UsageLimitReached) {
            for window in windows() {
                // 错误与 usage 的秒级 reset 可能相差一秒；只用它识别窗口，
                // 后续推进比较始终使用该窗口自己的 reset。
                let matches_error_reset = window.reset_at().zip(state.reset_at()).is_some_and(
                    |(window_reset, error_reset)| {
                        (window_reset.timestamp() - DateTime::<Utc>::from(error_reset).timestamp())
                            .abs()
                            <= 1
                    },
                );
                if matches_error_reset {
                    pending.insert(window.key().to_owned(), window.reset_at());
                }
            }
        }
        let unmatched = error_deadline
            .filter(|_| pending.is_empty() || conflicting_reset)
            .map(|reset_at| UnmatchedQuotaReset {
                reset_at,
                window_keys: windows().map(|w| w.key().to_owned()).collect(),
            });
        if pending.is_empty() && unmatched.is_none() {
            pending.extend(windows().map(|w| (w.key().to_owned(), w.reset_at())));
        }
        Self {
            exhausted_at_micros: exhaustion_time(state),
            pending,
            unmatched,
            candidates: BTreeMap::new(),
            allowed_candidates: BTreeSet::new(),
        }
    }

    fn observe(&mut self, refreshed: &CodexAccountQuotaSnapshot, state: QuotaState) -> bool {
        let windows = || refreshed.windows().iter().filter(|w| w.is_account_wide());
        let access = refreshed.quota().access();
        let has_window_hint = state.limit_window_minutes().is_some();
        let hinted_window_reported = state.limit_window_minutes().is_none_or(|minutes| {
            windows().any(|window| {
                window.window_seconds() == Some(u64::from(minutes) * 60)
                    && window.used_percent().is_some()
            })
        });
        if has_window_hint && access != QuotaAccessState::Allowed {
            self.candidates.clear();
            self.allowed_candidates.clear();
            for window in windows().filter(|w| w.limit_reached()) {
                self.pending
                    .entry(window.key().to_owned())
                    .or_insert(window.reset_at());
            }
            return false;
        }
        let had_baseline = self.unmatched.is_some() || !self.pending.is_empty();
        self.unmatched
            .take_if(|reset| hinted_window_reported && reset.recovered(refreshed));

        // A fresh explicit allow can supersede a stale percentage once all known windows are due.
        if !self.pending.is_empty()
            && access == QuotaAccessState::Allowed
            && self.pending.iter().all(|(key, reset)| {
                let reported = !has_window_hint
                    || windows().any(|w| w.key() == key && w.used_percent().is_some());
                reported
                    && reset.is_some_and(|reset| {
                        reset.timestamp_micros() > self.exhausted_at_micros
                            && SystemTime::from(reset) <= refreshed.observed_at()
                    })
            })
        {
            self.pending.clear();
            self.candidates.clear();
            self.allowed_candidates.clear();
            return hinted_window_reported && self.unmatched.is_none();
        }

        let mut candidates = BTreeMap::new();
        let mut allowed_candidates = BTreeSet::new();
        for window in windows() {
            let key = window.key();
            let Some(previous) = self.pending.get(key).copied() else {
                if window.limit_reached() {
                    self.pending.insert(key.to_owned(), window.reset_at());
                }
                continue;
            };
            let Some(reset) = window.reset_at() else {
                continue;
            };
            let Some(previous) = previous else {
                if !has_window_hint || window.used_percent().is_some() {
                    self.pending.insert(key.to_owned(), Some(reset));
                }
                continue;
            };
            let Some(used) = window.used_percent() else {
                continue;
            };
            if window.limit_reached() {
                continue;
            }
            if reset > previous && used < RESET_RECOVERY_MAX_USED_PERCENT {
                self.pending.remove(key);
                continue;
            }
            match access {
                QuotaAccessState::Allowed => {
                    if self.allowed_candidates.contains(key) {
                        self.pending.remove(key);
                    } else {
                        allowed_candidates.insert(key.to_owned());
                    }
                }
                QuotaAccessState::Unknown if reset >= previous => {
                    if self.candidates.get(key) == Some(&Some(reset)) {
                        self.pending.remove(key);
                    } else {
                        candidates.insert(key.to_owned(), Some(reset));
                    }
                }
                _ => {}
            }
        }
        self.candidates = candidates;
        self.allowed_candidates = allowed_candidates;
        had_baseline
            && hinted_window_reported
            && self.pending.is_empty()
            && self.unmatched.is_none()
    }
}

fn exhaustion_time(state: QuotaState) -> i64 {
    DateTime::<Utc>::from(state.observed_at().unwrap_or(SystemTime::UNIX_EPOCH)).timestamp_micros()
}

/// Reconcile the latest authoritative usage with the persisted exhaustion baseline.
pub(super) fn reconcile_refresh(
    current: QuotaState,
    auto_use_credits: bool,
    refreshed: &mut CodexAccountQuotaSnapshot,
    previous: Option<&CodexAccountQuotaSnapshot>,
    document: &mut Map<String, Value>,
) -> Result<QuotaState, CodexCredentialQuotaError> {
    document.remove(RECOVERY_FIELD);
    // 点数已不再可用时，旧点数依据不能随下一次开关切换复活
    let state = if current.evidence() == Some(QuotaEvidence::CreditsAvailable) {
        QuotaState::exhausted(
            QuotaEvidence::AccountLimitReached,
            current.observed_at().unwrap_or(refreshed.observed_at()),
            current.reset_at(),
        )
        .merge_observation(refreshed.quota())
    } else if current.is_exhausted() {
        current
    } else {
        current.merge_observation(refreshed.quota())
    };
    if !state.is_exhausted() {
        return Ok(state);
    }
    let mut recovery = previous
        .and_then(|previous| previous.recovery.as_ref())
        .filter(|recovery| recovery.exhausted_at_micros == exhaustion_time(state))
        .cloned()
        .unwrap_or_else(|| QuotaRecovery::new(state, previous.unwrap_or(refreshed)));
    if recovery.pending.is_empty() && recovery.unmatched.is_none() {
        recovery = QuotaRecovery::new(state, refreshed);
    }
    if current.is_exhausted() && recovery.observe(refreshed, state) {
        refreshed.recovery = None;
        return Ok(QuotaState::allowed(refreshed.observed_at()));
    }
    let reset_at = recovery
        .unmatched
        .as_ref()
        .map(|reset| reset.reset_at)
        .or_else(|| recovery.pending.values().filter_map(|reset| *reset).min())
        .map(SystemTime::from)
        .or(state.reset_at());
    let state = refreshed.access_with_credits(
        QuotaState::exhausted(
            state
                .evidence()
                .unwrap_or(QuotaEvidence::AccountLimitReached),
            state.observed_at().unwrap_or(refreshed.observed_at()),
            reset_at,
        )
        .with_limit_window_minutes(state.limit_window_minutes()),
        auto_use_credits,
    );
    if !state.is_exhausted() {
        refreshed.recovery = None;
        return Ok(state);
    }
    document.insert(
        RECOVERY_FIELD.to_owned(),
        serde_json::to_value(&recovery)
            .map_err(|_| CodexCredentialQuotaError::InvalidCredentialData)?,
    );
    refreshed.recovery = Some(recovery);
    Ok(state)
}
