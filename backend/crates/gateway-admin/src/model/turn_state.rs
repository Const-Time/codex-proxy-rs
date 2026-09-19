//! 管理员配置的轮次状态维护策略。这里只解析封装，不验证上游签名或推断模型能力。

use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use serde::{Deserialize, Serialize};

use super::AdminError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStatePolicy {
    pub enabled: bool,
    pub header_length: usize,
    pub cipher_blocks: usize,
    pub ttl_seconds: i64,
    pub refresh_before_seconds: i64,
    pub minimum_remaining_seconds: i64,
    pub concurrency: usize,
    pub max_attempts: usize,
    pub timeout_seconds: u64,
    pub retry_seconds: i64,
}

impl Default for TurnStatePolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            header_length: 0,
            cipher_blocks: 0,
            ttl_seconds: 3600,
            refresh_before_seconds: 600,
            minimum_remaining_seconds: 900,
            concurrency: 1,
            max_attempts: 3,
            timeout_seconds: 30,
            retry_seconds: 60,
        }
    }
}

impl TurnStatePolicy {
    pub fn validate(&self) -> Result<(), AdminError> {
        if !((self.header_length == 0 && self.cipher_blocks == 0)
            || ((100..=4096).contains(&self.header_length)
                && (1..=250).contains(&self.cipher_blocks)))
            || !(60..=86400).contains(&self.ttl_seconds)
            || self.refresh_before_seconds < 10
            || self.refresh_before_seconds >= self.ttl_seconds
            || self.minimum_remaining_seconds <= self.refresh_before_seconds
            || self.minimum_remaining_seconds >= self.ttl_seconds
            || !(1..=4).contains(&self.concurrency)
            || !(1..=10).contains(&self.max_attempts)
            || !(5..=60).contains(&self.timeout_seconds)
            || !(30..=3600).contains(&self.retry_seconds)
        {
            return Err(AdminError::invalid("时效、长度、并发或刷新预算不合法"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FernetShape {
    pub header_length: usize,
    pub issued_at: i64,
    pub total_bytes: usize,
    pub cipher_bytes: usize,
    pub blocks: usize,
}

impl FernetShape {
    /// 不透明 token 的结构检查；返回成功不代表 MAC 正确。
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.len() > 4096 || !value.is_ascii() {
            return None;
        }
        let raw = URL_SAFE
            .decode(value)
            .or_else(|_| URL_SAFE_NO_PAD.decode(value))
            .ok()?;
        if raw.len() < 73 || raw[0] != 0x80 {
            return None;
        }
        let cipher_bytes = raw.len().checked_sub(57)?;
        if cipher_bytes % 16 != 0 {
            return None;
        }
        let issued_at = i64::try_from(u64::from_be_bytes(raw[1..9].try_into().ok()?)).ok()?;
        if !(1_577_836_800..4_102_444_800).contains(&issued_at) {
            return None;
        }
        Some(Self {
            header_length: value.len(),
            issued_at,
            total_bytes: raw.len(),
            cipher_bytes,
            blocks: cipher_bytes / 16,
        })
    }

    pub fn candidate_expiry(&self, policy: &TurnStatePolicy, now: i64) -> Option<i64> {
        let expires = self.issued_at.checked_add(policy.ttl_seconds)?;
        let shape_matches = if policy.header_length == 0 && policy.cipher_blocks == 0 {
            matches!((self.header_length, self.blocks), (292, 10) | (332, 12))
        } else {
            self.header_length == policy.header_length && self.blocks == policy.cipher_blocks
        };
        (shape_matches
            && self.issued_at <= now + 60
            && expires - now >= policy.minimum_remaining_seconds)
            .then_some(expires)
    }
}

/// 秘密仅出现在提交命令中，不允许 Debug 派生。
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStatePoolInput {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub mode: String,
    /// 编辑时 None 表示保留；空字符串不表示保留。
    pub endpoint: Option<String>,
    pub bearer: Option<String>,
    pub json_pointer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateTargetInput {
    pub account_id: String,
    pub model: String,
    pub pool_id: String,
    pub enabled: bool,
}

/// 两个开关互相独立；不为未配置账号隐式开启。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateAccountPolicy {
    pub account_id: String,
    #[serde(default)]
    pub fingerprint_convergence: bool,
    #[serde(default)]
    pub takeover: bool,
    #[serde(default)]
    pub maintenance: TurnStateMaintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TurnStateMaintenance {
    pub max_probes_per_hour: u32,
    /// Zero disables the idle gate for manually selected models.
    pub idle_seconds: i64,
    pub auto_models: bool,
    pub pool_id: Option<String>,
}

impl Default for TurnStateMaintenance {
    fn default() -> Self {
        Self {
            max_probes_per_hour: 30,
            idle_seconds: 3600,
            auto_models: false,
            pool_id: None,
        }
    }
}

impl TurnStateMaintenance {
    pub fn validate(&self) -> Result<(), AdminError> {
        if !(1..=600).contains(&self.max_probes_per_hour)
            || !(0..=86400).contains(&self.idle_seconds)
            || (self.auto_models && self.pool_id.as_deref().is_none_or(str::is_empty))
        {
            return Err(AdminError::invalid(
                "小时预算须为 1–600，空闲窗口须为 0–86400 秒；自动模型需指定代理池",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateAccountUpdate {
    pub revision: u64,
    pub fingerprint_convergence: bool,
    pub takeover: bool,
    /// Omission by older clients preserves the existing maintenance policy.
    #[serde(default)]
    pub maintenance: Option<TurnStateMaintenance>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateSettingsInput {
    pub revision: u64,
    pub policy: TurnStatePolicy,
    pub pools: Vec<TurnStatePoolInput>,
    pub targets: Vec<TurnStateTargetInput>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStatePoolView {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub mode: String,
    pub endpoint: String,
    pub has_secret: bool,
    pub json_pointer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateObservation {
    pub at: i64,
    pub direction: String,
    pub fingerprint: String,
    pub shape: Option<FernetShape>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateTargetView {
    #[serde(flatten)]
    pub target: TurnStateTargetInput,
    pub status: String,
    pub fingerprint: Option<String>,
    pub issued_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub next_probe_at: i64,
    pub last_message: String,
    pub history: Vec<TurnStateObservation>,
    pub candidate_count: usize,
    pub takeover: bool,
    pub wait_reason: String,
    pub hourly_used: u32,
    pub hourly_limit: u32,
    pub budget_resets_at: Option<i64>,
    pub last_traffic_at: Option<i64>,
    pub automatic: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateView {
    pub revision: u64,
    pub policy: TurnStatePolicy,
    pub pools: Vec<TurnStatePoolView>,
    pub targets: Vec<TurnStateTargetView>,
    pub accounts: Vec<TurnStateAccountPolicy>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStatePoolTest {
    pub success: bool,
    pub ipv6: bool,
    pub exit_ip: Option<String>,
    pub ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub message: String,
}

/// Internal system requests: no user/key billing and no arbitrary raw response log.
pub struct TurnStateProbeStart {
    pub id: String,
    pub account_id: String,
    pub model: String,
    pub phase: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub timeout_seconds: u64,
}

#[derive(Default)]
pub struct TurnStateProbeResult {
    pub succeeded: bool,
    pub status: Option<u16>,
    pub request_id: Option<String>,
    pub response_id: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub latency_ms: u64,
    pub message: Option<String>,
    pub sent_state: Option<String>,
    pub returned_state: Option<String>,
}
