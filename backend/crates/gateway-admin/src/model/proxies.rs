//! 可复用的账号出口配置，以及脱敏后的连通性测试结果。

use chrono::{DateTime, Utc};
use gateway_core::account::OutboundProxy;

use super::{PageSize, Revision, account_groups::AccountGroupRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountProxySelection {
    Direct,
    Url(OutboundProxy),
    Saved(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportProxyBinding {
    pub id: String,
    pub proxy: OutboundProxy,
}

#[derive(Debug, Clone)]
pub struct ProxyListQuery {
    pub page: u32,
    pub page_size: PageSize,
    pub search: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyAccountRef {
    pub id: String,
    pub name: String,
    pub email: Option<String>,
    pub provider_kind: String,
    pub authentication_kind: String,
    pub plan_type: Option<String>,
    pub plan_type_display: Option<String>,
    pub groups: Vec<AccountGroupRef>,
    pub enabled: bool,
}

/// 按代理查询关联账号，分页与搜索均在存储层执行。
#[derive(Debug, Clone)]
pub struct ProxyAccountListQuery {
    pub proxy_id: String,
    pub page: u32,
    pub page_size: PageSize,
    pub search: String,
}

#[derive(Debug, Clone)]
pub struct ProxyAccountPage {
    pub items: Vec<ProxyAccountRef>,
    pub total: u64,
    pub page: u32,
    pub page_size: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyTestResult {
    pub success: bool,
    pub latency_ms: u64,
    pub exit_ip: Option<std::net::IpAddr>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ProxyRecord {
    pub id: String,
    pub name: String,
    pub proxy: OutboundProxy,
    pub revision: Revision,
    pub account_count: u64,
    pub last_test_at: Option<DateTime<Utc>>,
    pub last_test: Option<ProxyTestResult>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ProxyPage {
    pub items: Vec<ProxyRecord>,
    pub total: u64,
    pub page: u32,
    pub page_size: u16,
}

#[derive(Debug, Clone)]
pub struct NewProxy {
    pub name: String,
    pub proxy: OutboundProxy,
}

#[derive(Debug, Clone)]
pub struct UpdateProxy {
    pub id: String,
    pub revision: Revision,
    pub name: String,
    pub proxy: Option<OutboundProxy>,
}

#[derive(Debug, Clone)]
pub struct ProxyMutation {
    pub config_revision: Revision,
    pub record: ProxyRecord,
}

/// 探测状态只描述当前网络路径，不代表账号授权或 IP 信誉。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyQualityStatus {
    Passed,
    Warning,
    Failed,
    Challenge,
}

#[derive(Debug, Clone)]
pub struct ProxyQualityCheck {
    pub name: String,
    pub status: ProxyQualityStatus,
    pub http_status: Option<u16>,
    pub latency_ms: u64,
    pub message: String,
}

impl ProxyQualityCheck {
    /// HTTP 401/405 能证明目标可达，不能证明模型调用成功。
    pub fn http(
        name: &str,
        status: u16,
        challenge: bool,
        websocket: bool,
        latency_ms: u64,
    ) -> Self {
        let (state, message) = if challenge {
            (ProxyQualityStatus::Challenge, "目标返回人机验证挑战")
        } else if websocket && status == 101 {
            (
                ProxyQualityStatus::Passed,
                "WebSocket 握手成功，未发送模型请求",
            )
        } else if status == 407 {
            (ProxyQualityStatus::Failed, "代理认证失败")
        } else if websocket {
            (
                ProxyQualityStatus::Warning,
                "目标已响应，但未完成 WebSocket 升级；需结合账号测试确认",
            )
        } else if (200..300).contains(&status) || matches!(status, 401 | 405) {
            (
                ProxyQualityStatus::Passed,
                "目标可达；不代表账号授权或模型调用成功",
            )
        } else if status >= 500 {
            (ProxyQualityStatus::Failed, "目标服务返回错误")
        } else {
            (
                ProxyQualityStatus::Warning,
                "目标已响应，但存在访问限制或重定向",
            )
        };
        Self {
            name: name.to_owned(),
            status: state,
            http_status: Some(status),
            latency_ms,
            message: message.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProxyQualityReport {
    pub tested_at: DateTime<Utc>,
    pub duration_ms: u64,
    pub basic: ProxyTestResult,
    pub checks: Vec<ProxyQualityCheck>,
}
