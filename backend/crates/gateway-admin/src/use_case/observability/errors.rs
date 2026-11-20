//! 根据保存的事实展示错误来源，不参与执行分类、重试或计费。
use crate::model::observability::{OpsError, UsageErrorEvidence, UsageErrorOrigin, UsageRecord};

impl UsageErrorEvidence {
    #[must_use]
    pub fn from_recorded(
        raw: Option<&str>,
        continuation_reason: Option<String>,
        connection_exit_reason: Option<String>,
    ) -> Self {
        let value = raw.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
        let body = value.as_ref();
        let error = body.and_then(|body| {
            body.get("error")
                .or_else(|| body.pointer("/response/error"))
        });
        let has_upstream_error = error.is_some_and(|error| {
            ["code", "type", "message"].into_iter().any(|field| {
                error
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty())
            })
        });
        let websocket_close_code = body
            .filter(|body| {
                body.get("type").and_then(serde_json::Value::as_str) == Some("websocket.close")
            })
            .and_then(|body| body.get("code"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|code| u16::try_from(code).ok());
        Self {
            has_upstream_error,
            websocket_close_code,
            continuation_reason,
            connection_exit_reason,
        }
    }

    #[must_use]
    pub fn origin(
        &self,
        code: Option<&str>,
        status: Option<u16>,
        kind: Option<&str>,
    ) -> UsageErrorOrigin {
        error_origin(
            self.has_upstream_error,
            code,
            self.websocket_close_code,
            status,
            kind,
            self.continuation_reason.as_deref(),
            self.connection_exit_reason.as_deref(),
        )
    }
}

impl OpsError {
    #[must_use]
    pub fn error_origin(&self) -> UsageErrorOrigin {
        UsageErrorEvidence::from_recorded(
            self.raw_upstream_error.as_deref(),
            self.continuation_unavailable_reason.clone(),
            self.upstream_connection_exit_reason.clone(),
        )
        .origin(
            self.provider_error_code.as_deref(),
            self.upstream_status_code,
            Some(&self.failure_kind),
        )
    }
}

impl UsageRecord {
    #[must_use]
    pub fn error_origin(&self) -> UsageErrorOrigin {
        self.error_evidence.origin(
            self.provider_error_code.as_deref(),
            self.upstream_status_code,
            self.error_kind.as_deref(),
        )
    }
}

// 上游原始事实优先；客户端 HTTP 状态和任意错误文案不能证明错误来源
fn error_origin(
    has_upstream_error: bool,
    upstream_code: Option<&str>,
    websocket_close_code: Option<u16>,
    upstream_status_code: Option<u16>,
    error_kind: Option<&str>,
    continuation_reason: Option<&str>,
    connection_exit_reason: Option<&str>,
) -> UsageErrorOrigin {
    if has_upstream_error
        || upstream_code.is_some_and(|code| !code.is_empty())
        || websocket_close_code.is_some()
        || upstream_status_code.is_some_and(|status| status >= 400)
        || continuation_reason
            .is_some_and(|value| UsageErrorOrigin::UPSTREAM_CONTINUATION_REASONS.contains(&value))
        || error_kind.is_some_and(|value| UsageErrorOrigin::UPSTREAM_ERROR_KINDS.contains(&value))
    {
        return UsageErrorOrigin::Upstream;
    }
    if continuation_reason
        .is_some_and(|value| UsageErrorOrigin::PROXY_CONTINUATION_REASONS.contains(&value))
        || error_kind.is_some_and(|value| UsageErrorOrigin::PROXY_ERROR_KINDS.contains(&value))
        || connection_exit_reason
            .is_some_and(|value| UsageErrorOrigin::PROXY_EXIT_REASONS.contains(&value))
    {
        return UsageErrorOrigin::Proxy;
    }
    if connection_exit_reason
        .is_some_and(|value| UsageErrorOrigin::UPSTREAM_EXIT_REASONS.contains(&value))
    {
        return UsageErrorOrigin::Upstream;
    }
    if error_kind == Some(UsageErrorOrigin::CONNECTION_ERROR_KIND)
        || connection_exit_reason
            .is_some_and(|value| UsageErrorOrigin::CONNECTION_EXIT_REASONS.contains(&value))
    {
        return UsageErrorOrigin::Connection;
    }
    // invalid_request、protocol、rate_limited 等分类同时用于本地和上游失败
    // 发送状态与笼统的连接退出摘要也不能替代缺失的来源事实
    UsageErrorOrigin::Unknown
}
