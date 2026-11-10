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
        || matches!(
            continuation_reason,
            Some("upstream_rejected" | "quota_exhausted")
        )
        || matches!(
            error_kind,
            // 这里匹配内部 errorKind；message_too_big 只由收到的上游 close 1009 产生
            Some("permission_denied" | "message_too_big" | "upstream_capacity_unavailable")
        )
    {
        return UsageErrorOrigin::Upstream;
    }
    if matches!(
        continuation_reason,
        Some(
            "scope_unavailable"
                | "pool_unavailable"
                | "fresh_connection_required"
                | "connection_busy"
                | "latest_response_mismatch"
                | "reused_connection_lost"
                | "transport_payload_too_large"
        )
    ) || matches!(
        error_kind,
        Some(
            "process_interrupted"
                | "process_terminated"
                | "internal_error"
                | "model_not_found"
                | "no_available_provider"
                | "no_eligible_account"
                | "account_capacity_unavailable"
                | "middleware_capacity_unavailable"
                | "concurrency_queue_full"
                | "concurrency_queue_timeout"
                | "provider_infrastructure_unavailable"
                | "session_policy_restricted"
                | "request_policy_denied"
                | "policy_denied"
                | "budget_exhausted"
                | "key_budget_unavailable"
                | "client_budget_unavailable"
                | "user_concurrency_limited"
                | "user_rate_limited"
                | "client_key_concurrency_limited"
                | "client_key_rate_limited"
                | "group_daily_budget_exceeded"
                | "group_weekly_budget_exceeded"
        )
    ) || matches!(
        connection_exit_reason,
        Some(
            "command_channel_closed"
                | "local_close"
                | "message_receiver_closed"
                | "client_cancelled"
                | "downstream_send_failed"
                | "downstream_connection_closed"
                | "downstream_rebound"
                | "pool_shutdown"
                | "max_age_expired"
                | "idle_timeout_expired"
                | "proxy_capacity_evicted"
                | "search_activation_required"
        )
    ) {
        return UsageErrorOrigin::Proxy;
    }
    if matches!(
        connection_exit_reason,
        Some(
            "upstream_close"
                | "normal_close"
                | "unexpected_binary_event"
                | "incomplete_response"
                | "failed_response"
        )
    ) {
        return UsageErrorOrigin::Upstream;
    }
    if error_kind == Some("transport")
        || matches!(
            connection_exit_reason,
            Some(
                "eof"
                    | "unexpected_eof"
                    | "transport_error"
                    | "connection_reset"
                    | "connection_aborted"
                    | "broken_pipe"
                    | "tcp_reset"
                    | "reset_without_closing_handshake"
                    | "transport_timeout"
                    | "outbound_transport_error"
                    | "receive_idle_timeout"
                    | "pong_timeout"
                    | "ping_write_timeout"
                    | "pong_write_timeout"
            )
        )
    {
        return UsageErrorOrigin::Connection;
    }
    // invalid_request、protocol、rate_limited 等分类同时用于本地和上游失败
    // 发送状态与笼统的连接退出摘要也不能替代缺失的来源事实
    UsageErrorOrigin::Unknown
}
