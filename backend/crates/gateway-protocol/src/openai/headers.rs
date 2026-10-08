//! OpenAI 请求头的业务协议与传输边界。

/// 解析 HTTP 和流内错误共用的 Retry-After，保留零延迟并向上取整剩余秒数
pub fn parse_retry_after_seconds(value: &str) -> Option<u64> {
    let now = std::time::SystemTime::now();
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value.parse().ok();
    }
    let remaining = httpdate::parse_http_date(value)
        .ok()?
        .duration_since(now)
        .unwrap_or_default();
    Some(
        remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() != 0)),
    )
}

/// 判断小写请求头是否属于传输层管理的字段，不得作为业务扩展头透传。
///
/// API 入站和 Provider 编码共用此分类；`Connection` 动态声明的逐跳头由入站额外剥离。
/// 只用于请求方向，不影响上游响应中的代理诊断信息。
#[must_use]
pub fn is_transport_managed_request_header(name: &str) -> bool {
    // 代理命名空间描述的是下游链路；包括未知扩展也不能冒充上游连接事实。
    name.starts_with("cf-")
        || name.starts_with("x-forwarded-")
        || name.starts_with("sec-websocket-")
        || matches!(
            name,
            "connection"
                | "keep-alive"
                | "proxy-connection"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "host"
                | "content-length"
                // 请求实体与响应压缩能力属于各段 transport，不能继承下游协商。
                | "content-encoding"
                | "accept-encoding"
                | "forwarded"
                | "via"
                | "cdn-loop"
                | "x-real-ip"
                | "true-client-ip"
                | "x-request-id"
        )
}
