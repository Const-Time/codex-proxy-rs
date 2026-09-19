//! 探测网络只使用显式代理，不继承环境代理，不共享业务连接池，不跟随重定向。
use super::Pool;
use futures::StreamExt as _;
use gateway_admin::model::{AdminError, turn_state::TurnStatePoolTest};
use gateway_core::account::OutboundProxy;
use reqwest::Client;
use std::{net::IpAddr, time::Duration};
use url::Url;

pub(super) fn validate_pool(pool: &Pool) -> Result<(), AdminError> {
    if pool.endpoint.len() > 4096
        || pool.bearer.len() > 4096
        || pool.bearer.chars().any(char::is_control)
        || pool.json_pointer.len() > 256
        || (!pool.json_pointer.is_empty() && !pool.json_pointer.starts_with('/'))
    {
        return Err(AdminError::invalid("代理入口、认证或 JSON Pointer 不合法"));
    }
    match pool.mode.as_str() {
        "gateway" | "fixed" | "rotating" => {
            OutboundProxy::parse(&pool.endpoint)
                .map_err(|_| AdminError::invalid("代理地址须为带端口的 HTTP(S)/SOCKS5(H) URL"))?;
        }
        "api" => {
            let url = Url::parse(&pool.endpoint)
                .map_err(|_| AdminError::invalid("提取接口地址不合法"))?;
            if url.scheme() != "https"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
            {
                return Err(AdminError::invalid(
                    "提取接口须使用 HTTPS；认证使用 Bearer 字段或接口查询参数",
                ));
            }
        }
        _ => {
            return Err(AdminError::invalid(
                "请选择固定出口、每连接轮换入口或 API 提取式",
            ));
        }
    }
    Ok(())
}

pub(super) fn redacted_endpoint(endpoint: &str) -> String {
    let Ok(mut url) = Url::parse(endpoint) else {
        return "<已保存>".to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    // 提取接口的路径和查询串也可能包含供应商 API 密钥。
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

pub(super) fn client(
    proxy: Option<&OutboundProxy>,
    timeout: Duration,
) -> Result<Client, AdminError> {
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .connect_timeout(Duration::from_secs(10))
        .timeout(timeout);
    if let Some(proxy) = proxy {
        builder = builder.proxy(
            reqwest::Proxy::all(proxy.expose_url())
                .map_err(|_| AdminError::invalid("代理连接配置不合法"))?,
        );
    }
    crate::build_reqwest_client_with_custom_ca(builder)
        .map_err(|_| AdminError::unavailable("无法建立探测客户端"))
}

async fn limited_body(response: reqwest::Response, max: usize) -> Result<Vec<u8>, AdminError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes.map_err(|_| AdminError::bad_gateway("代理接口响应读取失败"))?;
        if body.len().saturating_add(bytes.len()) > max {
            return Err(AdminError::bad_gateway("代理接口响应超限"));
        }
        body.extend_from_slice(&bytes);
    }
    Ok(body)
}

/// 每次探测重新提取一个代理，不缓存未知租约、不猜测供应商的续期与换 IP API。
pub(super) async fn acquire(pool: &Pool) -> Result<OutboundProxy, AdminError> {
    if matches!(pool.mode.as_str(), "gateway" | "fixed" | "rotating") {
        return OutboundProxy::parse(&pool.endpoint)
            .map_err(|_| AdminError::invalid("代理配置不合法"));
    }
    let http = client(None, Duration::from_secs(15))?;
    let mut request = http.get(&pool.endpoint);
    if !pool.bearer.is_empty() {
        request = request.bearer_auth(&pool.bearer);
    }
    let response = request
        .send()
        .await
        .map_err(|_| AdminError::bad_gateway("代理提取接口连接失败"))?;
    if !response.status().is_success() {
        return Err(AdminError::bad_gateway("代理提取接口拒绝请求或重定向"));
    }
    let body = limited_body(response, 64 * 1024).await?;
    let value = if pool.json_pointer.is_empty() {
        std::str::from_utf8(&body)
            .map_err(|_| AdminError::bad_gateway("代理接口须返回 UTF-8 URL"))?
            .trim()
            .to_owned()
    } else {
        let json: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| AdminError::bad_gateway("代理接口不是 JSON"))?;
        json.pointer(&pool.json_pointer)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AdminError::bad_gateway("JSON Pointer 未指向代理 URL 字符串"))?
            .trim()
            .to_owned()
    };
    OutboundProxy::parse(&value).map_err(|_| AdminError::bad_gateway("提取结果不是完整代理 URL"))
}

async fn test_ip(proxy: &OutboundProxy, url: &str) -> Option<IpAddr> {
    let response = client(Some(proxy), Duration::from_secs(15))
        .ok()?
        .get(url)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = limited_body(response, 128).await.ok()?;
    std::str::from_utf8(&body).ok()?.trim().parse().ok()
}

pub(super) async fn test(pool: &Pool) -> Result<TurnStatePoolTest, AdminError> {
    let proxy = acquire(pool).await?;
    let (v4, v6) = tokio::join!(
        test_ip(&proxy, "https://api.ipify.org"),
        test_ip(&proxy, "https://api6.ipify.org")
    );
    let v4 = v4.filter(IpAddr::is_ipv4);
    let v6 = v6.filter(IpAddr::is_ipv6);
    Ok(TurnStatePoolTest {
        success: v4.is_some() || v6.is_some(), ipv6: v6.is_some(),
        exit_ip: v6.or(v4).map(|ip| ip.to_string()),
        ipv4_address: v4.map(|ip| ip.to_string()), ipv6_address: v6.map(|ip| ip.to_string()),
        message: "IPv4 / IPv6 分别检测；未取得地址表示本次检测未确认，不代表不支持。每连接轮换也不保证出口变化。".to_owned(),
    })
}
