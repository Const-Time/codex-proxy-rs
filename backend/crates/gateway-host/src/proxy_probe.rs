//! 使用与 Provider 请求一致的显式代理协议，执行有超时和响应大小限制的出口测试。

use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use gateway_admin::{
    model::proxies::{ProxyQualityCheck, ProxyQualityReport, ProxyQualityStatus, ProxyTestResult},
    ports::proxy::{ProxyProbe, ProxyWebSocketProbe},
};
use gateway_core::account::OutboundProxy;
use serde::Deserialize;

pub struct HttpProxyProbe {
    endpoint: String,
    location_endpoint: Option<String>,
    targets: Vec<(String, String)>,
    websocket: Option<Arc<dyn ProxyWebSocketProbe>>,
    build_client: Arc<ProxyClientBuilder>,
}

type ProxyClientBuilder =
    dyn Fn(reqwest::ClientBuilder) -> Result<reqwest::Client, &'static str> + Send + Sync;

impl Default for HttpProxyProbe {
    fn default() -> Self {
        // 双栈出口探测，保留本分支独立的位置探测。
        Self::new("https://api64.ipify.org?format=json").with_location_endpoint("https://ipwho.is/")
    }
}

impl HttpProxyProbe {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            location_endpoint: None,
            targets: [
                ("OpenAI API", "https://api.openai.com/v1/models"),
                ("Anthropic", "https://api.anthropic.com/v1/messages"),
                (
                    "Gemini",
                    "https://generativelanguage.googleapis.com/v1beta/models",
                ),
                ("Grok", "https://api.x.ai/v1/models"),
                (
                    "Codex HTTPS",
                    "https://chatgpt.com/backend-api/codex/responses",
                ),
            ]
            .into_iter()
            .map(|(name, url)| (name.to_owned(), url.to_owned()))
            .collect(),
            websocket: None,
            build_client: Arc::new(|builder| builder.build().map_err(|_| "无法创建代理连接")),
        }
    }

    /// Trusted composition only; query URLs are never accepted from admin requests.
    #[must_use]
    pub fn with_location_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.location_endpoint = Some(endpoint.into());
        self
    }

    async fn request_location(
        &self,
        proxy: Option<&OutboundProxy>,
        ip: IpAddr,
    ) -> Option<gateway_core::account::RequestLocation> {
        let endpoint = self.location_endpoint.as_ref()?;
        let client = self.client(proxy).ok()?;
        let mut response = client
            .get(format!("{endpoint}{ip}"))
            .query(&[("fields", "success,ip,country_code,region,city,timezone.id")])
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len() + chunk.len() > 8192 {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        #[derive(Deserialize)]
        struct Zone {
            id: String,
        }
        #[derive(Deserialize)]
        struct Location {
            success: bool,
            ip: IpAddr,
            country_code: String,
            region: String,
            city: String,
            timezone: Zone,
        }
        let data: Location = serde_json::from_slice(&body).ok()?;
        if !data.success || data.ip != ip {
            return None;
        }
        let location = gateway_core::account::RequestLocation {
            country: data.country_code,
            region: data.region,
            city: data.city,
            timezone: data.timezone.id,
        };
        gateway_admin::model::proxies::validate_request_location(&location).ok()?;
        Some(location)
    }

    /// 由组合根注入与 Provider 请求一致的证书信任策略。
    #[must_use]
    pub fn with_client_builder<E>(
        mut self,
        build: impl Fn(reqwest::ClientBuilder) -> Result<reqwest::Client, E> + Send + Sync + 'static,
    ) -> Self {
        self.build_client = Arc::new(move |builder| {
            build(builder).map_err(|_| "无法创建代理连接，请检查证书信任配置")
        });
        self
    }

    #[must_use]
    pub fn with_websocket_probe(mut self, probe: Arc<dyn ProxyWebSocketProbe>) -> Self {
        self.websocket = Some(probe);
        self
    }

    /// 供受信任的组合根及离线测试设置目标；管理 API 不接受 URL。
    #[must_use]
    pub fn with_quality_targets(mut self, targets: Vec<(String, String)>) -> Self {
        self.targets = targets;
        self
    }

    fn client(&self, proxy: Option<&OutboundProxy>) -> Result<reqwest::Client, &'static str> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(12))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(proxy) = proxy {
            builder = builder
                .proxy(reqwest::Proxy::all(proxy.expose_url()).map_err(|_| "代理地址不合法")?);
        }
        (self.build_client)(builder)
    }

    async fn check_target(
        &self,
        proxy: &OutboundProxy,
        name: &str,
        url: &str,
    ) -> ProxyQualityCheck {
        let started = Instant::now();
        let result = async {
            self.client(Some(proxy))?
                .get(url)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        "连接超时"
                    } else {
                        "连接失败，请检查代理认证、DNS、TCP 和 TLS"
                    }
                })
        }
        .await;
        let latency_ms = elapsed_ms(started);
        match result {
            Ok(response) => ProxyQualityCheck::http(
                name,
                response.status().as_u16(),
                response
                    .headers()
                    .get("cf-mitigated")
                    .is_some_and(|value| value == "challenge"),
                false,
                latency_ms,
            ),
            Err(message) => ProxyQualityCheck {
                name: name.to_owned(),
                status: ProxyQualityStatus::Failed,
                http_status: None,
                latency_ms,
                message: message.to_owned(),
            },
        }
    }

    async fn exit_ip(&self, proxy: Option<&OutboundProxy>) -> Result<IpAddr, &'static str> {
        let client = self.client(proxy)?;
        let mut response = client.get(&self.endpoint).send().await.map_err(|error| {
            if error.is_timeout() {
                "代理连接超时"
            } else {
                "代理连接失败，请检查地址、认证和网络"
            }
        })?;
        if !response.status().is_success() {
            return Err(
                if response.status() == reqwest::StatusCode::PROXY_AUTHENTICATION_REQUIRED {
                    "代理认证失败"
                } else {
                    "出口检测服务返回错误状态"
                },
            );
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "出口检测响应读取失败")?
        {
            if body.len() + chunk.len() > 1024 {
                return Err("出口检测响应过大");
            }
            body.extend_from_slice(&chunk);
        }
        #[derive(Deserialize)]
        struct Response {
            ip: IpAddr,
        }
        serde_json::from_slice::<Response>(&body)
            .map(|response| response.ip)
            .map_err(|_| "出口检测响应不合法")
    }
}

#[async_trait]
impl ProxyProbe for HttpProxyProbe {
    async fn quality(&self, proxy: &OutboundProxy) -> ProxyQualityReport {
        let started = Instant::now();
        let tested_at = chrono::Utc::now();
        let targets = stream::iter(self.targets.clone())
            .map(|(name, url)| async move { self.check_target(proxy, &name, &url).await })
            .buffered(3)
            .collect::<Vec<_>>();
        let websocket = async {
            if let Some(probe) = &self.websocket {
                match tokio::time::timeout(Duration::from_secs(16), probe.probe(proxy)).await {
                    Ok(check) => return check,
                    Err(_) => {
                        return ProxyQualityCheck {
                            name: "Codex WebSocket".to_owned(),
                            status: ProxyQualityStatus::Failed,
                            http_status: None,
                            latency_ms: 16000,
                            message: "WebSocket 建连超时".to_owned(),
                        };
                    }
                }
            }
            ProxyQualityCheck {
                name: "Codex WebSocket".to_owned(),
                status: ProxyQualityStatus::Warning,
                http_status: None,
                latency_ms: 0,
                message: "未配置 WebSocket 探测能力".to_owned(),
            }
        };
        let (basic, mut checks, websocket) = tokio::join!(self.test(proxy), targets, websocket);
        checks.insert(
            0,
            ProxyQualityCheck {
                name: "基础连通性".to_owned(),
                status: if basic.success {
                    ProxyQualityStatus::Passed
                } else {
                    ProxyQualityStatus::Failed
                },
                http_status: None,
                latency_ms: basic.latency_ms,
                message: basic.message.clone(),
            },
        );
        checks.push(websocket);
        ProxyQualityReport {
            tested_at,
            duration_ms: elapsed_ms(started),
            basic,
            checks,
        }
    }

    async fn test(&self, proxy: &OutboundProxy) -> ProxyTestResult {
        self.detect_egress(Some(proxy)).await
    }

    async fn test_egress(&self, proxy: Option<&OutboundProxy>) -> Option<ProxyTestResult> {
        Some(self.detect_egress(proxy).await)
    }
}

impl HttpProxyProbe {
    async fn detect_egress(&self, proxy: Option<&OutboundProxy>) -> ProxyTestResult {
        let started = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(15), self.exit_ip(proxy)).await;
        let result = result.unwrap_or(Err("代理连接超时"));
        let location = if let Ok(ip) = &result {
            tokio::time::timeout(Duration::from_secs(5), self.request_location(proxy, *ip))
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let message = if result.is_ok() && self.location_endpoint.is_some() && location.is_none() {
            "连接成功，地区识别暂不可用；可重试或手动设置".to_owned()
        } else {
            result
                .as_ref()
                .map_or_else(|message| (*message).to_owned(), |_| "连接成功".to_owned())
        };
        ProxyTestResult {
            location,
            success: result.is_ok(),
            latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            exit_ip: result.as_ref().ok().copied(),
            message,
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
