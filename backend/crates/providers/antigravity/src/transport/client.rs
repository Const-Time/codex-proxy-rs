//! 已注册资源服务的项目发现、模型查询和原生 HTTP 请求

use bytes::Bytes;
use gateway_core::{
    account::OutboundProxy,
    error::{ProviderError, ProviderErrorKind as Kind},
};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{json, value::RawValue};
use std::{collections::BTreeMap, time::Duration};
use url::Url;

use super::failure::{explicit_generation_rejection, network, sent, successful, unsent};
use crate::profile::ClientProfile;
// Desktop 独立搜索使用固定任务指令，常规生成与计数使用账号画像
static SEARCH_INSTRUCTION: std::sync::LazyLock<&RawValue> = std::sync::LazyLock::new(|| {
    serde_json::from_str(r#"{"parts":[{"text":"Search the web for the query and return a concise answer grounded in the sources."}]}"#).expect("valid static search instruction")
});

const GENERATION_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_CONTROL_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub(crate) project: String,
    #[serde(flatten)]
    pub(crate) available: AvailableModels,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AvailableModels {
    pub(crate) models: BTreeMap<String, Model>,
    #[serde(
        rename = "groupedModels",
        default,
        skip_serializing_if = "serde_json::Value::is_null"
    )]
    pub(crate) grouped_models: serde_json::Value,
    #[serde(
        rename = "webSearchModelIds",
        alias = "webSearchModels",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub(crate) web_search_models: Vec<String>,
}

impl Catalog {
    pub(crate) fn web_search_target(&self) -> Option<&str> {
        self.available.web_search_models.first().map(String::as_str)
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Model {
    #[serde(rename = "displayName", default)]
    pub(crate) name: String,
    #[serde(rename = "quotaInfo", default)]
    pub(crate) quota: Option<super::account::ModelQuota>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(rename = "tagDescription", default)]
    pub(crate) tag_description: Option<String>,
    #[serde(rename = "maxTokens", default)]
    pub(crate) context_window: Option<u64>,
    #[serde(rename = "maxOutputTokens", default)]
    pub(crate) max_output_tokens: Option<u64>,
    #[serde(rename = "supportsImages", default)]
    pub(crate) supports_images: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum GenerateMode {
    Json,
    Stream,
    WebSearch,
}

impl From<bool> for GenerateMode {
    fn from(stream: bool) -> Self {
        if stream { Self::Stream } else { Self::Json }
    }
}

/// HTTP client for the Cloud Code resource service.
pub struct CloudCodeClient {
    client: Client,
    origin: Url,
    use_quota_credits: bool,
}

impl CloudCodeClient {
    pub fn new(
        origin: &Url,
        proxy: Option<&OutboundProxy>,
        use_quota_credits: bool,
    ) -> Result<Self, ProviderError> {
        Self::with_profile(origin, proxy, use_quota_credits, None)
    }

    pub(crate) fn with_profile(
        origin: &Url,
        proxy: Option<&OutboundProxy>,
        use_quota_credits: bool,
        profile: Option<&ClientProfile>,
    ) -> Result<Self, ProviderError> {
        let client = super::http_client(proxy)
            .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?
            .use_rustls_tls()
            .http1_only()
            .http1_title_case_headers()
            .pool_max_idle_per_host(100)
            .read_timeout(GENERATION_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .user_agent(profile.map_or(
                concat!("codex-proxy-rs-antigravity/", env!("CARGO_PKG_VERSION")),
                ClientProfile::user_agent,
            ))
            .build()
            .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?;
        Ok(Self {
            client,
            origin: origin.clone(),
            use_quota_credits,
        })
    }

    pub async fn count_tokens(&self, token: &str, body: &[u8]) -> Result<Response, ProviderError> {
        #[derive(Serialize)]
        struct Envelope<'a> {
            request: BTreeMap<String, &'a RawValue>,
        }

        let mut request = Self::request_body(body)?;
        if let Some(nested) = request
            .get("generateContentRequest")
            .or_else(|| request.get("generate_content_request"))
            .copied()
        {
            request = Self::request_body(nested.get().as_bytes())?;
        }
        request.retain(|name, _| {
            !matches!(
                name.as_str(),
                "model"
                    | "safetySettings"
                    | "safety_settings"
                    | "toolConfig"
                    | "tool_config"
                    | "labels"
                    | "sessionId"
                    | "session_id"
            )
        });
        self.send(
            self.post("countTokens", token)
                .timeout(GENERATION_TIMEOUT)
                .json(&Envelope { request }),
            token,
            "application/json",
        )
        .await
    }

    pub(super) fn post(&self, action: &str, token: &str) -> reqwest::RequestBuilder {
        let mut url = self.origin.clone();
        url.set_path(&format!("/v1internal:{action}"));
        self.client.post(url).bearer_auth(token)
    }

    pub(crate) async fn catalog(&self, token: &str) -> Result<Catalog, ProviderError> {
        let project = self.account_info(token).await?.project;
        self.catalog_for_project(token, project).await
    }

    pub(crate) async fn catalog_for_project(
        &self,
        token: &str,
        project: String,
    ) -> Result<Catalog, ProviderError> {
        let available: AvailableModels = control(
            self.post("fetchAvailableModels", token)
                .timeout(Duration::from_secs(20))
                .json(&json!({"project":project})),
            token,
        )
        .await?;
        if available
            .models
            .keys()
            .any(|id| gateway_core::routing::UpstreamModelId::new(id).is_err())
        {
            return Err(sent(Kind::Protocol));
        }
        Ok(Catalog { project, available })
    }

    pub async fn generate(
        &self,
        project: &str,
        model: &str,
        token: &str,
        body: &[u8],
        mode: GenerateMode,
    ) -> Result<Response, ProviderError> {
        #[derive(Serialize)]
        struct Envelope<'a> {
            project: &'a str,
            model: &'a str,
            #[serde(rename = "requestType", skip_serializing_if = "Option::is_none")]
            request_type: Option<&'static str>,
            #[serde(rename = "enabledCreditTypes", skip_serializing_if = "Option::is_none")]
            enabled_credit_types: Option<[&'static str; 1]>,
            request: BTreeMap<String, &'a RawValue>,
        }
        let mut request = Self::request_body(body)?;
        if mode == GenerateMode::WebSearch {
            request
                .entry("systemInstruction".to_owned())
                .or_insert(*SEARCH_INSTRUCTION);
        }
        let (send, media) = if mode == GenerateMode::Stream {
            (
                self.post("streamGenerateContent", token)
                    .query(&[("alt", "sse")]),
                "text/event-stream",
            )
        } else {
            (
                self.post("generateContent", token)
                    .timeout(GENERATION_TIMEOUT),
                "application/json",
            )
        };
        self.send(
            send.json(&Envelope {
                project,
                model,
                request_type: (mode == GenerateMode::WebSearch).then_some("web_search"),
                enabled_credit_types: self
                    .use_quota_credits
                    .then_some([super::account::GOOGLE_ONE_AI]),
                request,
            }),
            token,
            media,
        )
        .await
    }

    fn request_body(body: &[u8]) -> Result<BTreeMap<String, &RawValue>, ProviderError> {
        serde_json::from_slice(body).map_err(|_| unsent(Kind::InvalidRequest))
    }

    async fn send(
        &self,
        send: reqwest::RequestBuilder,
        token: &str,
        media: &str,
    ) -> Result<Response, ProviderError> {
        let response = send.send().await.map_err(network)?;
        let response = successful(response, token)
            .await
            .map_err(explicit_generation_rejection)?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case(media))
        {
            return Err(sent(Kind::Protocol));
        }
        Ok(response)
    }
}

pub(super) async fn control<T: DeserializeOwned>(
    request: reqwest::RequestBuilder,
    token: &str,
) -> Result<T, ProviderError> {
    let mut response = successful(request.send().await.map_err(network)?, token).await?;
    let mut data = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| sent(Kind::Transport))? {
        if data.len().saturating_add(chunk.len()) > MAX_CONTROL_RESPONSE_BYTES {
            return Err(sent(Kind::MessageTooBig));
        }
        data.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&data).map_err(|_| sent(Kind::Protocol))
}

#[derive(Deserialize)]
struct Reply<'a> {
    #[serde(borrow)]
    response: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

pub(crate) fn unwrap_frame(data: &str) -> Result<Bytes, ProviderError> {
    let envelope: Reply = serde_json::from_str(data).map_err(|_| sent(Kind::Protocol))?;
    match (envelope.response, envelope.error) {
        (None, Some(err)) if err.get().starts_with('{') => {
            Ok(Bytes::copy_from_slice(data.as_bytes()))
        }
        (Some(body), None) if body.get().starts_with('{') => {
            Ok(Bytes::copy_from_slice(body.get().as_bytes()))
        }
        _ => Err(sent(Kind::Protocol)),
    }
}
