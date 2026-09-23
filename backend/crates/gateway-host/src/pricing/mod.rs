//! LiteLLM 格式的 Sub2API 社区价目。固定 HTTPS 来源，严格摘要校验，控制面异步拉取。

use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, pricing::PricingSyncPreview},
    ports::pricing::PricingSource,
};
use gateway_core::metering::{ModelPriceOverride, TokenPrice, TokenPriceOverride};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::Mutex;

const SOURCE_URL: &str = "https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.json";
const HASH_URL: &str = "https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.sha256";
const MAX_BYTES: usize = 32 * 1024 * 1024;

#[derive(Default)]
pub struct LiteLlmPricing {
    cached: Mutex<Option<PricingSyncPreview>>,
}

#[async_trait]
impl PricingSource for LiteLlmPricing {
    async fn fetch(&self) -> Result<PricingSyncPreview, AdminError> {
        tokio::time::timeout(Duration::from_secs(30), self.fetch_verified())
            .await
            .map_err(|_| source_error())?
    }
}

impl LiteLlmPricing {
    async fn fetch_verified(&self) -> Result<PricingSyncPreview, AdminError> {
        // 手动和定时同步共享同一个已校验缓存，避免同一版本并行下载。
        let mut cached = self.cached.lock().await;
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| source_error())?;
        let hash_bytes = download(&client, HASH_URL, 1024).await?;
        let hash = std::str::from_utf8(&hash_bytes)
            .map_err(|_| source_error())?
            .trim();
        if !valid_hash(hash) {
            return Err(source_error());
        }
        if let Some(preview) = cached.as_ref().filter(|p| p.source_hash == hash) {
            return Ok(preview.clone());
        }
        let body = download(&client, SOURCE_URL, MAX_BYTES).await?;
        let preview = decode_verified_catalog(&body, hash)?;
        *cached = Some(preview.clone());
        Ok(preview)
    }
}

async fn download(
    client: &reqwest::Client,
    url: &str,
    limit: usize,
) -> Result<Vec<u8>, AdminError> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| source_error())?
        .error_for_status()
        .map_err(|_| source_error())?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| source_error())? {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(source_error());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn source_error() -> AdminError {
    AdminError::bad_gateway(
        "远程 LiteLLM 价目下载、格式或 SHA-256 校验失败；保留现有价格，稍后重试",
    )
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// 先校验下载原文，后解析，绝不发布未通过摘要校验的数据。
pub fn decode_verified_catalog(
    body: &[u8],
    expected_hash: &str,
) -> Result<PricingSyncPreview, AdminError> {
    if !valid_hash(expected_hash) || hex::encode(Sha256::digest(body)) != expected_hash {
        return Err(source_error());
    }
    decode_catalog(body)
}

/// 只转换可表达的文本 Token 档位；缺失不是零，未知上下文规则不得套用短上下文价。
pub fn decode_catalog(body: &[u8]) -> Result<PricingSyncPreview, AdminError> {
    let catalog: Value = serde_json::from_slice(body).map_err(|_| source_error())?;
    let catalog = catalog.as_object().ok_or_else(source_error)?;
    let mut result = PricingSyncPreview {
        prices: BTreeMap::new(),
        skipped: Vec::new(),
        source_hash: hex::encode(Sha256::digest(body)),
    };
    for (model, value) in catalog {
        let Some(provider @ ("openai" | "xai")) =
            value.get("litellm_provider").and_then(Value::as_str)
        else {
            continue;
        };
        if model.is_empty()
            || model.len() > 128
            || model.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            continue;
        }
        match model_price(provider, value) {
            Some(price) => {
                result
                    .prices
                    .entry(provider.to_owned())
                    .or_default()
                    .insert(model.clone(), price);
            }
            None => result.skipped.push(format!("{provider}/{model}")),
        }
    }
    if result.prices.is_empty() {
        return Err(source_error());
    }
    Ok(result)
}

fn model_price(provider: &str, model: &Value) -> Option<ModelPriceOverride> {
    if model.get("mode")?.as_str()? != "chat" {
        return None;
    }
    if let Some(output) = model.get("supported_output_modalities") {
        let output = output.as_array()?;
        if output.len() != 1 || output[0].as_str()? != "text" {
            return None;
        }
    }
    let fields = model.as_object()?;
    let threshold = if provider == "xai" {
        "200k_tokens"
    } else {
        "272k_tokens"
    };
    for key in fields.keys() {
        if key.contains("cost")
            && (key.contains("audio") || key.contains("reasoning") || key.contains("image"))
        {
            return None;
        }
        if key.contains("cost")
            && let Some((_, suffix)) = key.split_once("_above_")
            && !["", "_priority", "_flex", "_batches"]
                .iter()
                .any(|service| suffix == format!("{threshold}{service}"))
        {
            return None;
        }
    }
    // 不能忽略另一套倍率/阈值规则；当前适配仅接受显式分档绝对价格。
    if fields.contains_key("long_context_input_token_threshold")
        || fields.contains_key("long_context_input_cost_multiplier")
        || fields.contains_key("long_context_output_cost_multiplier")
    {
        return None;
    }
    let mut bands = BTreeMap::from([("standard".to_owned(), rates(model, "")?)]);
    for (band, suffix) in [("fast", "_priority"), ("flex", "_flex")] {
        if (provider != "xai" || band != "flex")
            && let Some(rate) = rates(model, suffix)
        {
            bands.insert(band.to_owned(), rate);
        }
    }
    for (band, service) in [
        ("long_standard", ""),
        ("long_fast", "_priority"),
        ("long_flex", "_flex"),
    ] {
        if (provider != "xai" || band != "long_flex")
            && let Some(rate) = rates(model, &format!("_above_{threshold}{service}"))
        {
            bands.insert(band.to_owned(), rate);
        }
    }
    Some(ModelPriceOverride {
        multiplier_bps: 10_000,
        bands,
    })
}

fn rates(model: &Value, suffix: &str) -> Option<TokenPriceOverride> {
    let price = |field: &str| per_token_price(model.get(format!("{field}{suffix}"))?);
    Some(TokenPriceOverride {
        input: price("input_cost_per_token")?,
        output: price("output_cost_per_token")?,
        cache_read: price("cache_read_input_token_cost")?,
        cache_write: price("cache_creation_input_token_cost")?,
    })
}

/// LiteLLM 是 USD/Token，本项目是 USD/百万 Token；十进制移位避免浮点乘法误差。
fn per_token_price(value: &Value) -> Option<TokenPrice> {
    let text = value.as_number()?.to_string();
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((&text, "0"));
    let exponent: i32 = exponent.parse().ok()?;
    if !(-30..=30).contains(&exponent) || mantissa.starts_with('-') {
        return None;
    }
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: u128 = format!("{whole}{fraction}").parse().ok()?;
    let shift = exponent + 10 - i32::try_from(fraction.len()).ok()?;
    let ticks = if shift >= 0 {
        digits.checked_mul(10_u128.checked_pow(shift.unsigned_abs())?)?
    } else {
        let divisor = 10_u128.checked_pow(shift.unsigned_abs())?;
        if !digits.is_multiple_of(divisor) {
            return None;
        }
        digits / divisor
    };
    format!("{}.{:04}", ticks / 10_000, ticks % 10_000)
        .try_into()
        .ok()
}
