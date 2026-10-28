//! Curated release sources; bounded public-network downloads never carry administrator credentials.
use gateway_admin::model::{
    AdminError,
    plugins::{PluginArtifactInfo, PluginOnlineRelease},
};
use gateway_host::outbound::{HttpClient, HttpRequest, NetworkPolicy};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

static DOWNLOADS: Semaphore = Semaphore::const_new(2);
const MAX_ARCHIVE: usize = 32 * 1024 * 1024;

pub fn repository(plugin_id: &str) -> Option<&'static str> {
    match plugin_id {
        "local.model-health" => Some("Const-Time/codex-model-health-plugin"),
        "cpr.shared-budget-manager" => Some("Const-Time/codex-proxy-rs"),
        _ => None,
    }
}
fn target() -> Result<&'static str, AdminError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        _ => Err(AdminError::invalid("当前平台暂无配套在线更新包")),
    }
}
#[derive(Deserialize)]
struct Release {
    draft: bool,
    prerelease: bool,
    tag_name: String,
    assets: Vec<Asset>,
}
#[derive(Deserialize)]
struct Asset {
    name: String,
    size: u64,
    digest: Option<String>,
    browser_download_url: String,
}

/// Select only the matching platform and plugin identity, with a pinned archive digest.
pub fn select_release(
    bytes: &[u8],
    plugin_id: &str,
    current: &str,
    platform: &str,
) -> Result<Option<PluginOnlineRelease>, AdminError> {
    let repo =
        repository(plugin_id).ok_or_else(|| AdminError::invalid("此插件尚未配置在线更新源"))?;
    let release: Release =
        serde_json::from_slice(bytes).map_err(|_| AdminError::unavailable("更新源返回格式无效"))?;
    if release.draft || release.prerelease {
        return Ok(None);
    }
    if release.tag_name.is_empty()
        || !release
            .tag_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
    {
        return Err(AdminError::unavailable("更新源版本标签无效"));
    }
    let current = Version::parse(current).map_err(|_| AdminError::invalid("已安装插件版本无效"))?;
    let prefix = format!("{plugin_id}-");
    let suffix = format!("-{platform}.tar.gz");
    let mut selected: Option<PluginOnlineRelease> = None;
    for asset in release.assets {
        let Some(version) = asset
            .name
            .strip_prefix(&prefix)
            .and_then(|s| s.strip_suffix(&suffix))
        else {
            continue;
        };
        let Ok(parsed) = Version::parse(version) else {
            continue;
        };
        if !parsed.pre.is_empty() || parsed <= current {
            continue;
        }
        let digest = asset
            .digest
            .as_deref()
            .and_then(|d| d.strip_prefix("sha256:"))
            .filter(|d| d.len() == 64 && d.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or_else(|| AdminError::unavailable("更新包缺少可信 SHA-256 摘要"))?;
        let expected_url = format!(
            "https://github.com/{repo}/releases/download/{}/{}",
            release.tag_name, asset.name
        );
        if asset.browser_download_url != expected_url
            || asset.size == 0
            || asset.size > MAX_ARCHIVE as u64
        {
            return Err(AdminError::unavailable("更新包地址或大小无效"));
        }
        if selected.is_some() {
            return Err(AdminError::unavailable("更新源包含多个匹配包"));
        }
        selected = Some(PluginOnlineRelease {
            version: version.into(),
            sha256: digest.to_ascii_lowercase(),
            asset_name: asset.name,
            size: asset.size,
            release_url: format!(
                "https://github.com/{repo}/releases/tag/{}",
                release.tag_name
            ),
        });
    }
    Ok(selected)
}

pub fn allowed_url(url: &url::Url) -> bool {
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && matches!(
            url.host_str(),
            Some(
                "api.github.com"
                    | "github.com"
                    | "release-assets.githubusercontent.com"
                    | "objects.githubusercontent.com"
            )
        )
}
async fn fetch(client: &HttpClient, address: &str, maximum: usize) -> Result<Vec<u8>, AdminError> {
    let mut url = url::Url::parse(address).map_err(|_| AdminError::invalid("更新地址无效"))?;
    for _ in 0..4 {
        if !allowed_url(&url) {
            return Err(AdminError::invalid("更新源重定向到未允许的地址"));
        }
        let mut response = client
            .open(
                HttpRequest {
                    method: "GET".into(),
                    url: url.to_string(),
                    body: vec![],
                    headers: vec![
                        ("User-Agent".into(), b"codex-proxy-plugin-update".to_vec()),
                        ("Accept".into(), b"application/vnd.github+json".to_vec()),
                    ],
                },
                None,
                &NetworkPolicy::default(),
                Duration::from_secs(90),
            )
            .await
            .map_err(|error| {
                AdminError::unavailable(format!(
                    "无法连接 GitHub 更新源（{}），请检查服务器网络后重试",
                    error.reason()
                ))
            })?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            let location = response
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("location"))
                .and_then(|(_, v)| std::str::from_utf8(v).ok())
                .ok_or_else(|| AdminError::unavailable("更新源重定向无效"))?;
            url = url
                .join(location)
                .map_err(|_| AdminError::unavailable("更新源重定向无效"))?;
            continue;
        }
        if response.status == 403 || response.status == 429 {
            return Err(AdminError::unavailable("GitHub 请求受限，请稍后再检查"));
        }
        if response.status != 200 {
            return Err(AdminError::unavailable("更新源暂时不可用或没有正式发布"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .body
            .read(64 * 1024)
            .await
            .map_err(|_| AdminError::unavailable("下载更新包失败"))?
        {
            if bytes.len().saturating_add(chunk.len()) > maximum {
                return Err(AdminError::invalid("更新响应超过大小限制"));
            }
            bytes.extend_from_slice(&chunk);
        }
        return Ok(bytes);
    }
    Err(AdminError::unavailable("更新源重定向次数过多"))
}
async fn latest(
    client: &HttpClient,
    artifact: &PluginArtifactInfo,
) -> Result<Option<PluginOnlineRelease>, AdminError> {
    let Some(repo) = repository(&artifact.plugin_id) else {
        return Err(AdminError::invalid(
            "此插件暂不支持在线更新，请在原实例切换上传的版本",
        ));
    };
    let bytes = fetch(
        client,
        &format!("https://api.github.com/repos/{repo}/releases/latest"),
        2 * 1024 * 1024,
    )
    .await?;
    select_release(&bytes, &artifact.plugin_id, &artifact.version, target()?)
}
pub async fn check(
    artifact: &PluginArtifactInfo,
) -> Result<Option<PluginOnlineRelease>, AdminError> {
    let _permit = DOWNLOADS
        .try_acquire()
        .map_err(|_| AdminError::unavailable("正在处理其他插件更新，请稍后重试"))?;
    let client = HttpClient::new().map_err(|_| AdminError::unavailable("更新客户端初始化失败"))?;
    tokio::time::timeout(Duration::from_secs(30), latest(&client, artifact))
        .await
        .map_err(|_| AdminError::unavailable("检查更新超时"))?
}
pub async fn download(
    artifact: &PluginArtifactInfo,
    expected: &PluginOnlineRelease,
) -> Result<Arc<[u8]>, AdminError> {
    let _permit = DOWNLOADS
        .try_acquire()
        .map_err(|_| AdminError::unavailable("正在处理其他插件更新，请稍后重试"))?;
    let client = HttpClient::new().map_err(|_| AdminError::unavailable("更新客户端初始化失败"))?;
    tokio::time::timeout(Duration::from_secs(120), async {
        let release = latest(&client, artifact)
            .await?
            .ok_or_else(|| AdminError::conflict("没有可安装的新版本，请重新检查"))?;
        if release.sha256 != expected.sha256 || release.version != expected.version {
            return Err(AdminError::conflict("更新版本或摘要已变化，请重新检查"));
        }
        let url = release
            .release_url
            .replace("/releases/tag/", "/releases/download/")
            + "/"
            + &release.asset_name;
        let bytes = fetch(&client, &url, MAX_ARCHIVE).await?;
        if bytes.len() as u64 != release.size
            || hex::encode(Sha256::digest(&bytes)) != release.sha256
        {
            return Err(AdminError::invalid("下载包摘要校验失败，原实例未修改"));
        }
        Ok(Arc::from(bytes))
    })
    .await
    .map_err(|_| AdminError::unavailable("下载更新超时，原实例未修改"))?
}
