use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures::{
    StreamExt,
    future::{BoxFuture, ready},
    stream,
};
use gateway_core::task::{ScheduledTask, WorkerCycleContext, WorkerTaskError};
use serde::Deserialize;

use super::selection::{Architecture, ClientKind, Platform, ReleaseTarget, valid_version};
use crate::ConfigError;

const DESKTOP_PAGE: &str = "https://antigravity.google/download";
const CLI_MANIFESTS: &str =
    "https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests";
const DESKTOP_DOWNLOADS: &str =
    "https://storage.googleapis.com/antigravity-public/antigravity-hub/";
const CLI_DOWNLOADS: &str = "https://storage.googleapis.com/antigravity-public/antigravity-cli/";
const MAX_METADATA_BYTES: usize = 2 * 1024 * 1024;
const REFRESH_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
const RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Release {
    version: String,
    build: String,
    download_url: String,
}

#[derive(Clone)]
pub(super) struct CheckedRelease {
    pub(super) version: String,
    pub(super) build: String,
    pub(super) download_url: String,
    pub(super) checked_at: DateTime<Utc>,
}

#[derive(Clone, Default)]
pub(super) struct ReleaseSnapshot {
    pub(super) release: Option<CheckedRelease>,
    pub(super) failed_at: Option<DateTime<Utc>>,
    next_check: Option<Instant>,
}

pub struct ReleaseService {
    client: reqwest::Client,
    snapshots: RwLock<BTreeMap<ReleaseTarget, ReleaseSnapshot>>,
}

impl ReleaseService {
    pub fn new() -> Result<Self, ConfigError> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| ConfigError)?;
        Ok(Self {
            client,
            snapshots: RwLock::default(),
        })
    }

    pub(super) fn snapshot(&self, target: ReleaseTarget) -> ReleaseSnapshot {
        self.snapshots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&target)
            .cloned()
            .unwrap_or_default()
    }

    fn due(&self, client: ClientKind) -> Vec<ReleaseTarget> {
        let snapshots = self
            .snapshots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        ReleaseTarget::for_client(client)
            .filter(|target| {
                snapshots
                    .get(target)
                    .and_then(|snapshot| snapshot.next_check)
                    .is_none_or(|next| next <= now)
            })
            .collect()
    }

    fn publish(&self, target: ReleaseTarget, release: Option<Release>) {
        let mut snapshots = self
            .snapshots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = snapshots.entry(target).or_default();
        let checked_at = Utc::now();
        let interval = if let Some(release) = release {
            snapshot.release = Some(CheckedRelease {
                version: release.version,
                build: release.build,
                download_url: release.download_url,
                checked_at,
            });
            snapshot.failed_at = None;
            REFRESH_AFTER
        } else {
            snapshot.failed_at = Some(checked_at);
            RETRY_AFTER
        };
        snapshot.next_check = Some(Instant::now() + interval);
    }

    async fn refresh(&self) -> Result<(), WorkerTaskError> {
        let desktop = self.due(ClientKind::Desktop);
        let cli = self.due(ClientKind::Cli);
        let (desktop_result, cli_result) = tokio::join!(
            self.refresh_desktop(desktop),
            stream::iter(cli)
                .map(|target| self.refresh_cli(target))
                .buffer_unordered(3)
                .fold(Ok(()), |combined, result| ready(combined.and(result)))
        );
        desktop_result.and(cli_result)
    }

    async fn refresh_desktop(&self, targets: Vec<ReleaseTarget>) -> Result<(), WorkerTaskError> {
        if targets.is_empty() {
            return Ok(());
        }
        let page = self.fetch_metadata(DESKTOP_PAGE).await;
        let mut result = Ok(());
        for target in targets {
            result = result.and(self.apply_metadata(target, page.as_deref().ok()));
        }
        page?;
        result
    }

    async fn refresh_cli(&self, target: ReleaseTarget) -> Result<(), WorkerTaskError> {
        let manifest_url = format!(
            "{CLI_MANIFESTS}/{}_{}.json",
            target.platform.os_type(),
            target.arch.label(),
        );
        let body = self.fetch_metadata(&manifest_url).await;
        let result = self.apply_metadata(target, body.as_deref().ok());
        body?;
        result
    }

    /// Applies metadata fetched from the official endpoint for this target.
    #[doc(hidden)]
    pub fn apply_metadata(
        &self,
        target: ReleaseTarget,
        body: Option<&[u8]>,
    ) -> Result<(), WorkerTaskError> {
        let release = body
            .filter(|body| body.len() <= MAX_METADATA_BYTES)
            .and_then(|body| match target.client {
                ClientKind::Desktop => parse_desktop(str::from_utf8(body).ok()?, target),
                ClientKind::Cli => parse_cli(body, target),
            });
        let result = release.as_ref().map(|_| ()).ok_or_else(metadata_error);
        self.publish(target, release);
        result
    }

    async fn fetch_metadata(&self, endpoint: &str) -> Result<Vec<u8>, WorkerTaskError> {
        let mut response = self
            .client
            .get(endpoint)
            .send()
            .await
            .map_err(|_| metadata_error())?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_METADATA_BYTES as u64)
        {
            return Err(metadata_error());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| metadata_error())? {
            if chunk.len() > MAX_METADATA_BYTES - body.len() {
                return Err(metadata_error());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

fn parse_desktop(page: &str, target: ReleaseTarget) -> Option<Release> {
    let filename = match (target.platform, target.arch) {
        (Platform::Macos, _) => "Antigravity.dmg",
        (Platform::Linux, _) => "Antigravity.tar.gz",
        (Platform::Windows, Architecture::Arm64) => "Antigravity-arm64.exe",
        (Platform::Windows, Architecture::Amd64) => "Antigravity-x64.exe",
    };
    let suffix = target_path(target, filename);
    let mut selected = None;
    for attribute in page.split("href=").skip(1) {
        let Some(quote @ ('"' | '\'')) = attribute.chars().next() else {
            continue;
        };
        let Some((address, _)) = attribute[1..].split_once(quote) else {
            continue;
        };
        let Some(release) = parse_download(address, DESKTOP_DOWNLOADS, &suffix) else {
            continue;
        };
        if selected
            .as_ref()
            .is_some_and(|previous| previous != &release)
        {
            return None;
        }
        selected = Some(release);
    }
    selected
}

fn parse_cli(body: &[u8], target: ReleaseTarget) -> Option<Release> {
    #[derive(Deserialize)]
    struct Manifest {
        version: String,
        url: String,
        sha512: String,
    }
    let manifest: Manifest = serde_json::from_slice(body).ok()?;
    let (platform, extension) = match target.platform {
        Platform::Macos => ("mac", "tar.gz"),
        Platform::Linux => ("linux", "tar.gz"),
        Platform::Windows => ("windows", "exe"),
    };
    let arch = match target.arch {
        Architecture::Arm64 => "arm64",
        Architecture::Amd64 => "x64",
    };
    let filename = format!("cli_{platform}_{arch}.{extension}");
    let release = parse_download(
        &manifest.url,
        CLI_DOWNLOADS,
        &target_path(target, &filename),
    )?;
    (release.version == manifest.version
        && manifest.sha512.len() == 128
        && manifest.sha512.bytes().all(|byte| byte.is_ascii_hexdigit()))
    .then_some(release)
}

fn target_path(target: ReleaseTarget, filename: &str) -> String {
    format!(
        "/{}-{}/{filename}",
        target.platform.os_type(),
        target.arch.release_directory()
    )
}

fn parse_download(url: &str, prefix: &str, suffix: &str) -> Option<Release> {
    let (version, build) = url
        .strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .split_once('-')?;
    (valid_version(version)
        && (1..=20).contains(&build.len())
        && build.bytes().all(|byte| byte.is_ascii_digit()))
    .then(|| Release {
        version: version.to_owned(),
        build: build.to_owned(),
        download_url: url.to_owned(),
    })
}

fn metadata_error() -> WorkerTaskError {
    WorkerTaskError::safe("无法读取 Google 官方 Antigravity 发布资料")
}

pub(crate) struct ReleaseTask(pub(crate) Arc<ReleaseService>);

impl ScheduledTask for ReleaseTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = self.0.refresh() => result,
            }
        })
    }
}
