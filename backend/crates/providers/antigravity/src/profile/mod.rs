//! Resolves the saved Antigravity request profile and its official release metadata.

mod release;
mod selection;

use std::sync::Arc;

use gateway_admin::{
    model::{
        observability::{
            DashboardDesktopRelease, DashboardWireAttribute, DashboardWireProfile,
            DashboardWireTarget, DesktopReleaseStatus,
        },
        settings::{ProviderClientRelease, ProviderRequestProfile},
    },
    ports::provider::{ProviderAdminError, ProviderAdminErrorKind},
};
use gateway_core::{
    account::OpaqueProviderData, provider_ports::ProviderRuntimePolicyPort, routing::ProviderKind,
};
use serde_json::{Value, json};

pub use release::ReleaseService;
use release::ReleaseSnapshot;
pub(crate) use release::ReleaseTask;
use selection::{ClientKind, Platform, VersionMode};
pub use selection::{ClientProfile, ReleaseTarget, Selection};

pub struct ProfileService {
    provider: ProviderKind,
    policy: Arc<dyn ProviderRuntimePolicyPort>,
    releases: Arc<ReleaseService>,
}

impl ProfileService {
    pub fn new(
        provider: ProviderKind,
        policy: Arc<dyn ProviderRuntimePolicyPort>,
        releases: Arc<ReleaseService>,
    ) -> Self {
        Self {
            provider,
            policy,
            releases,
        }
    }

    async fn stored(&self) -> Result<Selection, ProviderAdminError> {
        let configuration = self
            .policy
            .load_client_profile(&self.provider)
            .await
            .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Unavailable))?;
        match configuration {
            None => Ok(Selection::default()),
            Some(configuration) => Selection::parse(&configuration)
                .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Internal)),
        }
    }

    fn resolve(&self, selection: &Selection) -> (ClientProfile, ReleaseSnapshot) {
        let snapshot = self.releases.snapshot(selection.target());
        let profile = selection.resolve(
            snapshot
                .release
                .as_ref()
                .map(|release| release.version.as_str()),
        );
        (profile, snapshot)
    }

    pub async fn current(&self) -> Result<ClientProfile, ProviderAdminError> {
        let selection = self.stored().await?;
        Ok(self.resolve(&selection).0)
    }

    pub async fn view(
        &self,
        configuration: Option<&OpaqueProviderData>,
    ) -> Result<ProviderRequestProfile, ProviderAdminError> {
        let selection = match configuration {
            Some(configuration) => Selection::parse(configuration)?,
            None => self.stored().await?,
        };
        let (profile, snapshot) = self.resolve(&selection);
        Ok(ProviderRequestProfile {
            configuration: selection.configuration(),
            user_agent: profile.user_agent().to_owned(),
            release: snapshot
                .release
                .filter(|release| release.version == profile.version)
                .map(|release| ProviderClientRelease {
                    version: release.version,
                    download_url: release.download_url,
                    checked_at: release.checked_at,
                }),
        })
    }

    pub async fn dashboard(&self) -> Result<DashboardWireProfile, ProviderAdminError> {
        let selection = self.stored().await?;
        let (profile, snapshot) = self.resolve(&selection);
        let latest = selection.version_mode == VersionMode::Latest;
        Ok(DashboardWireProfile {
            provider: self.provider.as_str().to_owned(),
            product: profile.target.client.product().to_owned(),
            version: profile.version.clone(),
            build: None,
            target: DashboardWireTarget {
                os_type: profile.target.platform.label().to_owned(),
                os_version: String::new(),
                arch: profile.target.arch.label().to_owned(),
                terminal: String::new(),
            },
            user_agent: profile.user_agent().to_owned(),
            attributes: vec![
                DashboardWireAttribute {
                    label: "客户端标识".to_owned(),
                    value: profile.client_identifier().to_owned(),
                },
                DashboardWireAttribute {
                    label: "Version policy".to_owned(),
                    value: if latest { "latest" } else { "fixed" }.to_owned(),
                },
            ],
            verified_at: None,
            release: latest.then(|| dashboard_release(snapshot)),
        })
    }
}

pub(crate) fn options() -> OpaqueProviderData {
    let presets =
        Platform::ALL
            .into_iter()
            .flat_map(|platform| {
                ClientKind::ALL.map(|client| {
                let configuration = Selection { client, platform, ..Selection::default() };
                json!({"configuration": configuration, "automaticAvailable": true, "reason": null})
            })
            })
            .collect::<Vec<_>>();
    let Value::Object(options) = json!({"presets": presets, "defaults": Selection::default()})
    else {
        unreachable!("profile options serialize as an object");
    };
    OpaqueProviderData::new(options)
}

fn dashboard_release(snapshot: ReleaseSnapshot) -> DashboardDesktopRelease {
    let status = if snapshot.failed_at.is_some() {
        DesktopReleaseStatus::Failed
    } else if snapshot.release.is_some() {
        DesktopReleaseStatus::Current
    } else {
        DesktopReleaseStatus::Unchecked
    };
    let release = snapshot.release;
    DashboardDesktopRelease {
        status,
        checked_at: snapshot
            .failed_at
            .or_else(|| release.as_ref().map(|release| release.checked_at)),
        latest_version: release.as_ref().map(|release| release.version.clone()),
        latest_build: release.as_ref().map(|release| release.build.clone()),
        download_url: release.map(|release| release.download_url),
        published_at: None,
        minimum_system_version: None,
        hardware_requirements: None,
        download_size: None,
        signature_present: None,
        error: snapshot
            .failed_at
            .map(|_| "无法读取 Google 官方客户端版本".to_owned()),
    }
}
