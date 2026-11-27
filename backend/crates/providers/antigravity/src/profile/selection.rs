use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::OpaqueProviderData;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ClientKind {
    #[default]
    Desktop,
    Cli,
}

impl ClientKind {
    pub(super) const ALL: [Self; 2] = [Self::Desktop, Self::Cli];

    pub(super) const fn product(self) -> &'static str {
        match self {
            Self::Desktop => "Antigravity Desktop",
            Self::Cli => "Antigravity CLI",
        }
    }

    const fn subclient(self) -> &'static str {
        match self {
            Self::Desktop => "hub",
            Self::Cli => "cli",
        }
    }

    const fn baseline_version(self) -> &'static str {
        match self {
            Self::Desktop => "2.34.0",
            Self::Cli => "1.5.17",
        }
    }

    fn changelist(self, version: &str) -> Option<u64> {
        match (self, version) {
            (Self::Desktop, "2.34.0") => Some(1_026_637_980),
            (Self::Cli, "1.5.17") => Some(1_029_496_841),
            (Self::Cli, "1.5.15") => Some(1_027_820_220),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Platform {
    #[default]
    Macos,
    Linux,
    Windows,
}

impl Platform {
    pub(super) const ALL: [Self; 3] = [Self::Macos, Self::Linux, Self::Windows];

    pub(super) const fn os_type(self) -> &'static str {
        match self {
            Self::Macos => "darwin",
            Self::Linux => "linux",
            Self::Windows => "windows",
        }
    }

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Macos => "macOS",
            Self::Linux => "Linux",
            Self::Windows => "Windows",
        }
    }

    const fn default_arch(self) -> Architecture {
        match self {
            Self::Macos => Architecture::Arm64,
            Self::Linux | Self::Windows => Architecture::Amd64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Architecture {
    Arm64,
    Amd64,
}

impl Architecture {
    pub(super) const ALL: [Self; 2] = [Self::Arm64, Self::Amd64];

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Arm64 => "arm64",
            Self::Amd64 => "amd64",
        }
    }

    pub(super) const fn release_directory(self) -> &'static str {
        match self {
            Self::Arm64 => "arm",
            Self::Amd64 => "x64",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum VersionMode {
    #[default]
    Latest,
    Fixed,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Selection {
    pub(super) client: ClientKind,
    pub(super) platform: Platform,
    pub(super) arch: Option<Architecture>,
    pub(super) version_mode: VersionMode,
    pub(super) client_version: Option<String>,
}

impl Selection {
    pub fn parse(configuration: &OpaqueProviderData) -> Result<Self, ProviderAdminError> {
        let selection: Self =
            serde_json::from_value(Value::Object(configuration.expose_to_provider().clone()))
                .map_err(|_| invalid())?;
        match (&selection.version_mode, selection.client_version.as_deref()) {
            (VersionMode::Latest, None) => Ok(selection),
            (VersionMode::Fixed, Some(version)) if valid_version(version) => Ok(selection),
            _ => Err(invalid()),
        }
    }

    pub(super) fn configuration(&self) -> Map<String, Value> {
        let Value::Object(configuration) = json!(self) else {
            unreachable!("the Antigravity selection serializes as an object");
        };
        configuration
    }

    pub fn target(&self) -> ReleaseTarget {
        ReleaseTarget {
            client: self.client,
            platform: self.platform,
            arch: self.arch.unwrap_or_else(|| self.platform.default_arch()),
        }
    }

    pub(super) fn resolve(&self, latest: Option<&str>) -> ClientProfile {
        let version = self
            .client_version
            .as_deref()
            .or(latest)
            .unwrap_or_else(|| self.client.baseline_version());
        let target = self.target();
        let client_identifier = format!("antigravity/{}/{version}", self.client.subclient());
        let mut user_agent = format!(
            "{client_identifier} (aidev_client; os_type={}; arch={}",
            target.platform.os_type(),
            target.arch.label(),
        );
        if let Some(changelist) = self.client.changelist(version) {
            use std::fmt::Write;
            let _ = write!(user_agent, "; cl={changelist}");
        }
        if self.client == ClientKind::Cli {
            user_agent.push_str("; auth_method=consumer");
        }
        user_agent.push(')');
        ClientProfile {
            target,
            version: version.to_owned(),
            client_identifier,
            user_agent,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReleaseTarget {
    pub(super) client: ClientKind,
    pub(super) platform: Platform,
    pub(super) arch: Architecture,
}

impl ReleaseTarget {
    pub(super) fn for_client(client: ClientKind) -> impl Iterator<Item = Self> {
        Platform::ALL.into_iter().flat_map(move |platform| {
            Architecture::ALL.into_iter().map(move |arch| Self {
                client,
                platform,
                arch,
            })
        })
    }
}

#[derive(Clone)]
pub struct ClientProfile {
    pub(super) target: ReleaseTarget,
    pub(super) version: String,
    client_identifier: String,
    user_agent: String,
}

impl ClientProfile {
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    pub(crate) fn client_identifier(&self) -> &str {
        &self.client_identifier
    }
}

pub(super) fn valid_version(version: &str) -> bool {
    let mut parts = version.split('.');
    parts.clone().count() == 3
        && parts.all(|part| {
            (1..=9).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn invalid() -> ProviderAdminError {
    ProviderAdminError::new(ProviderAdminErrorKind::Invalid)
        .with_public_message("Antigravity 客户端配置无效。自动版本须留空，固定版本须为三段数字。")
}
