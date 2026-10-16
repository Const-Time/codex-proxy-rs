//! 将已验证插件包解压到独立私有目录，并持有可执行资源

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{PackageError, ValidatedPackage};

/// 每次准备创建独立私有目录；存储中的包体为权威，缓存不会被原地升级
pub struct PreparedPackage {
    directory: tempfile::TempDir,
    executable: PathBuf,
    manifest: gateway_plugin_sdk::Manifest,
    digest: String,
    resources: BTreeMap<String, Vec<u8>>,
}

impl ValidatedPackage {
    pub fn prepare(self: &Arc<Self>, cache: &Path) -> Result<PreparedPackage, PackageError> {
        self.manifest
            .package_for_platform(std::env::consts::OS, std::env::consts::ARCH)?;
        fs::create_dir_all(cache).map_err(|_| PackageError::Cache)?;
        let directory = tempfile::Builder::new()
            .prefix("plugin-")
            .tempdir_in(cache)
            .map_err(|_| PackageError::Cache)?;
        for (path, data) in &self.files {
            let target = directory.path().join(path);
            fs::create_dir_all(target.parent().ok_or(PackageError::Path)?)
                .map_err(|_| PackageError::Cache)?;
            fs::write(&target, data).map_err(|_| PackageError::Cache)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let mode = if path == &self.manifest.main {
                    0o700
                } else {
                    0o600
                };
                fs::set_permissions(&target, fs::Permissions::from_mode(mode))
                    .map_err(|_| PackageError::Cache)?;
            }
        }
        let executable = directory.path().join(&self.manifest.main);
        Ok(PreparedPackage {
            directory,
            executable,
            manifest: self.manifest.clone(),
            digest: self.digest.clone(),
            resources: self
                .files
                .iter()
                .filter(|(path, _)| self.manifest.resources.contains_key(*path))
                .map(|(path, bytes)| (path.clone(), bytes.clone()))
                .collect(),
        })
    }
}

impl PreparedPackage {
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    #[must_use]
    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    #[must_use]
    pub fn manifest(&self) -> &gateway_plugin_sdk::Manifest {
        &self.manifest
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub(crate) fn resource(&self, path: &str) -> Option<&[u8]> {
        self.resources.get(path).map(Vec::as_slice)
    }
}
