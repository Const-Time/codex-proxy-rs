//! Durable delivery journal for the PostgreSQL budget owner, never a second balance ledger.
use fs2::FileExt as _;
use gateway_core::{
    engine::{
        ModelRequestId,
        budget::{ClientBudgetBilling, ClientBudgetCharge, ClientBudgetError, ClientBudgetScope},
    },
    metering::MultiplierScope,
    policy::ClientApiKeyId,
    routing::AccountGroupId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

#[derive(Clone)]
pub(super) struct Journal {
    root: PathBuf,
    slots: Arc<tokio::sync::Semaphore>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    request: String,
    key: String,
    user: String,
    group: String,
    amount: String,
    components: [String; 4],
    multiplier: String,
    scope: MultiplierScope,
    completed_at: SystemTime,
}
impl Pending {
    fn from_charge(charge: &ClientBudgetCharge) -> Result<Self, ClientBudgetError> {
        let scope = charge.scope.as_ref().ok_or(ClientBudgetError)?;
        Ok(Self {
            request: charge.request_id.as_str().to_owned(),
            key: charge.key_id.as_str().to_owned(),
            user: scope.user_id.clone(),
            group: scope.group_id.as_str().to_owned(),
            amount: charge.amount_usd.canonical(),
            components: charge.components_usd.map(|v| v.canonical()),
            multiplier: scope.billing.multiplier().canonical(),
            scope: scope.billing.scope().clone(),
            completed_at: charge.completed_at,
        })
    }
    fn charge(self) -> Result<ClientBudgetCharge, ClientBudgetError> {
        let [i, o, r, w] = self.components;
        Ok(ClientBudgetCharge {
            request_id: ModelRequestId::new(self.request).map_err(|_| ClientBudgetError)?,
            key_id: ClientApiKeyId::new(self.key).map_err(|_| ClientBudgetError)?,
            scope: Some(ClientBudgetScope {
                user_id: self.user,
                group_id: AccountGroupId::new(self.group).map_err(|_| ClientBudgetError)?,
                billing: ClientBudgetBilling::new(
                    self.multiplier.parse().map_err(|_| ClientBudgetError)?,
                    self.scope,
                )
                .map_err(|_| ClientBudgetError)?,
            }),
            amount_usd: self.amount.parse().map_err(|_| ClientBudgetError)?,
            components_usd: [
                i.parse().map_err(|_| ClientBudgetError)?,
                o.parse().map_err(|_| ClientBudgetError)?,
                r.parse().map_err(|_| ClientBudgetError)?,
                w.parse().map_err(|_| ClientBudgetError)?,
            ],
            completed_at: self.completed_at,
        })
    }
}
impl Journal {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }
    async fn io<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Path) -> Result<T, ClientBudgetError> + Send + 'static,
    ) -> Result<T, ClientBudgetError> {
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ClientBudgetError)?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            fs::create_dir_all(&root).map_err(|_| ClientBudgetError)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                    .map_err(|_| ClientBudgetError)?;
            }
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join(".lock"))
                .map_err(|_| ClientBudgetError)?;
            lock.lock_exclusive().map_err(|_| ClientBudgetError)?;
            operation(&root)
        })
        .await
        .map_err(|_| ClientBudgetError)?
    }
    pub(super) async fn persist(
        &self,
        charge: ClientBudgetCharge,
    ) -> Result<ClientBudgetCharge, ClientBudgetError> {
        let pending = Pending::from_charge(&charge)?;
        self.io(move |root| {
            let path = record_path(root, &pending.request);
            if path.exists() {
                return read(&path);
            }
            let bytes = serde_json::to_vec(&pending).map_err(|_| ClientBudgetError)?;
            if bytes.len() > 8192 {
                return Err(ClientBudgetError);
            }
            let temp = root.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options.open(&temp).map_err(|_| ClientBudgetError)?;
            let result = (|| {
                file.write_all(&bytes).map_err(|_| ClientBudgetError)?;
                file.sync_all().map_err(|_| ClientBudgetError)?;
                fs::rename(&temp, &path).map_err(|_| ClientBudgetError)?;
                sync_directory(root)?;
                Ok(charge)
            })();
            if result.is_err() {
                let _ = fs::remove_file(temp);
            }
            result
        })
        .await
    }
    pub(super) async fn remove(&self, request: String) -> Result<(), ClientBudgetError> {
        self.io(
            move |root| match fs::remove_file(record_path(root, &request)) {
                Ok(()) => sync_directory(root),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(ClientBudgetError),
            },
        )
        .await
    }
    pub(super) async fn pending(&self) -> Result<Vec<ClientBudgetCharge>, ClientBudgetError> {
        self.io(|root| {
            // Recover a complete fsynced record interrupted before atomic publication.
            // Incomplete records fail closed instead of silently losing a known charge.
            for entry in fs::read_dir(root).map_err(|_| ClientBudgetError)? {
                let entry = entry.map_err(|_| ClientBudgetError)?;
                let name = entry.file_name();
                let Some(id) = name
                    .to_str()
                    .and_then(|s| s.strip_prefix('.'))
                    .and_then(|s| s.strip_suffix(".tmp"))
                else {
                    continue;
                };
                if uuid::Uuid::parse_str(id).is_err() {
                    continue;
                }
                if !entry.file_type().map_err(|_| ClientBudgetError)?.is_file() {
                    return Err(ClientBudgetError);
                }
                let charge = read(&entry.path())?;
                let target = record_path(root, charge.request_id.as_str());
                if target.exists() {
                    fs::remove_file(entry.path()).map_err(|_| ClientBudgetError)?;
                } else {
                    fs::rename(entry.path(), target).map_err(|_| ClientBudgetError)?;
                }
                sync_directory(root)?;
            }
            let mut result = Vec::new();
            for entry in fs::read_dir(root).map_err(|_| ClientBudgetError)? {
                let entry = entry.map_err(|_| ClientBudgetError)?;
                if entry.path().extension().is_some_and(|e| e == "json") {
                    if !entry.file_type().map_err(|_| ClientBudgetError)?.is_file() {
                        return Err(ClientBudgetError);
                    }
                    let charge = read(&entry.path())?;
                    if entry.path() != record_path(root, charge.request_id.as_str()) {
                        return Err(ClientBudgetError);
                    }
                    result.push(charge);
                }
            }
            Ok(result)
        })
        .await
    }
}
fn record_path(root: &Path, request: &str) -> PathBuf {
    root.join(format!(
        "{}.json",
        hex::encode(Sha256::digest(request.as_bytes()))
    ))
}
fn read(path: &Path) -> Result<ClientBudgetCharge, ClientBudgetError> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| ClientBudgetError)?
        .take(8193)
        .read_to_end(&mut bytes)
        .map_err(|_| ClientBudgetError)?;
    if bytes.len() > 8192 {
        return Err(ClientBudgetError);
    }
    serde_json::from_slice::<Pending>(&bytes)
        .map_err(|_| ClientBudgetError)?
        .charge()
}
fn sync_directory(root: &Path) -> Result<(), ClientBudgetError> {
    #[cfg(unix)]
    fs::File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|_| ClientBudgetError)?;
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}
