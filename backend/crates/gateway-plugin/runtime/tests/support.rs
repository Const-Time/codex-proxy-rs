use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationActor, MutationContext, plugins::*},
    ports::plugins::PluginStore,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};
#[derive(Default)]
pub struct MemoryStore {
    pub save_fault: AtomicU8,
    unreadable: AtomicBool,
    pub records: Mutex<BTreeMap<String, PluginRecord>>,
    artifacts: Mutex<BTreeMap<String, PluginArtifact>>,
    state: Mutex<BTreeMap<(String, String, String), PluginStateRecord>>,
}
pub fn actor() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "plugin-test".into(),
    }
}
fn conflict() -> AdminError {
    AdminError::conflict("stale")
}
#[async_trait]
impl PluginStore for MemoryStore {
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        Ok(self
            .artifacts
            .lock()
            .unwrap()
            .values()
            .map(|a| a.info.clone())
            .collect())
    }
    async fn delete_artifact(&self, id: &str, _: &MutationContext) -> Result<(), AdminError> {
        if self
            .records
            .lock()
            .unwrap()
            .values()
            .any(|r| r.artifact_sha256 == id)
        {
            return Err(conflict());
        }
        self.artifacts.lock().unwrap().remove(id);
        Ok(())
    }
    async fn artifact_info(&self, id: &str) -> Result<PluginArtifactInfo, AdminError> {
        self.artifacts
            .lock()
            .unwrap()
            .get(id)
            .map(|a| a.info.clone())
            .ok_or_else(|| AdminError::not_found("artifact"))
    }
    async fn artifact(&self, id: &str) -> Result<PluginArtifact, AdminError> {
        self.artifacts
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| AdminError::not_found("artifact"))
    }
    async fn put_artifact(&self, a: PluginArtifact, _: &MutationContext) -> Result<(), AdminError> {
        self.artifacts
            .lock()
            .unwrap()
            .insert(a.info.sha256.clone(), a);
        Ok(())
    }
    async fn list(&self) -> Result<Vec<PluginRecord>, AdminError> {
        if self.unreadable.swap(false, Ordering::SeqCst) {
            return Err(AdminError::unavailable("read failed"));
        }
        Ok(self.records.lock().unwrap().values().cloned().collect())
    }
    async fn save(
        &self,
        r: &PluginRecord,
        expected: u64,
        _: &MutationContext,
    ) -> Result<(), AdminError> {
        let fault = self.save_fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(AdminError::unavailable("before commit"));
        }
        let mut records = self.records.lock().unwrap();
        if records.get(&r.id).map_or(0, |r| r.revision) != expected {
            return Err(conflict());
        }
        records.insert(r.id.clone(), r.clone());
        if fault == 3 {
            self.unreadable.store(true, Ordering::SeqCst);
        }
        if fault >= 2 {
            return Err(AdminError::unavailable("commit acknowledgement lost"));
        }
        Ok(())
    }
    async fn delete(&self, id: &str, expected: u64, _: &MutationContext) -> Result<(), AdminError> {
        let mut records = self.records.lock().unwrap();
        if records
            .get(id)
            .is_none_or(|r| r.revision != expected || r.enabled)
        {
            return Err(conflict());
        }
        records.remove(id);
        Ok(())
    }
    async fn state_get(
        &self,
        s: &PluginStateScope,
        key: &str,
    ) -> Result<Option<PluginStateRecord>, AdminError> {
        if self
            .records
            .lock()
            .unwrap()
            .get(&s.instance_id)
            .is_none_or(|r| r.revision != s.revision || !r.enabled)
        {
            return Err(conflict());
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .get(&(s.instance_id.clone(), s.namespace.clone(), key.into()))
            .map(|v| PluginStateRecord {
                value: v.value.clone(),
                version: v.version,
                schema_version: v.schema_version,
            }))
    }
    async fn state_put(
        &self,
        s: &PluginStateScope,
        key: &str,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
    ) -> Result<u64, AdminError> {
        if self
            .records
            .lock()
            .unwrap()
            .get(&s.instance_id)
            .is_none_or(|r| r.revision != s.revision || !r.enabled)
        {
            return Err(conflict());
        }
        let mut state = self.state.lock().unwrap();
        let key = (s.instance_id.clone(), s.namespace.clone(), key.into());
        if state.get(&key).map(|v| v.version) != expected {
            return Err(conflict());
        }
        let version = expected.unwrap_or(0) + 1;
        if let Some(value) = value {
            state.insert(
                key,
                PluginStateRecord {
                    value,
                    version,
                    schema_version: s.schema_version,
                },
            );
        } else {
            state.remove(&key);
        }
        Ok(version)
    }
}
pub fn archive(version: &str, extra_capability: bool) -> Arc<[u8]> {
    build_archive(
        version,
        extra_capability,
        false,
        false,
        false,
        (false, false, false, false),
    )
}
pub fn authentication_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        true,
        false,
        false,
        (false, false, false, false),
    )
}
pub fn routing_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        false,
        true,
        false,
        (false, false, false, false),
    )
}
pub fn scheduling_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        false,
        false,
        true,
        (false, false, false, false),
    )
}
pub fn retry_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        false,
        false,
        false,
        (true, false, false, false),
    )
}
pub fn http_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        true,
        false,
        false,
        false,
        (false, true, false, false),
    )
}
pub fn catalog_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        false,
        false,
        false,
        (false, false, true, false),
    )
}
pub fn upstream_archive(version: &str) -> Arc<[u8]> {
    build_archive(
        version,
        false,
        false,
        false,
        false,
        (false, false, false, true),
    )
}
fn build_archive(
    version: &str,
    extra_capability: bool,
    authentication: bool,
    routing: bool,
    scheduling: bool,
    modes: (bool, bool, bool, bool),
) -> Arc<[u8]> {
    let (retry, http, catalog, upstream) = modes;
    let executable = std::fs::read(env!("CARGO_BIN_EXE_gateway-plugin-test-worker")).unwrap();
    let page = b"<!doctype html><html><body>Plugin fixture</body></html>";
    let mut contributes =
        json!({"management":{"id":"local.fixture.management","version":1,"stages":["management"]}});
    if authentication {
        contributes["frontend_authentication"] =
            json!({"id":"local.fixture.auth","version":1,"stages":["authentication"]});
    }
    if routing {
        contributes["model_router"] =
            json!({"id":"local.fixture.routing","version":1,"stages":["routing"]});
    }
    if scheduling {
        contributes["scheduler"] =
            json!({"id":"local.fixture.scheduling","version":1,"stages":["scheduling"]});
    }
    if retry {
        contributes["retry_policy"] =
            json!({"id":"local.fixture.retry","version":1,"stages":["retry"]});
    }
    if extra_capability {
        contributes["middleware"] =
            json!({"id":"local.fixture.middleware","version":4,"stages":["request","attempt"]});
    }
    if http {
        contributes["middleware"]["stages"] =
            json!(["http", "websocket", "service", "request", "attempt"]);
    }
    if catalog {
        contributes["model_catalog"] =
            json!({"id":"local.fixture.catalog","version":1,"stages":["registration"]});
    }
    if upstream {
        contributes["upstream_adapter"] = json!({"id":"local.fixture.upstream","version":2,"stages":["upstream"],"inputFormats":["openai"],"outputFormats":["openai"]});
    }
    let manifest = json!({"manifestVersion":2,"name":"fixture","displayName":"Fixture","publisher":"local","version":version,"description":"Lifecycle fixture","license":"MIT",
        "engines":{"codex-proxy-rs":">=3.11.0, <4.0.0"},"main":"bin/plugin","runtime":"trustedProcess","contributes":contributes,
        "configurationSchema":{"type":"object","properties":{"adapter_models":{"type":"array","items":{"type":"string"}},"adapter_provider":{"type":"string"},"adapter_mode":{"type":"string"},"adapter_url":{"type":"string"},"adapter_transport":{"type":"string"},"catalog_mode":{"type":"string"},"retry":{"type":"string"},"schedule":{"type":"string"},"fail":{"type":"boolean"},"token":{"type":"string"}},"additionalProperties":false},"secretFields":["token"],
        "state":[{"namespace":"counter","schemaVersion":1,"schema":{"type":"integer"},"maximumRecords":10,"maximumBytes":1024,"maximumValueBytes":128}],
        "resources":{"index.html":"text/html"},"package":{"protocolVersion":2,"target":{"os":std::env::consts::OS,"architecture":std::env::consts::ARCH},
        "files":{"bin/plugin":hex::encode(Sha256::digest(&executable)),"index.html":hex::encode(Sha256::digest(page))}}});
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(encoder);
    for (path, bytes) in [
        ("plugin.json", serde_json::to_vec(&manifest).unwrap()),
        ("bin/plugin", executable),
        ("index.html", page.to_vec()),
    ] {
        let mut h = tar::Header::new_gnu();
        h.set_size(bytes.len() as u64);
        h.set_mode(0o700);
        h.set_cksum();
        tar.append_data(&mut h, path, bytes.as_slice()).unwrap();
    }
    Arc::from(tar.into_inner().unwrap().finish().unwrap())
}
pub fn update(sha: &str, revision: u64, enabled: bool) -> PluginUpdate {
    PluginUpdate {
        upstream_adapter: None,
        retry_policy: None,
        account_scheduler: None,
        model_router: None,
        middleware: None,
        frontend_authentication: None,
        revision,
        name: "Fixture".into(),
        artifact_sha256: sha.into(),
        enabled,
        trusted_process: true,
        configuration: json!({}),
        secrets: BTreeMap::from([("token".into(), Some("fixture-secret-must-not-leak".into()))]),
    }
}
pub fn request(operation: &str) -> PluginHttpRequest {
    PluginHttpRequest {
        invocation: None,
        actor: actor(),
        method: "POST".into(),
        path: "echo".into(),
        query: String::new(),
        content_type: Some("application/json".into()),
        body: serde_json::to_vec(&json!({"operation":operation})).unwrap(),
    }
}
