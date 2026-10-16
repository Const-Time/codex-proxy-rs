mod auth;
mod endpoint;
mod error;
mod execution;
mod images;
mod ingress;
pub(crate) mod middleware;
mod models;
mod responses;
mod router;
mod search;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_core::account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy};
use gateway_core::engine::admission::{
    ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort, ClientAdmissionRecovery,
    ClientAdmissionRequest, ClientAdmissionRestoreResult,
};
use gateway_core::engine::continuation::{
    NativeContinuationPin, NativeContinuationPort, NativeContinuationStoreError, PreviousResponseId,
};
use gateway_core::engine::execution::{
    AuthenticatedClient, ClientApiKeyUsageSink, DefaultExecutionService, ExecutionService,
};
use gateway_core::engine::provider::ProviderRegistry;
use gateway_core::engine::{
    AttemptRecord, ExecutionStore, IntermediateFailure, ModelRequestFinalization, ModelRequestId,
    NewModelRequest, RecoveryReport,
};
use gateway_core::error::StoreError;
use gateway_core::health::{WorkerHealthSnapshot, WorkerHealthSource};
use gateway_core::lifecycle::{ConnectionDraining, ConnectionGuard, ConnectionLifecycle};
use gateway_core::policy::{
    ClientApiKeyId, ClientPolicy, CodexClientMinVersions, CodexClientVersion,
    PlaintextClientApiKey, RateLimits,
};
use gateway_core::routing::{
    ClientRoutingScope, ConfigRevision, FrozenAccountScope, ModelCapabilities, ProviderKind,
    ProviderModel, RuntimeAccount, RuntimeAccountDirectory, RuntimeSnapshot, UpstreamModelId,
};
use gateway_core::runtime::RuntimeSnapshotHandle;
use gateway_core::upstream::UpstreamSendState;

pub(super) async fn api_router(execution: Arc<dyn ExecutionService>) -> axum::Router {
    api_router_with_origins(execution, Vec::new()).await
}

pub(super) async fn api_router_with_worker_health(
    execution: Arc<dyn ExecutionService>,
    worker_health: Arc<dyn WorkerHealthSource>,
) -> axum::Router {
    api_router_with_origins_and_worker_health(execution, Vec::new(), worker_health).await
}

pub(super) async fn api_router_with_origins(
    execution: Arc<dyn ExecutionService>,
    cors_allowed_origins: Vec<String>,
) -> axum::Router {
    api_router_with_origins_and_worker_health(
        execution,
        cors_allowed_origins,
        Arc::new(EmptyWorkerHealth),
    )
    .await
}

async fn api_router_with_origins_and_worker_health(
    execution: Arc<dyn ExecutionService>,
    cors_allowed_origins: Vec<String>,
    worker_health: Arc<dyn WorkerHealthSource>,
) -> axum::Router {
    let admin = crate::admin::AdminTestFixture::new().await;
    gateway_api::initialize(
        gateway_api::ApiConfig {
            ingress: Default::default(),
            asset_directory: std::env::temp_dir(),
            cors_allowed_origins,
            request_timeout_seconds: None,
            request_id_header: "x-request-id".to_owned(),
        },
        execution,
        admin.services,
        Vec::new(),
        worker_health,
        Arc::new(TestLifecycle::default()),
    )
    .expect("API bundle")
    .router()
}

pub(super) fn authenticated_client(plaintext: &str) -> AuthenticatedClient {
    authenticated_client_for_provider(plaintext, "openai")
}

pub(crate) fn bound_client(
    snapshots: RuntimeSnapshotHandle,
    key: &ClientApiKeyId,
    revocation: gateway_core::lifecycle::CancellationToken,
) -> Result<AuthenticatedClient, gateway_core::engine::execution::ClientAuthenticationError> {
    DefaultExecutionService::new(
        snapshots,
        Arc::new(UnusedExecutionStore),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(IgnoredClientApiKeyUsage),
    )
    .authenticate_bound_key(key, revocation)
}

pub(super) fn authenticated_client_for_provider(
    plaintext: &str,
    provider_name: &str,
) -> AuthenticatedClient {
    let source = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(snapshot(plaintext, provider_name)),
        Arc::new(UnusedExecutionStore),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(IgnoredClientApiKeyUsage),
    );
    source
        .authenticate(plaintext)
        .expect("authenticated client")
}

pub(super) fn authenticated_client_with_min_versions(
    plaintext: &str,
    desktop: Option<&str>,
    cli: Option<&str>,
) -> AuthenticatedClient {
    let snapshot =
        snapshot(plaintext, "openai").with_min_codex_client_versions(CodexClientMinVersions::new(
            desktop.map(|version| CodexClientVersion::parse(version).expect("Desktop min version")),
            cli.map(|version| CodexClientVersion::parse(version).expect("CLI min version")),
        ));
    let source = DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(snapshot),
        Arc::new(UnusedExecutionStore),
        ProviderRegistry::default(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(IgnoredClientApiKeyUsage),
    );
    source
        .authenticate(plaintext)
        .expect("authenticated client")
}

struct IgnoredClientApiKeyUsage;

impl ClientApiKeyUsageSink for IgnoredClientApiKeyUsage {
    fn record_used(&self, _: &ClientApiKeyId) {}
}

fn snapshot(plaintext: &str, provider_name: &str) -> RuntimeSnapshot {
    let capabilities = ModelCapabilities::new(
        BTreeSet::from([gateway_core::operation::OperationKind::Generate]),
        Some(16_000),
    );
    snapshot_with_capabilities(plaintext, provider_name, capabilities)
}
fn snapshot_with_capabilities(
    plaintext: &str,
    provider_name: &str,
    capabilities: ModelCapabilities,
) -> RuntimeSnapshot {
    snapshot_with_capabilities_and_accounts(
        plaintext,
        provider_name,
        capabilities,
        &["acct_api_test"],
    )
}
fn snapshot_with_capabilities_and_accounts(
    plaintext: &str,
    provider_name: &str,
    capabilities: ModelCapabilities,
    accounts: &[&str],
) -> RuntimeSnapshot {
    let provider = ProviderKind::new(provider_name).expect("provider");
    let account_directory = Arc::new(RuntimeAccountDirectory::new(
        accounts
            .iter()
            .map(|id| {
                (
                    ProviderAccountId::new(*id).expect("account ID"),
                    RuntimeAccount::new(provider.clone(), BTreeSet::new()),
                )
            })
            .collect(),
    ));
    RuntimeSnapshot::new(
        ConfigRevision::new(1).expect("revision"),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(2).expect("concurrency"),
            Duration::from_millis(1),
        ),
        vec![provider.clone()],
        ["model-a", "model-b"]
            .into_iter()
            .map(|model| {
                ProviderModel::new(
                    provider.clone(),
                    UpstreamModelId::new(model).expect("model"),
                    capabilities.clone(),
                )
            })
            .collect(),
        vec![ClientPolicy::new(
            ClientApiKeyId::new("key_api_test").expect("key ID"),
            PlaintextClientApiKey::new(plaintext).expect("plaintext key"),
            Arc::new(FrozenAccountScope::new(
                Arc::clone(&account_directory),
                ClientRoutingScope::all_accounts(),
            )),
            true,
            RateLimits::unlimited(),
        )],
    )
    .expect("runtime snapshot")
    .with_account_directory(account_directory)
}

#[derive(Default)]
pub(crate) struct TestLifecycle {
    cancellation: gateway_core::lifecycle::CancellationToken,
    pub(crate) active: Arc<std::sync::atomic::AtomicUsize>,
}

struct TestConnectionGuard(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for TestConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl ConnectionGuard for TestConnectionGuard {}

impl ConnectionLifecycle for TestLifecycle {
    fn try_register(&self) -> Result<Box<dyn ConnectionGuard>, ConnectionDraining> {
        if self.is_draining() {
            return Err(ConnectionDraining);
        }
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(TestConnectionGuard(self.active.clone())))
    }

    fn cancellation(&self) -> gateway_core::lifecycle::CancellationToken {
        self.cancellation.clone()
    }

    fn is_draining(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

pub(crate) struct EmptyWorkerHealth;

impl WorkerHealthSource for EmptyWorkerHealth {
    fn snapshot(&self) -> Vec<WorkerHealthSnapshot> {
        Vec::new()
    }
}

struct UnusedExecutionStore;

#[async_trait]
impl ExecutionStore for UnusedExecutionStore {
    fn maintain_request(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: Option<std::time::SystemTime>,
        _: std::time::Duration,
    ) -> Box<dyn gateway_core::engine::ExecutionRecoveryLeaseGuard> {
        Box::new(TestExecutionRecoveryLease)
    }

    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn finalize_model_request(&self, _: ModelRequestFinalization) -> Result<(), StoreError> {
        unreachable!("authentication fixture does not execute")
    }

    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        unreachable!("authentication fixture does not execute")
    }
}

struct UnusedAdmissions;

impl ClientAdmissionPort for UnusedAdmissions {
    fn maintain(
        &self,
        _: &gateway_core::policy::ClientApiKeyId,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::Duration,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Box<dyn gateway_core::engine::admission::ClientAdmissionLeaseGuard> {
        Box::new(TestAdmissionLease)
    }

    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async { unreachable!("authentication fixture does not execute") })
    }

    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async { unreachable!("authentication fixture does not execute") })
    }

    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { unreachable!("authentication fixture does not execute") })
    }
}

struct UnusedContinuation;

impl NativeContinuationPort for UnusedContinuation {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async { unreachable!("authentication fixture does not execute") })
    }

    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { unreachable!("authentication fixture does not execute") })
    }
}

fn api_router_with_config(
    admin: gateway_admin::AdminServices,
    config: gateway_api::ApiConfig,
) -> axum::Router {
    gateway_api::initialize(
        config,
        Arc::new(DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(snapshot("unused", "openai")),
            Arc::new(UnusedExecutionStore),
            ProviderRegistry::default(),
            Arc::new(UnusedAdmissions),
            Arc::new(UnusedContinuation),
            Arc::new(IgnoredClientApiKeyUsage),
        )),
        admin,
        Vec::new(),
        Arc::new(EmptyWorkerHealth),
        Arc::new(TestLifecycle::default()),
    )
    .expect("API bundle")
    .router()
}

#[tokio::test]
async fn api_should_serve_resolved_assets_with_relative_environment_override() {
    use axum::{body::Body, http::Request};
    use std::{fs, process::Command};
    use tower::ServiceExt as _;

    const CHILD_ENV: &str = "CPR_TEST_ASSET_DIRECTORY";
    let Ok(directory) = std::env::var(CHILD_ENV) else {
        let directory = std::env::temp_dir().join(format!("cpr-assets-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(directory.join("deploy")).expect("config directory");
        fs::create_dir_all(directory.join("web/dist/assets")).expect("asset directory");
        fs::write(directory.join("web/dist/index.html"), "resolved-web-assets").expect("index");
        fs::write(
            directory.join("web/dist/assets/app-v1.js"),
            "export default 1;",
        )
        .expect("chunk");
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "openai::api_should_serve_resolved_assets_with_relative_environment_override",
            ])
            .current_dir(&directory)
            .env(CHILD_ENV, &directory)
            .env("CPR_WEB_DIST_DIR", "../web/dist")
            .output()
            .expect("isolated asset directory test");
        fs::remove_dir_all(directory).expect("remove fixture");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    };
    let mut config = gateway_api::ApiConfig {
        ingress: Default::default(),
        asset_directory: "unused".into(),
        cors_allowed_origins: Vec::new(),
        request_timeout_seconds: None,
        request_id_header: "x-request-id".to_owned(),
    };
    config
        .resolve_and_validate(&std::path::Path::new(&directory).join("deploy"))
        .expect("resolved API config");
    let admin = crate::admin::AdminTestFixture::new().await;
    let router = api_router_with_config(admin.services, config);
    let response = router
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .expect("static response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-cache");
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"resolved-web-assets");
    for (path, status, cache, expected) in [
        ("/usage", 200, "no-cache", "resolved-web-assets"),
        (
            "/assets/app-v1.js",
            200,
            "public, max-age=31536000, immutable",
            "export default 1;",
        ),
        ("/assets/removed-v0.js", 404, "no-store", ""),
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .expect("static response");
        assert_eq!(response.status().as_u16(), status, "{path}");
        assert_eq!(response.headers()["cache-control"], cache, "{path}");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), expected.as_bytes(), "{path}");
    }
    let health = router
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(!health.headers().contains_key("cache-control"));
}

struct TestAdmissionLease;
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for TestAdmissionLease {}

struct TestExecutionRecoveryLease;
impl gateway_core::engine::ExecutionRecoveryLeaseGuard for TestExecutionRecoveryLease {}
