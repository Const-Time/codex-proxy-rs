//! Real HTTP routing, PluginManager publication, Core admission and settlement.
use super::*;
use crate::openai::{EmptyWorkerHealth, IgnoredClientApiKeyUsage, TestLifecycle, snapshot};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures::StreamExt;
use gateway_admin::{
    model::{AdminError, MutationContext, plugins::*},
    plugins::PluginManager,
    ports::plugins::*,
};
use gateway_core::{
    account::ProviderAccountId,
    engine::{admission::*, budget::*, continuation::*, execution::*, provider::*, *},
    error::{GatewayError, ProviderError, StoreError},
    event::{GatewayEvent, ResponseMeta},
    metering::{Decimal, ProviderReportedCost},
    operation::Operation,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, ProviderModelCapabilities},
    runtime::RuntimeSnapshotHandle,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use std::{sync::atomic::AtomicUsize, time::SystemTime};
use tower::ServiceExt;

type AdminResult<T> = Result<T, AdminError>;
struct Store {
    records: Vec<PluginRecord>,
}
fn artifact() -> PluginArtifact {
    PluginArtifact {
        info: PluginArtifactInfo {
            sha256: "a".repeat(64),
            plugin_id: "fixture".into(),
            version: "1".into(),
            display_name: "fixture".into(),
            manifest: json!({}),
            unsupported_capabilities: vec![],
            version_warning: None,
        },
        archive: Arc::from([]),
    }
}
#[async_trait]
impl PluginStore for Store {
    async fn list(&self) -> AdminResult<Vec<PluginRecord>> {
        Ok(self.records.clone())
    }
    async fn artifact(&self, _: &str) -> AdminResult<PluginArtifact> {
        Ok(artifact())
    }
    async fn artifacts(&self) -> AdminResult<Vec<PluginArtifactInfo>> {
        unreachable!()
    }
    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn artifact_info(&self, _: &str) -> AdminResult<PluginArtifactInfo> {
        unreachable!()
    }
    async fn put_artifact(&self, _: PluginArtifact, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn save(&self, _: &PluginRecord, _: u64, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: u64, _: &MutationContext) -> AdminResult<()> {
        unreachable!()
    }
    async fn state_get(
        &self,
        _: &PluginStateScope,
        _: &str,
    ) -> AdminResult<Option<PluginStateRecord>> {
        unreachable!()
    }
    async fn state_put(
        &self,
        _: &PluginStateScope,
        _: &str,
        _: Option<u64>,
        _: Option<serde_json::Value>,
    ) -> AdminResult<u64> {
        unreachable!()
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Rewrite,
    InvalidModel,
    Transport,
    BadResponse,
}
struct Plugin {
    access: PluginAccess,
    mode: Mode,
    seen: Mutex<Vec<MiddlewareContext>>,
    headers: Mutex<Vec<String>>,
}
struct Runtime(Arc<Plugin>);
#[async_trait]
impl PluginRuntime for Runtime {
    fn describe(&self, info: PluginArtifactInfo) -> AdminResult<PluginArtifactInfo> {
        Ok(info)
    }
    async fn inspect(&self, _: Arc<[u8]>) -> AdminResult<PluginArtifact> {
        unreachable!()
    }
    async fn validate(
        &self,
        _: &PluginRecord,
        _: &PluginArtifact,
        _: Option<&PluginArtifact>,
    ) -> AdminResult<()> {
        Ok(())
    }
    async fn prepare(
        &self,
        _: &PluginRecord,
        _: PluginArtifact,
    ) -> AdminResult<Arc<dyn PluginProcess>> {
        Ok(self.0.clone())
    }
}
#[async_trait]
impl PluginProcess for Plugin {
    fn access(&self) -> PluginAccess {
        self.access.clone()
    }
    fn status(&self) -> &'static str {
        "ready"
    }
    fn pages(&self) -> Vec<PluginPage> {
        vec![]
    }
    fn resources(&self) -> Vec<PluginResource> {
        vec![]
    }
    fn activate(&self) {}
    fn revoke(&self) {
        self.access.revoke();
    }
    async fn stop(&self) {}
    async fn resource(&self, _: &str) -> AdminResult<PluginHttpResponse> {
        unreachable!()
    }
    async fn handle(&self, _: PluginHttpRequest) -> AdminResult<PluginHttpResponse> {
        unreachable!()
    }
    async fn middleware(
        &self,
        context: MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        self.seen.lock().unwrap().push(context);
        *self.headers.lock().unwrap() = request.headers().iter().map(|h| h.name().into()).collect();
        let (_, mut headers, body) = request.into_parts();
        let mut body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        body["model"] = json!(if matches!(self.mode, Mode::InvalidModel) {
            "forbidden-model"
        } else {
            "model-b"
        });
        body["input"] = json!("changed input");
        if matches!(self.mode, Mode::Transport) {
            body["stream"] = json!(true);
        }
        for (name, value) in [
            ("authorization", "Bearer forged"),
            ("connection", "x-plugin-hop"),
            ("x-plugin-hop", "hidden"),
            ("chatgpt-account-id", "forged-account"),
        ] {
            headers.push(MiddlewareHeader::new(
                name,
                Bytes::from_static(value.as_bytes()),
            ));
        }
        let response = next
            .run(MiddlewareRequest::new(
                "openai",
                headers,
                Bytes::from(body.to_string()),
            ))
            .await?;
        Ok(rewrite(
            response,
            if matches!(self.mode, Mode::BadResponse) {
                Rewrite::Invalid
            } else {
                Rewrite::Pass
            },
        ))
    }
}
#[derive(Default)]
struct Effects {
    active: AtomicUsize,
    calls: AtomicUsize,
    commits: AtomicUsize,
    completion: Mutex<Option<Arc<tokio::sync::Notify>>>,
    charges: Mutex<Vec<ClientBudgetCharge>>,
    finalizations: Mutex<Vec<ModelRequestFinalization>>,
    requests: Mutex<Vec<(String, String, String, SystemTime)>>,
}
impl ClientAdmissionPort for Effects {
    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { unreachable!() })
    }
}
impl ClientBudgetPort for Effects {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<AccountGroupId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            Ok(ClientBudgetScope {
                user_id: "original-user".into(),
                group_id: AccountGroupId::new("grp_00000000000000000000000000000001").unwrap(),
            })
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            self.charges.lock().unwrap().push(charge);
            Ok(())
        })
    }
}
#[async_trait]
impl ExecutionStore for Effects {
    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
        Ok(())
    }
    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        Ok(())
    }
    async fn finalize_model_request(
        &self,
        value: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.finalizations.lock().unwrap().push(value);
        Ok(())
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        unreachable!()
    }
}
struct Continuations;
impl NativeContinuationPort for Continuations {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { Ok(()) })
    }
}
#[async_trait]
impl Provider for Effects {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(vec![])
    }
    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let Operation::Generate(generate) = request.operation() else {
            panic!("Responses operation")
        };
        let payload = generate.protocol_payload();
        let opaque = payload
            .context()
            .get("opaque_request_headers")
            .unwrap()
            .to_string();
        assert!(
            !opaque.contains("authorization")
                && !opaque.contains("x-plugin-hop")
                && !opaque.contains("chatgpt-account-id"),
            "{opaque}"
        );
        self.requests.lock().unwrap().push((
            context.client_api_key_ref().as_str().into(),
            request
                .candidate()
                .upstream_model()
                .unwrap()
                .as_str()
                .into(),
            payload.body()["input"].as_str().unwrap().into(),
            context.deadline(),
        ));
        let meta = ProviderCallMetadata::new(
            request.candidate().provider().clone(),
            request.candidate().upstream_model().unwrap().clone(),
            ProviderAccountId::new("acct_api_test").unwrap(),
            UpstreamTransport::new("http").unwrap(),
        );
        let created = ProviderEvent::canonical_with_wire(
            vec![GatewayEvent::Started(ResponseMeta::new(
                "resp_original",
                "model-b",
            ))],
            event("response.created").into_parts().1.unwrap(),
        );
        let completed = ProviderEvent::canonical_with_wire(
            vec![
                GatewayEvent::ProviderCost(ProviderReportedCost::from_usd_ticks(25).unwrap()),
                GatewayEvent::Completed(ResponseMeta::new("resp_original", "model-b")),
            ],
            event("response.completed").into_parts().1.unwrap(),
        );
        let wait = self.completion.lock().unwrap().clone();
        let stream =
            futures::stream::iter([Ok(created)]).chain(futures::stream::once(async move {
                if let Some(wait) = wait {
                    wait.notified().await;
                }
                Ok(completed)
            }));
        Ok(ProviderStream::new(meta, stream, ()))
    }
}
struct Fixture {
    app: axum::Router,
    manager: Arc<PluginManager>,
    plugin: Arc<Plugin>,
    effects: Arc<Effects>,
}
impl Fixture {
    async fn new(mode: Mode, installed: bool) -> Self {
        let plugin = Arc::new(Plugin {
            access: PluginAccess::default(),
            mode,
            seen: Mutex::new(vec![]),
            headers: Mutex::new(vec![]),
        });
        let records = if installed {
            vec![PluginRecord {
                id: "fixture".into(),
                revision: 1,
                name: "fixture".into(),
                artifact_sha256: "a".repeat(64),
                enabled: true,
                configuration: json!({}),
                secrets: Default::default(),
                frontend_authentication: None,
                middleware: Some(PluginMiddlewareBinding {
                    order: 0,
                    mounts: vec![PluginMiddlewareMount::Request],
                    scope: PluginMiddlewareScope::All {},
                }),
            }]
        } else {
            vec![]
        };
        let manager = Arc::new(PluginManager::new(
            Arc::new(Store { records }),
            Arc::new(Runtime(plugin.clone())),
        ));
        manager.restore().await.unwrap();
        let effects = Arc::new(Effects::default());
        let execution = Arc::new(
            DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(snapshot("sk_middleware", "openai")),
                effects.clone(),
                ProviderRegistry::new([effects.clone() as Arc<dyn Provider>]).unwrap(),
                effects.clone(),
                Arc::new(Continuations),
                Arc::new(IgnoredClientApiKeyUsage),
            )
            .with_budget(effects.clone()),
        );
        let admin = crate::admin::AdminTestFixture::new().await;
        let app = gateway_api::initialize(
            gateway_api::ApiConfig {
                asset_directory: std::env::temp_dir(),
                cors_allowed_origins: vec![],
                request_timeout_seconds: None,
                request_id_header: "x-request-id".into(),
            },
            execution,
            admin.services.with_plugins(manager.clone()),
            vec![],
            Arc::new(EmptyWorkerHealth),
            Arc::new(TestLifecycle::default()),
        )
        .unwrap()
        .router();
        Self {
            app,
            manager,
            plugin,
            effects,
        }
    }
    async fn request(&self, stream: bool) -> axum::response::Response {
        self.app
            .clone()
            .oneshot(
                Request::post("/v1/responses")
                    .header("authorization", "Bearer sk_middleware")
                    .header("content-type", "application/json")
                    .header("connection", "x-client-hop")
                    .header("x-client-hop", "hidden")
                    .header("x-openai-actor-authorization", "managed")
                    .header("x-custom", "preserved")
                    .body(Body::from(
                        json!({"model":"model-a","input":"original input","stream":stream})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
    async fn settled(&self) {
        for _ in 0..100 {
            if self.effects.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(self.effects.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.effects.finalizations.lock().unwrap().len(), 1);
        let charges = self.effects.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].key_id.as_str(), "key_api_test");
        assert_eq!(charges[0].scope.as_ref().unwrap().user_id, "original-user");
        assert!(charges[0].amount_usd > Decimal::ZERO);
    }
}

#[tokio::test]
async fn request_rewrite_preserves_core_identity_deadline_and_single_settlement() {
    for streaming in [false, true] {
        let f = Fixture::new(Mode::Rewrite, true).await;
        let response = f.request(streaming).await;
        if response.status() != StatusCode::OK {
            panic!(
                "response: {}",
                String::from_utf8_lossy(&bytes(response).await)
            );
        }
        let id = response.headers()["x-gateway-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let output = bytes(response).await;
        assert!(String::from_utf8_lossy(&output).contains("resp_original"));
        f.settled().await;
        assert_eq!(f.effects.commits.load(Ordering::SeqCst), 1);
        let contexts = f.plugin.seen.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].request_id.as_str(), id);
        let seen = f.effects.requests.lock().unwrap();
        assert_eq!(seen[0].0, "key_api_test");
        assert_eq!(seen[0].1, "model-b");
        assert_eq!(seen[0].2, "changed input");
        assert_eq!(seen[0].3, contexts[0].deadline_at);
        assert_eq!(f.effects.charges.lock().unwrap()[0].request_id.as_str(), id);
        let headers = f.plugin.headers.lock().unwrap();
        assert!(headers.iter().any(|h| h == "x-custom"));
        for excluded in [
            "authorization",
            "x-client-hop",
            "x-openai-actor-authorization",
            "connection",
        ] {
            assert!(!headers.iter().any(|h| h == excluded));
        }
    }
}
#[tokio::test]
async fn request_rewrite_cannot_bypass_model_validation_or_change_transport() {
    for mode in [Mode::InvalidModel, Mode::Transport] {
        let f = Fixture::new(mode, true).await;
        let response = f.request(false).await;
        assert!(response.status().is_client_error(), "{}", response.status());
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
        assert!(f.effects.charges.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn rejected_plugin_output_keeps_real_cost_without_committing_delivery() {
    let f = Fixture::new(Mode::BadResponse, true).await;
    let response = f.request(false).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    f.settled().await;
    assert_eq!(f.effects.commits.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn revoked_required_plugin_fails_closed_before_provider_start() {
    let f = Fixture::new(Mode::Rewrite, true).await;
    f.plugin.access.revoke();
    assert_eq!(
        f.request(false).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn empty_registry_preserves_original_request_and_response_path() {
    let f = Fixture::new(Mode::Rewrite, false).await;
    let response = f.request(false).await;
    if response.status() != StatusCode::OK {
        panic!(
            "response: {}",
            String::from_utf8_lossy(&bytes(response).await)
        );
    }
    bytes(response).await;
    f.settled().await;
    assert!(f.plugin.seen.lock().unwrap().is_empty());
    let seen = f.effects.requests.lock().unwrap();
    assert_eq!(seen[0].1, "model-a");
    assert_eq!(seen[0].2, "original input");
}

#[tokio::test]
async fn revoking_plugin_interrupts_live_sse_and_releases_core_admission() {
    let f = Fixture::new(Mode::Rewrite, true).await;
    *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let response = f.request(true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 1);
    f.plugin.access.revoke();
    let mut output = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => output.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }
    })
    .await
    .expect("revocation must interrupt stalled source");
    assert!(!String::from_utf8_lossy(&output).contains("response.completed"));
    for _ in 0..100 {
        if f.effects.active.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
    let charges = f.effects.charges.lock().unwrap();
    assert_eq!(charges.len(), 1);
    assert_eq!(charges[0].amount_usd, Decimal::ZERO);
}
