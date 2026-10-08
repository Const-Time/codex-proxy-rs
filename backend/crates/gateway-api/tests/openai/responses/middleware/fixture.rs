// Real HTTP routing, PluginManager publication, Core admission and settlement.
use super::*;
use crate::openai::{EmptyWorkerHealth, IgnoredClientApiKeyUsage, TestLifecycle, snapshot};
use async_trait::async_trait;
use axum::{body::Body, http::Request};
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
pub(super) enum Mode {
    Rewrite,
    InvalidModel,
    Transport,
    BadResponse,
    Head,
    ShortCircuit,
    ShortCircuitRaw,
    ResponseRewrite,
    ExpandInvalid,
}
pub(super) struct Plugin {
    pub(super) access: PluginAccess,
    pub(super) mode: Mode,
    pub(super) seen: Mutex<Vec<MiddlewareContext>>,
    pub(super) headers: Mutex<Vec<String>>,
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
        let attempt = context.mount == MiddlewareMount::Attempt;
        self.seen.lock().unwrap().push(context);
        *self.headers.lock().unwrap() = request.headers().iter().map(|h| h.name().into()).collect();
        let (_, mut headers, body) = request.into_parts();
        let mut body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if !attempt || matches!(self.mode, Mode::InvalidModel) {
            body["model"] = json!(if matches!(self.mode, Mode::InvalidModel) {
                "forbidden-model"
            } else {
                "model-b"
            });
        }
        if matches!(self.mode, Mode::ShortCircuit | Mode::ShortCircuitRaw) {
            return Ok(MiddlewareResponse::new(
                "openai".into(),
                403,
                vec![],
                Box::new(attempt::Synthetic(Some(MiddlewareFrame::new(
                    Bytes::from_static(if matches!(self.mode, Mode::ShortCircuitRaw) {
                        b"denied"
                    } else {
                        b"{\"error\":{\"message\":\"blocked\"}}"
                    }),
                    MiddlewareFraming::JsonDocument,
                    true,
                )))),
            ));
        }
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
        if matches!(self.mode, Mode::Head) {
            let (protocol, _, mut headers, body, envelope) = response.into_parts();
            headers.push(MiddlewareHeader::new(
                "x-plugin-result",
                Bytes::from_static(b"changed"),
            ));
            headers.push(MiddlewareHeader::new(
                "set-cookie",
                Bytes::from_static(b"must-not-forward"),
            ));
            let mut response = MiddlewareResponse::new(protocol, 201, headers, body);
            if let Some(envelope) = envelope {
                response = response.with_envelope(envelope);
            }
            return Ok(response);
        }
        Ok(rewrite(
            response,
            if matches!(self.mode, Mode::BadResponse) {
                Rewrite::Invalid
            } else if matches!(self.mode, Mode::ResponseRewrite) {
                Rewrite::Json
            } else if matches!(self.mode, Mode::ExpandInvalid) {
                Rewrite::ExpandInvalidTerminal
            } else {
                Rewrite::Pass
            },
        ))
    }
}
#[derive(Default)]
pub(super) struct Effects {
    pub(super) active: AtomicUsize,
    pub(super) calls: AtomicUsize,
    pub(super) commits: AtomicUsize,
    pub(super) completion: Mutex<Option<Arc<tokio::sync::Notify>>>,
    pub(super) charges: Mutex<Vec<ClientBudgetCharge>>,
    pub(super) finalizations: Mutex<Vec<ModelRequestFinalization>>,
    pub(super) requests: Mutex<Vec<(String, String, String, SystemTime)>>,
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
pub(super) struct Fixture {
    pub(super) app: axum::Router,
    pub(super) manager: Arc<PluginManager>,
    pub(super) plugin: Arc<Plugin>,
    pub(super) effects: Arc<Effects>,
}
impl Fixture {
    pub(super) async fn new(mode: Mode, installed: bool) -> Self {
        Self::with_mounts(
            mode,
            if installed {
                vec![PluginMiddlewareMount::Request]
            } else {
                vec![]
            },
        )
        .await
    }
    pub(super) async fn with_mounts(mode: Mode, mounts: Vec<PluginMiddlewareMount>) -> Self {
        let plugin = Arc::new(Plugin {
            access: PluginAccess::default(),
            mode,
            seen: Mutex::new(vec![]),
            headers: Mutex::new(vec![]),
        });
        let records = if !mounts.is_empty() {
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
                    mounts,
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
    pub(super) async fn request(&self, stream: bool) -> axum::response::Response {
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
    pub(super) async fn settled(&self) {
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
