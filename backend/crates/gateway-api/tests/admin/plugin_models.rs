use super::*;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures::{StreamExt, future::pending};
use gateway_admin::{
    model::{AdminError, plugins::*},
    ports::plugins::PluginService,
};
use gateway_core::{
    engine::{CommitRequirement, CoordinatedEvent, EngineError, ModelRequestId, execution::*},
    error::GatewayError,
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ProviderResponseHeader, ResponseMeta},
    runtime::{ClientAccessGuard, RuntimeSnapshotHandle},
};
use serde_json::{Value, json};
use std::{
    sync::atomic::AtomicUsize,
    time::{Duration as StdDuration, SystemTime},
};
use tower::ServiceExt;

pub(super) struct Plugins(pub(super) PluginAccess);
#[async_trait]
impl PluginService for Plugins {
    async fn authenticate(
        &self,
        authorization: String,
    ) -> Result<PluginAuthenticationDecision, AdminError> {
        if self.0.is_revoked() || authorization == "Bearer plugin-error" {
            return Err(AdminError::unavailable("private plugin error"));
        }
        Ok(match authorization.as_str() {
            "Bearer external" => PluginAuthenticationDecision::Authenticated {
                key: gateway_core::policy::ClientApiKeyId::new("key_api_test").unwrap(),
                access: self.0.clone(),
            },
            "Bearer missing-binding" => PluginAuthenticationDecision::Authenticated {
                key: gateway_core::policy::ClientApiKeyId::new("missing").unwrap(),
                access: self.0.clone(),
            },
            value if value == format!("Bearer {}", secret()) => {
                PluginAuthenticationDecision::Builtin {
                    access: self.0.clone(),
                }
            }
            _ => PluginAuthenticationDecision::Rejected,
        })
    }
    fn access(&self, id: &str, revision: u64) -> Result<PluginAccess, AdminError> {
        if id == "plugin" && revision == 1 && !self.0.is_revoked() {
            Ok(self.0.clone())
        } else {
            Err(AdminError::conflict("stale plugin"))
        }
    }
    async fn artifacts(&self) -> Result<Vec<PluginArtifactInfo>, AdminError> {
        unreachable!()
    }
    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn upload(
        &self,
        _: Arc<[u8]>,
        _: &MutationContext,
    ) -> Result<PluginArtifactInfo, AdminError> {
        unreachable!()
    }
    async fn list(&self) -> Result<Vec<PluginView>, AdminError> {
        unreachable!()
    }
    async fn update(
        &self,
        _: Option<&str>,
        _: PluginUpdate,
        _: &MutationContext,
    ) -> Result<PluginView, AdminError> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: u64, _: &MutationContext) -> Result<(), AdminError> {
        unreachable!()
    }
    async fn resource(&self, _: &str, _: u64, _: &str) -> Result<PluginHttpResponse, AdminError> {
        unreachable!()
    }
    async fn handle(
        &self,
        _: &str,
        _: u64,
        request: PluginHttpRequest,
    ) -> Result<PluginHttpResponse, AdminError> {
        Ok(PluginHttpResponse{status:200,content_type:"application/json".into(),body:serde_json::to_vec(&json!({"actor":format!("{:?}",request.actor.actor),"requestId":request.actor.request_id})).unwrap()})
    }
}
#[derive(Default)]
pub(crate) struct Trace {
    pub(crate) starts: Mutex<Vec<(String, String)>>,
    pub(crate) events: AtomicUsize,
    pub(crate) cancelled: AtomicUsize,
    pub(crate) finalized: AtomicUsize,
}
struct Execution {
    client: AuthenticatedClient,
    snapshots: RuntimeSnapshotHandle,
    trace: Arc<Trace>,
    pending_json: bool,
}
impl ExecutionService for Execution {
    fn authenticate_bound_key(
        &self,
        key: &gateway_core::policy::ClientApiKeyId,
        revocation: gateway_core::lifecycle::CancellationToken,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        crate::openai::bound_client(self.snapshots.clone(), key, revocation)
    }
    fn client_access(&self, client: &AuthenticatedClient) -> Option<ClientAccessGuard> {
        self.snapshots.client_access(client.policy()).ok()
    }
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        if plaintext == secret() {
            Ok(self.client.clone())
        } else {
            Err(ClientAuthenticationError::InvalidKey)
        }
    }
    fn public_models(&self, _: &AuthenticatedClient) -> Vec<PublicModelId> {
        vec![PublicModelId::new("model-a").unwrap()]
    }
    fn contains_public_model(&self, _: &AuthenticatedClient, model: &PublicModelId) -> bool {
        model.as_str() == "model-a"
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            self.trace.starts.lock().unwrap().push((
                request.client.policy().key_id().as_str().to_owned(),
                request.metadata.endpoint,
            ));
            Ok(StartedExecution {
                request_id: ModelRequestId::new("req_plugin_request").unwrap(),
                created_at: SystemTime::now(),
                stream: request.metadata.stream,
                session: Box::new(Session {
                    trace: self.trace.clone(),
                    buffered: !request.metadata.stream,
                    pending_json: self.pending_json,
                    finalized: false,
                }),
            })
        })
    }
    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            self.trace.starts.lock().unwrap().push((
                request.client.policy().key_id().as_str().into(),
                request.metadata.endpoint,
            ));
            Err(GatewayError::new(
                gateway_core::error::GatewayErrorKind::Internal,
                "fixture reached provider execution",
            ))
        })
    }
}
struct Session {
    trace: Arc<Trace>,
    buffered: bool,
    pending_json: bool,
    finalized: bool,
}
fn wire(completed: bool) -> ProviderEvent {
    let meta = ResponseMeta::new("resp_plugin", "model-a");
    let kind = if completed {
        "response.completed"
    } else {
        "response.created"
    };
    ProviderEvent::canonical_with_wire(vec![if completed {GatewayEvent::Completed(meta)}else{GatewayEvent::Started(meta)}],ProtocolWireEvent::json("openai",Some(kind.into()),json!({"type":kind,"response":{"id":"resp_plugin","model":"model-a","status":if completed{"completed"}else{"in_progress"},"output":[]}})).unwrap())
}
impl ExecutionSession for Session {
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            self.trace.events.fetch_add(1, Ordering::SeqCst);
            Ok(Some(CoordinatedEvent::single(
                wire(false),
                CommitRequirement::CommitBeforeDelivery,
            )))
        })
    }
    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        Box::pin(async move {
            self.trace.events.fetch_add(1, Ordering::SeqCst);
            if self.pending_json {
                pending::<()>().await;
            }
            Ok(vec![wire(true)])
        })
    }
    fn response_headers(&self) -> &[ProviderResponseHeader] {
        &[]
    }
    fn commit_downstream(&mut self, _: Option<u16>) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            self.finalized = self.buffered;
            Ok(())
        })
    }
    fn record_client_status(&mut self, _: u16) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }
    fn is_finalized(&self) -> bool {
        self.finalized
    }
    fn cancel(&self) {
        self.trace.cancelled.fetch_add(1, Ordering::SeqCst);
    }
    fn detach_finalize(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.trace.finalized.fetch_add(1, Ordering::SeqCst);
        })
    }
}
fn secret() -> String {
    format!("sk_{}", "a".repeat(43))
}
pub(crate) struct Fixture {
    admin: AdminTestFixture,
    pub(crate) app: axum::Router,
    pub(crate) plugin: PluginAccess,
    pub(crate) snapshots: RuntimeSnapshotHandle,
    pub(crate) trace: Arc<Trace>,
}
impl Fixture {
    pub(crate) async fn new(pending_json: bool) -> Self {
        let admin = AdminTestFixture::new().await;
        admin.auth.insert_session("admin");
        admin.auth.insert_user_session("ordinary", "ordinary");
        admin.auth.set_api_key(&format!("admin-{}", "b".repeat(64)));
        admin.client_keys.owners.lock().unwrap().extend([
            ("key_api_test".into(), "admin_1".into()),
            ("other-key".into(), "other-user".into()),
        ]);
        let client = crate::openai::authenticated_client(&secret());
        let snapshots = RuntimeSnapshotHandle::new(client.snapshot().as_ref().clone());
        let trace = Arc::new(Trace::default());
        let plugin = PluginAccess::default();
        let app = gateway_api::initialize(
            gateway_api::ApiConfig {
                asset_directory: std::env::temp_dir(),
                cors_allowed_origins: Vec::new(),
                request_timeout_seconds: None,
                request_id_header: "x-request-id".into(),
            },
            Arc::new(Execution {
                client,
                snapshots: snapshots.clone(),
                trace: trace.clone(),
                pending_json,
            }),
            admin
                .services
                .clone()
                .with_plugins(Arc::new(Plugins(plugin.clone()))),
            Vec::new(),
            Arc::new(crate::openai::EmptyWorkerHealth),
            Arc::new(crate::openai::TestLifecycle::default()),
        )
        .unwrap()
        .router();
        Self {
            admin,
            app,
            plugin,
            snapshots,
            trace,
        }
    }
    fn request(&self, cookie: &str, revision: u64, key: &str, stream: bool) -> Request<Body> {
        Request::builder().method("POST").uri("/api/admin/plugins/models/responses").header("cookie",format!("cpr_admin_session={cookie}")).header("content-type","application/json").body(Body::from(json!({"id":"plugin","revision":revision,"clientKeyId":key,"body":{"model":"model-a","input":"hello","stream":stream}}).to_string())).unwrap()
    }
}
pub(crate) async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(StdDuration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observable cleanup must finish without polling response body");
}
#[tokio::test]
async fn model_calls_require_personal_admin_session_own_key_and_current_plugin() {
    let f = Fixture::new(false).await;
    for (cookie, revision, key, expected) in [
        ("", 1, "key_api_test", StatusCode::UNAUTHORIZED),
        ("ordinary", 1, "key_api_test", StatusCode::FORBIDDEN),
        ("admin", 2, "key_api_test", StatusCode::CONFLICT),
        ("admin", 1, "other-key", StatusCode::NOT_FOUND),
    ] {
        let response = f
            .app
            .clone()
            .oneshot(f.request(cookie, revision, key, false))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let mut request = f.request("admin", 1, "key_api_test", false);
    request.headers_mut().insert(
        "x-api-key",
        format!("admin-{}", "b".repeat(64)).parse().unwrap(),
    );
    assert_eq!(
        f.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert!(f.trace.starts.lock().unwrap().is_empty());
    assert_eq!(
        *f.admin.client_keys.reveals.lock().unwrap(),
        vec![("admin_1".into(), "other-key".into())]
    );
}
#[tokio::test]
async fn selected_key_uses_normal_responses_dispatch_and_never_returns_the_secret() {
    let f = Fixture::new(false).await;
    let response = f
        .app
        .clone()
        .oneshot(f.request("admin", 1, "key_api_test", false))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["id"], "resp_plugin");
    assert!(!String::from_utf8_lossy(&bytes).contains(&secret()));
    assert_eq!(
        *f.trace.starts.lock().unwrap(),
        vec![(
            "key_api_test".into(),
            "/api/admin/plugins/models/responses".into()
        )]
    );
    assert_eq!(
        *f.admin.client_keys.reveals.lock().unwrap(),
        vec![("admin_1".into(), "key_api_test".into())]
    );
}
#[tokio::test]
async fn plugin_disable_key_revocation_and_disconnect_cleanup_even_with_no_body_reads() {
    for cause in ["plugin", "key", "disconnect"] {
        let f = Fixture::new(false).await;
        let response = f
            .app
            .clone()
            .oneshot(f.request("admin", 1, "key_api_test", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        until(|| f.trace.events.load(Ordering::SeqCst) >= 2).await;
        let count = f.trace.events.load(Ordering::SeqCst);
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        assert_eq!(
            f.trace.events.load(Ordering::SeqCst),
            count,
            "backpressure bounds read ahead"
        );
        let mut response = Some(response);
        match cause {
            "plugin" => f.plugin.revoke(),
            "key" => f.snapshots.suspend(),
            _ => drop(response.take()),
        }
        until(|| f.trace.finalized.load(Ordering::SeqCst) == 1).await;
        assert!(f.trace.cancelled.load(Ordering::SeqCst) > 0, "{cause}");
        if let Some(response) = response {
            assert!(
                response
                    .into_body()
                    .into_data_stream()
                    .next()
                    .await
                    .unwrap()
                    .is_err(),
                "queued data must not escape revoked authority"
            );
        }
    }
}
#[tokio::test]
async fn plugin_disable_cancels_buffered_inference_before_headers() {
    let f = Fixture::new(true).await;
    let task = tokio::spawn(
        f.app
            .clone()
            .oneshot(f.request("admin", 1, "key_api_test", false)),
    );
    until(|| f.trace.events.load(Ordering::SeqCst) > 0).await;
    f.plugin.revoke();
    let response = task.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    until(|| f.trace.finalized.load(Ordering::SeqCst) == 1).await;
    assert_eq!(f.trace.cancelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn revocation_also_blocks_a_completed_response_waiting_for_delivery() {
    let f = Fixture::new(false).await;
    let response = f
        .app
        .clone()
        .oneshot(f.request("admin", 1, "key_api_test", false))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    f.snapshots.suspend();
    assert!(
        response
            .into_body()
            .into_data_stream()
            .next()
            .await
            .unwrap()
            .is_err()
    );
}
