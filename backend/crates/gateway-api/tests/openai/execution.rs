//! Exercise request cancellation against real Core admission and settlement.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use axum::{body::Body, http::Request};
use futures::{future::BoxFuture, poll};
use gateway_core::{
    engine::{
        AttemptRecord, ExecutionStore, IntermediateFailure, ModelRequestFinalization,
        ModelRequestId, NewModelRequest, RecoveryReport, admission::*, budget::*, execution::*,
        provider::ProviderRegistry,
    },
    error::{GatewayError, StoreError},
    metering::Decimal,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, PublicModelId},
    runtime::RuntimeSnapshotHandle,
    upstream::UpstreamSendState,
};
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

#[derive(Clone, Copy, PartialEq)]
enum PauseAt {
    Admission,
    Budget,
}

struct Effects {
    pause_at: PauseAt,
    entered: Notify,
    resume: Semaphore,
    started: Notify,
    released: Notify,
    active: AtomicBool,
    admissions: AtomicUsize,
    releases: AtomicUsize,
    starts: AtomicUsize,
    charges: Mutex<Vec<ClientBudgetCharge>>,
    admission_ids: Mutex<Vec<ModelRequestId>>,
}

impl Effects {
    fn new(pause_at: PauseAt) -> Arc<Self> {
        Arc::new(Self {
            pause_at,
            entered: Notify::new(),
            resume: Semaphore::new(0),
            started: Notify::new(),
            released: Notify::new(),
            active: AtomicBool::new(false),
            admissions: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            charges: Mutex::new(vec![]),
            admission_ids: Mutex::new(vec![]),
        })
    }

    async fn pause(&self, at: PauseAt) {
        if self.pause_at == at {
            self.entered.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
    }

    async fn assert_cleaned_once(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.released.notified())
            .await
            .expect("detached start must release admission");
        assert!(!self.active.load(Ordering::SeqCst));
        assert_eq!(self.admissions.load(Ordering::SeqCst), 1);
        assert_eq!(self.releases.load(Ordering::SeqCst), 1);
        assert_eq!(self.starts.load(Ordering::SeqCst), 1);
        let charges = self.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount_usd, Decimal::ZERO);
        assert_eq!(charges[0].request_id, self.admission_ids.lock().unwrap()[0]);
        assert_eq!(charges[0].key_id.as_str(), "key_api_test");
        assert_eq!(charges[0].scope.as_ref().unwrap().user_id, "user-original");
    }
}

impl ClientAdmissionPort for Effects {
    fn admit(
        &self,
        request: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async move {
            assert!(!self.active.swap(true, Ordering::SeqCst));
            self.admissions.fetch_add(1, Ordering::SeqCst);
            self.admission_ids
                .lock()
                .unwrap()
                .push(request.model_request_id);
            // Simulate an already committed lease while the storage reply is pending.
            self.pause(PauseAt::Admission).await;
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            assert_eq!(
                self.charges.lock().unwrap().len(),
                1,
                "settlement precedes release"
            );
            self.releases.fetch_add(1, Ordering::SeqCst);
            let active = self.active.swap(false, Ordering::SeqCst);
            self.released.notify_one();
            Ok(active)
        })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { unreachable!("no recovery during request") })
    }
}

impl ClientBudgetPort for Effects {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<AccountGroupId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            assert!(self.active.load(Ordering::SeqCst));
            self.pause(PauseAt::Budget).await;
            Ok(ClientBudgetScope {
                user_id: "user-original".into(),
                group_id: AccountGroupId::new("grp_00000000000000000000000000000001").unwrap(),
            })
        })
    }
    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            assert!(self.active.load(Ordering::SeqCst));
            self.charges.lock().unwrap().push(charge);
            Ok(())
        })
    }
}

/// No Provider is installed: an abandoned start must never poll upstream.
struct Observations;
#[async_trait]
impl ExecutionStore for Observations {
    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
        panic!("abandoned start must not execute a Provider")
    }
    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        panic!("abandoned start must not send upstream")
    }
    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        panic!("abandoned start must not commit downstream")
    }
    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        Ok(())
    }
    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        Ok(())
    }
    async fn finalize_model_request(&self, _: ModelRequestFinalization) -> Result<(), StoreError> {
        Ok(())
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        unreachable!("no recovery during request")
    }
}

struct Execution {
    inner: DefaultExecutionService,
    effects: Arc<Effects>,
}
impl Execution {
    fn new(effects: Arc<Effects>) -> Arc<Self> {
        Arc::new(Self {
            inner: DefaultExecutionService::new(
                RuntimeSnapshotHandle::new(super::snapshot("sk_start", "openai")),
                Arc::new(Observations),
                ProviderRegistry::default(),
                effects.clone(),
                Arc::new(super::UnusedContinuation),
                Arc::new(super::IgnoredClientApiKeyUsage),
            )
            .with_budget(effects.clone()),
            effects,
        })
    }
    fn finished(
        &self,
        result: Result<StartedExecution, GatewayError>,
    ) -> Result<StartedExecution, GatewayError> {
        assert!(result.is_ok(), "fixture must start successfully");
        self.effects.starts.fetch_add(1, Ordering::SeqCst);
        self.effects.started.notify_one();
        result
    }
}
impl ExecutionService for Execution {
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        self.inner.authenticate(plaintext)
    }
    fn public_models(&self, client: &AuthenticatedClient) -> Vec<PublicModelId> {
        self.inner.public_models(client)
    }
    fn contains_public_model(&self, client: &AuthenticatedClient, model: &PublicModelId) -> bool {
        self.inner.contains_public_model(client, model)
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move { self.finished(self.inner.start(request).await) })
    }
    fn start_provider_endpoint(
        &self,
        request: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move { self.finished(self.inner.start_provider_endpoint(request).await) })
    }
}

fn requests() -> Vec<Request<Body>> {
    [
        (
            "/v1/responses",
            r#"{"model":"model-a","input":"hello","stream":false}"#,
        ),
        (
            "/v1/responses",
            r#"{"model":"model-a","input":"hello","stream":true}"#,
        ),
        ("/v1/images/generations", r#"{"prompt":"a tree"}"#),
        (
            "/v1/images/edits",
            r#"{"prompt":"a tree","images":[{"image_url":"data:image/png;base64,AAEC"}]}"#,
        ),
        (
            "/v1/alpha/search",
            r#"{"id":"search","model":"model-a","commands":{"search_query":[{"q":"tree"}]}}"#,
        ),
    ]
    .into_iter()
    .map(|(path, body)| {
        Request::post(path)
            .header("authorization", "Bearer sk_start")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    })
    .collect()
}

#[tokio::test]
async fn dropped_handlers_complete_start_and_release_admission_and_budget() {
    for at in [PauseAt::Admission, PauseAt::Budget] {
        for request in requests() {
            let effects = Effects::new(at);
            let router = super::api_router(Execution::new(effects.clone())).await;
            let mut response = Box::pin(router.oneshot(request));
            assert!(poll!(&mut response).is_pending());
            tokio::time::timeout(Duration::from_secs(5), effects.entered.notified())
                .await
                .unwrap();
            drop(response);
            effects.resume.add_permits(1);
            effects.assert_cleaned_once().await;
        }
    }
}

#[tokio::test]
async fn dropping_unread_started_result_keeps_its_finalization_owner() {
    for request in requests() {
        let effects = Effects::new(PauseAt::Budget);
        let router = super::api_router(Execution::new(effects.clone())).await;
        let mut response = Box::pin(router.oneshot(request));
        assert!(poll!(&mut response).is_pending());
        tokio::time::timeout(Duration::from_secs(5), effects.entered.notified())
            .await
            .unwrap();
        effects.resume.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), effects.started.notified())
            .await
            .unwrap();
        // The current-thread runtime has completed the synchronous channel send.
        // Never poll the response again: its result remains queued in the channel.
        assert!(effects.active.load(Ordering::SeqCst));
        assert!(effects.charges.lock().unwrap().is_empty());
        drop(response);
        effects.assert_cleaned_once().await;
    }
}
