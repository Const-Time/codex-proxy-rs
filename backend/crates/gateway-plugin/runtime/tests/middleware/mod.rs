mod capabilities;
mod plan;
mod settings;
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    engine::{CommitRequirement, execution::ClientTransport, middleware::*},
    event::{GatewayEvent, ProtocolWireEvent, ProviderEvent},
    lifecycle::CancellationToken,
    metering::ProviderReportedCost,
    operation::OperationKind,
};
use gateway_host::process::ProcessSupervisor;
use gateway_plugin_runtime::{
    CallbackHandler, PackageLimits, RpcLimits, RpcReply, RpcSession, ValidatedPackage,
    middleware::MiddlewareCallbacks,
};
use gateway_plugin_sdk::{CallContext, ErrorCode, Handshake, PluginFault};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

struct RejectCallbacks;
impl CallbackHandler for RejectCallbacks {
    fn call(
        &self,
        _: CallContext,
        _: String,
        _: Value,
        _: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        Box::pin(async {
            Err(PluginFault::new(
                ErrorCode::Unsupported,
                "no management authority",
            ))
        })
    }
}
struct Fixture {
    callbacks: Arc<MiddlewareCallbacks>,
    session: Arc<RpcSession>,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let package = Arc::new(
            ValidatedPackage::read(
                crate::support::archive("1.0.0", true),
                None,
                PackageLimits::default(),
            )
            .unwrap(),
        );
        let prepared = Arc::new(package.prepare(cache.path()).unwrap());
        let callbacks = MiddlewareCallbacks::new(Arc::new(RejectCallbacks));
        let handshake = Handshake {
            protocol_version: gateway_plugin_sdk::PROTOCOL_VERSION,
            artifact_sha256: package.digest().into(),
            plugin_id: package.manifest().plugin_id().unwrap(),
            instance_id: "middleware-fixture".into(),
            generation: 1,
            incarnation: "incarnation-one".into(),
            configuration: json!({}),
            contributes: package.manifest().contributes.clone(),
        };
        let session = Arc::new(
            RpcSession::start(
                prepared,
                handshake,
                RpcLimits::default(),
                &ProcessSupervisor::default(),
                callbacks.clone(),
            )
            .await
            .unwrap(),
        );
        Self {
            callbacks,
            session,
            _cache: cache,
        }
    }
    async fn run(
        &self,
        mode: &str,
        streaming: bool,
        trace: Arc<Trace>,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        self.callbacks
            .invoke(
                self.session.clone(),
                context(streaming),
                request(mode),
                terminal(trace, streaming),
            )
            .await
    }
    async fn stop(self) {
        self.session.shutdown(Duration::from_secs(2)).await;
    }
}
fn context(streaming: bool) -> MiddlewareContext {
    MiddlewareContext {
        request_id: gateway_core::engine::ModelRequestId::new("req_original").unwrap(),
        client_key_id: gateway_core::policy::ClientApiKeyId::new("key_original").unwrap(),
        account_group_ids: vec![],
        mount: MiddlewareMount::Request,
        attempt_index: None,
        operation: OperationKind::Generate,
        endpoint: "/v1/responses".into(),
        transport: if streaming {
            ClientTransport::HttpSse
        } else {
            ClientTransport::HttpJson
        },
        provider: None,
        model: Some("model-original".into()),
        account_id: None,
        cancellation: CancellationToken::new(),
        deadline_at: (SystemTime::now() + Duration::from_secs(20)).into(),
    }
}
fn request(mode: &str) -> MiddlewareRequest {
    MiddlewareRequest::new(
        "openai",
        vec![],
        Bytes::from(serde_json::to_vec(&json!({"mode":mode})).unwrap()),
    )
}
#[derive(Default)]
struct Trace {
    next: AtomicUsize,
    reads: AtomicUsize,
    commits: AtomicUsize,
    drops: AtomicUsize,
    entered: tokio::sync::Notify,
    closed: tokio::sync::Notify,
    request: Mutex<Option<MiddlewareRequest>>,
}
impl Trace {
    async fn closed_once(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.closed.notified())
            .await
            .unwrap();
        assert_eq!(self.drops.load(Ordering::SeqCst), 1);
    }
}
struct Body {
    trace: Arc<Trace>,
    frames: VecDeque<MiddlewareFrame>,
}
impl Drop for Body {
    fn drop(&mut self) {
        self.trace.drops.fetch_add(1, Ordering::SeqCst);
        self.trace.closed.notify_one();
    }
}
impl MiddlewareBody for Body {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async {
            self.trace.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.frames.pop_front())
        })
    }
    fn commit_downstream(&mut self, _: Option<u16>) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async {
            assert_eq!(self.trace.commits.fetch_add(1, Ordering::SeqCst), 0);
            Ok(())
        })
    }
    fn is_finalized(&self) -> bool {
        self.trace.commits.load(Ordering::SeqCst) > 0
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            drop(self);
        })
    }
}
fn frame(payload: &'static [u8], streaming: bool, terminal: bool) -> MiddlewareFrame {
    let framing = if streaming {
        MiddlewareFraming::SseEvent
    } else {
        MiddlewareFraming::JsonDocument
    };
    let wire =
        ProtocolWireEvent::raw_json("openai", Bytes::from_static(br#"{"original":true}"#)).unwrap();
    let event = ProviderEvent::canonical_with_wire(
        if terminal {
            vec![GatewayEvent::ProviderCost(
                ProviderReportedCost::from_usd_ticks(123).unwrap(),
            )]
        } else {
            vec![]
        },
        wire,
    );
    let frame =
        MiddlewareFrame::from_provider_event(Bytes::from_static(payload), framing, terminal, event);
    if !terminal {
        return frame;
    }
    let (bytes, framing, terminal, envelope) = frame.into_parts();
    MiddlewareFrame::new(bytes, framing, terminal).with_envelope(
        envelope
            .unwrap()
            .with_batch_end(CommitRequirement::CommitBeforeDelivery),
    )
}
fn terminal(trace: Arc<Trace>, streaming: bool) -> MiddlewareNext {
    gateway_core::middleware::compose(vec![], move |request| {
        Box::pin(async move {
            assert_eq!(trace.next.fetch_add(1, Ordering::SeqCst), 0);
            *trace.request.lock().unwrap() = Some(request);
            trace.entered.notify_one();
            let frames = if streaming {
                VecDeque::from([
                    frame(b"data: {\"first\":true}\n\n", true, false),
                    frame(b"data: {\"last\":true}\n\n", true, true),
                ])
            } else {
                VecDeque::from([frame(br#"{"original":true}"#, false, true)])
            };
            Ok(MiddlewareResponse::new(
                "openai".into(),
                200,
                vec![],
                Box::new(Body { trace, frames }),
            ))
        })
    })
}
#[tokio::test]
async fn sdk_next_rewrites_only_request_facts_and_returns_unread_body() {
    let fixture = Fixture::new().await;
    for mode in ["pass", "rewrite", "raw-double"] {
        let trace = Arc::new(Trace::default());
        let response = fixture.run(mode, false, trace.clone()).await.unwrap();
        assert_eq!(trace.next.load(Ordering::SeqCst), 1);
        assert_eq!(
            trace.reads.load(Ordering::SeqCst),
            0,
            "pass-through stays lazy"
        );
        assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
        if mode == "rewrite" {
            let guard = trace.request.lock().unwrap();
            let request = guard.as_ref().unwrap();
            assert_eq!(request.body().as_ref(), br#"{"model":"changed"}"#);
            assert_eq!(request.headers()[0].name(), "x-plugin");
        }
        let (_, _, _, mut body, _) = response.into_parts();
        let frame = body.next_frame().await.unwrap().unwrap();
        let (_, _, _, envelope) = frame.into_parts();
        assert_eq!(
            envelope.unwrap().batch_end(),
            Some(CommitRequirement::CommitBeforeDelivery)
        );
        body.commit_downstream(Some(200)).await.unwrap();
        assert!(body.is_finalized());
        body.close().await;
        trace.closed_once().await;
    }
    fixture.stop().await;
}
#[tokio::test]
async fn mapped_output_preserves_cost_and_atomic_batch_boundary_only_once() {
    let fixture = Fixture::new().await;
    for (mode, streaming, expected) in [
        ("inspect", false, 1),
        ("map", false, 1),
        ("expand", true, 4),
        ("drop-first", true, 2),
    ] {
        let trace = Arc::new(Trace::default());
        let response = fixture.run(mode, streaming, trace.clone()).await.unwrap();
        let (_, _, _, mut body, _) = response.into_parts();
        let mut frames = vec![];
        while let Some(frame) = body.next_frame().await.unwrap() {
            frames.push(frame);
        }
        assert_eq!(frames.len(), expected);
        assert_eq!(
            trace.commits.load(Ordering::SeqCst),
            0,
            "RPC completion cannot commit Core"
        );
        let mut costs = 0;
        let mut boundaries = vec![];
        for (index, frame) in frames.into_iter().enumerate() {
            if mode == "map" {
                assert_eq!(frame.bytes().as_ref(), br#"{"mapped":true}"#);
            }
            let (_, _, _, envelope) = frame.into_parts();
            if let Some(envelope) = envelope {
                if let Some(boundary) = envelope.batch_end() {
                    boundaries.push((index, boundary));
                }
                for fact in envelope.event().canonical_facts() {
                    if let GatewayEvent::ProviderCost(cost) = fact {
                        assert_eq!(cost.total().amount().scaled(), 123);
                        costs += 1;
                    }
                }
            }
        }
        assert_eq!(costs, 1);
        assert_eq!(
            boundaries,
            vec![(expected - 1, CommitRequirement::CommitBeforeDelivery)]
        );
        body.commit_downstream(Some(200)).await.unwrap();
        assert!(
            body.is_finalized(),
            "the authority survives RPC End until delivery commits"
        );
        body.close().await;
        trace.closed_once().await;
    }
    fixture.stop().await;
}
#[tokio::test]
async fn malformed_response_handles_and_source_claims_never_commit_and_release_body() {
    for mode in [
        "raw-forged",
        "raw-empty",
        "raw-read-pass",
        "raw-replay",
        "raw-standalone",
    ] {
        let fixture = Fixture::new().await;
        let trace = Arc::new(Trace::default());
        let result = fixture.run(mode, false, trace.clone()).await;
        if let Ok(response) = result {
            let (_, _, _, mut body, _) = response.into_parts();
            assert!(body.next_frame().await.is_err(), "{mode}");
            drop(body);
        }
        trace.closed_once().await;
        assert_eq!(trace.next.load(Ordering::SeqCst), 1);
        assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
        fixture.stop().await;
    }
}
#[tokio::test]
async fn cancellation_revokes_rpc_resources_but_keeps_sibling_process_usable() {
    let fixture = Fixture::new().await;
    let trace = Arc::new(Trace::default());
    let context = context(false);
    let cancellation = context.cancellation.clone();
    let mut future = Box::pin(fixture.callbacks.invoke(
        fixture.session.clone(),
        context,
        request("wait"),
        terminal(trace.clone(), false),
    ));
    assert!(futures::poll!(&mut future).is_pending());
    tokio::time::timeout(Duration::from_secs(3), trace.entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(future.await.is_err());
    trace.closed_once().await;
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    let sibling = Arc::new(Trace::default());
    drop(fixture.run("pass", false, sibling.clone()).await.unwrap());
    sibling.closed_once().await;
    fixture.stop().await;
}
#[tokio::test]
async fn short_circuit_rejection_and_unscoped_settings_never_run_terminal() {
    let fixture = Fixture::new().await;
    for mode in ["short", "reject", "settings"] {
        let trace = Arc::new(Trace::default());
        let result = fixture.run(mode, false, trace.clone()).await;
        if mode == "short" {
            let (_, status, _, mut body, _) = result.unwrap().into_parts();
            assert_eq!(status, 403);
            assert_eq!(
                body.next_frame().await.unwrap().unwrap().bytes().as_ref(),
                b"denied"
            );
            assert!(body.next_frame().await.unwrap().is_none());
        } else {
            assert!(result.is_err());
        }
        assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    }
    fixture.stop().await;
}

#[tokio::test]
async fn nested_sdk_middleware_retains_one_terminal_and_one_delivery_receipt() {
    let fixture = Fixture::new().await;
    let trace = Arc::new(Trace::default());
    let layers = (0..2)
        .map(
            |index| -> gateway_core::middleware::Middleware<
                MiddlewareRequest,
                MiddlewareResponse,
                MiddlewareError,
            > {
                let callbacks = fixture.callbacks.clone();
                let session = fixture.session.clone();
                Box::new(move |input, next| {
                    Box::pin(async move {
                        let input = if index == 1 { request("map") } else { input };
                        callbacks.invoke(session, context(false), input, next).await
                    })
                })
            },
        )
        .collect();
    let next = terminal(trace.clone(), false);
    let response = gateway_core::middleware::compose(layers, move |input| next.run(input))
        .run(request("inspect-transformed"))
        .await
        .unwrap();
    let (_, _, _, mut body, _) = response.into_parts();
    let frame = body.next_frame().await.unwrap().unwrap();
    assert_eq!(frame.bytes().as_ref(), br#"{"mapped":true}"#);
    assert!(frame.transformed());
    let (_, _, _, envelope) = frame.into_parts();
    let envelope = envelope.unwrap();
    assert_eq!(
        envelope.batch_end(),
        Some(CommitRequirement::CommitBeforeDelivery)
    );
    assert_eq!(envelope.event().canonical_facts().len(), 1);
    assert!(body.next_frame().await.unwrap().is_none());
    assert_eq!(trace.next.load(Ordering::SeqCst), 1);
    assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
    body.commit_downstream(Some(200)).await.unwrap();
    body.close().await;
    trace.closed_once().await;
    fixture.stop().await;
}

#[tokio::test]
async fn only_declared_mounts_can_run_and_attempt_facts_use_same_wire_contract() {
    let fixture = Fixture::new().await;
    let trace = Arc::new(Trace::default());
    let mut attempt = context(false);
    attempt.mount = MiddlewareMount::Attempt;
    attempt.attempt_index = Some(1);
    attempt.account_id =
        Some(gateway_core::account::ProviderAccountId::new("acct_original").unwrap());
    let response = fixture
        .callbacks
        .invoke(
            fixture.session.clone(),
            attempt,
            request("pass"),
            terminal(trace.clone(), false),
        )
        .await
        .unwrap();
    drop(response);
    trace.closed_once().await;
    let trace = Arc::new(Trace::default());
    let mut unsupported = context(false);
    unsupported.mount = MiddlewareMount::Http;
    assert!(
        fixture
            .callbacks
            .invoke(
                fixture.session.clone(),
                unsupported,
                request("pass"),
                terminal(trace.clone(), false)
            )
            .await
            .is_err()
    );
    assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    fixture.stop().await;
}

#[tokio::test]
async fn original_terminal_policy_error_survives_the_plugin_rpc_roundtrip() {
    use gateway_core::error::{GatewayError, GatewayErrorKind};
    let fixture = Fixture::new().await;
    let next = gateway_core::middleware::compose(vec![], |_| {
        Box::pin(async {
            Err(MiddlewareError::Gateway(GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "original policy refusal",
            )))
        })
    });
    let result = fixture
        .callbacks
        .invoke(
            fixture.session.clone(),
            context(false),
            request("pass"),
            next,
        )
        .await;
    assert!(
        matches!(result, Err(MiddlewareError::Gateway(error)) if error.kind() == GatewayErrorKind::PolicyDenied)
    );
    fixture.stop().await;
}
