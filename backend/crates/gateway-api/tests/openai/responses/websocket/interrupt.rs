use super::*;
use crate::openai::*;
use gateway_core::engine::AttemptContext;
use gateway_core::engine::budget::{
    ClientBudgetCharge, ClientBudgetError, ClientBudgetPort, ClientBudgetScope,
};
use gateway_core::engine::provider::{
    Provider, ProviderCallMetadata, ProviderCatalogGeneration, ProviderRequest, ProviderStream,
};
use gateway_core::routing::ProviderModelCapabilities;
use gateway_core::upstream::UpstreamTransport;

use gateway_core::engine::response_control::{ResponseControl, ResponseInterruptError};

#[derive(Default)]
struct InterruptProvider {
    calls: AtomicUsize,
    controls: Mutex<Vec<ResponseControl>>,
    inputs: Mutex<Vec<Value>>,
    steering: bool,
}

#[async_trait]
impl Provider for InterruptProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let number = self.calls.fetch_add(1, Ordering::SeqCst);
        let Operation::Generate(generate) = request.operation() else {
            panic!("expected response generation");
        };
        self.inputs
            .lock()
            .unwrap()
            .push(generate.protocol_payload().body()["input"].clone());
        let id = format!("resp_interrupt_{number}");
        let control = context
            .response_control()
            .expect("root control propagated through Core")
            .clone();
        if self.steering {
            return steering_stream(request, control);
        }
        let active = control.activate(id.clone()).unwrap();
        self.controls.lock().unwrap().push(control);
        let candidate = request.candidate();
        let metadata = ProviderCallMetadata::new(
            candidate.provider().clone(),
            candidate.upstream_model().unwrap().clone(),
            ProviderAccountId::new("acct_api_test").unwrap(),
            UpstreamTransport::new("websocket").unwrap(),
        );
        let body = futures::stream::unfold(
            (0, Some(active), id),
            |(phase, mut active, id)| async move {
                let (event_type, status, event) = match phase {
                    0 => (
                        "response.created",
                        "in_progress",
                        GatewayEvent::Started(ResponseMeta::new(&id, "model-a")),
                    ),
                    1 => {
                        active.as_ref().unwrap().requested().await;
                        drop(active.take());
                        (
                            "response.incomplete",
                            "incomplete",
                            GatewayEvent::Completed(ResponseMeta::new(&id, "model-a")),
                        )
                    }
                    _ => return None,
                };
                let mut response = json!({"id":id,"model":"model-a","status":status,"output":[]});
                if phase == 1 {
                    response["incomplete_details"] = json!({"reason":"interrupted"});
                }
                let event = ProviderEvent::canonical_with_wire(
                    vec![event],
                    ProtocolWireEvent::json(
                        "openai",
                        Some(event_type.to_owned()),
                        json!({
                            "type":event_type, "response":response
                        }),
                    )
                    .unwrap(),
                );
                Some((Ok(event), (phase + 1, active, id)))
            },
        );
        Ok(ProviderStream::new(metadata, body, ()))
    }
}

#[derive(Default)]
struct InterruptAdmissions {
    active: AtomicUsize,
}

impl ClientAdmissionPort for InterruptAdmissions {
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
        Box::pin(async { unreachable!("no recovery in live connection test") })
    }
}

type TestSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn send(socket: &mut TestSocket, value: Value) {
    socket
        .send(ClientMessage::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn next(socket: &mut TestSocket) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("response deadline")
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        if value["type"] != "response.metadata" {
            return value;
        }
    }
}

#[tokio::test]
async fn controls_keep_active_response_and_queued_creates_isolated() {
    let provider = Arc::new(InterruptProvider::default());
    let admissions = Arc::new(InterruptAdmissions::default());
    let execution = Arc::new(DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(snapshot("sk_ws_interrupt", "openai")),
        Arc::new(InterruptStore::default()),
        ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
        admissions.clone(),
        Arc::new(UnusedContinuation),
        Arc::new(IgnoredClientApiKeyUsage),
    ));
    let app = api_router(execution).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer sk_ws_interrupt"),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    let create = json!({"type":"response.create","model":"model-a","input":"hello"});
    let interrupt =
        |id: &str, mode: &str| json!({"type":"response.interrupt","response_id":id,"mode":mode});
    send(
        &mut socket,
        interrupt("resp_interrupt_0", "discard_partial_items"),
    )
    .await;
    let idle_interrupt = next(&mut socket).await;
    assert_eq!(idle_interrupt["type"], "error");
    assert_eq!(idle_interrupt["error"]["param"], "response_id");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    // Idle controls must not pass through the response.create decoder/admission.
    assert_steer_rejected(&mut socket, "resp_interrupt_0", "response_not_found").await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    send(&mut socket, create.clone()).await;
    assert_eq!(next(&mut socket).await["type"], "response.created");
    send(&mut socket, create.clone()).await;
    // A queued create must not delay the steering failure until after the terminal.
    assert_steer_rejected(&mut socket, "resp_interrupt_0", "steering_not_supported").await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    for invalid in [
        interrupt("resp_other", "discard_partial_items"),
        interrupt("resp_interrupt_0", "unknown"),
        interrupt("", "discard_partial_items"),
    ] {
        send(&mut socket, invalid).await;
        let error = next(&mut socket).await;
        assert_eq!(error["type"], "error");
        assert_eq!(error["status"], 400);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
    send(
        &mut socket,
        interrupt("resp_interrupt_0", "discard_partial_items"),
    )
    .await;
    let terminal = next(&mut socket).await;
    assert_eq!(terminal["type"], "response.incomplete");
    assert_eq!(
        terminal["response"]["incomplete_details"]["reason"],
        "interrupted"
    );
    assert_eq!(
        next(&mut socket).await["response"]["id"],
        "resp_interrupt_1"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    send(
        &mut socket,
        interrupt("resp_interrupt_0", "discard_partial_items"),
    )
    .await;
    assert_eq!(next(&mut socket).await["type"], "error");
    assert_eq!(
        provider.controls.lock().unwrap()[0].interrupt("resp_interrupt_0"),
        Err(ResponseInterruptError::Unavailable)
    );
    send(
        &mut socket,
        interrupt("resp_interrupt_1", "discard_partial_items"),
    )
    .await;
    assert_eq!(next(&mut socket).await["type"], "response.incomplete");
    // Late controls retain their meaning after the response owner has gone away.
    send(
        &mut socket,
        interrupt("resp_interrupt_1", "discard_partial_items"),
    )
    .await;
    assert_eq!(next(&mut socket).await["error"]["param"], "response_id");
    assert_steer_rejected(&mut socket, "resp_interrupt_1", "response_not_found").await;
    let mut fallback = create;
    fallback["input"] = json!("additional requirement");
    send(&mut socket, fallback).await;
    assert_eq!(
        next(&mut socket).await["response"]["id"],
        "resp_interrupt_2"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *provider.inputs.lock().unwrap(),
        vec![
            json!("hello"),
            json!("hello"),
            json!("additional requirement")
        ]
    );
    socket.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while admissions.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect releases active execution");
    assert_eq!(
        provider.controls.lock().unwrap()[2].interrupt("resp_interrupt_2"),
        Err(ResponseInterruptError::Unavailable)
    );
}

async fn assert_steer_rejected(socket: &mut TestSocket, response_id: &str, code: &str) {
    for input in [
        json!("additional requirement"),
        json!([{"role":"user","content":[
            {"type":"input_text","text":"inspect these"},
            {"type":"input_image","image_url":"https://example.test/image.png"},
            {"type":"input_file","file_id":"file_test"}
        ]}]),
    ] {
        send(
            socket,
            json!({
                "type":"response.steer", "previous_response_id":response_id, "input":input
            }),
        )
        .await;
        let failed = next(socket).await;
        assert_eq!(failed["type"], "response.steer.failed");
        assert_eq!(failed["error"]["type"], "invalid_request_error");
        assert_eq!(failed["error"]["code"], code);
        assert_eq!(failed["steer"]["previous_response_id"], response_id);
        assert_eq!(failed["steer"]["input"], input);
        assert!(
            failed["steer"].get("id").is_none(),
            "unaccepted input has no steering ID"
        );
        assert!(failed["sequence_number"].is_u64());
    }
    for invalid in [
        json!({"type":"response.steer","input":"missing target"}),
        json!({"type":"response.steer","previous_response_id":response_id,"input":[]}),
        json!({"type":"response.steer","previous_response_id":response_id,"input":"hello","stream_id":"wrong"}),
    ] {
        send(socket, invalid).await;
        let failed = next(socket).await;
        assert_eq!(failed["type"], "response.steer.failed");
        assert_eq!(failed["error"]["code"], "invalid_input");
    }
}

#[derive(Default)]
struct InterruptStore {
    finalizations: Mutex<Vec<ModelRequestFinalization>>,
    charges: Mutex<Vec<ClientBudgetCharge>>,
}

#[async_trait]
impl ExecutionStore for InterruptStore {
    fn maintain_request(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: Option<std::time::SystemTime>,
        _: std::time::Duration,
    ) -> Box<dyn gateway_core::engine::ExecutionRecoveryLeaseGuard> {
        Box::new(TestExecutionRecoveryLease)
    }

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
        Ok(RecoveryReport::default())
    }
}

struct TestAdmissionLease;
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for TestAdmissionLease {}

struct TestExecutionRecoveryLease;
impl gateway_core::engine::ExecutionRecoveryLeaseGuard for TestExecutionRecoveryLease {}

pub(crate) fn steering_stream(
    request: ProviderRequest,
    control: ResponseControl,
) -> Result<ProviderStream, ProviderError> {
    let (active, commands) = control
        .activate_with_steering("resp_parent".into())
        .unwrap();
    let candidate = request.candidate();
    let metadata = ProviderCallMetadata::new(
        candidate.provider().clone(),
        candidate.upstream_model().unwrap().clone(),
        ProviderAccountId::new("acct_api_test").unwrap(),
        UpstreamTransport::new("websocket").unwrap(),
    );
    let body = futures::stream::unfold(
        (0, Some(active), commands, control),
        |(phase, mut active, mut commands, control)| async move {
            use gateway_core::metering::{CalculatedCost, Usage};
            let event = match phase {
                0 | 3 => {
                    let id = if phase == 0 {
                        "resp_parent"
                    } else {
                        "resp_child"
                    };
                    if phase == 3 {
                        let (owner, receiver) = control.activate_with_steering(id.into()).unwrap();
                        active = Some(owner);
                        commands = receiver;
                    }
                    ProviderEvent::canonical_with_wire(vec![GatewayEvent::Started(ResponseMeta::new(id,"model-a"))], ProtocolWireEvent::json("openai",Some("response.created".into()),json!({"type":"response.created","response":{"id":id,"model":"model-a","status":"in_progress","output":[]}})).unwrap())
                }
                1 => {
                    let command = commands.next().await.unwrap();
                    assert_eq!(command["previous_response_id"], "resp_parent");
                    assert_eq!(command["input"], "support Windows");
                    ProviderEvent::wire(ProtocolWireEvent::json("openai", Some("response.steer.accepted".into()),json!({"type":"response.steer.accepted","sequence_number":1,"steer":{"id":"steer_test","previous_response_id":"resp_parent"}})).unwrap())
                }
                2 | 4 => {
                    drop(active.take());
                    let (id, kind, tokens, cost) = if phase == 2 {
                        ("resp_parent", "response.incomplete", 10, 100)
                    } else {
                        ("resp_child", "response.completed", 20, 200)
                    };
                    let mut response = json!({"id":id,"model":"model-a","status":if phase == 2 {"incomplete"} else {"completed"},"output":[]});
                    if phase == 2 {
                        response["incomplete_details"] = json!({"reason":"steered"});
                    }
                    ProviderEvent::canonical_with_wire(
                        vec![
                            GatewayEvent::Usage(Usage {
                                input_tokens: Some(tokens),
                                output_tokens: Some(2),
                                total_tokens: Some(tokens + 2),
                                ..Usage::new()
                            }),
                            GatewayEvent::CalculatedCost(
                                CalculatedCost::from_usd_ticks(cost).unwrap(),
                            ),
                        ],
                        ProtocolWireEvent::json(
                            "openai",
                            Some(kind.into()),
                            json!({"type":kind,"response":response}),
                        )
                        .unwrap(),
                    )
                    .with_response_boundary()
                }
                5 => ProviderEvent::canonical(GatewayEvent::Completed(ResponseMeta::new(
                    "resp_child",
                    "model-a",
                ))),
                _ => return None,
            };
            Some((Ok(event), (phase + 1, active, commands, control)))
        },
    );
    Ok(ProviderStream::new(metadata, body, ()))
}

#[tokio::test]
async fn steering_successor_streams_without_another_create_and_settles_combined_usage_once() {
    let provider = Arc::new(InterruptProvider {
        steering: true,
        ..Default::default()
    });
    let admissions = Arc::new(InterruptAdmissions::default());
    let store = Arc::new(InterruptStore::default());
    let execution = Arc::new(
        DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(snapshot("sk_ws_steer", "openai")),
            store.clone(),
            ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
            admissions.clone(),
            Arc::new(UnusedContinuation),
            Arc::new(IgnoredClientApiKeyUsage),
        )
        .with_budget(store.clone()),
    );
    let app = api_router(execution).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer sk_ws_steer"),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    send(
        &mut socket,
        json!({"type":"response.create","model":"model-a","input":"hello"}),
    )
    .await;
    assert_eq!(next(&mut socket).await["response"]["id"], "resp_parent");
    send(&mut socket,json!({"type":"response.steer","previous_response_id":"resp_parent","input":"support Windows"})).await;
    assert_eq!(next(&mut socket).await["type"], "response.steer.accepted");
    assert_eq!(next(&mut socket).await["type"], "response.incomplete");
    assert_eq!(next(&mut socket).await["response"]["id"], "resp_child");
    assert_eq!(next(&mut socket).await["type"], "response.completed");
    tokio::time::timeout(Duration::from_secs(5), async {
        while admissions.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let charges = store.charges.lock().unwrap();
    assert_eq!(charges.len(), 1);
    assert_eq!(charges[0].amount_usd.scaled(), 300);
    let finalized = store.finalizations.lock().unwrap();
    assert_eq!(finalized.len(), 1);
    assert_eq!(finalized[0].usage.input_tokens, Some(30));
    assert_eq!(finalized[0].usage.output_tokens, Some(4));
    assert_eq!(finalized[0].usage.total_tokens, Some(34));
    assert_eq!(finalized[0].cost.total().unwrap().amount().scaled(), 300);
    assert_eq!(
        finalized[0].client_response_id.as_deref(),
        Some("resp_child")
    );
}

impl ClientBudgetPort for InterruptStore {
    fn admit(
        &self,
        _: ClientApiKeyId,
        _: Option<gateway_core::routing::AccountGroupId>,
        _: Option<PublicModelId>,
    ) -> BoxFuture<'_, Result<ClientBudgetScope, GatewayError>> {
        Box::pin(async {
            Ok(ClientBudgetScope {
                billing: Default::default(),
                user_id: "steering-user".into(),
                group_id: gateway_core::routing::AccountGroupId::new(
                    "grp_00000000000000000000000000000001",
                )
                .unwrap(),
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
