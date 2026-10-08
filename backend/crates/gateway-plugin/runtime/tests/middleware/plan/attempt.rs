//! Real SDK subprocess -> Responses wire bridge -> Core attempt and receipts.
use super::*;
use gateway_core::{
    account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy},
    engine::{provider::*, *},
    error::{ProviderError, StoreError},
    event::{ProviderResponseHeader, ProviderResponseObservation, ResponseMeta},
    operation::Operation,
    routing::*,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use std::{collections::BTreeSet, num::NonZeroU32};

#[derive(Default)]
struct Execution {
    calls: AtomicUsize,
    commits: AtomicUsize,
    rows: Mutex<Vec<ModelRequestFinalization>>,
}
#[async_trait]
impl ExecutionStore for Execution {
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
        row: ModelRequestFinalization,
    ) -> Result<(), StoreError> {
        self.rows.lock().unwrap().push(row);
        Ok(())
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        Ok(RecoveryReport::default())
    }
}
#[async_trait]
impl Provider for Execution {
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
        assert_eq!(context.client_api_key_ref().as_str(), "key_original");
        let metadata = if let Some(model) = request.candidate().upstream_model() {
            ProviderCallMetadata::new(
                request.candidate().provider().clone(),
                model.clone(),
                ProviderAccountId::new("acct_one").unwrap(),
                UpstreamTransport::new("http").unwrap(),
            )
        } else {
            ProviderCallMetadata::for_provider_endpoint(
                request.candidate().provider().clone(),
                ProviderAccountId::new("acct_one").unwrap(),
                UpstreamTransport::new("http").unwrap(),
            )
        };
        let mut events = vec![Ok(ProviderEvent::observation(
            ProviderResponseObservation::new(UpstreamTransport::new("http").unwrap())
                .with_status_code(200)
                .with_client_headers(vec![ProviderResponseHeader::new(
                    "x-upstream",
                    Bytes::from_static(b"original"),
                )]),
        ))];
        if !matches!(request.operation(), Operation::Generate(_)) {
            let meta = ResponseMeta::for_provider_endpoint(context.request_id().as_str());
            events.extend([
                Ok(ProviderEvent::canonical(GatewayEvent::Started(
                    meta.clone(),
                ))),
                Ok(ProviderEvent::canonical(GatewayEvent::ProviderCost(
                    ProviderReportedCost::from_usd_ticks(123).unwrap(),
                ))),
                Ok(ProviderEvent::wire(
                    ProtocolWireEvent::raw_json(
                        "openai",
                        Bytes::from_static(b"{ \"data\": [1], \"data\": [2] }\n"),
                    )
                    .unwrap(),
                )),
                Ok(ProviderEvent::canonical(GatewayEvent::Completed(meta))),
            ]);
            return Ok(ProviderStream::new(
                metadata,
                futures::stream::iter(events),
                (),
            ));
        }
        for (kind, facts) in [
            (
                "response.created",
                vec![GatewayEvent::Started(ResponseMeta::new(
                    "resp_original",
                    "gpt-5",
                ))],
            ),
            (
                "response.completed",
                vec![
                    GatewayEvent::ProviderCost(ProviderReportedCost::from_usd_ticks(123).unwrap()),
                    GatewayEvent::Completed(ResponseMeta::new("resp_original", "gpt-5")),
                ],
            ),
        ] {
            events.push(Ok(ProviderEvent::canonical_with_wire(
                facts,
                ProtocolWireEvent::json(
                    "openai",
                    Some(kind.into()),
                    json!({"type":kind,"response":{"id":"resp_original","output":[]}}),
                )
                .unwrap(),
            )));
        }
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(events),
            (),
        ))
    }
}
fn routing(operation: &Operation) -> RoutingPlan {
    let provider = ProviderKind::new("openai").unwrap();
    let directory = Arc::new(RuntimeAccountDirectory::new(
        [(
            ProviderAccountId::new("acct_one").unwrap(),
            RuntimeAccount::new(provider.clone(), BTreeSet::new()),
        )]
        .into(),
    ));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(2).unwrap(),
            Duration::ZERO,
        ),
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider.clone(),
            UpstreamModelId::new("gpt-5").unwrap(),
            ModelCapabilities::new(BTreeSet::from([operation.kind()]), Some(32000))
                .with_upstream_feature_validation(),
        )],
        vec![],
    )
    .unwrap()
    .with_account_directory(directory.clone());
    if !matches!(operation, Operation::Generate(_)) {
        return snapshot
            .plan_provider_endpoint(
                &provider,
                operation,
                Arc::new(FrozenAccountScope::new(
                    directory,
                    ClientRoutingScope::all_accounts(),
                )),
                &RoutingContext::default(),
            )
            .unwrap();
    }
    snapshot
        .plan(
            &PublicModelId::new("gpt-5").unwrap(),
            operation,
            Arc::new(FrozenAccountScope::new(
                directory,
                ClientRoutingScope::all_accounts(),
            )),
            &RoutingContext::default(),
        )
        .unwrap()
}
fn request_row(context: &MiddlewareContext, operation: &Operation) -> NewModelRequest {
    NewModelRequest {
        budget_scope: None,
        admission_decision_ms: None,
        id: context.request_id.clone(),
        client_api_key_id: Some(context.client_key_id.clone()),
        client_api_key_ref: context.client_key_id.clone(),
        config_revision: ConfigRevision::new(1).unwrap(),
        routing: AccountRoutingSnapshot::all(),
        protocol: "openai".into(),
        operation: operation.kind(),
        endpoint: "responses".into(),
        client_transport: "http_json".into(),
        requested_model: Some(PublicModelId::new("gpt-5").unwrap()),
        client_ip: None,
        user_agent: None,
        reasoning_effort: None,
        reasoning_preset: None,
        request_kind: None,
        subagent_kind: None,
        compact: false,
        continuation: Default::default(),
        image_generation_requested: false,
        started_at: SystemTime::now(),
        deadline_at: context.deadline_at,
    }
}
#[tokio::test]
async fn sdk_attempt_bridge_preserves_receipts_across_pass_mapping_expansion_and_fact_edits() {
    let install = Installation::new().await;
    let mut command = install.command(0, 0);
    command.middleware.as_mut().unwrap().mounts = vec![PluginMiddlewareMount::Attempt];
    install
        .manager
        .update(None, command, &actor())
        .await
        .unwrap();
    for (mode, streaming) in [
        ("short", false),
        ("short", true),
        ("pass", false),
        ("inspect", false),
        ("map", false),
        ("pass", true),
        ("inspect", true),
        ("expand", true),
        ("chain", true),
    ] {
        let context = context(streaming);
        let decoded = gateway_api::openai::responses::decode_request_with_headers(
            serde_json::to_string(
                &json!({"model":"gpt-5","input":"original","stream":streaming,"mode":mode}),
            )
            .unwrap()
            .as_bytes(),
            &Default::default(),
        )
        .unwrap();
        let operation = decoded.into_parts().0;
        let execution = Arc::new(Execution::default());
        let adapter = gateway_api::openai::middleware::responses_attempt_middleware(
            Arc::new(install.manager.middleware_registry().unwrap()),
            context.clone(),
        );
        let coordinator = AttemptCoordinator::new(GatewayEngine::new(
            execution.clone(),
            ProviderRegistry::new([execution.clone() as Arc<dyn Provider>]).unwrap(),
        ))
        .with_attempt_middleware(Some(adapter));
        let mut session = coordinator
            .start(
                request_row(&context, &operation),
                operation.clone(),
                routing(&operation),
                None,
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let events = tokio::time::timeout(Duration::from_secs(5), session.collect_uncommitted())
            .await
            .expect("SDK/Core rendezvous must not deadlock")
            .unwrap();
        if mode == "short" {
            assert_eq!(events.len(), 1);
            assert_eq!(
                &events[0].wire_event().unwrap().raw_json_body().unwrap()[..],
                b"denied"
            );
            assert_eq!(session.middleware_response_head().unwrap().status_code, 403);
            session.commit_downstream(Some(403)).await.unwrap();
            assert_eq!(execution.calls.load(Ordering::SeqCst), 0);
            assert_eq!(session.budget_charge().amount_usd.scaled(), 0);
            continue;
        }
        let mut encoder = gateway_api::openai::responses::OpenAiResponsesEncoder::new();
        let mut output = vec![];
        for event in &events {
            for frame in encoder.push_sse(event) {
                output.extend_from_slice(&frame);
            }
        }
        let value = encoder.finish().unwrap();
        if mode == "map" {
            assert_eq!(value, json!({"mapped":true}));
        } else {
            assert_eq!(value["id"], "resp_original");
        }
        if mode == "expand" {
            assert!(String::from_utf8_lossy(&output).contains("prefix"));
        }
        assert_eq!(session.response_status_code(), Some(200));
        assert_eq!(session.middleware_response_head().unwrap().status_code, 200);
        if mode == "chain" {
            assert!(
                session
                    .response_headers()
                    .iter()
                    .any(|h| h.name() == "x-chain")
            );
        }
        session.commit_downstream(Some(200)).await.unwrap();
        assert_eq!(execution.calls.load(Ordering::SeqCst), 1);
        assert_eq!(execution.commits.load(Ordering::SeqCst), 1);
        assert_eq!(session.budget_charge().amount_usd.scaled(), 123);
        let rows = execution.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].upstream_response_id.as_deref(),
            Some("resp_original")
        );
    }
    install.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_raw_endpoint_attempt_bridge_preserves_bytes_and_receipts() {
    use gateway_core::operation::{
        ImageRequest, ImageRequestKind, RawJsonPayload, StandaloneSearchRequest,
    };
    let install = Installation::new().await;
    let mut command = install.command(0, 0);
    command.middleware.as_mut().unwrap().mounts = vec![PluginMiddlewareMount::Attempt];
    install
        .manager
        .update(None, command, &actor())
        .await
        .unwrap();
    for endpoint in [
        "/v1/images/generations",
        "/v1/images/edits",
        "/v1/alpha/search",
    ] {
        for mode in ["short", "pass", "inspect", "map"] {
            let payload = RawJsonPayload::new(
                "openai",
                Bytes::from(json!({"mode":mode,"prompt":"original"}).to_string()),
            )
            .unwrap();
            let operation = match endpoint {
                "/v1/images/generations" => Operation::GenerateImage(ImageRequest::from_raw_json(
                    ImageRequestKind::Generation,
                    payload,
                )),
                "/v1/images/edits" => Operation::GenerateImage(ImageRequest::from_raw_json(
                    ImageRequestKind::Edit,
                    payload,
                )),
                _ => Operation::Search(StandaloneSearchRequest::from_raw_json(payload)),
            };
            let mut context = context(false);
            context.operation = operation.kind();
            context.endpoint = endpoint.into();
            context.model = None;
            let execution = Arc::new(Execution::default());
            let adapter = gateway_api::openai::middleware::raw_json_attempt_middleware(
                Arc::new(install.manager.middleware_registry().unwrap()),
                context.clone(),
            );
            let coordinator = AttemptCoordinator::new(GatewayEngine::new(
                execution.clone(),
                ProviderRegistry::new([execution.clone() as Arc<dyn Provider>]).unwrap(),
            ))
            .with_attempt_middleware(Some(adapter));
            let mut row = request_row(&context, &operation);
            row.endpoint = endpoint.into();
            row.requested_model = None;
            let mut session = coordinator
                .start(
                    row,
                    operation.clone(),
                    routing(&operation),
                    None,
                    None,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let events =
                tokio::time::timeout(Duration::from_secs(5), session.collect_uncommitted())
                    .await
                    .expect("raw SDK/Core rendezvous must not deadlock")
                    .unwrap();
            let bodies: Vec<_> = events
                .iter()
                .filter_map(|event| event.wire_event().and_then(|wire| wire.raw_json_body()))
                .collect();
            assert_eq!(bodies.len(), 1);
            let expected: &[u8] = match mode {
                "short" => b"denied",
                "map" => b"{\"mapped\":true}",
                _ => b"{ \"data\": [1], \"data\": [2] }\n",
            };
            assert_eq!(&bodies[0][..], expected, "{endpoint} {mode}");
            let status = if mode == "short" { 403 } else { 200 };
            assert_eq!(
                session.middleware_response_head().unwrap().status_code,
                status
            );
            session.commit_downstream(Some(status)).await.unwrap();
            assert_eq!(
                execution.calls.load(Ordering::SeqCst),
                usize::from(mode != "short")
            );
            assert_eq!(
                execution.commits.load(Ordering::SeqCst),
                usize::from(mode != "short")
            );
            assert_eq!(
                session.budget_charge().amount_usd.scaled(),
                if mode == "short" { 0 } else { 123 }
            );
            assert_eq!(execution.rows.lock().unwrap().len(), 1);
        }
    }
    install.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_websocket_attempt_preserves_json_framing_identity_and_original_receipts() {
    let install = Installation::new().await;
    let mut command = install.command(0, 0);
    command.middleware.as_mut().unwrap().mounts = vec![PluginMiddlewareMount::Attempt];
    install
        .manager
        .update(None, command, &actor())
        .await
        .unwrap();
    for mode in ["pass", "inspect", "ws-map", "ws-expand", "ws-short"] {
        let mut context = context(true);
        context.transport = ClientTransport::WebSocket;
        let decoded = gateway_api::openai::responses::decode_response_create_with_context(
            &json!({"type":"response.create","model":"gpt-5","input":"original","mode":mode})
                .to_string(),
            &Default::default(),
        )
        .unwrap();
        let operation = decoded.into_parts().0;
        let execution = Arc::new(Execution::default());
        let adapter = gateway_api::openai::middleware::responses_attempt_middleware(
            Arc::new(install.manager.middleware_registry().unwrap()),
            context.clone(),
        );
        let coordinator = AttemptCoordinator::new(GatewayEngine::new(
            execution.clone(),
            ProviderRegistry::new([execution.clone() as Arc<dyn Provider>]).unwrap(),
        ))
        .with_attempt_middleware(Some(adapter));
        let mut row = request_row(&context, &operation);
        row.client_transport = "websocket".into();
        let mut session = coordinator
            .start(
                row,
                operation.clone(),
                routing(&operation),
                None,
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let events = tokio::time::timeout(Duration::from_secs(5), session.collect_uncommitted())
            .await
            .expect("WS SDK/Core rendezvous must not deadlock")
            .unwrap();
        let values: Vec<_> = events
            .iter()
            .filter_map(|event| event.wire_event())
            .map(|wire| {
                assert!(wire.has_json_data());
                assert!(wire.raw_sse_frame().is_none());
                wire.data().clone()
            })
            .collect();
        let short = mode == "ws-short";
        if short {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0]["type"], "error");
            assert_eq!(values[0]["status"], 403);
            assert_eq!(values[0]["error"]["message"], "blocked");
        } else {
            assert_eq!(values.last().unwrap()["type"], "response.completed");
            assert_eq!(values.last().unwrap()["response"]["id"], "resp_original");
            if mode == "ws-map" {
                assert!(values.iter().all(|v| v["plugin"] == true));
            }
            if mode == "ws-expand" {
                assert_eq!(values.iter().filter(|v| v["delta"] == "prefix").count(), 2);
            }
        }
        let status = if short { 403 } else { 200 };
        assert_eq!(
            session.middleware_response_head().unwrap().status_code,
            status
        );
        session.commit_downstream(Some(status)).await.unwrap();
        assert_eq!(
            session.budget_charge().amount_usd.scaled(),
            if short { 0 } else { 123 }
        );
        assert_eq!(execution.calls.load(Ordering::SeqCst), usize::from(!short));
        assert_eq!(execution.rows.lock().unwrap().len(), 1);
    }
    install.manager.shutdown().await;
}
