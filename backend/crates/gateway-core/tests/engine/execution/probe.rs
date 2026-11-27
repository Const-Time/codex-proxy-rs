use super::*;
use gateway_core::event::FinishReason;
use gateway_core::routing::RoutingContext;
use gateway_protocol::genai::GENAI_PROTOCOL;

struct ProbeProvider {
    name: &'static str,
    account: ProviderAccountId,
    model: UpstreamModelId,
    diagnostic_catalog: Option<Vec<ProviderModelCapabilities>>,
    diagnostic_selects_generation_protocol: bool,
    calls: AtomicUsize,
}

#[async_trait]
impl Provider for ProbeProvider {
    fn name(&self) -> &'static str {
        self.name
    }

    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }

    fn diagnostic_selects_generation_protocol(&self) -> bool {
        self.diagnostic_selects_generation_protocol
    }

    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        panic!("the probe uses a frozen or account-scoped catalog")
    }

    async fn query_diagnostic_model_catalog(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Option<Vec<ProviderModelCapabilities>>, ProviderError> {
        assert_eq!(account, &self.account);
        Ok(self.diagnostic_catalog.clone())
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        assert!(context.is_diagnostic_required_account());
        assert_eq!(context.required_account(), Some(&self.account));
        assert_eq!(request.candidate().provider().as_str(), self.name);
        assert_eq!(request.candidate().upstream_model(), Some(&self.model));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let metadata = ProviderCallMetadata::new(
            request.candidate().provider().clone(),
            self.model.clone(),
            self.account.clone(),
            UpstreamTransport::new("http").unwrap(),
        );
        let response = ResponseMeta::new("resp_probe", self.model.as_str());
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter([
                Ok(GatewayEvent::Started(response.clone()).into()),
                Ok(GatewayEvent::Completed(response.with_finish_reason(FinishReason::Stop)).into()),
            ]),
            (),
        ))
    }
}

fn probe_service(
    snapshot: RuntimeSnapshot,
    providers: impl IntoIterator<Item = Arc<dyn Provider>>,
) -> DefaultExecutionService {
    DefaultExecutionService::new(
        RuntimeSnapshotHandle::new(snapshot),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::new(providers).unwrap(),
        Arc::new(UnusedAdmissions),
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
    )
}

#[test]
fn openai_and_xai_connection_probes_keep_the_snapshot_path_and_pin() {
    let model = UpstreamModelId::new("gpt-probe").unwrap();
    let account = ProviderAccountId::new("acct_probe").unwrap();
    let providers = ["openai", "xai"].map(|name| {
        Arc::new(ProbeProvider {
            name,
            model: model.clone(),
            account: account.clone(),
            diagnostic_catalog: None,
            diagnostic_selects_generation_protocol: false,
            calls: AtomicUsize::new(0),
        })
    });
    let kinds = providers
        .each_ref()
        .map(|provider| ProviderKind::new(provider.name).unwrap());
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::MIN,
            Duration::ZERO,
        ),
        kinds.to_vec(),
        kinds
            .iter()
            .map(|kind| {
                ProviderModel::new(
                    kind.clone(),
                    model.clone(),
                    ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None),
                )
            })
            .collect(),
        Vec::new(),
    )
    .unwrap();
    let service = probe_service(
        snapshot,
        providers
            .iter()
            .cloned()
            .map(|provider| provider as Arc<dyn Provider>),
    );
    for kind in kinds {
        let result = block_on(service.probe(AccountProbeRequest {
            account_id: account.clone(),
            provider_kind: kind,
            upstream_model: model.clone(),
            operation: probe_operation(),
        }))
        .unwrap();
        assert!(result.complete);
    }
    for provider in providers {
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn fixed_probe_can_select_the_credential_protocol_without_widening_public_routing() {
    let kind = ProviderKind::new("openai").unwrap();
    let account = ProviderAccountId::new("acct_messages_probe").unwrap();
    let model = UpstreamModelId::new("claude-probe").unwrap();
    let capabilities = ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
        .with_generation_protocol(gateway_protocol::anthropic::ANTHROPIC_PROTOCOL);
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::MIN,
            Duration::ZERO,
        ),
        vec![kind.clone()],
        vec![ProviderModel::new(
            kind.clone(),
            model.clone(),
            capabilities,
        )],
        Vec::new(),
    )
    .unwrap();
    let operation = probe_operation();
    assert!(
        snapshot
            .plan(
                &PublicModelId::new(model.as_str()).unwrap(),
                &operation,
                snapshot.all_account_scope(),
                &RoutingContext {
                    required_provider: Some(kind.clone()),
                    ..RoutingContext::default()
                },
            )
            .is_err()
    );
    let provider = Arc::new(ProbeProvider {
        name: "openai",
        account: account.clone(),
        model: model.clone(),
        diagnostic_catalog: None,
        diagnostic_selects_generation_protocol: true,
        calls: AtomicUsize::new(0),
    });
    let service = probe_service(snapshot, [provider.clone() as Arc<dyn Provider>]);
    let result = block_on(service.probe(AccountProbeRequest {
        account_id: account,
        provider_kind: kind,
        upstream_model: model,
        operation,
    }))
    .unwrap();
    assert!(result.complete);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn antigravity_probe_accepts_a_paused_only_model_and_requires_account_ownership() {
    let kind = ProviderKind::new("antigravity").unwrap();
    let account = ProviderAccountId::new("acct_paused").unwrap();
    let model = UpstreamModelId::new("gemini-paused-only").unwrap();
    let other_model = UpstreamModelId::new("gemini-other-account").unwrap();
    let capabilities = ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
        .with_generation_protocol(GENAI_PROTOCOL);
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            std::num::NonZeroU32::MIN,
            Duration::ZERO,
        ),
        vec![kind.clone()],
        vec![ProviderModel::new(
            kind.clone(),
            other_model.clone(),
            capabilities.clone(),
        )],
        Vec::new(),
    )
    .unwrap();
    let provider = Arc::new(ProbeProvider {
        name: "antigravity",
        account: account.clone(),
        model: model.clone(),
        diagnostic_catalog: Some(vec![
            ProviderModelCapabilities::new(model.clone(), capabilities.clone())
                .with_catalog_accounts(BTreeSet::from([account.clone()])),
            ProviderModelCapabilities::new(other_model.clone(), capabilities)
                .with_catalog_accounts(BTreeSet::from([
                    ProviderAccountId::new("acct_other").unwrap()
                ])),
        ]),
        diagnostic_selects_generation_protocol: false,
        calls: AtomicUsize::new(0),
    });
    let body = json!({"contents": [{"parts": [{"text": "ping"}]}]});
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(GENAI_PROTOCOL, body.as_object().unwrap().clone()).unwrap(),
    ));
    let public_model = PublicModelId::new(model.to_string()).unwrap();
    let context = RoutingContext {
        required_provider: Some(kind.clone()),
        ..RoutingContext::default()
    };
    assert!(!snapshot.contains_public_model_for_provider(&public_model, &kind));
    assert!(matches!(
        snapshot.plan(
            &public_model,
            &operation,
            snapshot.all_account_scope(),
            &context
        ),
        Err(gateway_core::validation::RoutingError::ModelNotFound { .. }),
    ));
    let snapshot = snapshot.with_model_mappings(BTreeMap::from([(
        model.to_string(),
        other_model.to_string(),
    )]));
    let public_plan = snapshot
        .plan(
            &public_model,
            &operation,
            snapshot.all_account_scope(),
            &context,
        )
        .unwrap();
    assert_eq!(
        public_plan.candidates()[0].upstream_model(),
        Some(&other_model)
    );

    let service = probe_service(snapshot, [provider.clone() as Arc<dyn Provider>]);
    for invalid in [other_model, UpstreamModelId::new("gemini-unknown").unwrap()] {
        let error = block_on(service.probe(AccountProbeRequest {
            account_id: account.clone(),
            provider_kind: kind.clone(),
            upstream_model: invalid,
            operation: operation.clone(),
        }))
        .unwrap_err();
        assert_eq!(error.kind(), GatewayErrorKind::ModelNotFound);
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    let result = block_on(service.probe(AccountProbeRequest {
        account_id: account,
        provider_kind: kind,
        upstream_model: model,
        operation,
    }))
    .unwrap();
    assert!(result.complete);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}
