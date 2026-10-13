use super::*;
use gateway_core::engine::provider::EventStream;
use gateway_core::engine::upstream_adapter::{
    UpstreamAdapter, UpstreamAdapterInvocation, UpstreamAdapterPlan,
};
use gateway_core::error::ProviderError;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Adapter {
    calls: AtomicUsize,
    invocation: Mutex<Option<UpstreamAdapterInvocation>>,
}
impl std::fmt::Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Adapter")
    }
}
impl UpstreamAdapter for Adapter {
    fn transport(&self) -> &str {
        "http_json"
    }
    fn execute(self: Arc<Self>, invocation: UpstreamAdapterInvocation) -> EventStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.invocation.lock().unwrap() = Some(invocation);
        Box::pin(futures::stream::once(async {
            Err(ProviderError::new(
                ProviderErrorKind::Unavailable,
                UpstreamSendState::NotSent,
            ))
        }))
    }
}
#[derive(Debug)]
struct Plan {
    adapter: Arc<Adapter>,
    cancellation: CancellationToken,
    reject: bool,
}
impl UpstreamAdapterPlan for Plan {
    fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    fn select(
        &self,
        _: &AttemptContext,
        _: &ProviderKind,
        _: &UpstreamModelId,
    ) -> Result<Option<Arc<dyn UpstreamAdapter>>, ProviderError> {
        if self.reject {
            return Err(ProviderError::new(
                ProviderErrorKind::ExecutionPolicyRejected,
                UpstreamSendState::NotSent,
            ));
        }
        Ok(Some(self.adapter.clone()))
    }
}
fn custom_operation() -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "custom_adapter",
            json!({"opaque": [1, 2], "model": "untrusted-model"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap(),
    ))
}

fn adapter_context(plan: Arc<Plan>) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_adapter").unwrap(),
            ClientApiKeyId::new("key_adapter").unwrap(),
        )
        .with_upstream_adapters(Some(plan)),
        NonZeroU32::new(1).unwrap(),
        gateway_core::lifecycle::Deadline::default(),
        account_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    )
}
#[tokio::test]
async fn upstream_adapter_uses_selected_account_frozen_model_and_local_tier_billing() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let provider = provider(&store);
    let adapter = Arc::new(Adapter::default());
    let plan = Arc::new(Plan {
        adapter: adapter.clone(),
        cancellation: CancellationToken::new(),
        reject: false,
    });
    let operation = custom_operation();
    let request = planned_request("openai", operation.clone());
    let mut output = provider
        .execute(request, adapter_context(plan))
        .await
        .unwrap();
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        output.metadata().provider_account_id().as_str(),
        "acct_provider_contract"
    );
    assert_eq!(output.metadata().transport().as_str(), "http_json");
    assert_eq!(
        output.next().await.unwrap().unwrap_err().kind(),
        ProviderErrorKind::Unavailable
    );
    let invocation = adapter.invocation.lock().unwrap().take().unwrap();
    assert_eq!(invocation.operation, operation);
    assert_eq!(
        invocation.account.account_id(),
        output.metadata().provider_account_id()
    );
    let headers = invocation.account.authorization().unwrap();
    assert!(headers.iter().any(|h| h.name() == "authorization"
        && h.value().as_ref() == b"Bearer at-acct_provider_contract"));
    assert!(headers.iter().any(|h| h.name() == "chatgpt-account-id"));
    assert!(!headers.iter().any(|h| h.name() == "cookie"));
    let usage = Usage {
        input_tokens: Some(100),
        output_tokens: Some(10),
        ..Usage::default()
    };
    let actual = invocation
        .account
        .calculate_cost(Some("priority"), &usage)
        .unwrap();
    let expected = provider_openai::transport::usage::openai_billing_breakdown(
        "gpt-5.4",
        provider_openai::transport::usage::OpenAiBillingUsage::from(
            gateway_protocol::openai::events::TokenUsage {
                input_tokens: 100,
                output_tokens: 10,
                ..Default::default()
            },
        ),
        Some("priority"),
    )
    .unwrap()
    .calculated_cost();
    assert_eq!(actual, expected);
    assert_ne!(
        actual,
        invocation
            .account
            .calculate_cost(Some("default"), &usage)
            .unwrap()
    );
    assert!(
        invocation
            .account
            .calculate_cost(None, &Usage::default())
            .is_none()
    );
    let failure = invocation
        .account
        .record_failure(ProviderError::new(
            ProviderErrorKind::Unauthorized,
            UpstreamSendState::NotSent,
        ))
        .await;
    assert_eq!(failure.send_state(), UpstreamSendState::NotSent);
}
#[tokio::test]
async fn upstream_adapter_revocation_before_poll_never_invokes_plugin() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let provider = provider(&store);
    let adapter = Arc::new(Adapter::default());
    let cancellation = CancellationToken::new();
    let plan = Arc::new(Plan {
        adapter: adapter.clone(),
        cancellation: cancellation.clone(),
        reject: false,
    });
    let mut output = provider
        .execute(
            planned_request("openai", custom_operation()),
            adapter_context(plan),
        )
        .await
        .unwrap();
    cancellation.cancel();
    assert_eq!(
        output.next().await.unwrap().unwrap_err().kind(),
        ProviderErrorKind::Cancelled
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn upstream_adapter_rejection_and_account_scope_do_not_fall_back_to_native() {
    let store = Arc::new(MemoryAccountStore::default());
    let provider = provider(&store);
    let adapter = Arc::new(Adapter::default());
    let plan = Arc::new(Plan {
        adapter: adapter.clone(),
        cancellation: CancellationToken::new(),
        reject: true,
    });
    let error = provider
        .execute(
            planned_request("openai", custom_operation()),
            adapter_context(plan),
        )
        .await
        .err()
        .expect("rejected");
    assert_eq!(error.kind(), ProviderErrorKind::ExecutionPolicyRejected);
    create_account_with_enabled(&store, "acct_provider_contract", false).await;
    let plan = Arc::new(Plan {
        adapter: adapter.clone(),
        cancellation: CancellationToken::new(),
        reject: false,
    });
    let error = provider
        .execute(
            planned_request("openai", custom_operation()),
            adapter_context(plan),
        )
        .await
        .err()
        .expect("rejected");
    assert_eq!(error.kind(), ProviderErrorKind::NoEligibleAccount);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn extension_session_owner_cannot_fall_through_to_native_openai_without_adapter() {
    use gateway_core::{error::ContinuationRecoveryDisposition, operation::ExtensionSessionOwner};
    let store = Arc::new(MemoryAccountStore::default());
    let provider = provider(&store);
    for protocol in ["openai", "custom_adapter"] {
        let state = ProviderSessionState::new(
            "openai",
            json!({"private":"adapter-only"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap()
        .with_extension_owner(ExtensionSessionOwner {
            instance_id: "old-instance".into(),
            contribution_id: "contribution".into(),
            adapter_id: "adapter".into(),
            generation: 1,
            incarnation: "old-process".into(),
            connection_local: true,
        });
        let request = GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                protocol,
                json!({"input":[]}).as_object().unwrap().clone(),
            )
            .unwrap(),
        )
        .with_provider_session_state(state);
        let result = provider
            .execute(
                planned_request("openai", Operation::Generate(request)),
                context("req_extension_owner", CancellationToken::new()),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("native execution must reject adapter state"),
        };
        assert_eq!(
            error.kind(),
            ProviderErrorKind::ContinuationRecoveryRequired
        );
        assert_eq!(error.send_state(), UpstreamSendState::NotSent);
        assert_eq!(
            error.continuation_recovery_disposition(),
            Some(ContinuationRecoveryDisposition::ClientReplayRequired)
        );
    }
}
