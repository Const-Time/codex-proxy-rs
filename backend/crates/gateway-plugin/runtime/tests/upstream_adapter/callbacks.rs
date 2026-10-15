use super::execution::Server;
use crate::{
    models::fixture::Fixture,
    support::{actor, update, upstream_archive},
};
use futures::future::BoxFuture;
use gateway_admin::{
    model::plugins::*,
    ports::plugins::{PluginService, PluginStore},
};
use gateway_core::{
    account::{CredentialRevision, OutboundProxy, ProviderAccountId},
    engine::{
        ExecutionOutcome,
        execution::*,
        middleware::{MiddlewareHeader, http as core},
        upstream_adapter::UpstreamAccountConnection,
    },
    error::ProviderError,
    metering::{CalculatedCost, Usage},
    operation::{GenerateRequest, Operation, ProtocolPayload},
    routing::PublicModelId,
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

// The original Core test Provider retains the account lease in ProviderStream.
pub(crate) struct Connection(pub(crate) ProviderAccountId);
impl UpstreamAccountConnection for Connection {
    fn account_id(&self) -> &ProviderAccountId {
        &self.0
    }
    fn credential_revision(&self) -> CredentialRevision {
        CredentialRevision::new(3).unwrap()
    }
    fn authentication_kind(&self) -> &str {
        "oauth"
    }
    fn outbound_proxy(&self) -> Option<&OutboundProxy> {
        None
    }
    fn authorization(&self) -> Result<Vec<MiddlewareHeader>, ProviderError> {
        Ok(vec![])
    }
    fn credential_updates(
        &self,
    ) -> Option<&dyn gateway_core::engine::upstream_adapter::UpstreamCredentialUpdate> {
        None
    }
    fn calculate_cost(&self, tier: Option<&str>, usage: &Usage) -> Option<CalculatedCost> {
        assert_eq!(tier, Some("priority"));
        assert_eq!(usage.input_tokens, Some(10));
        Some(CalculatedCost::from_usd_ticks(500).unwrap())
    }
    fn record_failure(&self, error: ProviderError) -> BoxFuture<'_, ProviderError> {
        Box::pin(async move { error })
    }
}

pub(crate) async fn fixture() -> (Fixture, Server) {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    f.snapshot
        .publish(crate::models::fixture::snapshot_with_attempts(
            2,
            true,
            &["acct_allowed", "acct_alternative"],
            2,
        ));
    let server = Server::new().await;
    let artifact = f
        .manager
        .upload(upstream_archive("2.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 1, true);
    command.configuration = json!({"adapter_url":server.url,"adapter_models":["gpt-native"]});
    command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
        scope: PluginExecutionScope::All {},
    });
    f.manager
        .update(Some(f.id.as_str()), command, &actor())
        .await
        .unwrap();
    (f, server)
}
async fn start(
    f: &Fixture,
    mode: &str,
    dispatcher: Option<Arc<dyn core::Dispatcher>>,
) -> StartedExecution {
    start_with_factory(f, mode, dispatcher, None).await
}
pub(crate) async fn start_with_factory(
    f: &Fixture,
    mode: &str,
    dispatcher: Option<Arc<dyn core::Dispatcher>>,
    factory: Option<Arc<dyn gateway_core::engine::middleware::service::Factory>>,
) -> StartedExecution {
    let mut prepared = f
        .execution
        .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap()
        .with_upstream_adapters(Arc::new(f.manager.middleware_registry().unwrap()));
    if let Some(factory) = factory {
        prepared = prepared.with_service_factory(factory);
    }
    if let Some(dispatcher) = dispatcher {
        prepared = prepared.with_http_dispatcher(dispatcher);
    }
    let external_url = f.store.records.lock().unwrap()[&f.id].configuration["adapter_url"].clone();
    f.execution.start_prepared(prepared, PreparedExecutionRequest {
        public_model: PublicModelId::new("gpt-native").unwrap(),
        operation: Operation::Generate(GenerateRequest::from_protocol_payload(ProtocolPayload::json_object("openai", serde_json::from_value(json!({"model":"gpt-native","input":"fixture","behavior":mode,"fixture_account":"acct_allowed","fixture_model":"gpt-native","external_url":external_url})).unwrap()).unwrap())),
        metadata: ExecutionRequestMetadata { protocol:"openai".into(), endpoint:"/v1/responses".into(), transport:ClientTransport::HttpJson, stream:false, client_ip:None, user_agent:None, previous_response_id:None },
    }).await.unwrap()
}
async fn complete(f: &Fixture, mode: &str, dispatcher: Option<Arc<dyn core::Dispatcher>>) {
    let mut started = start(f, mode, dispatcher).await;
    started.session.collect_uncommitted().await.unwrap();
    started.session.commit_downstream(Some(200)).await.unwrap();
    started.session.detach_finalize().await;
}

#[tokio::test]
async fn upstream_model_callbacks_inherit_parent_authority_and_settle_each_execution_once() {
    for mode in ["callback-model", "callback-model-stream"] {
        let (f, server) = fixture().await;
        complete(&f, mode, None).await;
        f.clean(2).await;
        {
            let rows = f.core.requests.lock().unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[1].parent_request_id.as_ref(), Some(&rows[0].id));
            assert_eq!(
                rows[1].initiating_plugin_instance_id.as_deref(),
                Some(f.id.as_str())
            );
            assert_eq!(rows[0].client_api_key_ref, rows[1].client_api_key_ref);
            assert_eq!(rows[0].config_revision, rows[1].config_revision);
            assert_eq!(
                f.core
                    .selected
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|a| a.as_str())
                    .collect::<Vec<_>>(),
                ["acct_allowed", "acct_alternative"]
            );
            let mut costs = f
                .core
                .charges
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.amount_usd.scaled())
                .collect::<Vec<_>>();
            costs.sort();
            assert_eq!(costs, [123, 500]);
            assert!(
                f.core
                    .finalizations
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| r.outcome == ExecutionOutcome::Succeeded && r.attempt_count == 1)
            );
            assert!(f.core.leases.lock().unwrap().is_empty());
            assert!(server.requests.lock().unwrap().is_empty());
        }
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn upstream_private_state_uses_original_schema_and_rejects_management_authority() {
    let (f, server) = fixture().await;
    complete(&f, "callback-state", None).await;
    f.clean(1).await;
    assert_eq!(f.core.requests.lock().unwrap().len(), 1);
    assert!(server.requests.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn upstream_model_callbacks_cannot_reacquire_parent_account_or_use_disabled_key() {
    let (f, _) = fixture().await;
    complete(&f, "callback-model-deny", None).await;
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 500);
    assert!(f.core.leases.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn upstream_model_callback_cancellation_releases_original_parent_and_child_leases() {
    for action in ["cancel", "key", "plugin"] {
        let (f, _) = fixture().await;
        let mut started = start(&f, "callback-model-cancel", None).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), started.session.next_event())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while f.core.selected.lock().unwrap().len() != 2
                || f.core.polled.load(Ordering::SeqCst) < 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match action {
            "key" => f
                .snapshot
                .publish(crate::models::fixture::snapshot(3, false)),
            "plugin" => {
                let mut command =
                    update(&f.store.list().await.unwrap()[0].artifact_sha256, 2, false);
                command.configuration =
                    json!({"adapter_url":"http://127.0.0.1/","adapter_models":["gpt-native"]});
                command.upstream_adapter = Some(PluginUpstreamAdapterBinding {
                    scope: PluginExecutionScope::All {},
                });
                f.manager
                    .update(Some(f.id.as_str()), command, &actor())
                    .await
                    .unwrap();
            }
            _ => started.session.cancel(),
        }
        assert!(
            tokio::time::timeout(
                Duration::from_secs(3),
                started.session.collect_uncommitted()
            )
            .await
            .unwrap()
            .is_err()
        );
        started.session.detach_finalize().await;
        f.clean(2).await;
        assert!(f.core.leases.lock().unwrap().is_empty());
        assert_eq!(
            f.core
                .charges
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.amount_usd.scaled())
                .sum::<u128>(),
            123
        );
        assert!(
            f.core
                .finalizations
                .lock()
                .unwrap()
                .iter()
                .all(|r| r.outcome == ExecutionOutcome::Cancelled)
        );
        f.manager.shutdown().await;
    }
}

#[derive(Default)]
struct Echo {
    parents: Mutex<Vec<HttpFacts>>,
}
struct HttpFacts {
    parent: gateway_core::engine::ModelRequestId,
    key: String,
    excluded: std::collections::BTreeSet<String>,
}

struct PendingHttp {
    entered: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
struct HttpGuard(Arc<AtomicUsize>);
impl Drop for HttpGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl core::Dispatcher for PendingHttp {
    fn dispatch(
        &self,
        context: core::Context,
        _: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, gateway_core::engine::middleware::MiddlewareError>>
    {
        assert!(context.call_scope.is_http_child());
        self.entered.fetch_add(1, Ordering::SeqCst);
        let guard = HttpGuard(self.dropped.clone());
        Box::pin(async move {
            let _guard = guard;
            futures::future::pending().await
        })
    }
}

#[tokio::test]
async fn upstream_invocation_finish_closes_unread_child_model_stream_without_charging_unread_output()
 {
    let (f, _) = fixture().await;
    complete(&f, "callback-model-abandon", None).await;
    f.clean(2).await;
    assert_eq!(
        f.core.starts.load(Ordering::SeqCst),
        1,
        "the unread child must remain cold"
    );
    assert_eq!(f.core.requests.lock().unwrap().len(), 2);
    assert_eq!(f.core.selected.lock().unwrap().len(), 1);
    assert!(f.core.leases.lock().unwrap().is_empty());
    let mut costs = f
        .core
        .charges
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.amount_usd.scaled())
        .collect::<Vec<_>>();
    costs.sort();
    assert_eq!(costs, [0, 500]);
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(
            rows.iter()
                .filter(|r| r.outcome == ExecutionOutcome::Succeeded)
                .count(),
            1
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.outcome == ExecutionOutcome::Cancelled)
                .count(),
            1
        );
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn upstream_cancellation_drops_pending_http_callback_and_releases_parent_lease() {
    let (f, _) = fixture().await;
    let http = Arc::new(PendingHttp {
        entered: Arc::default(),
        dropped: Arc::default(),
    });
    let mut started = start(&f, "callback-http", Some(http.clone())).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), started.session.next_event())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while http.entered.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    started.session.cancel();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            started.session.collect_uncommitted()
        )
        .await
        .unwrap()
        .is_err()
    );
    started.session.detach_finalize().await;
    f.clean(1).await;
    assert!(f.core.leases.lock().unwrap().is_empty());
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
    tokio::time::timeout(Duration::from_secs(3), async {
        while http.dropped.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.manager.shutdown().await;
}
impl core::Dispatcher for Echo {
    fn dispatch(
        &self,
        context: core::Context,
        request: core::Request,
    ) -> BoxFuture<'static, Result<core::Response, gateway_core::engine::middleware::MiddlewareError>>
    {
        assert!(context.call_scope.is_http_child());
        let parent = context.model_parent.as_ref().unwrap();
        self.parents.lock().unwrap().push(HttpFacts {
            parent: parent.request_id().clone(),
            key: parent.client().policy().key_id().as_str().into(),
            excluded: context.call_scope.excluded_instances().clone(),
        });
        Box::pin(async move { Ok(core::Response::new(request.into_body())) })
    }
}

#[tokio::test]
async fn upstream_http_callbacks_use_frozen_parent_dispatcher_and_original_graph() {
    let (f, _) = fixture().await;
    let global = Arc::new(Echo::default());
    let frozen = Arc::new(Echo::default());
    let global_port: Arc<dyn core::Dispatcher> = global.clone();
    f.http.attach(&global_port);
    complete(&f, "callback-http", Some(frozen.clone())).await;
    f.clean(1).await;
    assert!(global.parents.lock().unwrap().is_empty());
    {
        let contexts = frozen.parents.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].parent, f.core.requests.lock().unwrap()[0].id);
        assert_eq!(contexts[0].key, "key_alice");
        assert!(contexts[0].excluded.contains(&f.id));
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn upstream_callback_effects_veto_parent_replay_even_before_provider_send() {
    for mode in ["callback-model-fault", "callback-http-fault"] {
        let (f, server) = fixture().await;
        let echo = Arc::new(Echo::default());
        let mut started = start(&f, mode, Some(echo.clone())).await;
        assert!(started.session.collect_uncommitted().await.is_err());
        started.session.detach_finalize().await;
        f.clean(if mode == "callback-model-fault" { 2 } else { 1 })
            .await;
        {
            let rows = f.core.finalizations.lock().unwrap();
            let parent = rows
                .iter()
                .find(|r| r.outcome == ExecutionOutcome::Failed)
                .unwrap();
            assert_eq!(
                parent.attempt_count, 1,
                "original policy permits two attempts; callback effects must veto replay"
            );
            assert!(server.requests.lock().unwrap().is_empty());
            assert_eq!(
                f.core
                    .charges
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|c| c.amount_usd.scaled())
                    .sum::<u128>(),
                if mode == "callback-model-fault" {
                    123
                } else {
                    0
                }
            );
        }
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn upstream_generic_http_effect_uses_original_core_replay_and_settlement_owner() {
    let (f, server) = fixture().await;
    let mut started = start(&f, "callback-external-fault", None).await;
    assert!(started.session.collect_uncommitted().await.is_err());
    started.session.detach_finalize().await;
    f.clean(1).await;
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, ExecutionOutcome::Failed);
        assert_eq!(
            rows[0].attempt_count, 1,
            "original policy allows two attempts"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert_eq!(
            f.core
                .charges
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.amount_usd.scaled())
                .sum::<u128>(),
            0
        );
    }
    f.manager.shutdown().await;
}
