use crate::{
    models::fixture::{Fixture, snapshot},
    support::{actor, scheduling_archive, update},
};
use gateway_admin::{
    model::plugins::*,
    ports::plugins::{PluginService, PluginStore},
};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

async fn install(
    fixture: &Fixture,
    mode: &str,
    order: i32,
    scope: PluginExecutionScope,
    failure_policy: PluginPolicyFailurePolicy,
) -> (String, String) {
    let artifact = fixture
        .manager
        .upload(scheduling_archive("1.0.0"), &actor())
        .await
        .unwrap();
    assert!(artifact.unsupported_capabilities.is_empty());
    let mut command = update(&artifact.sha256, 0, true);
    command.configuration = json!({"schedule":mode});
    command.account_scheduler = Some(PluginAccountSchedulerBinding {
        order,
        scope,
        failure_policy,
    });
    let plugin = fixture
        .manager
        .update(None, command, &actor())
        .await
        .unwrap();
    assert!(plugin.account_scheduler.is_some());
    (plugin.id, artifact.sha256)
}
async fn calls(fixture: &Fixture, id: &str) -> u64 {
    fixture
        .store
        .state_get(
            &PluginStateScope {
                instance_id: id.into(),
                revision: 1,
                namespace: "counter".into(),
                schema_version: 1,
                maximum_records: 10,
                maximum_bytes: 1024,
            },
            "schedules",
        )
        .await
        .unwrap()
        .map_or(0, |record| record.value.as_u64().unwrap())
}

#[tokio::test]
async fn sdk_scheduler_picks_only_qualified_accounts_and_preserves_hard_pins_and_fees() {
    let fixture =
        Fixture::with_accounts(&["acct_allowed", "acct_alternative", "acct_fallback"]).await;
    let (id, _) = install(
        &fixture,
        "pick-last",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture.call(json!({"key":"key_alice"})).await;
    assert!(result.get("error").is_none(), "{result}");
    assert_eq!(
        fixture.core.selected.lock().unwrap()[0].as_str(),
        "acct_alternative"
    );
    let result = fixture
        .call(json!({"key":"key_alice","account":"acct_allowed"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    let result = fixture
        .call(json!({"key":"key_alice","provider":"xai"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(3).await;
    assert_eq!(
        fixture.core.selected.lock().unwrap()[1].as_str(),
        "acct_allowed"
    );
    assert_eq!(
        fixture.core.selected.lock().unwrap()[2].as_str(),
        "acct_fallback"
    );
    assert_eq!(calls(&fixture, &id).await, 3);
    assert!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .all(|c| c.amount_usd.scaled() == 123)
    );
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_scheduler_scope_and_order_consult_only_the_first_matching_policy() {
    let fixture = Fixture::new().await;
    let (skip, _) = install(
        &fixture,
        "reject",
        -2,
        PluginExecutionScope::Keys {
            ids: vec!["key_other".into()],
        },
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let (first, _) = install(
        &fixture,
        "delegate",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let (last, _) = install(
        &fixture,
        "reject",
        1,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture.call(json!({"key":"key_alice"})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(1).await;
    assert_eq!(calls(&fixture, &skip).await, 0);
    assert_eq!(calls(&fixture, &first).await, 1);
    assert_eq!(calls(&fixture, &last).await, 0);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_scheduler_rejection_and_invalid_decisions_cannot_fall_back_to_another_provider() {
    for (mode, code) in [
        ("reject", "permission_denied"),
        ("foreign", "upstream"),
        ("fault", "upstream"),
        ("malformed", "upstream"),
        ("payload", "upstream"),
    ] {
        let fixture = Fixture::with_accounts(&["acct_allowed", "acct_fallback"]).await;
        install(
            &fixture,
            mode,
            0,
            PluginExecutionScope::All {},
            PluginPolicyFailurePolicy::Reject,
        )
        .await;
        let result = fixture.call(json!({"key":"key_alice"})).await;
        assert_eq!(result["error"]["code"], code, "{mode}: {result}");
        assert!(
            !result
                .to_string()
                .contains("private scheduling error detail")
        );
        fixture.clean(1).await;
        assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 1, "{mode}");
        assert!(fixture.core.selected.lock().unwrap().is_empty());
        assert_eq!(
            fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
            0
        );
        fixture.manager.shutdown().await;
    }
}

#[tokio::test]
async fn sdk_scheduler_fault_delegation_uses_builtin_selection_but_never_delegates_reject() {
    for (mode, success) in [("fault", true), ("foreign", true), ("reject", false)] {
        let fixture = Fixture::new().await;
        install(
            &fixture,
            mode,
            0,
            PluginExecutionScope::All {},
            PluginPolicyFailurePolicy::Delegate,
        )
        .await;
        let (later, _) = install(
            &fixture,
            "reject",
            1,
            PluginExecutionScope::All {},
            PluginPolicyFailurePolicy::Reject,
        )
        .await;
        let result = fixture.call(json!({"key":"key_alice"})).await;
        assert_eq!(result.get("error").is_none(), success, "{mode}: {result}");
        fixture.clean(1).await;
        assert_eq!(calls(&fixture, &later).await, 0);
        assert_eq!(
            fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
            if success { 123 } else { 0 }
        );
        fixture.manager.shutdown().await;
    }
}

#[tokio::test]
async fn key_revocation_interrupts_sdk_scheduling_and_releases_original_admission() {
    let fixture = Fixture::new().await;
    install(
        &fixture,
        "wait",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice"})));
    tokio::time::timeout(Duration::from_secs(5),async {
        tokio::select! {
            result=&mut call=>panic!("scheduling must be waiting: {result}"),
            _=async {while fixture.core.catalogs.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}
        }
    }).await.unwrap();
    fixture.snapshot.publish(snapshot(2, false));
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(1).await;
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn scheduling_child_uses_original_authority_skips_its_ancestor_and_settles_separately() {
    let fixture = Fixture::new().await;
    let (id, _) = install(
        &fixture,
        "child",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture.call(json!({"key":"key_alice"})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(2).await;
    assert_eq!(calls(&fixture, &id).await, 1);
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    {
        let rows = fixture.core.requests.lock().unwrap();
        let parent = rows.iter().find(|r| r.parent_request_id.is_none()).unwrap();
        let child = rows.iter().find(|r| r.parent_request_id.is_some()).unwrap();
        assert_eq!(child.parent_request_id.as_ref(), Some(&parent.id));
        assert_eq!(
            child.initiating_plugin_instance_id.as_deref(),
            Some(id.as_str())
        );
        assert_eq!(child.client_api_key_ref.as_str(), "key_alice");
    }
    assert_eq!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.amount_usd.scaled())
            .sum::<u128>(),
        246
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn disabling_scheduler_after_selection_cancels_waiting_provider_without_charge() {
    let fixture = Fixture::new().await;
    let (id, sha) = install(
        &fixture,
        "delegate",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.selection_gate.lock().unwrap() = Some(receiver);
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice"})));
    tokio::time::timeout(Duration::from_secs(5),async {
        tokio::select! {
            result=&mut call=>panic!("Provider must wait after scheduling: {result}"),
            _=async {while fixture.core.selection_waiters.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}
        }
    }).await.unwrap();
    assert_eq!(calls(&fixture, &id).await, 1);
    fixture
        .manager
        .update(Some(&id), update(&sha, 1, false), &actor())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(1).await;
    assert!(sender.send(()).is_err());
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn scheduler_process_exit_after_rpc_revokes_original_running_execution_and_keeps_cost() {
    let fixture = Fixture::new().await;
    let (id, _) = install(
        &fixture,
        "delegate",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice","provider_mode":"stall"})));
    tokio::time::timeout(Duration::from_secs(5),async {
        tokio::select! {
            result=&mut call=>panic!("Provider must remain active: {result}"),
            _=async {while fixture.core.polled.load(Ordering::SeqCst)<2 {tokio::task::yield_now().await;}}=>{}
        }
    }).await.unwrap();
    assert_eq!(calls(&fixture, &id).await, 1);
    assert!(
        fixture
            .manager
            .handle(&id, 1, crate::support::request("crash"))
            .await
            .is_err()
    );
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(1).await;
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        123
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn failed_scheduler_upgrade_preserves_ready_generation_and_choice() {
    let fixture = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (id, _) = install(
        &fixture,
        "pick-last",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let access = fixture.manager.access(&id, 1).unwrap();
    let artifact = fixture
        .manager
        .upload(scheduling_archive("2.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 1, true);
    command.configuration = json!({"schedule":"reject","fail":true});
    command.account_scheduler = Some(PluginAccountSchedulerBinding {
        order: 0,
        scope: PluginExecutionScope::All {},
        failure_policy: PluginPolicyFailurePolicy::Reject,
    });
    assert!(
        fixture
            .manager
            .update(Some(&id), command, &actor())
            .await
            .is_err()
    );
    assert!(!access.is_revoked());
    let result = fixture.call(json!({"key":"key_alice"})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(1).await;
    assert_eq!(
        fixture.core.selected.lock().unwrap()[0].as_str(),
        "acct_alternative"
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn scheduler_binding_restores_after_shutdown_with_same_choice_and_private_state() {
    use gateway_core::engine::execution::ExecutionService;
    use std::sync::Arc;
    let fixture = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (id, _) = install(
        &fixture,
        "pick-last",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture.call(json!({"key":"key_alice"})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(1).await;
    fixture.manager.shutdown().await;
    let cache = tempfile::tempdir().unwrap();
    let restored = gateway_admin::plugins::PluginManager::new(
        fixture.store.clone(),
        Arc::new(gateway_plugin_runtime::PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            fixture.store.clone(),
        )),
    );
    restored.restore().await.unwrap();
    let view = restored
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.id == id)
        .unwrap();
    assert_eq!(view.status, "ready");
    assert_eq!(view.account_scheduler.unwrap().order, 0);
    let prepared = fixture
        .execution
        .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap()
        .with_account_scheduler(Arc::new(restored.middleware_registry().unwrap()));
    let mut started = fixture.start(prepared).await;
    started.session.collect_uncommitted().await.unwrap();
    started.session.commit_downstream(Some(200)).await.unwrap();
    fixture.clean(2).await;
    assert_eq!(calls(&fixture, &id).await, 2);
    assert!(
        fixture
            .core
            .selected
            .lock()
            .unwrap()
            .iter()
            .all(|id| id.as_str() == "acct_alternative")
    );
    restored.shutdown().await;
}

#[tokio::test]
async fn scheduling_rpc_has_a_local_timeout_on_an_unbounded_core_request() {
    let fixture = Fixture::new().await;
    install(
        &fixture,
        "wait",
        0,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = tokio::time::timeout(
        Duration::from_secs(6),
        fixture.call(json!({"key":"key_alice"})),
    )
    .await
    .unwrap();
    assert_eq!(result["error"]["code"], "upstream", "{result}");
    fixture.clean(1).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 1);
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    fixture.manager.shutdown().await;
}
