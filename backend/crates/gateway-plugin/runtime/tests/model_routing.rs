use crate::{
    models::fixture::{Fixture, snapshot},
    support::{actor, routing_archive, update},
};
use gateway_admin::{model::plugins::*, ports::plugins::PluginService};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

async fn install(
    fixture: &Fixture,
    scope: PluginExecutionScope,
    failure_policy: PluginPolicyFailurePolicy,
) -> (String, String) {
    let artifact = fixture
        .manager
        .upload(routing_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 0, true);
    command.model_router = Some(PluginModelRouterBinding {
        order: 0,
        scope,
        failure_policy,
    });
    let router = fixture
        .manager
        .update(None, command, &actor())
        .await
        .unwrap();
    (router.id, artifact.sha256)
}

#[tokio::test]
async fn sdk_routing_selects_an_authorized_provider_and_public_model_before_admission() {
    let fixture = Fixture::with_accounts(&["acct_allowed", "acct_fallback"]).await;
    install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture.call(json!({"key":"key_alice","route":"xai"})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(1).await;
    assert_eq!(
        fixture.core.attempts.lock().unwrap()[0]
            .provider_kind
            .as_str(),
        "xai"
    );
    assert_eq!(
        fixture.core.selected.lock().unwrap()[0].as_str(),
        "acct_fallback"
    );
    let result = fixture.call(json!({"key":"key_alice","model":"gpt-alias","route":{"decision":"route","model":"gpt-native","provider":"openai"}})).await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(2).await;
    {
        let attempts = fixture.core.attempts.lock().unwrap();
        assert_eq!(attempts[1].provider_kind.as_str(), "openai");
        assert_eq!(
            attempts[1].upstream_model_id.as_ref().unwrap().as_str(),
            "gpt-native"
        );
        assert_eq!(
            fixture.core.requests.lock().unwrap()[1]
                .requested_model
                .as_ref()
                .unwrap()
                .as_str(),
            "gpt-alias"
        );
        assert!(
            fixture
                .core
                .charges
                .lock()
                .unwrap()
                .iter()
                .all(|c| c.amount_usd.scaled() == 123)
        );
    }
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_routing_rejects_cross_scope_targets_and_invalid_decisions_without_charge() {
    let fixture = Fixture::new().await;
    install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    for (route, provider, expected) in [
        ("reject", None, "permission_denied"),
        ("xai", None, "permission_denied"),
        ("xai", Some("openai"), "permission_denied"),
        ("empty", None, "upstream"),
        ("fault", None, "upstream"),
        ("malformed", None, "upstream"),
        ("malformed-unhandled", None, "upstream"),
        ("malformed-reject", None, "upstream"),
        ("payload", None, "upstream"),
    ] {
        let result = fixture
            .call(json!({"key":"key_alice","route":route,"provider":provider}))
            .await;
        assert_eq!(result["error"]["code"], expected, "{route}: {result}");
        assert!(!result.to_string().contains("private plugin error detail"));
    }
    fixture.clean(0).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    assert!(fixture.core.requests.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_routing_child_inherits_core_authority_skips_ancestors_and_keeps_independent_fees() {
    let fixture = Fixture::new().await;
    let (id, _) = install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture
        .call(json!({"key":"key_alice","route":"child"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(2).await;
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
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_routing_scope_and_explicit_fault_delegation_do_not_delegate_a_rejection() {
    let fixture = Fixture::new().await;
    let (id, sha) = install(
        &fixture,
        PluginExecutionScope::Keys {
            ids: vec!["key_other".into()],
        },
        PluginPolicyFailurePolicy::Delegate,
    )
    .await;
    let result = fixture
        .call(json!({"key":"key_alice","route":"reject"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    let mut command = update(&sha, 1, true);
    command.model_router = Some(PluginModelRouterBinding {
        order: 0,
        scope: PluginExecutionScope::All {},
        failure_policy: PluginPolicyFailurePolicy::Delegate,
    });
    fixture
        .manager
        .update(Some(&id), command, &actor())
        .await
        .unwrap();
    let result = fixture
        .call(json!({"key":"key_alice","route":"fault"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    let result = fixture
        .call(json!({"key":"key_alice","route":"reject"}))
        .await;
    assert_eq!(result["error"]["code"], "permission_denied", "{result}");
    fixture.clean(2).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn key_revocation_interrupts_sdk_routing_before_core_admission() {
    let fixture = Fixture::new().await;
    install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice","route":"wait"})));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut call => panic!("routing must still be waiting: {result}"),
            _ = async { while fixture.core.catalogs.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
        }
    }).await.unwrap();
    fixture.snapshot.publish(snapshot(2, false));
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(0).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn sent_routing_child_blocks_parent_provider_fallback_without_duplicate_charge() {
    let fixture = Fixture::with_accounts(&["acct_allowed", "acct_fallback"]).await;
    install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let result = fixture
        .call(json!({"key":"key_alice","route":"child-unhandled","failure":"fail-no-account"}))
        .await;
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(2).await;
    assert_eq!(
        fixture.core.starts.load(Ordering::SeqCst),
        2,
        "a sent child must prevent replaying the parent on xai"
    );
    assert!(
        fixture
            .core
            .selected
            .lock()
            .unwrap()
            .iter()
            .all(|account| account.as_str() == "acct_allowed")
    );
    assert_eq!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.amount_usd.scaled())
            .sum::<u128>(),
        123
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn disabling_a_consulted_router_revokes_execution_after_its_rpc_has_returned() {
    let fixture = Fixture::new().await;
    let (id, sha) = install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.selection_gate.lock().unwrap() = Some(receiver);
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice","route":"openai"})));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut call => panic!("Provider must be waiting after the routing RPC: {result}"),
            _ = async { while fixture.core.selection_waiters.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
        }
    }).await.unwrap();
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
    assert!(fixture.core.leases.lock().unwrap().is_empty());
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn a_failed_router_upgrade_keeps_the_ready_generation_and_binding() {
    let fixture = Fixture::new().await;
    let (id, _) = install(
        &fixture,
        PluginExecutionScope::All {},
        PluginPolicyFailurePolicy::Reject,
    )
    .await;
    let old_access = fixture.manager.access(&id, 1).unwrap();
    let candidate = fixture
        .manager
        .upload(routing_archive("2.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&candidate.sha256, 1, true);
    command.configuration = json!({"fail":true});
    command.model_router = Some(PluginModelRouterBinding {
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
    assert!(!old_access.is_revoked());
    let result = fixture
        .call(json!({"key":"key_alice","route":"reject"}))
        .await;
    assert_eq!(result["error"]["code"], "permission_denied", "{result}");
    fixture.clean(0).await;
    fixture.manager.shutdown().await;
}
