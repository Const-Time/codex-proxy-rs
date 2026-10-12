use crate::{
    models::fixture::{Fixture, snapshot},
    support::{actor, retry_archive, update},
};
use gateway_admin::{
    model::plugins::*,
    ports::plugins::{PluginService, PluginStore},
};
use serde_json::json;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

async fn install(
    f: &Fixture,
    mode: &str,
    order: i32,
    scope: PluginExecutionScope,
) -> (String, String) {
    let artifact = f
        .manager
        .upload(retry_archive("1.0.0"), &actor())
        .await
        .unwrap();
    assert!(artifact.unsupported_capabilities.is_empty());
    let mut command = update(&artifact.sha256, 0, true);
    command.configuration = json!({"retry":mode});
    command.retry_policy = Some(PluginRetryPolicyBinding { order, scope });
    let view = f.manager.update(None, command, &actor()).await.unwrap();
    assert!(view.retry_policy.is_some());
    (view.id, artifact.sha256)
}
async fn state(f: &Fixture, id: &str, key: &str) -> u64 {
    let revision = f.store.records.lock().unwrap()[id].revision;
    f.store
        .state_get(
            &PluginStateScope {
                instance_id: id.into(),
                revision,
                namespace: "counter".into(),
                schema_version: 1,
                maximum_records: 10,
                maximum_bytes: 1024,
            },
            key,
        )
        .await
        .unwrap()
        .map_or(0, |r| r.value.as_u64().unwrap())
}
async fn wait_entered(
    f: &Fixture,
    id: &str,
    call: &mut std::pin::Pin<Box<impl std::future::Future<Output = serde_json::Value>>>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result=call=>panic!("retry must still be waiting: {result}"),
            _=async {while state(f,id,"retry_calls").await==0 {tokio::task::yield_now().await;}}=>{}
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn sdk_retry_stop_keeps_original_failure_while_delegate_and_retry_use_one_ledger() {
    for (mode, success, attempts, cost) in [
        ("stop", false, 1, 123),
        ("delegate", true, 2, 246),
        ("retry", true, 2, 246),
    ] {
        let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
        let (id, _) = install(&f, mode, 0, PluginExecutionScope::All {}).await;
        let result = f
            .call(json!({"key":"key_alice","failure":"retry-once"}))
            .await;
        assert_eq!(result.get("error").is_none(), success, "{mode}: {result}");
        if !success {
            assert_eq!(result["error"]["code"], "capacity");
        }
        f.clean(1).await;
        assert_eq!(f.core.starts.load(Ordering::SeqCst), attempts);
        assert_eq!(state(&f, &id, "retry_calls").await, 1);
        assert_eq!(state(&f, &id, "retry_allowed").await, 1);
        assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), cost);
        assert_eq!(f.core.finalizations.lock().unwrap().len(), 1);
        assert!(f.core.leases.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn sdk_retry_scope_order_and_faults_delegate_to_later_matching_policy() {
    for mode in ["delegate", "fault", "payload", "malformed", "unknown"] {
        let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
        let (skip, _) = install(
            &f,
            "retry",
            -2,
            PluginExecutionScope::Keys {
                ids: vec!["key_other".into()],
            },
        )
        .await;
        let (first, _) = install(&f, mode, -1, PluginExecutionScope::All {}).await;
        let (stop, _) = install(&f, "stop", 0, PluginExecutionScope::All {}).await;
        let (last, _) = install(&f, "retry", 1, PluginExecutionScope::All {}).await;
        let result = f
            .call(json!({"key":"key_alice","failure":"retry-once"}))
            .await;
        assert!(result.get("error").is_some(), "{mode}: {result}");
        assert!(!result.to_string().contains("private retry error detail"));
        f.clean(1).await;
        assert_eq!(state(&f, &skip, "retry_calls").await, 0);
        assert_eq!(state(&f, &first, "retry_calls").await, 1);
        assert_eq!(state(&f, &stop, "retry_calls").await, 1);
        assert_eq!(state(&f, &last, "retry_calls").await, 0);
        assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
        assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 123);
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn sdk_retry_cannot_expand_unsafe_send_or_hard_account_recovery() {
    for input in [
        json!({"key":"key_alice","failure":"fail-sent"}),
        json!({"key":"key_alice","failure":"fail-ambiguous"}),
        json!({"key":"key_alice","failure":"retry-once","account":"acct_allowed"}),
    ] {
        let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
        let (id, _) = install(&f, "retry", 0, PluginExecutionScope::All {}).await;
        let result = f.call(input).await;
        assert!(result.get("error").is_some(), "{result}");
        f.clean(1).await;
        assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
        assert_eq!(state(&f, &id, "retry_calls").await, 1);
        assert_eq!(state(&f, &id, "retry_allowed").await, 0);
        assert!(f.core.leases.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn sdk_retry_chain_shares_its_two_second_rpc_budget() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (first, _) = install(&f, "slow-delegate", 0, PluginExecutionScope::All {}).await;
    let (second, _) = install(&f, "stop", 1, PluginExecutionScope::All {}).await;
    let result = f
        .call(json!({"key":"key_alice","failure":"retry-once"}))
        .await;
    assert!(result.get("error").is_some(), "{result}");
    f.clean(1).await;
    let first_budget = state(&f, &first, "retry_budget").await;
    let second_budget = state(&f, &second, "retry_budget").await;
    assert!(
        first_budget <= 2000 && first_budget >= second_budget + 700,
        "{first_budget} {second_budget}"
    );
    assert!(
        second_budget < 1300,
        "remaining SDK timeout must not reset: {second_budget}"
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_retry_timeout_delegates_on_unbounded_request_without_losing_failed_attempt_cost() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    install(&f, "wait", 0, PluginExecutionScope::All {}).await;
    let began = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(6),
        f.call(json!({"key":"key_alice","failure":"retry-once"})),
    )
    .await
    .unwrap();
    assert!(result.get("error").is_none(), "{result}");
    assert!(began.elapsed() < Duration::from_secs(5));
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 246);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_retry_wait_is_revoked_by_key_or_plugin_disable_and_settles_once() {
    for disable_plugin in [false, true] {
        let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
        let (id, sha) = install(&f, "wait", 0, PluginExecutionScope::All {}).await;
        let mut call = Box::pin(f.call(json!({"key":"key_alice","failure":"retry-once"})));
        wait_entered(&f, &id, &mut call).await;
        if disable_plugin {
            f.manager
                .update(Some(&id), update(&sha, 1, false), &actor())
                .await
                .unwrap();
        } else {
            f.snapshot.publish(snapshot(2, false));
        }
        let result = tokio::time::timeout(Duration::from_secs(3), call)
            .await
            .unwrap();
        assert!(result.get("error").is_some(), "{result}");
        f.clean(1).await;
        assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
        assert_eq!(f.core.finalizations.lock().unwrap().len(), 1);
        assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 123);
        assert!(f.core.leases.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn preceding_sdk_retry_delegate_crash_cancels_later_pending_policy() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (first, _) = install(&f, "delegate", 0, PluginExecutionScope::All {}).await;
    let (later, _) = install(&f, "wait", 1, PluginExecutionScope::All {}).await;
    let mut call = Box::pin(f.call(json!({"key":"key_alice","failure":"retry-once"})));
    wait_entered(&f, &later, &mut call).await;
    assert_eq!(state(&f, &first, "retry_calls").await, 1);
    assert!(
        f.manager
            .handle(&first, 1, crate::support::request("crash"))
            .await
            .is_err()
    );
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 123);
    assert!(f.core.leases.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn sdk_retry_process_crash_after_reply_revokes_recovered_running_provider() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (id, _) = install(&f, "delegate", 0, PluginExecutionScope::All {}).await;
    let mut call = Box::pin(f.call(json!({"key":"key_alice","failure":"retry-stall"})));
    tokio::time::timeout(Duration::from_secs(5),async {
        tokio::select! {
            result=&mut call=>panic!("recovered Provider must wait: {result}"),
            _=async {while f.core.polled.load(Ordering::SeqCst)<5 {tokio::task::yield_now().await;}}=>{}
        }
    }).await.unwrap();
    assert!(
        f.manager
            .handle(&id, 1, crate::support::request("crash"))
            .await
            .is_err()
    );
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 246);
    assert!(f.core.leases.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}

#[tokio::test]
async fn failed_retry_upgrade_preserves_old_generation_and_stop_binding() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (id, _) = install(&f, "stop", 0, PluginExecutionScope::All {}).await;
    let access = f.manager.access(&id, 1).unwrap();
    let artifact = f
        .manager
        .upload(retry_archive("2.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 1, true);
    command.configuration = json!({"retry":"retry","fail":true});
    command.retry_policy = Some(PluginRetryPolicyBinding {
        order: 0,
        scope: PluginExecutionScope::All {},
    });
    assert!(
        f.manager
            .update(Some(&id), command, &actor())
            .await
            .is_err()
    );
    assert!(!access.is_revoked());
    let result = f
        .call(json!({"key":"key_alice","failure":"retry-once"}))
        .await;
    assert!(result.get("error").is_some(), "{result}");
    f.clean(1).await;
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 1);
    assert_eq!(state(&f, &id, "retry_calls").await, 1);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn retry_binding_and_private_state_restore_after_host_restart() {
    use gateway_core::engine::execution::ExecutionService;
    use std::sync::Arc;
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let (id, _) = install(&f, "stop", 0, PluginExecutionScope::All {}).await;
    assert!(
        f.call(json!({"key":"key_alice","failure":"retry-once"}))
            .await
            .get("error")
            .is_some()
    );
    f.clean(1).await;
    f.manager.shutdown().await;
    let cache = tempfile::tempdir().unwrap();
    let restored = gateway_admin::plugins::PluginManager::new(
        f.store.clone(),
        Arc::new(gateway_plugin_runtime::PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            f.store.clone(),
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
    assert_eq!(view.retry_policy.unwrap().order, 0);
    let prepared = f
        .execution
        .prepare_execution(f.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap()
        .with_retry_policy(Arc::new(restored.middleware_registry().unwrap()));
    let mut started = f.start_with_failure(prepared, Some("retry-once")).await;
    assert!(started.session.collect_uncommitted().await.is_err());
    f.clean(2).await;
    assert_eq!(state(&f, &id, "retry_calls").await, 2);
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 2);
    restored.shutdown().await;
}

#[tokio::test]
async fn sdk_retry_excludes_initiating_plugin_without_changing_native_child_recovery() {
    let f = Fixture::with_accounts(&["acct_allowed", "acct_alternative"]).await;
    let artifact = f
        .manager
        .upload(retry_archive("1.0.0"), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 1, true);
    command.configuration = json!({"retry":"stop"});
    command.retry_policy = Some(PluginRetryPolicyBinding {
        order: 0,
        scope: PluginExecutionScope::All {},
    });
    f.manager
        .update(Some(&f.id), command, &actor())
        .await
        .unwrap();
    let mut request = crate::support::request("models");
    request.actor.actor = gateway_admin::model::MutationActor::AdminSession {
        admin_user_id: "alice".into(),
    };
    request.body =
        serde_json::to_vec(&json!({"operation":"models","key":"key_alice","failure":"retry-once"}))
            .unwrap();
    let response = f.manager.handle(&f.id, 2, request).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert!(result.get("error").is_none(), "{result}");
    f.clean(1).await;
    assert_eq!(state(&f, &f.id, "retry_calls").await, 0);
    assert_eq!(f.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 246);
    f.manager.shutdown().await;
}
