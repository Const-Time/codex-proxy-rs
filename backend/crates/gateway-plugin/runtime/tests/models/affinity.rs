//! Real SDK callbacks query the existing Redis owner through native Core authority.
use super::*;
use gateway_admin::model::plugins::PluginInvocationContext;
use gateway_core::{
    engine::execution::PreparedRootExecution, provider_ports::ProviderSessionAffinityKey,
};
use serde_json::Value;

async fn query(
    fixture: &Fixture,
    parent: Option<&PreparedRootExecution>,
    params: Value,
    body: Option<Value>,
) -> Value {
    let mut request = crate::support::request("models");
    let mut input =
        json!({"operation":"models","mode":"raw","method":"host.affinity.lookup","params":params});
    if let Some(body) = body {
        input["body"] = body;
    }
    request.body = serde_json::to_vec(&input).unwrap();
    request.invocation = parent.map(|parent| PluginInvocationContext {
        context: gateway_core::engine::middleware::http::Context::from_execution(
            parent.nested_context(),
        ),
        http: None,
        services: None,
    });
    let response = fixture
        .manager
        .handle(&fixture.id, 1, request)
        .await
        .unwrap();
    serde_json::from_slice(&response.body).unwrap()
}

#[tokio::test]
async fn affinity_protocol_denies_unbound_parent_and_rejects_forged_scope_and_binary() {
    let fixture = Fixture::new().await;
    assert_eq!(
        query(
            &fixture,
            None,
            json!({"provider":"openai","key":"fixture-affinity"}),
            None
        )
        .await["error"]["code"],
        "permission_denied"
    );
    let parent = fixture
        .execution
        .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    for params in [
        json!({"provider":"openai","key":"bad/key"}),
        json!({"provider":"openai","key":"fixture-affinity","request_id":"another-parent"}),
        json!({"provider":"openai","key":"fixture-affinity","account_id":"acct_forbidden"}),
    ] {
        assert_eq!(
            query(&fixture, Some(&parent), params, None).await["error"]["code"],
            "invalid_input"
        );
    }
    assert_eq!(
        query(
            &fixture,
            Some(&parent),
            json!({"provider":"openai","key":"fixture-affinity"}),
            Some(json!({}))
        )
        .await["error"]["code"],
        "invalid_input"
    );
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn affinity_reads_existing_redis_binding_and_never_grants_foreign_account_or_execution() {
    let Some(env) = crate::services::upstream_sdk::environment::Environment::new().await else {
        return;
    };
    let affinity = env.store.provider_ports().session_affinity();
    let fixture = Fixture::with_affinity(affinity.clone()).await;
    let parent = fixture
        .execution
        .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    let provider = gateway_core::identity::ProviderKind::new("openai").unwrap();
    let key =
        ProviderSessionAffinityKey::try_new(format!("plugin-affinity-{}", uuid::Uuid::new_v4()))
            .unwrap();
    let params = json!({"provider":"openai","key":key.expose_to_store()});
    assert_eq!(
        query(&fixture, Some(&parent), params.clone(), None).await,
        json!({"result":{"account_id":null}})
    );
    let allowed = gateway_core::account::ProviderAccountId::new("acct_allowed").unwrap();
    affinity
        .bind(&provider, &key, &allowed, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        query(&fixture, Some(&parent), params.clone(), None).await,
        json!({"result":{"account_id":"acct_allowed"}})
    );
    assert_eq!(affinity.load(&provider, &key).await.unwrap(), Some(allowed));
    let foreign = gateway_core::account::ProviderAccountId::new("acct_foreign").unwrap();
    affinity
        .bind(&provider, &key, &foreign, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        query(&fixture, Some(&parent), params.clone(), None).await,
        json!({"result":{"account_id":null}})
    );
    assert_eq!(affinity.load(&provider, &key).await.unwrap(), Some(foreign));
    assert_eq!(
        query(
            &fixture,
            Some(&parent),
            json!({"provider":"xai","key":key.expose_to_store()}),
            None
        )
        .await["error"]["code"],
        "permission_denied"
    );
    parent.cancellation().cancel();
    assert_eq!(
        query(&fixture, Some(&parent), params, None).await["error"]["code"],
        "permission_denied"
    );
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    assert!(fixture.core.requests.lock().unwrap().is_empty());
    assert!(fixture.core.charges.lock().unwrap().is_empty());
    drop(parent);
    fixture.manager.shutdown().await;
    drop(fixture);
    env.close().await;
}
