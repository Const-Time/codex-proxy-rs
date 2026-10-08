use super::*;
use gateway_core::{
    account::{AccountSelectionPolicy, FastMode, RotationStrategy},
    policy::{ClientApiKeyId, ClientPolicy, PlaintextClientApiKey, RateLimits},
    routing::{
        ClientRoutingScope, ConfigRevision, FrozenAccountScope, RequestSettings,
        RuntimeAccountDirectory, RuntimeSnapshot,
    },
};

fn settings() -> RequestSettings {
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            3.try_into().unwrap(),
            Duration::from_millis(50),
        ),
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let policy = ClientPolicy::new(
        ClientApiKeyId::new("key_original").unwrap(),
        PlaintextClientApiKey::new("sk_test").unwrap(),
        Arc::new(FrozenAccountScope::new(
            Arc::new(RuntimeAccountDirectory::new(Default::default())),
            ClientRoutingScope::all_accounts(),
        )),
        true,
        RateLimits::unlimited(),
    );
    RequestSettings::new(Arc::new(snapshot)).with_execution(&policy, None)
}

#[tokio::test]
async fn sdk_settings_chain_keeps_host_provenance_and_isolates_parent() {
    let f = Fixture::new().await;
    let root = settings();
    let trace = Arc::new(Trace::default());
    let callbacks = f.callbacks.clone();
    let session = f.session.clone();
    let inner_trace = trace.clone();
    let next = gateway_core::middleware::compose(vec![], move |input: MiddlewareRequest| {
        Box::pin(async move {
            let inherited = input.settings().unwrap().clone();
            callbacks
                .invoke(
                    session,
                    context(false),
                    request("settings-second").with_settings(inherited),
                    terminal(inner_trace, false),
                )
                .await
        })
    });
    let response = f
        .callbacks
        .invoke(
            f.session.clone(),
            context(false),
            request("settings-first").with_settings(root.clone()),
            next,
        )
        .await
        .unwrap();
    drop(response);
    let actual = trace
        .request
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .settings()
        .unwrap()
        .clone();
    let values = actual.execution_values().unwrap();
    assert_eq!(values.fast_mode, FastMode::Disabled);
    assert_eq!(values.timeout_ms, Some(0));
    assert_eq!(
        serde_json::to_value(&values.runtime).unwrap()["request_interval_ms"],
        0
    );
    assert_eq!(actual.inspect()["execution"]["fast_mode"]["order"], 2);
    assert_eq!(
        actual.inspect()["overrides"]["request_interval_ms"]["instance_id"],
        "middleware-fixture"
    );
    assert!(actual.inspect()["overrides"].get("forged").is_none());
    assert_eq!(
        root.execution_values().unwrap().fast_mode,
        FastMode::Default
    );
    assert_eq!(
        serde_json::to_value(root.values()).unwrap()["request_interval_ms"],
        50
    );
    f.stop().await;
}

#[tokio::test]
async fn sdk_attempt_reads_effective_sources_but_cannot_rewrite_settings() {
    let f = Fixture::new().await;
    let root = settings();
    let mut values = root.execution_values().unwrap();
    values.fast_mode = FastMode::Enabled;
    let changed = root.replace_execution(&values, "request-plugin").unwrap();
    let mut attempt = context(false);
    attempt.mount = MiddlewareMount::Attempt;
    attempt.attempt_index = Some(1);
    let trace = Arc::new(Trace::default());
    drop(
        f.callbacks
            .invoke(
                f.session.clone(),
                attempt.clone(),
                request("settings-attempt").with_settings(changed.clone()),
                terminal(trace.clone(), false),
            )
            .await
            .unwrap(),
    );
    assert_eq!(trace.next.load(Ordering::SeqCst), 1);
    let rejected = Arc::new(Trace::default());
    assert!(
        f.callbacks
            .invoke(
                f.session.clone(),
                attempt,
                request("settings").with_settings(changed),
                terminal(rejected.clone(), false)
            )
            .await
            .is_err()
    );
    assert_eq!(rejected.next.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn sdk_settings_replacement_rejects_unknown_fields_without_calling_next() {
    let f = Fixture::new().await;
    let trace = Arc::new(Trace::default());
    assert!(
        f.callbacks
            .invoke(
                f.session.clone(),
                context(false),
                request("settings-unknown").with_settings(settings()),
                terminal(trace.clone(), false)
            )
            .await
            .is_err()
    );
    assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    f.stop().await;
}
