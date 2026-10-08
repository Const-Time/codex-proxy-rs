use super::*;
use gateway_core::routing::RequestSettings;
use gateway_core::settings::SettingsValues;
use serde_json::{Value, json};

fn replace(settings: &RequestSettings, changes: Value, instance: &str) -> RequestSettings {
    let mut value = serde_json::to_value(settings.values()).unwrap();
    for (key, replacement) in changes.as_object().unwrap() {
        value[key] = replacement.clone();
    }
    settings
        .replace(serde_json::from_value(value).unwrap(), instance)
        .unwrap()
}
fn plan(snapshot: &RuntimeSnapshot, model: &str) -> gateway_core::routing::RoutingPlan {
    snapshot
        .plan(
            &PublicModelId::new(model).unwrap(),
            &operation(),
            account_scope(),
            &RoutingContext::default(),
        )
        .unwrap()
}

#[test]
fn runtime_overrides_change_routing_scheduling_and_pricing_only_for_the_request_branch() {
    let host = Arc::new(snapshot());
    let root = RequestSettings::new(host.clone());
    let changed = replace(
        &root,
        json!({
            "model_mappings":{"alias":"grok-4.5"},
            "max_concurrent_per_account":7, "request_interval_ms":123, "rotation_strategy":"round_robin",
            "pricing":{"xai":{"grok-4.5":{"multiplierBps":15000,"bands":{}}}},
            "min_codex_cli_version":"1.2.3"
        }),
        "plugin-a",
    );
    let derived = changed.snapshot();
    let routed = plan(&derived, "alias");
    assert_eq!(routed.candidates()[0].provider().as_str(), "xai");
    assert_eq!(
        routed.candidates()[0].upstream_model().unwrap().as_str(),
        "grok-4.5"
    );
    assert_eq!(
        routed
            .account_selection_policy()
            .max_concurrent_per_account()
            .get(),
        7
    );
    assert_eq!(
        routed.account_selection_policy().request_interval(),
        Duration::from_millis(123)
    );
    assert_eq!(
        routed.account_selection_policy().strategy(),
        RotationStrategy::RoundRobin
    );
    assert_eq!(routed.pricing()["xai"]["grok-4.5"].multiplier_bps, 15000);
    assert_eq!(derived.revision(), host.revision());
    assert_eq!(root.values(), host.settings());
    assert_eq!(
        plan(&host, "gpt-5.4").account_selection_policy(),
        scheduling()
    );
    assert!(plan(&host, "gpt-5.4").pricing().is_empty());
    assert!(
        host.plan(
            &PublicModelId::new("alias").unwrap(),
            &operation(),
            account_scope(),
            &RoutingContext::default()
        )
        .is_err()
    );
    assert_eq!(
        derived
            .min_codex_client_versions()
            .cli()
            .unwrap()
            .to_string(),
        "1.2.3"
    );
    assert!(host.min_codex_client_versions().cli().is_none());
}

#[test]
fn nested_overrides_keep_provenance_and_do_not_leak_to_siblings() {
    let root = RequestSettings::new(Arc::new(snapshot()));
    let first = replace(&root, json!({"request_interval_ms":100}), "first");
    let second = replace(&first, json!({"max_concurrent_per_account":9}), "second");
    let sources = second.inspect();
    assert_eq!(
        sources["overrides"]["request_interval_ms"],
        json!({"instance_id":"first","order":1,"value":100})
    );
    assert_eq!(
        sources["overrides"]["max_concurrent_per_account"],
        json!({"instance_id":"second","order":2,"value":9})
    );
    assert_eq!(first.inspect()["overrides"].as_object().unwrap().len(), 1);
    assert!(root.inspect()["overrides"].as_object().unwrap().is_empty());
    let unchanged = second.replace(second.values().clone(), "pass").unwrap();
    assert!(Arc::ptr_eq(&second.snapshot(), &unchanged.snapshot()));
    assert_eq!(unchanged.inspect(), sources);
    let restored = replace(&second, json!({"request_interval_ms":50}), "third");
    assert_eq!(
        restored.inspect()["overrides"]["request_interval_ms"]["order"],
        3
    );
    assert_eq!(second.inspect(), sources);
}

#[test]
fn rebase_inherits_only_explicit_fields_including_explicit_restoration() {
    let root = RequestSettings::new(Arc::new(snapshot()));
    let changed = replace(
        &root,
        json!({"request_interval_ms":100,"model_mappings":{"alias":"gpt-5.5"}}),
        "a",
    );
    let restored = replace(&changed, json!({"request_interval_ms":50}), "b");
    let fresh = root
        .snapshot()
        .resolve_settings(SettingsValues::new(
            11,
            300,
            "sticky",
            BTreeMap::from([("host-new".into(), "gpt-5.5".into())]),
            None,
            None,
        ))
        .unwrap();
    let rebased = restored.rebase(fresh.clone()).unwrap();
    let values = serde_json::to_value(rebased.values()).unwrap();
    assert_eq!(values["max_concurrent_per_account"], 11);
    assert_eq!(values["rotation_strategy"], "sticky");
    assert_eq!(values["request_interval_ms"], 50);
    assert_eq!(values["model_mappings"], json!({"alias":"gpt-5.5"}));
    assert_eq!(rebased.inspect()["host"]["request_interval_ms"], 300);
    assert_eq!(
        rebased.inspect()["overrides"],
        restored.inspect()["overrides"]
    );
    assert!(Arc::ptr_eq(
        &root.rebase(fresh.clone()).unwrap().snapshot(),
        &fresh
    ));
    assert!(Arc::ptr_eq(
        &rebased.rebase(fresh).unwrap().snapshot(),
        &rebased.snapshot()
    ));
}

#[test]
fn invalid_values_fail_before_changing_the_branch() {
    let root = RequestSettings::new(Arc::new(snapshot()));
    let before = root.inspect();
    for change in [
        json!({"max_concurrent_per_account":0}),
        json!({"rotation_strategy":"invalid"}),
        json!({"min_codex_cli_version":"not-semver"}),
        json!({"pricing":{"openai":{"gpt-5.5":{"multiplierBps":1000001,"bands":{}}}}}),
    ] {
        let mut value = serde_json::to_value(root.values()).unwrap();
        for (key, replacement) in change.as_object().unwrap() {
            value[key] = replacement.clone();
        }
        let values: SettingsValues = serde_json::from_value(value).unwrap();
        assert!(root.replace(values.clone(), "invalid").is_err());
        assert!(root.snapshot().resolve_settings(values).is_err());
        assert_eq!(root.inspect(), before);
    }
    let mut value = serde_json::to_value(root.values()).unwrap();
    value["unknown"] = json!(true);
    assert!(serde_json::from_value::<SettingsValues>(value).is_err());
}

#[test]
fn derived_snapshot_retains_original_policies_and_account_scope() {
    let policy = client_policy("key", "sk_test", true);
    let host = Arc::new(
        RuntimeSnapshot::new(
            ConfigRevision::new(4).unwrap(),
            scheduling(),
            vec![ProviderKind::new("openai").unwrap()],
            vec![model("openai", "gpt-5.5", capabilities())],
            vec![policy],
        )
        .unwrap()
        .with_account_directory(account_directory()),
    );
    let root = RequestSettings::new(host.clone());
    let changed = replace(&root, json!({"request_interval_ms":99}), "plugin");
    let effective = changed.snapshot();
    let original = host.client_policies().next().unwrap();
    let derived = effective.client_policies().next().unwrap();
    assert_eq!(original, derived);
    assert!(Arc::ptr_eq(
        original.account_scope(),
        derived.account_scope()
    ));
    assert_eq!(
        effective.provider_catalog_generations(),
        host.provider_catalog_generations()
    );
    assert_eq!(effective.client_policies().count(), 1);
}

#[test]
fn snapshot_constructor_rejects_interval_precision_loss_and_overflow() {
    for interval in [Duration::from_nanos(1), Duration::MAX] {
        let policy = AccountSelectionPolicy::new(
            RotationStrategy::Smart,
            NonZeroU32::new(1).unwrap(),
            interval,
        );
        let result = RuntimeSnapshot::new(
            ConfigRevision::new(1).unwrap(),
            policy,
            vec![],
            vec![],
            vec![],
        );
        assert!(matches!(
            result,
            Err(gateway_core::validation::RoutingError::InvalidSettings)
        ));
    }
}

#[test]
fn execution_settings_keep_scope_and_field_provenance_without_cross_key_leaks() {
    use gateway_core::account::FastMode;
    let policy = client_policy("a", "sk_a", true);
    let root = RequestSettings::new(Arc::new(snapshot())).with_execution(&policy, Some(600_000));
    let original = root.execution_values().unwrap();
    assert_eq!(original.fast_mode, FastMode::Default);
    let mut values = original.clone();
    values.fast_mode = FastMode::Enabled;
    values.timeout_ms = None;
    values.client_limits = RateLimits {
        max_concurrency: 2,
        requests_per_minute: 7,
    };
    values.runtime = values
        .runtime
        .with_model_mappings(BTreeMap::from([("alias".into(), "gpt-5.5".into())]));
    let changed = root.replace_execution(&values, "first").unwrap();
    assert_eq!(changed.execution_values().unwrap(), values);
    assert_eq!(root.execution_values().unwrap(), original);
    let sources = changed.inspect();
    assert_eq!(
        sources["execution"]["fast_mode"],
        json!({"instance_id":"first","order":1,"value":"enabled"})
    );
    assert_eq!(sources["overrides"]["model_mappings"]["order"], 1);
    assert_eq!(sources["execution"]["client_limits"]["order"], 1);
    assert_eq!(sources["execution"]["timeout_ms"]["order"], 1);
    assert!(sources["execution"]["timeout_ms"]["value"].is_null());
    let same = changed.replace_execution(&values, "unchanged").unwrap();
    assert_eq!(same.inspect(), sources);
    let mut next = values.clone();
    next.fast_mode = FastMode::Disabled;
    let nested = changed.replace_execution(&next, "second").unwrap();
    assert_eq!(nested.inspect()["execution"]["fast_mode"]["order"], 2);
    assert_eq!(nested.inspect()["execution"]["client_limits"]["order"], 1);
    assert_eq!(
        nested
            .clone()
            .with_execution(&policy, Some(600_000))
            .execution_values()
            .unwrap()
            .fast_mode,
        FastMode::Disabled
    );
    let other = client_policy("b", "sk_b", true);
    let child = nested
        .with_execution(&other, Some(600_000))
        .execution_values()
        .unwrap();
    assert_eq!(child.fast_mode, FastMode::Default);
    assert_eq!(child.timeout_ms, Some(600_000));
    assert_eq!(child.client_limits, other.limits());
    assert_eq!(child.runtime, values.runtime);
    let encoded = serde_json::to_value(&values).unwrap();
    assert_eq!(
        serde_json::from_value::<gateway_core::routing::ExecutionSettings>(encoded).unwrap(),
        values
    );
}

#[test]
fn execution_settings_reject_missing_scope_invalid_types_and_preserve_zero_timeout() {
    let policy = client_policy("a", "sk_a", true);
    let root = RequestSettings::new(Arc::new(snapshot()));
    let scoped = root.clone().with_execution(&policy, Some(600_000));
    let mut values = scoped.execution_values().unwrap();
    assert!(root.replace_execution(&values, "a").is_err());
    values.timeout_ms = Some(0);
    assert_eq!(
        scoped
            .replace_execution(&values, "a")
            .unwrap()
            .execution_values()
            .unwrap()
            .timeout_ms,
        Some(0)
    );
    let mut json = serde_json::to_value(scoped.execution_values().unwrap()).unwrap();
    json["fast_mode"] = json!(true);
    assert!(serde_json::from_value::<gateway_core::routing::ExecutionSettings>(json).is_err());
}
