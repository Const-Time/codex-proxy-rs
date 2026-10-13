use super::*;
use gateway_core::routing::snapshot::RuntimeSnapshotCompileError;
use gateway_core::routing::{ContributedModelAlias, ModelPresentation};
fn alias(id: &str, provider: &str, target: &str) -> ContributedModelAlias {
    ContributedModelAlias {
        owner: "catalog-plugin".into(),
        id: PublicModelId::new(id).unwrap(),
        provider: ProviderKind::new(provider).unwrap(),
        target: UpstreamModelId::new(target).unwrap(),
    }
}
#[test]
fn contributed_alias_compilation_rejects_conflicts_without_changing_the_previous_snapshot() {
    let base = snapshot();
    for aliases in [
        vec![
            alias("same", "openai", "gpt-5.5"),
            alias("same", "xai", "grok-4.5"),
        ],
        vec![alias("gpt-5.5", "xai", "grok-4.5")],
        vec![alias("gpt-5.4", "openai", "gpt-5.5")],
        vec![alias("new", "openai", "gpt-5.4")],
        vec![alias("new", "openai", "absent")],
        vec![alias("new", "absent", "gpt-5.5")],
        vec![
            alias("new", "openai", "other"),
            alias("other", "openai", "gpt-5.5"),
        ],
        vec![alias("loop", "openai", "loop")],
    ] {
        assert_eq!(
            base.clone().with_model_aliases(aliases).unwrap_err(),
            RuntimeSnapshotCompileError::InvalidExtensionModels
        );
    }
    let mut nameless = alias("new", "openai", "gpt-5.5");
    nameless.owner.clear();
    assert!(base.clone().with_model_aliases(vec![nameless]).is_err());
    assert_eq!(base.mapped_model("new"), "new");
    let mapped = base.with_model_mappings(BTreeMap::from([("configured".into(), "new".into())]));
    assert!(
        mapped
            .with_model_aliases(vec![alias("new", "openai", "gpt-5.5")])
            .is_err()
    );
}
#[test]
fn contributed_alias_is_bound_to_its_provider_even_when_both_offer_the_target_model() {
    let base = RuntimeSnapshot::new(
        ConfigRevision::new(7).unwrap(),
        scheduling(),
        vec![
            ProviderKind::new("openai").unwrap(),
            ProviderKind::new("xai").unwrap(),
        ],
        vec![
            model("openai", "shared", capabilities()),
            model("xai", "shared", capabilities()),
        ],
        vec![],
    )
    .unwrap();
    let native = base
        .plan(
            &PublicModelId::new("shared").unwrap(),
            &operation(),
            account_scope(),
            &RoutingContext::default(),
        )
        .unwrap();
    let compiled = base
        .with_model_aliases(vec![alias("plugin-model", "openai", "shared")])
        .unwrap();
    let plan = compiled
        .plan(
            &PublicModelId::new("plugin-model").unwrap(),
            &operation(),
            account_scope(),
            &RoutingContext::default(),
        )
        .unwrap();
    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(plan.candidates()[0].provider().as_str(), "openai");
    assert_eq!(
        plan.candidates()[0].upstream_model().unwrap().as_str(),
        "shared"
    );
    assert_eq!(plan.pricing(), native.pricing());
    assert_eq!(plan.config_revision(), native.config_revision());
    assert!(!compiled.contains_public_model_for_provider(
        &PublicModelId::new("plugin-model").unwrap(),
        &ProviderKind::new("xai").unwrap()
    ));
    assert!(
        !compiled
            .public_models_for_provider(&ProviderKind::new("xai").unwrap())
            .contains(&PublicModelId::new("plugin-model").unwrap())
    );
    let restricted = RoutingContext {
        required_provider: Some(ProviderKind::new("xai").unwrap()),
        ..Default::default()
    };
    assert!(
        compiled
            .plan(
                &PublicModelId::new("plugin-model").unwrap(),
                &operation(),
                account_scope(),
                &restricted
            )
            .is_err()
    );
}
#[test]
fn contributed_alias_keeps_target_capabilities_presentation_and_account_permissions() {
    use gateway_core::account::{AccountModelAccess, AccountModelAccessMode};
    let provider = ProviderKind::new("openai").unwrap();
    let presentation = ModelPresentation::new(
        Some("Native target".into()),
        Some("Provider supplied".into()),
    );
    let compiled = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        scheduling(),
        vec![provider.clone()],
        vec![model("openai", "target", capabilities()).with_presentation(presentation.clone())],
        vec![],
    )
    .unwrap()
    .with_model_aliases(vec![alias("plugin-model", "openai", "target")])
    .unwrap();
    let profiles = compiled.public_model_profiles_for_provider(&provider);
    assert_eq!(
        profiles
            .iter()
            .find(|p| p.model().as_str() == "plugin-model")
            .unwrap()
            .presentation(),
        &presentation
    );
    let plan = compiled
        .plan(
            &PublicModelId::new("plugin-model").unwrap(),
            &operation(),
            account_scope(),
            &RoutingContext::default(),
        )
        .unwrap();
    assert_eq!(
        plan.candidates()[0].model_presentation(),
        Some(&presentation)
    );
    assert!(
        compiled
            .plan(
                &PublicModelId::new("plugin-model").unwrap(),
                &image_operation(),
                account_scope(),
                &RoutingContext::default()
            )
            .is_err()
    );
    let denied = FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([(
            ProviderAccountId::new("acct_openai").unwrap(),
            RuntimeAccount::new(provider, BTreeSet::new()).with_model_access(
                AccountModelAccess::new(AccountModelAccessMode::Denylist, vec!["target".into()])
                    .unwrap(),
            ),
        )]))),
        ClientRoutingScope::all_accounts(),
    );
    assert!(
        !compiled
            .contains_public_model_for_scope(&PublicModelId::new("plugin-model").unwrap(), &denied)
    );
    assert!(compiled.public_models_for_scope(&denied).is_empty());
    assert!(compiled.public_model_profiles_for_scope(&denied).is_empty());
    // The original frozen account scope still decides candidate eligibility.
    let plan = compiled
        .plan(
            &PublicModelId::new("plugin-model").unwrap(),
            &operation(),
            Arc::new(denied),
            &RoutingContext::default(),
        )
        .unwrap();
    assert!(
        !plan.candidates()[0]
            .account_scope()
            .allows_provider_model(plan.candidates()[0].provider(), "target")
    );
}
#[test]
fn contributed_alias_rejects_unknown_catalog_and_revalidates_request_settings_rewrites() {
    let unknown = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        scheduling(),
        vec![ProviderKind::new("openai").unwrap()],
        vec![],
        vec![],
    )
    .unwrap();
    assert!(
        unknown
            .with_model_aliases(vec![alias("plugin-model", "openai", "target")])
            .is_err()
    );
    let frozen = snapshot()
        .with_model_aliases(vec![alias("plugin-model", "openai", "gpt-5.5")])
        .unwrap();
    for mappings in [
        BTreeMap::from([("plugin-model".into(), "gpt-5.5".into())]),
        BTreeMap::from([("another".into(), "plugin-model".into())]),
        BTreeMap::from([("gpt-5.5".into(), "grok-4.5".into())]),
    ] {
        let values = frozen.settings().clone().with_model_mappings(mappings);
        assert!(frozen.resolve_settings(values).is_err());
    }
    let values = frozen
        .settings()
        .clone()
        .with_model_mappings(BTreeMap::from([("other".into(), "gpt-5.5".into())]));
    let derived = frozen.resolve_settings(values).unwrap();
    assert_eq!(derived.mapped_model("plugin-model"), "gpt-5.5");
    assert_eq!(frozen.mapped_model("other"), "other");
    let removed = frozen.clone().with_model_aliases(vec![]).unwrap();
    assert_eq!(removed.mapped_model("plugin-model"), "plugin-model");
    assert_eq!(frozen.mapped_model("plugin-model"), "gpt-5.5");
}
