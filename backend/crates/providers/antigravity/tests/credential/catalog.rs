use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

use gateway_core::{
    account::{AccountCatalogObservation, CredentialRevision, ProviderAccountId},
    operation::OperationKind,
    routing::{ModelCapabilities, ReasoningModelVariant, UpstreamModelId},
};
use gateway_protocol::{chat_completions::CHAT_PROTOCOL, genai::GENAI_PROTOCOL};
use provider_antigravity::CloudCodeCatalog as Catalog;
use serde_json::{Value, json};

fn catalog(account: &ProviderAccountId, body: Value) -> (Vec<AccountCatalogObservation>, Catalog) {
    (
        vec![AccountCatalogObservation {
            account_id: account.clone(),
            revision: CredentialRevision::new(1).unwrap(),
            plan: None,
            etag: None,
            observed_at: SystemTime::UNIX_EPOCH,
        }],
        serde_json::from_value(body).unwrap(),
    )
}

#[test]
fn catalog_compiles_declared_variants_and_binds_search_per_account() {
    let first = ProviderAccountId::new("acct_first").unwrap();
    let second = ProviderAccountId::new("acct_second").unwrap();
    let models = Catalog::compile_capabilities(vec![
        catalog(&first, json!({
            "project":"one",
            "models":{"gemini-high":{"maxOutputTokens":80},"gemini-low":{},"plain":{}},
            "groupedModels":{
                "gemini":{"supportedThinkingLevels":[
                    {"thinkingSuffix":"HIGH"},{"thinkingSuffix":"medium"},{"thinkingSuffix":"../low"}
                ]},
                "plain":{"supportedThinkingLevels":[{"thinkingSuffix":"high"}]}
            },
            "webSearchModelIds":["search-one","ignored"]
        })),
        catalog(&second, json!({
            "project":"two",
            "models":{"gemini-medium":{"maxOutputTokens":40},"plain-high":{}},
            "groupedModels":{
                "gemini":{"supportedThinkingLevels":[{"thinkingSuffix":"medium"}]},
                "plain":{"supportedThinkingLevels":[{"thinkingSuffix":"high"}]}
            },
            "webSearchModels":["search-two"]
        })),
    ]).unwrap();
    let model = |id| UpstreamModelId::new(id).unwrap();
    let capabilities = |max| {
        ModelCapabilities::new(
            [OperationKind::Generate, OperationKind::CountTokens].into(),
            max,
        )
        .with_generation_protocol(GENAI_PROTOCOL)
        .with_translated_generation_protocols([CHAT_PROTOCOL])
        .with_upstream_feature_validation()
        .with_dedicated_web_search(BTreeMap::from([
            (first.clone(), model("search-one")),
            (second.clone(), model("search-two")),
        ]))
    };
    let entry = |id| {
        models
            .iter()
            .find(|model| model.upstream_model().as_str() == id)
            .unwrap()
    };
    assert_eq!(
        entry("gemini").catalog_accounts(),
        Some(&BTreeSet::from([first.clone(), second.clone()]))
    );
    assert_eq!(
        entry("gemini").capabilities(),
        &capabilities(None).with_reasoning_variants(BTreeMap::from([
            (
                "high".to_owned(),
                ReasoningModelVariant {
                    model: model("gemini-high"),
                    accounts: BTreeSet::from([first.clone()])
                }
            ),
            (
                "medium".to_owned(),
                ReasoningModelVariant {
                    model: model("gemini-medium"),
                    accounts: BTreeSet::from([second.clone()])
                }
            ),
        ]))
    );
    assert_eq!(entry("gemini-high").capabilities(), &capabilities(Some(80)));
    assert_eq!(entry("plain").capabilities(), &capabilities(None));
    assert!(
        !models
            .iter()
            .any(|model| model.upstream_model().as_str().starts_with("search-"))
    );
}

#[test]
fn project_admits_only_the_declared_target_and_enforces_available_limits() {
    let catalog: Catalog = serde_json::from_value(json!({
        "project":"fixture",
        "models":{"gemini-high":{"maxOutputTokens":80}},
        "webSearchModelIds":["search-only","gemini-high"]
    }))
    .unwrap();
    let supports = |id, limit, search| {
        catalog.supports_project_model(&UpstreamModelId::new(id).unwrap(), limit, search)
    };
    assert!(supports("search-only", None, true));
    assert!(!supports("search-only", None, false));
    assert!(!supports("gemini-high", None, true));
    assert!(supports("gemini-high", Some(80), false));
    assert!(!supports("gemini-high", Some(81), false));
    let legacy = serde_json::from_value::<Catalog>(json!({
        "project":"old",
        "models":{"gemini-high":{}}
    }))
    .unwrap();
    assert!(!legacy.supports_project_model(
        &UpstreamModelId::new("gemini-high").unwrap(),
        None,
        true
    ));
    assert!(legacy.supports_project_model(
        &UpstreamModelId::new("gemini-high").unwrap(),
        None,
        false
    ));
}
