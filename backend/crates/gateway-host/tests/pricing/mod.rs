use gateway_host::pricing::{decode_catalog, decode_verified_catalog};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

fn model() -> Value {
    json!({
        "litellm_provider":"openai", "mode":"chat", "supported_output_modalities":["text"],
        "input_cost_per_token":2e-6, "output_cost_per_token":1e-5,
        "cache_read_input_token_cost":2e-7, "cache_creation_input_token_cost":2.5e-6,
        "input_cost_per_token_priority":4e-6, "output_cost_per_token_priority":2e-5,
        "cache_read_input_token_cost_priority":4e-7, "cache_creation_input_token_cost_priority":5e-6,
        "input_cost_per_token_flex":1e-6, "output_cost_per_token_flex":5e-6,
        "cache_read_input_token_cost_flex":1e-7, "cache_creation_input_token_cost_flex":1.25e-6,
        "input_cost_per_token_above_272k_tokens":4e-6, "output_cost_per_token_above_272k_tokens":1.5e-5,
        "cache_read_input_token_cost_above_272k_tokens":4e-7, "cache_creation_input_token_cost_above_272k_tokens":5e-6
    })
}

fn decode(
    value: Value,
) -> Result<gateway_admin::model::pricing::PricingSyncPreview, gateway_admin::model::AdminError> {
    decode_catalog(&serde_json::to_vec(&json!({"new-model":value})).unwrap())
}

#[test]
fn source_converts_per_token_units_and_preserves_explicit_service_and_context_bands() {
    let result = decode(model()).unwrap();
    let price = &result.prices["openai"]["new-model"];
    assert_eq!(price.bands.len(), 4);
    assert_eq!(String::from(price.bands["standard"].input), "2");
    assert_eq!(String::from(price.bands["fast"].input), "4");
    assert_eq!(String::from(price.bands["flex"].cache_write), "1.25");
    assert_eq!(String::from(price.bands["long_standard"].output), "15");
    assert!(!price.bands.contains_key("long_fast"));
}

#[test]
fn hash_mismatch_and_malformed_documents_never_publish_prices() {
    let body = serde_json::to_vec(&json!({"new-model":model()})).unwrap();
    let hash = hex::encode(Sha256::digest(&body));
    assert_eq!(
        decode_verified_catalog(&body, &hash).unwrap().source_hash,
        hash
    );
    for bad in ["a".repeat(64), "".to_owned(), "z".repeat(64)] {
        assert!(decode_verified_catalog(&body, &bad).is_err());
    }
    for body in [b"{}".as_slice(), b"[]", b"null", b"invalid"] {
        assert!(decode_catalog(body).is_err());
    }
}

#[test]
fn missing_cache_prices_are_not_assumed_free_or_ordinary_input() {
    for key in [
        "cache_read_input_token_cost",
        "cache_creation_input_token_cost",
        "input_cost_per_token",
        "output_cost_per_token",
    ] {
        let mut value = model();
        value.as_object_mut().unwrap().remove(key);
        assert!(decode(value).is_err(), "{key}");
    }
    let mut value = model();
    value["cache_creation_input_token_cost"] = json!(0);
    assert_eq!(
        decode(value).unwrap().prices["openai"]["new-model"].bands["standard"]
            .cache_write
            .ticks_per_token(),
        0
    );
}

#[test]
fn malformed_extra_tier_or_non_text_rules_do_not_fall_back_to_short_context_prices() {
    for (key, value) in [
        ("input_cost_per_token_above_200k_tokens", json!(1e-5)),
        ("input_cost_per_audio_token", json!(1e-5)),
        ("long_context_input_cost_multiplier", json!(2)),
        ("supported_output_modalities", json!(["text", "audio"])),
        ("mode", json!("image_generation")),
    ] {
        let mut item = model();
        item[key] = value;
        assert!(decode(item).is_err(), "{key}");
    }
}

#[test]
fn incomplete_priority_band_is_unknown_not_inferred_from_standard() {
    let mut item = model();
    item.as_object_mut()
        .unwrap()
        .remove("cache_creation_input_token_cost_priority");
    let parsed = decode(item).unwrap();
    assert!(
        !parsed.prices["openai"]["new-model"]
            .bands
            .contains_key("fast")
    );
}

#[test]
fn decimal_conversion_rejects_negative_overflow_and_unrepresentable_precision() {
    for literal in [
        "-0.000001",
        "1e100",
        "0.00000000001",
        "0.0000010000000000000001",
    ] {
        let mut item = model();
        item["input_cost_per_token"] = serde_json::from_str(literal).unwrap();
        assert!(decode(item).is_err(), "{literal}");
    }
    let mut item = model();
    item["input_cost_per_token"] = serde_json::from_str("1e-10").unwrap();
    assert_eq!(
        String::from(decode(item).unwrap().prices["openai"]["new-model"].bands["standard"].input),
        "0.0001"
    );
}

#[test]
fn provider_filter_and_skipped_entries_do_not_invent_model_aliases() {
    let parsed = decode_catalog(
        &serde_json::to_vec(&json!({
            "gpt-6-sol":model(), "openai/gpt-6-sol":model(),
            "bad":{"litellm_provider":"openai","mode":"chat"},
            "other":{"litellm_provider":"anthropic","mode":"chat"}
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(parsed.prices["openai"].len(), 2);
    assert_eq!(parsed.skipped, ["openai/bad"]);
    assert!(!parsed.prices["openai"].contains_key("gpt-6"));
}

#[test]
fn xai_context_threshold_is_not_openai_context_threshold() {
    let mut item = model();
    item["litellm_provider"] = json!("xai");
    assert!(decode(item.clone()).is_err());
    let fields = item.as_object_mut().unwrap();
    for field in [
        "input_cost_per_token",
        "output_cost_per_token",
        "cache_read_input_token_cost",
        "cache_creation_input_token_cost",
    ] {
        let value = fields
            .remove(&format!("{field}_above_272k_tokens"))
            .unwrap();
        fields.insert(format!("{field}_above_200k_tokens"), value);
    }
    let parsed = decode(item).unwrap();
    let bands = &parsed.prices["xai"]["new-model"].bands;
    assert!(bands.contains_key("long_standard"));
    assert!(!bands.contains_key("flex"));
}
