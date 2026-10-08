use bytes::Bytes;
use gateway_core::{engine::middleware::*, operation::*};
use serde_json::{Value, json};
fn request(body: Value) -> MiddlewareRequest {
    MiddlewareRequest::new("openai", vec![], Bytes::from(body.to_string()))
}
fn operation(body: Value) -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone()).unwrap(),
    ))
}
fn declaration(handled: &[Feature], required: &[Feature]) -> MiddlewareCapabilityDeclaration {
    MiddlewareCapabilityDeclaration {
        handled: handled.iter().copied().collect(),
        required: required.iter().copied().collect(),
    }
}
fn rewrite(
    request: MiddlewareRequest,
    body: Value,
    handled: &[Feature],
    required: &[Feature],
) -> Result<MiddlewareRequest, MiddlewareError> {
    request.replace_parts(
        "openai".into(),
        vec![],
        Bytes::from(body.to_string()),
        Some(declaration(handled, required)),
    )
}
#[test]
fn declarations_retain_unhandled_origin_and_actual_body_requirements() {
    let original =
        json!({"tools":[{"type":"function"}],"reasoning":{},"previous_response_id":"original"});
    let changed = json!({"input": [{"type":"input_image"}], "max_output_tokens":37});
    let request = rewrite(
        request(original),
        changed.clone(),
        &[Feature::Tools],
        &[Feature::JsonSchema],
    )
    .unwrap();
    let requirements = request
        .apply_capabilities(operation(changed))
        .unwrap()
        .capability_requirements();
    assert_eq!(
        requirements.features(),
        &[
            Feature::Reasoning,
            Feature::NativeContinuation,
            Feature::Vision,
            Feature::JsonSchema
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(requirements.requested_output_tokens(), Some(37));
}
#[test]
fn declaration_rejects_missing_unconverted_overlapping_and_native_claims() {
    for (before, after, handled, required) in [
        (json!({}), json!({}), vec![Feature::Tools], vec![]),
        (
            json!({"tools":[{}]}),
            json!({"tools":[{}]}),
            vec![Feature::Tools],
            vec![],
        ),
        (
            json!({"tools":[{}]}),
            json!({}),
            vec![Feature::Tools],
            vec![Feature::Tools],
        ),
        (
            json!({"previous_response_id":"x"}),
            json!({}),
            vec![Feature::NativeContinuation],
            vec![],
        ),
        (json!({}), json!({}), vec![], vec![]),
    ] {
        assert!(rewrite(request(before), after, &handled, &required).is_err());
    }
    assert!(
        request(json!({"tools":[{}]}))
            .replace_parts(
                "other".into(),
                vec![],
                Bytes::from_static(b"{}"),
                Some(declaration(&[Feature::Tools], &[]))
            )
            .is_err()
    );
}
#[test]
fn nested_rewrites_keep_original_requirements_and_cannot_erase_upstream_requirements() {
    let first = rewrite(
        request(json!({"tools":[{}],"input":[{"type":"input_image"}],"reasoning":{}})),
        json!({"input":[{"type":"input_image"}]}),
        &[Feature::Tools],
        &[Feature::JsonSchema],
    )
    .unwrap();
    let (protocol, headers, body) = first.clone().into_parts();
    let pass = first.replace_parts(protocol, headers, body, None).unwrap();
    let second = rewrite(pass, json!({}), &[Feature::Vision], &[]).unwrap();
    let result = second.apply_capabilities(operation(json!({}))).unwrap();
    assert_eq!(
        result.capability_requirements().features(),
        &[Feature::Reasoning, Feature::JsonSchema]
            .into_iter()
            .collect()
    );
    let again = second
        .apply_capabilities(operation(json!({"tools":[{}]})))
        .unwrap();
    assert!(
        again
            .capability_requirements()
            .features()
            .contains(&Feature::Tools)
    );
}
#[test]
fn attempt_redecode_retains_declarations_without_masking_changes_to_actual_features() {
    let declared = rewrite(
        request(json!({})),
        json!({"tools":[{}]}),
        &[],
        &[Feature::Reasoning],
    )
    .unwrap()
    .apply_capabilities(operation(json!({"tools":[{}]})))
    .unwrap();
    let Operation::Generate(original) = declared else {
        panic!()
    };
    let Operation::Generate(mut decoded) = operation(json!({})) else {
        panic!()
    };
    decoded.inherit_capability_requirements(&original);
    let requirements = Operation::Generate(decoded).capability_requirements();
    assert!(requirements.features().contains(&Feature::Reasoning));
    assert!(!requirements.features().contains(&Feature::Tools));
    assert_ne!(
        requirements,
        Operation::Generate(original).capability_requirements()
    );
}
#[test]
fn declaration_cannot_apply_to_raw_endpoint_or_other_protocol() {
    let request = rewrite(request(json!({})), json!({}), &[], &[Feature::Tools]).unwrap();
    let raw = Operation::Search(StandaloneSearchRequest::from_raw_json(
        RawJsonPayload::new("openai", Bytes::from_static(b"{}")).unwrap(),
    ));
    assert!(request.apply_capabilities(raw).is_err());
    let other = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("other", Default::default()).unwrap(),
    ));
    assert!(request.apply_capabilities(other).is_err());
}

#[test]
fn each_convertible_feature_requires_its_removal_from_the_body() {
    for (feature, body) in [
        (Feature::Tools, json!({"tools":[{}]})),
        (Feature::Vision, json!({"input":[{"type":"input_image"}]})),
        (Feature::Reasoning, json!({"reasoning":{}})),
        (
            Feature::JsonSchema,
            json!({"text":{"format":{"type":"json_schema"}}}),
        ),
    ] {
        assert!(rewrite(request(body.clone()), body.clone(), &[feature], &[]).is_err());
        let request = rewrite(request(body), json!({}), &[feature], &[]).unwrap();
        assert!(
            request
                .apply_capabilities(operation(json!({})))
                .unwrap()
                .capability_requirements()
                .features()
                .is_empty()
        );
    }
}
