use super::*;
use gateway_core::operation::{Feature, GenerateRequest, Operation, ProtocolPayload};

fn input(mode: &str) -> MiddlewareRequest {
    MiddlewareRequest::new(
        "openai",
        vec![],
        Bytes::from(json!({"mode":mode,"tools":[{}],"reasoning":{}}).to_string()),
    )
}
#[tokio::test]
async fn sdk_declarations_reach_core_without_replacing_body_derived_requirements() {
    let fixture = Fixture::new().await;
    for mode in ["caps-handled", "caps-required"] {
        let trace = Arc::new(Trace::default());
        let response = fixture
            .callbacks
            .invoke(
                fixture.session.clone(),
                context(false),
                input(mode),
                terminal(trace.clone(), false),
            )
            .await
            .unwrap();
        {
            let original = trace.request.lock().unwrap();
            let request = original.as_ref().unwrap();
            assert!(request.has_capability_declaration());
            let body = serde_json::from_slice(request.body()).unwrap();
            let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body).unwrap(),
            ));
            let features = request
                .apply_capabilities(operation)
                .unwrap()
                .capability_requirements();
            assert!(features.features().contains(&Feature::Reasoning));
            assert_eq!(
                features.features().contains(&Feature::Tools),
                mode == "caps-required"
            );
        }
        response.into_parts().3.close().await;
        trace.closed_once().await;
    }
    fixture.stop().await;
}
#[tokio::test]
async fn sdk_declarations_reject_wrong_mount_operation_and_unproven_claims_before_next() {
    let fixture = Fixture::new().await;
    for mode in [
        "caps-retain",
        "caps-missing",
        "caps-duplicate",
        "caps-overlap",
        "caps-preserve",
    ] {
        let trace = Arc::new(Trace::default());
        assert!(
            fixture
                .callbacks
                .invoke(
                    fixture.session.clone(),
                    context(false),
                    input(mode),
                    terminal(trace.clone(), false)
                )
                .await
                .is_err(),
            "{mode}"
        );
        assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    }
    for (mount, operation) in [
        (MiddlewareMount::Attempt, OperationKind::Generate),
        (MiddlewareMount::Request, OperationKind::GenerateImage),
        (MiddlewareMount::Request, OperationKind::Search),
    ] {
        let mut context = context(false);
        context.mount = mount;
        context.operation = operation;
        let trace = Arc::new(Trace::default());
        assert!(
            fixture
                .callbacks
                .invoke(
                    fixture.session.clone(),
                    context,
                    input("caps-handled"),
                    terminal(trace.clone(), false)
                )
                .await
                .is_err()
        );
        assert_eq!(trace.next.load(Ordering::SeqCst), 0);
    }
    fixture.stop().await;
}
#[tokio::test]
async fn nested_sdk_next_preserves_earlier_capability_declaration() {
    let fixture = Fixture::new().await;
    let trace = Arc::new(Trace::default());
    let callbacks = fixture.callbacks.clone();
    let session = fixture.session.clone();
    let terminal = terminal(trace.clone(), false);
    let next = gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
        Box::pin(async move {
            let (protocol, headers, body) = request.clone().into_parts();
            let mut body: Value = serde_json::from_slice(&body).unwrap();
            body["mode"] = json!("pass");
            let request =
                request.replace_parts(protocol, headers, Bytes::from(body.to_string()), None)?;
            callbacks
                .invoke(session, context(false), request, terminal)
                .await
        })
    });
    let response = fixture
        .callbacks
        .invoke(
            fixture.session.clone(),
            context(false),
            input("caps-required"),
            next,
        )
        .await
        .unwrap();
    {
        let stored = trace.request.lock().unwrap();
        let request = stored.as_ref().unwrap();
        assert!(request.has_capability_declaration());
        let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object("openai", serde_json::from_slice(request.body()).unwrap())
                .unwrap(),
        ));
        assert!(
            request
                .apply_capabilities(operation)
                .unwrap()
                .capability_requirements()
                .features()
                .contains(&Feature::Tools)
        );
    }
    response.into_parts().3.close().await;
    trace.closed_once().await;
    fixture.stop().await;
}
