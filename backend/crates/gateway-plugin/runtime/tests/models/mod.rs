mod fixture;
use fixture::*;
use gateway_admin::ports::plugins::PluginService;
use gateway_core::engine::{execution::ExecutionService, middleware::*};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

fn native_context(
    parent: &gateway_core::engine::execution::PreparedRootExecution,
) -> MiddlewareContext {
    MiddlewareContext {
        excluded_plugin_instances: parent.nested_context().scope().excluded_instances().clone(),
        request_id: parent.request_id().clone(),
        client_key_id: parent.client().policy().key_id().clone(),
        account_group_ids: vec![],
        mount: MiddlewareMount::Request,
        attempt_index: None,
        operation: gateway_core::operation::OperationKind::Generate,
        endpoint: "/v1/responses".into(),
        transport: gateway_core::engine::execution::ClientTransport::HttpJson,
        provider: None,
        model: Some("gpt-native".into()),
        account_id: None,
        cancellation: parent.cancellation(),
        deadline_at: parent.deadline_at(),
    }
}

#[tokio::test]
async fn native_children_run_request_and_attempt_rewrites_with_one_original_charge() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    for (index, mode) in ["unary", "stream", "small"].into_iter().enumerate() {
        let result = fixture
            .call(json!({"mode":mode,"key":"key_alice","provider_mode":"native-transform"}))
            .await;
        assert!(result.get("error").is_none(), "{mode}: {result}");
        fixture.clean(index + 1).await;
        {
            let bodies = fixture.core.bodies.lock().unwrap();
            assert_eq!(bodies[index]["plugin_request"], true);
            assert_eq!(bodies[index]["plugin_attempt"], true);
        }
        let snapshots = result["batch"].as_array().unwrap();
        let costs = snapshots
            .iter()
            .map(|e| {
                e.pointer("/host/costs")
                    .and_then(|c| c.as_array())
                    .map_or(0, Vec::len)
            })
            .sum::<usize>();
        assert_eq!(
            costs, 1,
            "rewritten facts remain the original read-only receipt: {result}"
        );
        assert!(
            snapshots.iter().any(|e| {
                let data = &e["wire"]["payload"]["data"];
                let projected = if mode == "unary" {
                    &data["response"]
                } else {
                    data
                };
                projected["plugin_request"] == true && projected["plugin_attempt"] == true
            }),
            "{result}"
        );
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .all(|c| c.amount_usd.scaled() == 123)
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_request_short_circuit_returns_wire_without_admission_or_charge() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    let result = fixture
        .call(json!({"key":"key_alice","provider_mode":"native-short"}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    assert_eq!(result["events"], 1);
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    assert!(fixture.core.requests.lock().unwrap().is_empty());
    assert!(fixture.core.charges.lock().unwrap().is_empty());
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_chain_errors_keep_provider_send_state_and_http_status() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    for (index, state) in ["not-sent", "sent", "ambiguous"].into_iter().enumerate() {
        let result = fixture.call(json!({"key":"key_alice","provider_mode":"native-transform","failure":format!("fail-{state}")})).await;
        assert_eq!(result["error"]["code"], "invalid_input", "{result}");
        assert_eq!(
            result["error"]["send_state"],
            state.replace('-', "_"),
            "{result}"
        );
        assert_eq!(result["error"]["http_status"], 400, "{result}");
        fixture.clean(index + 1).await;
    }
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_grandchild_skips_every_initiating_ancestor_and_charges_each_call_once() {
    let fixture = Fixture::with_middleware().await;
    let other = fixture.install_layers().await;
    let result = fixture
        .call(json!({"key":"key_alice","provider_mode":"native-transform","nested_model":true}))
        .await;
    assert!(result.get("error").is_none(), "{result}");
    fixture.clean(2).await;
    {
        let rows = fixture.core.requests.lock().unwrap();
        assert_eq!(rows.len(), 2);
        let child = rows.iter().find(|r| r.parent_request_id.is_none()).unwrap();
        let grandchild = rows.iter().find(|r| r.parent_request_id.is_some()).unwrap();
        assert_eq!(grandchild.parent_request_id.as_ref(), Some(&child.id));
        assert_eq!(
            grandchild.initiating_plugin_instance_id.as_deref(),
            Some(other.as_str())
        );
        assert_eq!(grandchild.client_api_key_ref.as_str(), "key_alice");
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .all(|c| c.amount_usd.scaled() == 123)
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_raw_endpoints_run_both_mounts_and_preserve_original_fees() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    for (index, operation) in ["generate_image", "edit_image", "search"]
        .into_iter()
        .enumerate()
    {
        let result = fixture.call(json!({"mode":"raw","method":"host.model.execute","params":{"client_key_id":"key_alice","model":"gpt-native","protocol":"openai","operation":operation},"body":{"mode":"native-transform"}})).await;
        assert!(result.get("error").is_none(), "{result}");
        fixture.clean(index + 1).await;
        let wire = result["batch"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|event| event.pointer("/wire/payload/body"))
            .unwrap();
        let bytes = wire
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap() as u8)
            .collect::<Vec<_>>();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["plugin_request"], true);
        assert_eq!(body["plugin_attempt"], true);
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .core
            .charges
            .lock()
            .unwrap()
            .iter()
            .all(|c| c.amount_usd.scaled() == 123)
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_chain_retains_a_commit_when_its_rpc_receiver_is_cancelled() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.commit_gate.lock().unwrap() = Some(receiver);
    let mut call = Box::pin(
        fixture.call(json!({"mode":"stream","key":"key_alice","provider_mode":"native-transform"})),
    );
    tokio::select! { _=&mut call=>panic!("commit should wait"), _=async {while fixture.core.commits.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{} }
    drop(call);
    sender.send(()).unwrap();
    fixture.clean(1).await;
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 1);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn cancelling_native_chain_during_admission_retains_the_granted_slot_cleanup() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.admit_gate.lock().unwrap() = Some(receiver);
    let mut call =
        Box::pin(fixture.call(json!({"key":"key_alice","provider_mode":"native-transform"})));
    tokio::select! { _=&mut call=>panic!("admission should wait"), _=async {while fixture.core.active.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{} }
    drop(call);
    sender.send(()).unwrap();
    fixture.clean(1).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn revoking_key_while_native_chain_waits_keeps_observed_cost_and_releases() {
    let fixture = Fixture::with_middleware().await;
    fixture.install_layers().await;
    let mut call = Box::pin(fixture.call(json!({"mode":"stream","key":"key_alice","provider_mode":"native-transform","failure":"stall"})));
    tokio::select! { _=&mut call=>panic!("provider should wait"), _=async {while fixture.core.polled.load(Ordering::SeqCst)<2 {tokio::task::yield_now().await;}}=>{} }
    fixture.snapshot.publish(snapshot(2, false));
    let result = tokio::time::timeout(Duration::from_secs(3), call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(1).await;
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        123
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_protocol_and_metadata_mismatches_fail_before_admission() {
    let fixture = Fixture::new().await;
    let base = json!({"client_key_id":"key_alice","model":"gpt-native","protocol":"openai","operation":"generate"});
    for (field, value, code) in [
        ("protocol", json!("unsupported"), "unsupported"),
        ("operation", json!("unknown"), "invalid_input"),
        ("model", json!("other-model"), "invalid_input"),
        (
            "previous_response_id",
            json!("resp_missing_body"),
            "invalid_input",
        ),
    ] {
        let mut params = base.clone();
        params[field] = value;
        let result=fixture.call(json!({"mode":"raw","method":"host.model.execute","params":params,"body":{"model":"gpt-native"}})).await;
        assert_eq!(result["error"]["code"], code, "{result}");
    }
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_request_callback_inherits_opaque_parent_authority_and_records_ancestry() {
    let fixture = Fixture::with_middleware().await;
    let parent = fixture
        .execution
        .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    let request = MiddlewareRequest::new(
        "openai",
        vec![],
        bytes::Bytes::from(
            serde_json::to_vec(&json!({"mode":"native-model","native":{"mode":"stream"}})).unwrap(),
        ),
    )
    .with_execution_context(parent.nested_context())
    .with_settings(parent.settings().clone());
    let response = fixture
        .manager
        .freeze_middleware(native_context(&parent))
        .unwrap()
        .run(
            request,
            gateway_core::middleware::compose(vec![], |_| {
                Box::pin(async { panic!("fixture short circuits only after real child execution") })
            }),
        )
        .await
        .unwrap();
    let mut body = response.into_parts().3;
    let frame = body.next_frame().await.unwrap().unwrap();
    let result: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(result["events"], 3);
    assert_eq!(result["ended"], true);
    body.close().await;
    fixture.clean(1).await;
    {
        let rows = fixture.core.requests.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].parent_request_id.as_ref(),
            Some(parent.request_id())
        );
        assert_eq!(
            rows[0].initiating_plugin_instance_id.as_deref(),
            Some(fixture.id.as_str())
        );
    }
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn nested_native_execution_rejects_the_account_already_held_by_its_parent() {
    let fixture = Fixture::with_middleware().await;
    let parent = fixture
        .execution
        .prepare_execution(fixture.execution.authenticate("sk_native_fixture").unwrap())
        .unwrap();
    let mut context = native_context(&parent);
    context.account_id =
        Some(gateway_core::account::ProviderAccountId::new("acct_allowed").unwrap());
    let request = MiddlewareRequest::new(
        "openai",
        vec![],
        bytes::Bytes::from_static(
            br#"{"mode":"native-model","native":{"account":"acct_allowed"}}"#,
        ),
    )
    .with_execution_context(parent.nested_context());
    let response = fixture
        .manager
        .freeze_middleware(context)
        .unwrap()
        .run(
            request,
            gateway_core::middleware::compose(vec![], |_| {
                Box::pin(async { panic!("short circuit") })
            }),
        )
        .await
        .unwrap();
    let mut body = response.into_parts().3;
    let frame = body.next_frame().await.unwrap().unwrap();
    let result: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(result["error"]["code"], "upstream");
    body.close().await;
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn native_unary_stream_and_small_reads_commit_and_settle_each_real_execution_once() {
    let fixture = Fixture::new().await;
    for (index, mode) in ["unary", "stream", "small"].into_iter().enumerate() {
        let result = fixture
            .call(
                json!({"mode":mode,"key":"key_alice","provider":"openai","account":"acct_allowed"}),
            )
            .await;
        assert!(result.get("error").is_none(), "{result}");
        assert_eq!(result["events"], 3);
        if mode != "unary" {
            assert_eq!(result["ended"], true);
        }
        fixture.clean(index + 1).await;
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 3);
    {
        let requests = fixture.core.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
        requests
            .iter()
            .all(|r| r.parent_request_id.is_none() && r.initiating_plugin_instance_id.is_none())
    );
        let charges = fixture.core.charges.lock().unwrap();
        for charge in charges.iter() {
            assert_eq!(charge.amount_usd.scaled(), 123);
            assert_eq!(charge.key_id.as_str(), "key_alice");
            assert_eq!(charge.scope.as_ref().unwrap().user_id, "alice");
        }
        assert_eq!(
            charges
                .iter()
                .map(|c| c.request_id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    }
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_key_listing_catalog_and_model_selection_preserve_personal_ownership() {
    let fixture = Fixture::new().await;
    let first = fixture
        .call(json!({"mode":"raw","method":"host.keys.list","params":{"cursor":null,"limit":1}}))
        .await;
    assert_eq!(first["result"]["keys"][0]["id"], "key_alice");
    let second=fixture.call(json!({"mode":"raw","method":"host.keys.list","params":{"cursor":first["result"]["next_cursor"],"limit":1}})).await;
    assert_eq!(second["result"]["keys"][0]["id"], "key_disabled");
    assert_eq!(second["result"]["keys"][0]["enabled"], false);
    let catalog=fixture.call(json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}})).await;
    assert_eq!(catalog["result"]["models"], json!(["gpt-native"]));
    for key in ["key_bob", "key_disabled"] {
        let result = fixture.call(json!({"key":key})).await;
        assert_eq!(result["error"]["code"], "permission_denied");
    }
    let denied = fixture
        .call_as(
            json!({"key":"key_alice"}),
            gateway_admin::model::MutationActor::AdminApiKey,
        )
        .await;
    assert_eq!(denied["error"]["code"], "permission_denied");
    for body in [
        json!({}),
        json!({"key":"key_alice","account":"acct_other"}),
        json!({"key":"key_alice","provider":"xai"}),
    ] {
        assert!(fixture.call(body).await.get("error").is_some());
    }
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_handles_close_on_parent_return_and_cannot_be_reused_in_another_call() {
    let fixture = Fixture::new().await;
    let result = fixture.call(json!({"mode":"leak","key":"key_alice"})).await;
    assert!(result["stream"].is_string());
    fixture.clean(1).await;
    let rejected=fixture.call(json!({"mode":"raw","method":"host.model.stream_read","params":{"stream":result["stream"],"maximum_bytes":8388608}})).await;
    assert_eq!(rejected["error"]["code"], "invalid_input");
    let result = fixture
        .call(json!({"mode":"close","key":"key_alice","provider_mode":"stall"}))
        .await;
    assert_eq!(result["closed"], true);
    fixture.clean(2).await;
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 1);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn cancelling_native_start_during_admission_does_not_leak_the_granted_slot() {
    let fixture = Fixture::new().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.admit_gate.lock().unwrap() = Some(receiver);
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice"})));
    tokio::select! { _=&mut call=>panic!("admission should wait"), _=async {while fixture.core.active.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{} }
    // Dropping the external management request drops RpcSession's parent-call guard.
    drop(call);
    sender.send(()).unwrap();
    fixture.manager.shutdown().await;
    fixture.clean(1).await;
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        0
    );
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn revoking_key_while_native_stream_waits_settles_observed_cost_and_releases() {
    let fixture = Fixture::new().await;
    let call = fixture.call(json!({"mode":"stream","key":"key_alice","provider_mode":"stall"}));
    tokio::pin!(call);
    tokio::select! { result=&mut call=>panic!("provider should wait: {result}"), _=async {while fixture.core.polled.load(Ordering::SeqCst)<2 {tokio::task::yield_now().await;}}=>{} }
    fixture.snapshot.publish(snapshot(2, false));
    let result = tokio::time::timeout(Duration::from_secs(5), &mut call)
        .await
        .unwrap();
    assert!(result.get("error").is_some(), "{result}");
    fixture.clean(1).await;
    assert_eq!(result["error"]["send_state"], "sent");
    assert_eq!(
        fixture.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        123
    );
    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn revoking_a_key_during_native_catalog_lookup_rejects_the_frozen_result() {
    let fixture = Fixture::new().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.catalog_gate.lock().unwrap() = Some(receiver);
    let call=fixture.call(json!({"mode":"raw","method":"host.models.list","params":{"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}}));
    tokio::pin!(call);
    tokio::select! {_=&mut call=>panic!("catalog should wait"),_=async {while fixture.core.catalogs.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    fixture.snapshot.publish(snapshot(2, false));
    sender.send(()).unwrap();
    assert_eq!(call.await["error"]["code"], "permission_denied");
    assert_eq!(fixture.core.active.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn unary_settlement_finishes_after_its_rpc_receiver_is_cancelled() {
    let fixture = Fixture::new().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.settle_gate.lock().unwrap() = Some(receiver);
    let mut call = Box::pin(fixture.call(json!({"key":"key_alice"})));
    tokio::select! { _=&mut call=>panic!("settlement should wait"), _=async {while fixture.core.finalizations.lock().unwrap().is_empty() {tokio::task::yield_now().await;}}=>{} }
    drop(call);
    sender.send(()).unwrap();
    fixture.clean(1).await;
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 1);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_failures_preserve_monotonic_send_state_and_upstream_status() {
    let fixture = Fixture::new().await;
    for (index, mode) in ["not-sent", "sent", "ambiguous"].into_iter().enumerate() {
        let result = fixture
            .call(json!({"key":"key_alice","provider_mode":format!("fail-{mode}")}))
            .await;
        assert_eq!(result["error"]["code"], "invalid_input");
        assert_eq!(result["error"]["send_state"], mode.replace('-', "_"));
        assert_eq!(result["error"]["http_status"], 400);
        fixture.clean(index + 1).await;
    }
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_stable_image_and_search_operations_use_the_existing_openai_endpoint_path() {
    let fixture = Fixture::new().await;
    for (index, (operation, endpoint)) in [
        ("generate_image", "/v1/images/generations"),
        ("edit_image", "/v1/images/edits"),
        ("search", "/v1/alpha/search"),
    ]
    .into_iter()
    .enumerate()
    {
        let result=fixture.call(json!({"mode":"raw","method":"host.model.execute","params":{"client_key_id":"key_alice","model":"gpt-native","protocol":"openai","operation":operation},"body":{"model":"gpt-native","prompt":"fixture"}})).await;
        assert!(result.get("error").is_none(), "{result}");
        fixture.clean(index + 1).await;
        assert_eq!(
            fixture.core.requests.lock().unwrap()[index].endpoint,
            endpoint
        );
        assert!(
            fixture.core.requests.lock().unwrap()[index]
                .requested_model
                .is_none()
        );
    }
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn native_stream_capacity_is_bounded_and_all_closed_sessions_release() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture
            .call(json!({"mode":"capacity","key":"key_alice"}))
            .await["capacity"],
        true
    );
    fixture.clean(16).await;
    assert_eq!(fixture.core.starts.load(Ordering::SeqCst), 0);
    fixture.manager.shutdown().await;
}
#[tokio::test]
async fn cancelling_a_stream_read_during_commit_finishes_that_commit_and_cleanup_once() {
    let fixture = Fixture::new().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *fixture.core.commit_gate.lock().unwrap() = Some(receiver);
    let mut call =
        Box::pin(fixture.call(json!({"mode":"stream","key":"key_alice","provider_mode":"stall"})));
    tokio::select! { _=&mut call=>panic!("commit should wait"), _=async {while fixture.core.commits.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{} }
    drop(call);
    sender.send(()).unwrap();
    fixture.clean(1).await;
    assert_eq!(fixture.core.commits.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.core.finalizations.lock().unwrap().len(), 1);
    fixture.manager.shutdown().await;
}
