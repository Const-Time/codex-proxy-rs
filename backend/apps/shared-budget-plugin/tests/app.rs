use crate::support::Host;
use serde_json::json;

#[tokio::test]
async fn package_process_delegates_budget_scope_and_limits_without_own_accounting() {
    let mut host = Host::new(None).await;
    let target = json!({"user_id":"user-one","group_id":"group-one"});
    let (status, budget) = host.request("api/budget", Some(target.clone())).await;
    assert_eq!(status, 200);
    assert_eq!(budget["target"], target);
    let input = json!({"group_id":"group-one","expected_revision":7,"daily_limit_usd":"3","weekly_limit_usd":"12"});
    assert_eq!(host.request("api/limits", Some(input.clone())).await.0, 200);
    assert_eq!(host.limits, vec![input]);
    assert!(host.resets.is_empty());
    assert!(host.state.is_none());
    let bad = json!({"user_id":"user-one","group_id":"group-one","actor":"system"});
    assert_eq!(host.request("api/budget", Some(bad)).await.0, 400);
    host.close().await;
}

#[tokio::test]
async fn reset_draft_survives_restart_and_reuses_exact_native_id_after_uncertain_result() {
    let mut host = Host::new(None).await;
    let input =
        json!({"target":{"user_id":"user-one","group_id":"group-one"},"expected_version":null});
    let (status, draft) = host.request("api/reset/prepare", Some(input.clone())).await;
    assert_eq!(status, 200);
    assert!(host.resets.is_empty());
    let event = draft["draft"]["command"]["event_id"].as_str().unwrap();
    assert_eq!(uuid::Uuid::parse_str(event).unwrap().get_version_num(), 4);
    let execute = json!({"event_id":event});
    host.uncertain = true;
    let (status, error) = host
        .request("api/reset/execute", Some(execute.clone()))
        .await;
    assert_eq!(status, 502);
    assert!(!error.to_string().contains("fixture private"));
    let command = host.resets[0].clone();
    assert_eq!(host.request("api/reset/prepare", Some(input)).await.0, 409);
    let state = host.close().await;
    let mut host = Host::new(state).await;
    assert_eq!(host.request("api/reset", None).await.1, draft);
    // Native reset succeeds but saving its UI receipt is uncertain.
    host.fail_receipt = true;
    assert_eq!(
        host.request("api/reset/execute", Some(execute.clone()))
            .await
            .0,
        502
    );
    assert_eq!(host.resets, vec![command.clone()]);
    host.fail_receipt = false;
    let (status, finished) = host
        .request("api/reset/execute", Some(execute.clone()))
        .await;
    assert_eq!(status, 200);
    assert_eq!(finished["draft"]["receipt"]["affected"], 1);
    assert_eq!(host.resets, vec![command.clone(), command]);
    assert_eq!(
        host.request("api/reset/execute", Some(execute)).await.1,
        finished
    );
    assert_eq!(
        host.resets.len(),
        2,
        "confirmed UI receipt must not invoke another reset"
    );
    host.close().await;
}

#[tokio::test]
async fn explicit_archival_never_claims_cancellation_or_executes_a_new_reset() {
    let mut host = Host::new(None).await;
    let input =
        json!({"target":{"user_id":"user-one","group_id":"group-one"},"expected_version":null});
    let draft = host
        .request("api/reset/prepare", Some(input.clone()))
        .await
        .1;
    let event = draft["draft"]["command"]["event_id"].clone();
    assert_eq!(
        host.request(
            "api/reset/archive",
            Some(json!({"event_id":event,"acknowledge_unknown":false}))
        )
        .await
        .0,
        409
    );
    let archived = host
        .request(
            "api/reset/archive",
            Some(json!({"event_id":event,"acknowledge_unknown":true})),
        )
        .await
        .1;
    assert_eq!(archived["draft"]["archived"], true);
    assert!(archived["draft"]["receipt"].is_null());
    assert_eq!(
        host.request("api/reset/execute", Some(json!({"event_id":event})))
            .await
            .0,
        409
    );
    assert_eq!(
        host.request("api/reset/prepare", Some(input.clone()))
            .await
            .0,
        409
    );
    let mut next = input;
    next["expected_version"] = archived["version"].clone();
    let fresh = host.request("api/reset/prepare", Some(next)).await.1;
    assert_ne!(fresh["draft"]["command"]["event_id"], event);
    assert!(host.resets.is_empty());
    host.close().await;
}
