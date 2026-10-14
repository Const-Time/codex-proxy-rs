//! Real xAI OAuth preparation through an owned proxy, native Admin and the SDK adapter.
mod fixture;
use super::{capture, fixture::Fixture};
use fixture::{Proxy, Trust};
use gateway_core::{engine::ExecutionOutcome, upstream::UpstreamSendState};
use serde_json::json;
use std::{path::PathBuf, time::Duration};

#[tokio::test]
async fn native_xai_bound_save_verifies_official_userinfo_before_commit_and_adoption() {
    run(
        "native_xai_bound_save_verifies_official_userinfo_before_commit_and_adoption",
        "save",
    )
    .await;
}
#[tokio::test]
async fn native_xai_bound_save_adopts_prepared_refresh_tokens_instead_of_sdk_candidate() {
    run(
        "native_xai_bound_save_adopts_prepared_refresh_tokens_instead_of_sdk_candidate",
        "refresh",
    )
    .await;
}
#[tokio::test]
async fn native_xai_bound_save_rejects_foreign_verified_subject_without_commit_or_replay() {
    run(
        "native_xai_bound_save_rejects_foreign_verified_subject_without_commit_or_replay",
        "subject",
    )
    .await;
}
#[tokio::test]
async fn native_xai_bound_save_rejects_foreign_discovery_issuer_before_token_verification() {
    run(
        "native_xai_bound_save_rejects_foreign_discovery_issuer_before_token_verification",
        "issuer",
    )
    .await;
}

async fn run(name: &str, mode: &str) {
    let root = match std::env::var_os("CPR_NATIVE_XAI_TRUST") {
        Some(root) => PathBuf::from(root),
        None => {
            for key in ["CPR_TEST_DATABASE_URL", "CPR_TEST_REDIS_URL"] {
                if std::env::var_os(key).is_none() {
                    let ci = std::env::var("CI")
                        .ok()
                        .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
                    assert!(
                        !ci,
                        "{key} is required for native plugin integration tests in CI"
                    );
                    return;
                }
            }
            Trust::new()
                .run(&format!("upstream_adapter::native::xai_save::{name}"))
                .await;
            return;
        }
    };
    let f = Fixture::for_provider("xai")
        .await
        .expect("isolated native database and Redis are configured");
    let proxy = Proxy::start(
        &root,
        &format!("subject-{}", f.account),
        &f.server.url,
        mode,
    )
    .await;
    // Persist the account's actual proxy before native selection; preparation,
    // adoption and managed sends retain the same native proxy identity.
    sqlx::query("UPDATE provider_accounts SET outbound_proxy_url=$1 WHERE id=$2")
        .bind(&proxy.url)
        .bind(f.account.as_str())
        .execute(&f.env.pool)
        .await
        .unwrap();
    let (plan, selected) = capture::plan(&f);
    let mut fields = json!({"fixture_save_token":"fixture-xai-candidate-token"})
        .as_object()
        .unwrap()
        .clone();
    if mode == "refresh" {
        fields.insert(
            "fixture_save_expiry_ms".into(),
            json!((chrono::Utc::now() + chrono::Duration::seconds(20)).timestamp_millis()),
        );
    }
    let mut started = f
        .start_with_fields("callback-credentials-save", fields, Some(plan))
        .await;
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        started.session.collect_uncommitted(),
    )
    .await
    .unwrap();
    let captured = selected
        .lock()
        .unwrap()
        .take()
        .expect("original selected native account must be used");
    let success = matches!(mode, "save" | "refresh");
    let revision = if success { 2 } else { 1 };
    let stored = f
        .ports
        .accounts()
        .load_current_credential(&f.account)
        .await
        .unwrap();
    assert_eq!(stored.account.revision().get(), revision);
    assert_eq!(captured.account.credential_revision().get(), revision);
    assert_eq!(f.env.published.lock().unwrap().len(), usize::from(success));
    let audits: Vec<(String, Vec<String>)> = sqlx::query_as("SELECT actor_ref,changed_fields FROM admin_audit_events WHERE actor_kind='plugin' ORDER BY created_at,id")
        .fetch_all(&f.env.pool).await.unwrap();
    if success {
        assert_eq!(result.unwrap().len(), 2);
        assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
        assert_eq!(
            f.in_flight().await,
            1,
            "adoption must keep the original capacity guard"
        );
        let expected = if mode == "refresh" {
            "fixture-xai-verified-token"
        } else {
            "fixture-xai-candidate-token"
        };
        let material = stored.credential.expose_to_provider();
        assert_eq!(material["access_token"], expected);
        assert_eq!(
            material["refresh_token"],
            if mode == "refresh" {
                "fixture-xai-verified-refresh"
            } else {
                "fixture-refresh-token"
            }
        );
        assert_eq!(
            audits,
            vec![(
                format!("plugin:{}@{}", f.installed.id, f.installed.revision),
                vec!["credentials".into()]
            )]
        );
        let requests = f.server.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains(&format!("authorization: Bearer {expected}")));
        assert!(requests[0].contains(&format!("x-grok-user-id: subject-{}", f.account)));
        assert!(!requests[0].contains("fixture-provider-secret"));
        started.session.commit_downstream(Some(200)).await.unwrap();
    } else {
        assert!(result.is_err());
        assert!(audits.is_empty());
        assert!(f.server.requests.lock().unwrap().is_empty());
        assert_eq!(
            stored.credential.expose_to_provider()["access_token"],
            "fixture-provider-secret"
        );
    }
    started.session.detach_finalize().await;
    captured.scope.wait_until_idle().await;
    drop(captured);
    f.clean(1).await;
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attempt_count, 1);
        assert_eq!(
            rows[0].outcome,
            if success {
                ExecutionOutcome::Succeeded
            } else {
                ExecutionOutcome::Cancelled
            }
        );
        assert_eq!(
            f.core.charges.lock().unwrap()[0].amount_usd.scaled(),
            if success { 570_000 } else { 0 }
        );
    }
    let events = proxy.events();
    assert!(events.iter().any(|v| v["kind"] == "discovery"));
    assert!(
        !events.iter().any(|v| v["kind"] == "forbidden"),
        "the fixture must never forward external traffic"
    );
    let userinfo: Vec<_> = events.iter().filter(|v| v["kind"] == "userinfo").collect();
    assert_eq!(userinfo.len(), usize::from(mode != "issuer"));
    assert!(userinfo.iter().all(|v| v["authorization"] == true));
    let refreshed: Vec<_> = events.iter().filter(|v| v["kind"] == "refresh").collect();
    assert_eq!(refreshed.len(), usize::from(mode == "refresh"));
    assert!(refreshed.iter().all(|v| v["validated"] == true));
    let sends: Vec<_> = events.iter().filter(|v| v["kind"] == "inference").collect();
    assert_eq!(sends.len(), usize::from(success));
    assert!(sends.iter().all(|v| v["authorization"] == true));
    f.close().await;
    proxy.stop().await;
}
