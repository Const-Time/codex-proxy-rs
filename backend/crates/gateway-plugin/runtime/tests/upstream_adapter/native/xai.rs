use super::fixture::Fixture;
use async_trait::async_trait;
use chrono::Utc;
use gateway_core::{
    account::{AccountFeedbackStats, CredentialState, ProviderAccountId},
    engine::{ExecutionOutcome, provider::Provider},
    provider_ports::ProviderStorePorts,
    routing::ProviderKind,
    upstream::UpstreamSendState,
};
use provider_xai::*;
use std::{sync::Arc, time::Duration};

pub(super) async fn seed(ports: &ProviderStorePorts, id: &ProviderAccountId, access: &str) {
    let input = GrokCredentialAdmin
        .prepare_import(&CreateGrokCredential {
            account_id: id.clone(),
            name: id.to_string(),
            secret: GrokOAuthSecret {
                access_token: SecretValue::new(access),
                refresh_token: SecretValue::new("fixture-refresh-token"),
                id_token: None,
                scope: OFFICIAL_SCOPES.join(" "),
            },
            account: GrokAccountProfile {
                subject: format!("subject-{id}"),
                email: None,
                upstream_account_id: None,
                plan_type: Some("pro".into()),
                access_token_expires_at: Utc::now() + chrono::Duration::hours(1),
                refresh_token_expires_at: None,
            },
            enabled: true,
            initial_credential_state: CredentialState::Ready,
            initial_error_reason: None,
            initial_error_message: None,
        })
        .unwrap();
    ports.accounts().create_account(input).await.unwrap();
}

// All native services use their real stores. Only external network ports are replaced:
// an accidental fallback to the official inference endpoint fails the test before sending.
struct NoExternalNetwork;
impl GrokInferenceTransport for NoExternalNetwork {
    fn execute(&self, _: GrokInferenceRequest) -> GrokInferenceTransportFuture<'_> {
        Box::pin(async { panic!("adapter request must not fall through to native xAI inference") })
    }
}
impl GrokModelCatalogTransport for NoExternalNetwork {
    fn execute(&self, _: GrokModelCatalogRequest) -> GrokModelCatalogTransportFuture<'_> {
        Box::pin(async { panic!("native adapter fixture must not fetch an external catalog") })
    }
}
impl GrokBillingTransport for NoExternalNetwork {
    fn execute(&self, _: GrokBillingRequest) -> GrokBillingTransportFuture<'_> {
        Box::pin(async { panic!("request scheduling must use the original quota projection") })
    }
}
#[async_trait]
impl GrokCredentialRefresher for NoExternalNetwork {
    async fn prepare_cycle(&self) -> Result<(), GrokRefreshFailure> {
        Err(GrokRefreshFailure::Transient)
    }
    async fn refresh(&self, _: &SecretValue) -> Result<GrokRefreshTokens, GrokRefreshFailure> {
        panic!("native adapter fixture must not rotate an external OAuth credential")
    }
}

pub(super) fn provider(ports: &ProviderStorePorts) -> Arc<dyn Provider> {
    let profile: XaiConfig = serde_json::from_value(serde_json::json!({"wire_profile":{
        "client_identifier":"grok-shell","client_version":"0.2.106","client_mode":"headless",
        "target_os":"linux","target_arch":"x86_64","verified_at":"2026-10-12T00:00:00Z"
    }}))
    .unwrap();
    let profile = profile.wire_profile_state();
    let repository = GrokCredentialRepository::new(ports.accounts());
    let cache = Arc::new(GrokCatalogCache::new(ports.catalog_cache()).unwrap());
    let network = Arc::new(NoExternalNetwork);
    let catalog = Arc::new(GrokCredentialCatalogService::new(
        repository.clone(),
        network.clone(),
        cache.clone(),
        profile.clone(),
    ));
    let quota = Arc::new(GrokCredentialQuotaService::new(
        repository.clone(),
        network.clone(),
        profile.clone(),
    ));
    let recovery = Arc::new(GrokCredentialRefreshService::new(
        repository.clone(),
        network.clone(),
        catalog.clone(),
        ports.leases(),
        ports.credential_state(),
        ports.runtime_policy(),
    ));
    let feedback = Arc::new(AccountFeedbackStats::default());
    let selector = Arc::new(GrokAccountSessionSelector::new(
        ProviderKind::new("xai").unwrap(),
        repository,
        cache,
        quota,
        ports.leases(),
        ports.cooldowns(),
        feedback.clone(),
    ));
    Arc::new(
        GrokBuildProvider::new(selector, network, catalog, recovery, feedback, profile).unwrap(),
    )
}

#[tokio::test]
async fn real_core_xai_and_sdk_adapter_use_native_auth_scope_and_observed_pricing() {
    let Some(f) = Fixture::for_provider("xai").await else {
        return;
    };
    for (index, mode) in ["http", "http-stream"].into_iter().enumerate() {
        let mut started = f.start(mode).await;
        assert_eq!(f.server.requests.lock().unwrap().len(), index);
        let events = started.session.collect_uncommitted().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(started.session.send_state(), UpstreamSendState::Sent);
        started.session.commit_downstream(Some(200)).await.unwrap();
        started.session.detach_finalize().await;
        f.clean(index + 1).await;
        let rows = f.core.finalizations.lock().unwrap();
        let row = &rows[index];
        assert_eq!(row.outcome, ExecutionOutcome::Succeeded);
        assert_eq!(row.attempt_count, 1);
        assert_eq!(row.usage.input_tokens, Some(10));
        assert_eq!(row.usage.output_tokens, Some(2));
        assert_eq!(row.service_tier.as_deref(), Some("priority"));
        assert_eq!(row.cost.total().unwrap().amount().scaled(), 570_000);
        assert_eq!(
            f.core.charges.lock().unwrap()[index].amount_usd.scaled(),
            570_000
        );
        let requests = f.server.requests.lock().unwrap();
        assert_eq!(requests.len(), index + 1);
        assert!(requests[index].contains("authorization: Bearer fixture-provider-secret"));
        assert!(requests[index].contains(&format!("x-grok-user-id: subject-{}", f.account)));
        assert!(requests[index].contains("x-xai-token-auth: xai-grok-cli"));
        assert!(!requests[index].contains("chatgpt-account-id"));
        assert!(!requests[index].contains("fixture-forbidden-secret"));
        assert!(!requests[index].contains("cookie:"));
    }
    assert_eq!(f.core.attempts.lock().unwrap().len(), 2);
    f.close().await;
}

#[tokio::test]
async fn real_core_xai_adapter_tail_fault_preserves_cost_and_sent_failure_without_replay() {
    let Some(f) = Fixture::for_provider("xai").await else {
        return;
    };
    let mut started = f.start("tail-fault").await;
    assert!(started.session.collect_uncommitted().await.is_err());
    started.session.detach_finalize().await;
    f.clean(1).await;
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(rows[0].outcome, ExecutionOutcome::Failed);
        assert_eq!(rows[0].attempt_count, 1);
        assert_eq!(rows[0].send_state, UpstreamSendState::Sent);
        assert_eq!(rows[0].cost.total().unwrap().amount().scaled(), 570_000);
    }
    assert_eq!(
        f.core.charges.lock().unwrap()[0].amount_usd.scaled(),
        570_000
    );
    assert!(
        f.ports
            .cooldowns()
            .read(&f.account)
            .await
            .unwrap()
            .is_none(),
        "a plugin RPC tail fault must not penalize the native account"
    );
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.close().await;
}

#[tokio::test]
async fn real_core_xai_adapter_key_revocation_releases_native_account_and_settles_once() {
    let Some(f) = Fixture::for_provider("xai").await else {
        return;
    };
    let mut started = f.start("wait").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), started.session.next_event())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), f.server.entered.notified())
        .await
        .unwrap();
    assert_eq!(f.in_flight().await, 1);
    f.revoke_key();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            started.session.collect_uncommitted()
        )
        .await
        .unwrap()
        .is_err()
    );
    started.session.detach_finalize().await;
    f.clean(1).await;
    assert_eq!(
        f.core.finalizations.lock().unwrap()[0].outcome,
        ExecutionOutcome::Cancelled
    );
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.close().await;
}

#[tokio::test]
async fn real_core_xai_adapter_cancel_and_plugin_disable_release_native_lease() {
    let Some(f) = Fixture::for_provider("xai").await else {
        return;
    };
    for (index, revoke) in [false, true].into_iter().enumerate() {
        let mut started = f.start("wait").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), started.session.next_event())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(3), f.server.entered.notified())
            .await
            .unwrap();
        assert_eq!(f.in_flight().await, 1);
        if revoke {
            f.disable().await;
        } else {
            started.session.cancel();
        }
        assert!(
            tokio::time::timeout(
                Duration::from_secs(3),
                started.session.collect_uncommitted()
            )
            .await
            .unwrap()
            .is_err()
        );
        started.session.detach_finalize().await;
        f.clean(index + 1).await;
        assert_eq!(
            f.core.finalizations.lock().unwrap()[index].outcome,
            ExecutionOutcome::Cancelled
        );
        assert_eq!(f.core.charges.lock().unwrap()[index].amount_usd.scaled(), 0);
        assert_eq!(f.server.requests.lock().unwrap().len(), index + 1);
    }
    f.close().await;
}

#[tokio::test]
async fn real_core_xai_adapter_upstream_failure_uses_native_cooldown_feedback() {
    let Some(f) = Fixture::for_provider("xai").await else {
        return;
    };
    let mut started = f.start("failure").await;
    assert!(started.session.collect_uncommitted().await.is_err());
    started.session.detach_finalize().await;
    f.clean(1).await;
    {
        let rows = f.core.finalizations.lock().unwrap();
        assert_eq!(rows[0].outcome, ExecutionOutcome::Failed);
        assert_eq!(rows[0].attempt_count, 1);
        assert_eq!(rows[0].send_state, UpstreamSendState::Sent);
    }
    assert!(
        f.ports
            .cooldowns()
            .read(&f.account)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        f.ports
            .cooldowns()
            .read(&f.forbidden)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.core.charges.lock().unwrap()[0].amount_usd.scaled(), 0);
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    f.close().await;
}
