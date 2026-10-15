//! Native adoption uses the actual selected Core/Provider/SDK account lease.
use super::fixture::Fixture;
use gateway_core::{
    account::CredentialRevision,
    engine::{execution::StartedExecution, upstream_adapter::UpstreamAccountConnection},
};
use std::{sync::Arc, time::Duration};

async fn live(f: &Fixture) -> (StartedExecution, Arc<dyn UpstreamAccountConnection>) {
    let (plan, selected) = super::capture::plan(f);
    let mut started = f.start_with_adapter_plan("wait", plan).await;
    super::fixture::drive_until(&mut started, &f.server.entered, Duration::from_secs(5)).await;
    let account = selected
        .lock()
        .unwrap()
        .take()
        .expect("native Provider connection was selected")
        .account;
    assert_eq!(account.account_id(), &f.account);
    assert_eq!(account.credential_revision().get(), 1);
    assert_eq!(f.in_flight().await, 1);
    (started, account)
}
fn revision(value: u64) -> CredentialRevision {
    CredentialRevision::new(value).unwrap()
}
fn has_token(account: &dyn UpstreamAccountConnection, token: &str) -> bool {
    account.authorization().unwrap().iter().any(|header| {
        header.name() == "authorization"
            && header.value().as_ref() == format!("Bearer {token}").as_bytes()
    })
}
async fn finish(
    f: Fixture,
    mut started: StartedExecution,
    account: Arc<dyn UpstreamAccountConnection>,
) {
    started.session.cancel();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            started.session.collect_uncommitted()
        )
        .await
        .unwrap()
        .is_err()
    );
    started.session.detach_finalize().await;
    // The test observer still owns the same native capacity guard; it has not acquired a new one.
    assert_eq!(f.in_flight().await, 1);
    drop(account);
    f.clean(1).await;
    assert_eq!(f.server.requests.lock().unwrap().len(), 1);
    assert_eq!(f.core.finalizations.lock().unwrap()[0].attempt_count, 1);
    f.close().await;
}

#[tokio::test]
async fn native_credential_adoption_updates_both_provider_headers_on_the_original_capacity_lease() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let (started, account) = live(&f).await;
        assert!(has_token(account.as_ref(), "fixture-provider-secret"));
        let committed = f
            .advance_credential_with(|material| {
                assert!(material.contains_key("access_token"));
                material.insert(
                    "access_token".into(),
                    serde_json::json!("fixture-adopted-secret"),
                );
            })
            .await;
        account
            .credential_updates()
            .expect("native credential owner")
            .adopt(revision(1), committed)
            .await
            .unwrap();
        assert_eq!(account.credential_revision(), committed);
        assert!(has_token(account.as_ref(), "fixture-adopted-secret"));
        assert_eq!(f.in_flight().await, 1);
        assert!(
            account
                .credential_updates()
                .unwrap()
                .adopt(revision(1), revision(3))
                .await
                .is_err()
        );
        assert!(
            has_token(account.as_ref(), "fixture-adopted-secret"),
            "stale expected version cannot invalidate a newer confirmed epoch"
        );
        finish(f, started, account).await;
    }
}

#[tokio::test]
async fn native_credential_adoption_rejects_a_superseded_commit_and_never_restores_old_authorization()
 {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let (started, account) = live(&f).await;
        let committed = f.advance_credential_with(|_| {}).await;
        f.advance_credential().await;
        assert!(
            account
                .credential_updates()
                .unwrap()
                .adopt(revision(1), committed)
                .await
                .is_err()
        );
        assert_eq!(account.credential_revision(), revision(1));
        assert!(
            account.authorization().is_err(),
            "a confirmed commit with an unavailable epoch must fence the old material"
        );
        finish(f, started, account).await;
    }
}

#[tokio::test]
async fn native_credential_adoption_rejects_account_identity_drift_on_the_same_record() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let (started, account) = live(&f).await;
        let committed = f.advance_credential_with(|_| {}).await;
        sqlx::query("update provider_accounts set upstream_user_id=$1 where id=$2")
            .bind("fixture-unrelated-identity")
            .bind(f.account.as_str())
            .execute(&f.env.pool)
            .await
            .unwrap();
        assert!(
            account
                .credential_updates()
                .unwrap()
                .adopt(revision(1), committed)
                .await
                .is_err()
        );
        assert!(account.authorization().is_err());
        assert_eq!(account.account_id(), &f.account);
        finish(f, started, account).await;
    }
    let Some(mut f) = Fixture::new().await else {
        return;
    };
    let (started, account) = live(&f).await;
    let committed = f
        .advance_credential_with(|material| {
            let principal = material
                .get_mut("principal")
                .unwrap()
                .as_object_mut()
                .unwrap();
            principal.insert(
                "oauth_subject".into(),
                serde_json::json!("fixture-unrelated-oauth-subject"),
            );
        })
        .await;
    assert!(
        account
            .credential_updates()
            .unwrap()
            .adopt(revision(1), committed)
            .await
            .is_err()
    );
    assert!(account.authorization().is_err());
    finish(f, started, account).await;
}

#[tokio::test]
async fn native_credential_adoption_uses_original_provider_codec_before_accepting_material() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let (started, account) = live(&f).await;
        let committed = f
            .advance_credential_with(|material| {
                material.insert("schema_version".into(), serde_json::json!(999));
            })
            .await;
        assert!(
            account
                .credential_updates()
                .unwrap()
                .adopt(revision(1), committed)
                .await
                .is_err()
        );
        assert!(account.authorization().is_err());
        finish(f, started, account).await;
    }
}

#[tokio::test]
async fn native_credential_adoption_drop_while_native_read_is_blocked_keeps_old_material_fenced() {
    for provider in ["openai", "xai"] {
        let Some(mut f) = Fixture::for_provider(provider).await else {
            return;
        };
        let (started, account) = live(&f).await;
        let committed = f.advance_credential_with(|_| {}).await;
        let mut lock = f.env.pool.begin().await.unwrap();
        sqlx::query("lock table provider_accounts in access exclusive mode")
            .execute(&mut *lock)
            .await
            .unwrap();
        let owner = account.clone();
        let pending = tokio::spawn(async move {
            owner
                .credential_updates()
                .unwrap()
                .adopt(revision(1), committed)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while account.authorization().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !pending.is_finished(),
            "native account read is blocked by the real table lock"
        );
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        lock.rollback().await.unwrap();
        assert_eq!(account.credential_revision(), revision(1));
        assert!(account.authorization().is_err());
        finish(f, started, account).await;
    }
}
