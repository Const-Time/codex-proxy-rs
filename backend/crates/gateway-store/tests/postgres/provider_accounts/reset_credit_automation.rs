use super::*;
#[tokio::test]
async fn reset_credit_automation_persists_pending_checks_cas_and_follows_account_lifecycle() {
    use gateway_admin::{
        model::reset_credit_automation::*,
        ports::reset_credit_automation::ResetCreditAutomationStore as _,
    };
    let Some(database) = TestDatabase::create("reset_credit_automation").await else {
        return;
    };
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    repository
        .insert_provider_account(account("acct_automation", "user-automation"))
        .await
        .unwrap();
    let store = admin_account_store(&database.pool);
    let now = Utc::now();
    let mut record = ResetCreditAutomation {
        account_id: "acct_automation".into(),
        enabled: true,
        revision: 0,
        next_run_at: now + TimeDelta::hours(2),
        state: ResetCreditAutomationState {
            next_discovery_at: now + TimeDelta::days(1),
            credits: vec![],
            pending: Some(PendingResetCredit {
                credit: PlannedResetCredit {
                    id: "card_1".into(),
                    expires_at: now + TimeDelta::minutes(5),
                },
                redeem_request_id: uuid::Uuid::new_v4().to_string(),
            }),
            retry_at: Some(now + TimeDelta::seconds(30)),
            failures: 1,
            last_result: Some("pending".into()),
        },
    };
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "automation-test".into(),
    };
    record.revision = store
        .save_reset_credit_automation(&record, Some(&context))
        .await
        .unwrap();
    let loaded = store
        .reset_credit_automation(&record.account_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.state, record.state);
    use gateway_store::postgres::{PgRuntimeSettingsRepository, RuntimeSettingsRepository as _};
    let settings = PgRuntimeSettingsRepository::new(database.pool.clone());
    assert_eq!(
        settings
            .load_runtime_settings()
            .await
            .unwrap()
            .reset_credit_lead_minutes,
        10
    );
    let mut update = super::super::runtime_settings::settings_with_margin(3600);
    settings
        .update_runtime_settings(update.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .reset_credit_automation(&record.account_id)
            .await
            .unwrap()
            .unwrap(),
        loaded,
        "unrelated settings must not replan"
    );
    update.reset_credit_lead_minutes = Some(20);
    settings.update_runtime_settings(update).await.unwrap();
    let replanned = store
        .reset_credit_automation(&record.account_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replanned.state, record.state,
        "replan preserves pending idempotency key"
    );
    assert!(replanned.revision > record.revision);
    assert!(replanned.next_run_at <= Utc::now());
    record = replanned;

    assert_eq!(
        store
            .due_reset_credit_automations(now + TimeDelta::seconds(1), 1)
            .await
            .unwrap(),
        [record.account_id.clone()]
    );
    repository
        .update_provider_account(profile(&record.account_id, "updated profile"))
        .await
        .unwrap();
    repository
        .set_provider_account_enabled(&record.account_id, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .reset_credit_automation(&record.account_id)
            .await
            .unwrap()
            .unwrap(),
        record,
        "account edits preserve the automation plan"
    );
    assert!(
        store
            .due_reset_credit_automations(now + TimeDelta::seconds(1), 10)
            .await
            .unwrap()
            .is_empty()
    );
    repository
        .set_provider_account_enabled(&record.account_id, true)
        .await
        .unwrap();
    let stale = record.clone();
    record.enabled = false;
    record.revision = store
        .save_reset_credit_automation(&record, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .save_reset_credit_automation(&stale, None)
            .await
            .unwrap_err()
            .kind(),
        gateway_admin::ports::store::AdminStoreErrorKind::Conflict
    );
    assert!(
        store
            .due_reset_credit_automations(now + TimeDelta::seconds(1), 10)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query("delete from provider_accounts where id = $1")
        .bind(&record.account_id)
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(
        store
            .reset_credit_automation(&record.account_id)
            .await
            .unwrap()
            .is_none()
    );
    database.close().await;
}
