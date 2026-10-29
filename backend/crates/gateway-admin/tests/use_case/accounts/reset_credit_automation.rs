//! 自动重置的持久幂等恢复、低频发现与账号独立错峰

use super::*;
use gateway_admin::{
    model::reset_credit_automation::*,
    ports::reset_credit_automation::ResetCreditAutomationStore as _,
};
fn enable_capability(_provider: &FakeProviderAdmin) {}

fn plan(expires_at: chrono::DateTime<Utc>) -> ResetCreditAutomation {
    ResetCreditAutomation {
        account_id: "acct_test".into(),
        enabled: true,
        revision: 1,
        next_run_at: Utc::now(),
        state: ResetCreditAutomationState {
            next_discovery_at: Utc::now() + TimeDelta::days(1),
            credits: vec![PlannedResetCredit {
                id: "card_1".into(),
                expires_at,
            }],
            pending: None,
            retry_at: None,
            failures: 0,
            last_result: None,
        },
    }
}

fn card(
    expires_at: chrono::DateTime<Utc>,
) -> gateway_admin::model::provider_credentials::ProviderResetCredit {
    gateway_admin::model::provider_credentials::ProviderResetCredit {
        id: "card_1".into(),
        status: Some("available".into()),
        expires_at: Some(expires_at),
        title: None,
        reset_type: None,
    }
}

#[tokio::test]
async fn automation_restores_pending_idempotency_after_service_restart() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events.clone());
    let expiry = Utc::now() + TimeDelta::minutes(5);
    provider.reset_credits.lock().unwrap().push(card(expiry));
    *provider.reset_credit_failure.lock().unwrap() = Some(ProviderAdminErrorKind::Ambiguous);
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), plan(expiry));
    let account_id = ProviderAccountId::new("acct_test").unwrap();
    let services = accounts_service(provider.clone(), store.clone()).await;
    services
        .accounts()
        .reconcile_reset_credit_automation(account_id.clone())
        .await
        .expect_err("unknown consume result");
    let persisted = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    let pending = persisted.state.pending.as_ref().unwrap();
    let command = provider.reset_credit_commands()[0].clone();
    assert_eq!(
        pending.redeem_request_id,
        command.redeem_request_id.to_string()
    );
    assert_eq!(command.redeem_request_id.get_version_num(), 4);
    assert_eq!(provider.quota_requests().len(), 0);
    services
        .accounts()
        .reconcile_reset_credit_automation(account_id.clone())
        .await
        .unwrap();
    assert_eq!(
        provider.reset_credit_commands().len(),
        1,
        "wait until persisted retry time"
    );
    store
        .automations
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .retry_at = Some(Utc::now() - TimeDelta::seconds(1));
    drop(services);
    let restarted = threshold_services(provider.clone(), store.clone(), Some(0)).await;
    restarted
        .accounts()
        .reconcile_reset_credit_automation(account_id)
        .await
        .unwrap();
    assert_eq!(provider.reset_credit_commands(), [command.clone(), command]);
    assert_eq!(
        recorded(&events)
            .iter()
            .filter(|e| **e == "provider.reset_credits")
            .count(),
        1
    );
    let record = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert!(record.state.pending.is_none());
    assert!(record.state.credits.is_empty());
    assert_eq!(record.state.last_result.as_deref(), Some("reset"));
    assert!(provider.quota_requests().iter().any(|q| q.refresh));
}

#[tokio::test]
async fn automation_only_queries_due_accounts_and_rechecks_credit_before_consume() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events.clone());
    let mut record = plan(Utc::now() + TimeDelta::hours(6));
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), record.clone());
    let services = accounts_service(provider.clone(), store.clone()).await;
    let account_id = ProviderAccountId::new("acct_test").unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(account_id.clone())
        .await
        .unwrap();
    assert!(!recorded(&events).contains(&"provider.reset_credits"));
    record.state.credits[0].expires_at = Utc::now() + TimeDelta::minutes(5);
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), record);
    services
        .accounts()
        .reconcile_reset_credit_automation(account_id)
        .await
        .unwrap();
    assert!(recorded(&events).contains(&"provider.reset_credits"));
    assert!(
        provider.reset_credit_commands().is_empty(),
        "disappeared credit must never be consumed"
    );
}

#[tokio::test]
async fn automation_defaults_off_and_disabled_pending_survives_reenable() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events.clone());
    let services = accounts_service(provider.clone(), store.clone()).await;
    let account_id = ProviderAccountId::new("acct_test").unwrap();
    assert!(
        !services
            .accounts()
            .reset_credit_automation_status(account_id.clone())
            .await
            .unwrap()
            .enabled
    );
    services
        .accounts()
        .set_reset_credit_automation(&context("enable"), account_id.clone(), true)
        .await
        .unwrap();
    let first = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert!(first.state.next_discovery_at >= Utc::now() - TimeDelta::seconds(1));
    let pending = PendingResetCredit {
        credit: PlannedResetCredit {
            id: "pending".into(),
            expires_at: Utc::now() + TimeDelta::hours(1),
        },
        redeem_request_id: uuid::Uuid::new_v4().to_string(),
    };
    store
        .automations
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .pending = Some(pending.clone());
    services
        .accounts()
        .set_reset_credit_automation(&context("disable"), account_id.clone(), false)
        .await
        .unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(account_id.clone())
        .await
        .unwrap();
    assert!(provider.reset_credit_commands().is_empty());
    services
        .accounts()
        .set_reset_credit_automation(&context("reenable"), account_id, true)
        .await
        .unwrap();
    let next = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.state.pending, Some(pending));
    assert_eq!(next.state.next_discovery_at, first.state.next_discovery_at);
}

#[tokio::test]
async fn automation_schedules_each_account_inside_the_lead_window_and_next_local_day() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events);
    let now = Utc::now();
    let expiry = now + TimeDelta::days(2);
    provider.reset_credits.lock().unwrap().push(card(expiry));
    let accounts = (0..100)
        .map(|n| {
            let mut account = account_record("openai");
            account.id = format!("acct_{n}");
            let mut record = plan(expiry);
            record.account_id = account.id.clone();
            record.state.next_discovery_at = now - TimeDelta::seconds(1);
            store
                .automations
                .lock()
                .unwrap()
                .insert(account.id.clone(), record);
            account
        })
        .collect::<Vec<_>>();
    store.set_accounts(accounts.clone());
    let services = accounts_service(provider, store.clone()).await;
    use chrono::TimeZone;
    let tomorrow = now
        .with_timezone(&chrono::Local)
        .date_naive()
        .succ_opt()
        .unwrap();
    let start = chrono::Local
        .from_local_datetime(&tomorrow.and_hms_opt(0, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .with_timezone(&Utc);
    let end = chrono::Local
        .from_local_datetime(&tomorrow.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .with_timezone(&Utc);
    let mut times = std::collections::BTreeSet::new();
    for account in accounts {
        let id = ProviderAccountId::new(account.id.clone()).unwrap();
        services
            .accounts()
            .reconcile_reset_credit_automation(id.clone())
            .await
            .unwrap();
        let status = services
            .accounts()
            .reset_credit_automation_status(id)
            .await
            .unwrap();
        let discovery = status.next_discovery_at.unwrap();
        assert!(discovery >= start && discovery < end);
        times.insert(status.next_consume_at.unwrap());
    }
    assert!(
        times.len() > 30,
        "accounts must not all fire at the same instant"
    );
    assert!(times.iter().all(
        |due| *due >= expiry - TimeDelta::minutes(10) && *due < expiry - TimeDelta::minutes(9)
    ));
}

#[tokio::test]
async fn automation_discards_externally_used_credit_and_stops_on_already_redeemed() {
    for already_redeemed_after_query in [false, true] {
        let events = events();
        let provider = FakeProviderAdmin::new("openai", events.clone());
        enable_capability(&provider);
        let store = FakeAccountStore::new("openai", events.clone());
        let expiry = Utc::now() + TimeDelta::minutes(5);
        let mut remote_credit = card(expiry);
        if !already_redeemed_after_query {
            remote_credit.status = Some("redeemed".into());
        }
        provider.reset_credits.lock().unwrap().push(remote_credit);
        *provider.reset_credit_result.lock().unwrap() = "already_redeemed".into();
        store
            .automations
            .lock()
            .unwrap()
            .insert("acct_test".into(), plan(expiry));
        let services = accounts_service(provider.clone(), store.clone()).await;
        let account_id = ProviderAccountId::new("acct_test").unwrap();
        services
            .accounts()
            .reconcile_reset_credit_automation(account_id.clone())
            .await
            .unwrap();
        services
            .accounts()
            .reconcile_reset_credit_automation(account_id)
            .await
            .unwrap();
        assert_eq!(
            provider.reset_credit_commands().len(),
            usize::from(already_redeemed_after_query)
        );
        let stored = store
            .reset_credit_automation("acct_test")
            .await
            .unwrap()
            .unwrap();
        assert!(stored.state.credits.is_empty());
        assert!(stored.state.pending.is_none());
        if already_redeemed_after_query {
            assert_eq!(
                stored.state.last_result.as_deref(),
                Some("already_redeemed")
            );
        }
    }
}

#[tokio::test]
async fn automation_daily_discovery_is_not_polled_again_after_success_or_failure() {
    for failed in [false, true] {
        let events = events();
        let provider = FakeProviderAdmin::new("openai", events.clone());
        enable_capability(&provider);
        if failed {
            provider.fail_next(ProviderAdminErrorKind::Unavailable);
        }
        let store = FakeAccountStore::new("openai", events.clone());
        let mut record = plan(Utc::now() + TimeDelta::days(3));
        record.state.credits.clear();
        record.state.next_discovery_at = Utc::now() - TimeDelta::seconds(1);
        store
            .automations
            .lock()
            .unwrap()
            .insert("acct_test".into(), record);
        let services = accounts_service(provider.clone(), store.clone()).await;
        let id = ProviderAccountId::new("acct_test").unwrap();
        let first = services
            .accounts()
            .reconcile_reset_credit_automation(id.clone())
            .await;
        assert_eq!(first.is_err(), failed);
        let scheduled = store
            .reset_credit_automation("acct_test")
            .await
            .unwrap()
            .unwrap()
            .state
            .next_discovery_at;
        services
            .accounts()
            .reconcile_reset_credit_automation(id)
            .await
            .unwrap();
        assert_eq!(
            recorded(&events)
                .iter()
                .filter(|e| **e == "provider.reset_credits")
                .count(),
            1
        );
        assert_eq!(
            store
                .reset_credit_automation("acct_test")
                .await
                .unwrap()
                .unwrap()
                .state
                .next_discovery_at,
            scheduled
        );
        assert!(scheduled > Utc::now());
    }
}

#[tokio::test]
async fn automation_cancelled_discovery_preserves_next_random_time_across_restart() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events.clone());
    let mut record = plan(Utc::now() + TimeDelta::days(3));
    record.state.credits.clear();
    record.state.next_discovery_at = Utc::now() - TimeDelta::seconds(1);
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), record);
    let (release, gate) = tokio::sync::oneshot::channel();
    *provider.reset_credit_query_gate.lock().unwrap() = Some(gate);
    let services = accounts_service(provider.clone(), store.clone()).await;
    let task = tokio::spawn(async move {
        services
            .accounts()
            .reconcile_reset_credit_automation(ProviderAccountId::new("acct_test").unwrap())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        provider.reset_credit_query_started.notified(),
    )
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(release);
    let before = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert!(before.state.next_discovery_at > Utc::now());
    let restarted = accounts_service(provider.clone(), store.clone()).await;
    restarted
        .accounts()
        .reconcile_reset_credit_automation(ProviderAccountId::new("acct_test").unwrap())
        .await
        .unwrap();
    let after = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before.state.next_discovery_at,
        after.state.next_discovery_at
    );
    assert_eq!(
        recorded(&events)
            .iter()
            .filter(|e| **e == "provider.reset_credits")
            .count(),
        1
    );
}

#[tokio::test]
async fn disabled_automation_only_blocks_manual_consume_until_pending_credit_expires() {
    let events = events();
    let provider = FakeProviderAdmin::new("openai", events.clone());
    enable_capability(&provider);
    let store = FakeAccountStore::new("openai", events);
    let mut record = plan(Utc::now() + TimeDelta::minutes(5));
    record.enabled = false;
    record.state.pending = Some(PendingResetCredit {
        credit: record.state.credits[0].clone(),
        redeem_request_id: uuid::Uuid::new_v4().to_string(),
    });
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), record.clone());
    let services = accounts_service(provider.clone(), store.clone()).await;
    let command = ConsumeProviderResetCredit {
        account_id: ProviderAccountId::new("acct_test").unwrap(),
        credit_id: Some("another_card".into()),
        redeem_request_id: uuid::Uuid::new_v4(),
    };
    services
        .accounts()
        .consume_reset_credit(&context("manual-with-pending"), command.clone())
        .await
        .expect_err("unresolved automation is still active");
    assert!(provider.reset_credit_commands().is_empty());
    record.state.pending.as_mut().unwrap().credit.expires_at = Utc::now() - TimeDelta::seconds(1);
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), record);
    services
        .accounts()
        .consume_reset_credit(&context("manual-with-pending"), command)
        .await
        .unwrap();
    assert_eq!(provider.reset_credit_commands().len(), 1);
}

async fn threshold_services(
    provider: Arc<FakeProviderAdmin>,
    store: Arc<FakeAccountStore>,
    max_remaining: Option<u32>,
) -> AdminServices {
    super::super::AdminHarness::new()
        .accounts(store)
        .settings(Arc::new(StaticSettingsStore {
            reset_credit_max_weekly_remaining_percent: max_remaining,
        }))
        .provider(provider)
        .probe(Arc::new(SuccessfulAccountProbe))
        .build()
        .await
}

fn weekly_quota(used_percent: f64) -> ProviderQuota {
    ProviderQuota {
        windows: vec![ProviderQuotaWindow {
            key: "week".into(),
            group: "shortTerm".into(),
            label: "周额度".into(),
            limit_id: None,
            limit_name: None,
            role: None,
            local_usage_attribution: QuotaLocalUsageAttribution::AccountWide,
            window_seconds: Some(7 * 86_400),
            used_percent: Some(used_percent),
            estimate_hint: None,
            estimated_quota: None,
            reset_at: Some(Utc::now() + TimeDelta::days(1)),
            limit_reached: false,
            local_usage: None,
            provider_data: None,
        }],
        ..empty_quota()
    }
}

fn schedule_due_credit(provider: &FakeProviderAdmin, store: &FakeAccountStore) {
    enable_capability(provider);
    let expiry = Utc::now() + TimeDelta::minutes(5);
    provider.reset_credits.lock().unwrap().push(card(expiry));
    store
        .automations
        .lock()
        .unwrap()
        .insert("acct_test".into(), plan(expiry));
}

fn advance_retry(store: &FakeAccountStore) {
    store
        .automations
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .retry_at = Some(Utc::now() - TimeDelta::seconds(1));
}

#[tokio::test]
async fn automation_weekly_threshold_is_optional_inclusive_and_uses_remaining_quota() {
    for (threshold, used, consume) in [
        (None, 0.0, true),
        (Some(70), 29.9, false),
        (Some(70), 30.0, true),
        (Some(0), 99.9, false),
        (Some(0), 100.0, true),
        (Some(100), 0.0, true),
    ] {
        let provider = FakeProviderAdmin::new("openai", events());
        let store = FakeAccountStore::new("openai", events());
        schedule_due_credit(&provider, &store);
        provider.set_quota(weekly_quota(used));
        let services = threshold_services(provider.clone(), store.clone(), threshold).await;
        services
            .accounts()
            .reconcile_reset_credit_automation(ProviderAccountId::new("acct_test").unwrap())
            .await
            .unwrap();
        assert_eq!(
            !provider.reset_credit_commands().is_empty(),
            consume,
            "threshold={threshold:?}, used={used}"
        );
        let quota_requests = provider.quota_requests();
        // 消费成功已有一次回读；未设置上限时不增加消费前查询
        assert_eq!(
            quota_requests.len(),
            usize::from(threshold.is_some()) + usize::from(consume)
        );
        assert!(quota_requests.iter().all(|request| request.refresh));
        let record = store
            .reset_credit_automation("acct_test")
            .await
            .unwrap()
            .unwrap();
        assert!(record.state.pending.is_none());
        assert_eq!(record.state.credits.is_empty(), consume);
        if !consume {
            assert_eq!(
                record.state.last_result.as_deref(),
                Some("weekly_quota_above_threshold")
            );
            assert!(record.state.retry_at.unwrap() > Utc::now());
            assert!(record.state.retry_at.unwrap() <= record.state.credits[0].expires_at);
        }
    }
}

#[tokio::test]
async fn automation_retains_skipped_credit_and_refreshes_quota_on_next_attempt() {
    let provider = FakeProviderAdmin::new("openai", events());
    let store = FakeAccountStore::new("openai", events());
    schedule_due_credit(&provider, &store);
    provider.set_quota(weekly_quota(10.0));
    let services = threshold_services(provider.clone(), store.clone(), Some(70)).await;
    let id = ProviderAccountId::new("acct_test").unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(id.clone())
        .await
        .unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(id.clone())
        .await
        .unwrap();
    assert!(provider.reset_credit_commands().is_empty());
    assert_eq!(provider.quota_requests().len(), 1);
    provider.set_quota(weekly_quota(40.0));
    advance_retry(&store);
    services
        .accounts()
        .reconcile_reset_credit_automation(id)
        .await
        .unwrap();
    assert_eq!(provider.reset_credit_commands().len(), 1);
    let record = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert!(record.state.credits.is_empty());
    assert!(record.state.retry_at.is_none());
}

#[tokio::test]
async fn automation_defers_missing_invalid_or_failed_weekly_quota() {
    let weekly = weekly_quota(90.0).windows.remove(0);
    let mut short = weekly.clone();
    short.window_seconds = Some(18_000);
    let mut model = weekly.clone();
    model.local_usage_attribution = QuotaLocalUsageAttribution::Unavailable;
    let mut month = weekly.clone();
    month.group = "monthly".into();
    month.window_seconds = Some(30 * 86_400);
    let mut missing_percent = weekly.clone();
    missing_percent.used_percent = None;
    for (quota, failure) in [
        (empty_quota(), None),
        (
            ProviderQuota {
                windows: vec![short, model, month],
                ..empty_quota()
            },
            None,
        ),
        (
            ProviderQuota {
                windows: vec![missing_percent],
                ..empty_quota()
            },
            None,
        ),
        (weekly_quota(f64::NAN), None),
        (
            weekly_quota(90.0),
            Some(ProviderAdminErrorKind::Unavailable),
        ),
    ] {
        let provider = FakeProviderAdmin::new("openai", events());
        let store = FakeAccountStore::new("openai", events());
        schedule_due_credit(&provider, &store);
        provider.set_quota(quota);
        *provider.quota_failure.lock().unwrap() = failure;
        let services = threshold_services(provider.clone(), store.clone(), Some(70)).await;
        let result = services
            .accounts()
            .reconcile_reset_credit_automation(ProviderAccountId::new("acct_test").unwrap())
            .await;
        assert_eq!(result.is_err(), failure.is_some());
        assert!(provider.reset_credit_commands().is_empty());
        let record = store
            .reset_credit_automation("acct_test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.state.last_result.as_deref(),
            Some("weekly_quota_unavailable")
        );
        assert!(record.state.pending.is_none());
        assert_eq!(record.state.credits.len(), 1);
        assert!(record.state.retry_at.is_some());
    }
}

#[tokio::test]
async fn automation_nothing_to_reset_retains_credit_and_retries_with_a_new_key() {
    let provider = FakeProviderAdmin::new("openai", events());
    let store = FakeAccountStore::new("openai", events());
    schedule_due_credit(&provider, &store);
    *provider.reset_credit_result.lock().unwrap() = "nothing_to_reset".into();
    let services = accounts_service(provider.clone(), store.clone()).await;
    let id = ProviderAccountId::new("acct_test").unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(id.clone())
        .await
        .unwrap();
    let record = store
        .reset_credit_automation("acct_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.state.last_result.as_deref(),
        Some("nothing_to_reset")
    );
    assert!(record.state.pending.is_none());
    assert_eq!(record.state.credits.len(), 1);
    services
        .accounts()
        .reconcile_reset_credit_automation(id.clone())
        .await
        .unwrap();
    assert_eq!(provider.reset_credit_commands().len(), 1);
    advance_retry(&store);
    *provider.reset_credit_result.lock().unwrap() = "reset".into();
    services
        .accounts()
        .reconcile_reset_credit_automation(id)
        .await
        .unwrap();
    let commands = provider.reset_credit_commands();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0].credit_id, commands[1].credit_id);
    assert_ne!(commands[0].redeem_request_id, commands[1].redeem_request_id);
    assert!(
        store
            .reset_credit_automation("acct_test")
            .await
            .unwrap()
            .unwrap()
            .state
            .credits
            .is_empty()
    );
}

#[tokio::test]
async fn automatic_success_keeps_native_subscription_reset_and_does_not_repeat_it() {
    let provider = FakeProviderAdmin::new("openai", events());
    let store = FakeAccountStore::new("openai", events());
    schedule_due_credit(&provider, &store);
    let services = accounts_service(provider.clone(), store.clone()).await;
    let id = ProviderAccountId::new("acct_test").unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(id.clone())
        .await
        .unwrap();
    services
        .accounts()
        .reconcile_reset_credit_automation(id)
        .await
        .unwrap();
    let commands = provider.reset_credit_commands();
    assert_eq!(commands.len(), 1);
    assert_eq!(
        *store.reset_events.lock().unwrap(),
        vec![(
            "acct_test".into(),
            commands[0].redeem_request_id.to_string()
        )]
    );
}

#[tokio::test]
async fn disabled_accounts_do_not_query_or_consume_reset_cards() {
    let provider = FakeProviderAdmin::new("openai", events());
    let store = FakeAccountStore::new("openai", events());
    schedule_due_credit(&provider, &store);
    store.accounts.lock().unwrap()[0].enabled = false;
    let services = accounts_service(provider.clone(), store).await;
    services
        .accounts()
        .reconcile_reset_credit_automation(ProviderAccountId::new("acct_test").unwrap())
        .await
        .unwrap();
    assert!(provider.reset_credit_commands().is_empty());
    assert!(provider.quota_requests().is_empty());
    assert!(
        !provider
            .events
            .lock()
            .unwrap()
            .contains(&"provider.reset_credits")
    );
}
