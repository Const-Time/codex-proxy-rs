use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use gateway_admin::model::{MutationActor, MutationContext};
use gateway_core::{
    account::OpaqueProviderData, provider_ports::ProviderRuntimePolicyPort, routing::ProviderKind,
};
use gateway_store::postgres::{
    PgRuntimeSettingsRepository, RuntimeSettingsRepository, RuntimeSettingsUpdate,
};
use serde_json::{Value, json};

use super::TestDatabase;

pub(super) fn settings_with_margin(refresh_margin_seconds: u64) -> RuntimeSettingsUpdate {
    RuntimeSettingsUpdate {
        admin_api_key: None,
        refresh_margin_seconds,
        refresh_concurrency: 2,
        max_concurrent_per_account: 3,
        request_interval_ms: 50,
        rotation_strategy: "smart".to_owned(),
        model_mappings: BTreeMap::from([
            ("gpt-5.4".to_owned(), "gpt-5.5".to_owned()),
            ("grok-latest".to_owned(), "grok-4.5".to_owned()),
        ]),
        min_codex_desktop_version: None,
        min_codex_cli_version: None,
        usage_retention_days: 31,
        ops_event_retention_days: 30,
        max_retry_after_seconds: None,
        reset_credit_lead_minutes: None,
        reset_credit_max_weekly_remaining_percent: None,
        subscription_auto_reset_enabled: None,
        audit_retention_days: 90,
    }
}

#[test]
fn runtime_settings_keep_account_rotation_global() {
    let settings = settings_with_margin(3_600);
    assert!(settings.validate().is_ok());
}

#[tokio::test]
async fn client_profile_write_is_audited_and_read_by_provider_runtime_policy() {
    let Some(redis_url) = crate::support::test_env("CPR_TEST_REDIS_URL") else {
        return;
    };
    let Some(database) = TestDatabase::create("client_profile").await else {
        return;
    };
    let mut database_url = url::Url::parse(
        &crate::support::test_env("CPR_TEST_DATABASE_URL").expect("test database URL"),
    )
    .unwrap();
    database_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={}", database.schema));
    let connection = |mut url: url::Url| {
        let password = url.password().expect("test password").to_owned();
        url.set_password(None).unwrap();
        json!({"url": url.as_str(), "password": password})
    };
    let mut config: gateway_store::StoreConfig = serde_json::from_value(json!({
        "database": connection(database_url),
        "redis": connection(url::Url::parse(&redis_url).unwrap()),
    }))
    .unwrap();
    let runtime = tempfile::tempdir().unwrap();
    config.resolve_and_validate(runtime.path()).unwrap();
    let bundle = gateway_store::initialize(config).await.unwrap();
    let settings = bundle.admin_ports().settings();
    let policy = bundle.provider_ports().runtime_policy();
    let provider = ProviderKind::new("antigravity").unwrap();
    assert!(
        policy
            .load_client_profile(&provider)
            .await
            .unwrap()
            .is_none()
    );

    let Value::Object(configuration) = json!({
        "client": "cli", "platform": "linux", "arch": null,
        "versionMode": "fixed", "clientVersion": "1.5.17"
    }) else {
        unreachable!();
    };
    let configuration = OpaqueProviderData::new(configuration);
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "client-profile-test".to_owned(),
    };
    let revision = settings
        .replace_client_profile(&provider, configuration.clone(), &context)
        .await
        .unwrap();
    PgRuntimeSettingsRepository::new(database.pool.clone())
        .update_runtime_settings(settings_with_margin(3600))
        .await
        .unwrap();
    assert_eq!(
        policy.load_client_profile(&provider).await.unwrap(),
        Some(configuration)
    );
    let audited_revision = sqlx::query_scalar::<_, i64>(
        "select config_revision from admin_audit_events
         where action = 'settings.client_profile.replace' and entity_ref = 'antigravity'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(u64::try_from(audited_revision).unwrap(), revision.get());
    drop(bundle);
    database.close().await;
}

#[tokio::test]
async fn maintenance_scheduling_policy_reads_current_shared_limits_and_interval() {
    let Some(database) = TestDatabase::create("maintenance_scheduling_policy").await else {
        return;
    };
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    for (limit, interval) in [(10, 250), (4, 0)] {
        let mut update = settings_with_margin(3600);
        update.max_concurrent_per_account = limit;
        update.request_interval_ms = interval;
        repository.update_runtime_settings(update).await.unwrap();
        let policy = repository.load_account_selection_policy().await.unwrap();
        assert_eq!(policy.max_concurrent_per_account().get(), limit);
        assert_eq!(
            policy.request_interval(),
            std::time::Duration::from_millis(interval)
        );
    }
    database.close().await;
}

#[tokio::test]
async fn subscription_auto_reset_setting_persists_and_omission_preserves_disabled_value() {
    let Some(database) = TestDatabase::create("subscription_reset_setting").await else {
        return;
    };
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    assert!(
        repository
            .load_runtime_settings()
            .await
            .unwrap()
            .subscription_auto_reset_enabled
    );
    let mut update = settings_with_margin(3600);
    update.subscription_auto_reset_enabled = Some(false);
    repository.update_runtime_settings(update).await.unwrap();
    assert!(
        !repository
            .load_runtime_settings()
            .await
            .unwrap()
            .subscription_auto_reset_enabled
    );
    repository
        .update_runtime_settings(settings_with_margin(1800))
        .await
        .unwrap();
    assert!(
        !repository
            .load_runtime_settings()
            .await
            .unwrap()
            .subscription_auto_reset_enabled
    );
    let mut update = settings_with_margin(1800);
    update.subscription_auto_reset_enabled = Some(true);
    repository.update_runtime_settings(update).await.unwrap();
    assert!(
        repository
            .load_runtime_settings()
            .await
            .unwrap()
            .subscription_auto_reset_enabled
    );
    database.close().await;
}

#[test]
fn runtime_settings_reject_invalid_model_mapping() {
    let settings = RuntimeSettingsUpdate {
        model_mappings: BTreeMap::from([("".to_owned(), "gpt-5.5".to_owned())]),
        ..settings_with_margin(3_600)
    };

    assert!(settings.validate().is_err());
}

#[test]
fn runtime_settings_reject_non_semver_client_min() {
    let settings = RuntimeSettingsUpdate {
        min_codex_cli_version: Some("v0.40.0".to_owned()),
        ..settings_with_margin(3_600)
    };

    assert!(settings.validate().is_err());
}

#[tokio::test]
async fn refresh_margin_change_should_preserve_existing_account_refresh_facts() {
    let Some(database) = TestDatabase::create("refresh_margin_reschedule").await else {
        return;
    };
    let expires_at = timestamp_micros(Utc::now() + TimeDelta::hours(2));
    insert_refreshable_account(
        &database.pool,
        "acct_refresh_margin_changed",
        expires_at,
        expires_at - TimeDelta::hours(1),
    )
    .await;
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    let before = account_refresh_facts(&database.pool, "acct_refresh_margin_changed").await;

    repository
        .update_runtime_settings(settings_with_margin(1_800))
        .await
        .expect("update refresh margin");

    assert_eq!(
        account_refresh_facts(&database.pool, "acct_refresh_margin_changed").await,
        before
    );
    database.close().await;
}

#[tokio::test]
async fn client_min_versions_should_round_trip_as_nullable_settings() {
    let Some(database) = TestDatabase::create("client_min_versions").await else {
        return;
    };
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    let mut update = settings_with_margin(3_600);
    update.min_codex_desktop_version = Some("26.825.6671".to_owned());
    update.min_codex_cli_version = Some("0.40.0".to_owned());

    repository
        .update_runtime_settings(update)
        .await
        .expect("update client min versions");
    let settings = repository
        .load_runtime_settings()
        .await
        .expect("load client min versions");

    assert_eq!(
        settings.min_codex_desktop_version.as_deref(),
        Some("26.825.6671")
    );
    assert_eq!(settings.min_codex_cli_version.as_deref(), Some("0.40.0"));
    database.close().await;
}

#[tokio::test]
async fn unchanged_refresh_margin_should_preserve_existing_account_refresh_facts() {
    let Some(database) = TestDatabase::create("refresh_margin_unchanged").await else {
        return;
    };
    let expires_at = timestamp_micros(Utc::now() + TimeDelta::hours(2));
    let retry_at = timestamp_micros(Utc::now() + TimeDelta::minutes(5));
    insert_refreshable_account(
        &database.pool,
        "acct_refresh_margin_unchanged",
        expires_at,
        retry_at,
    )
    .await;
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    let before = account_refresh_facts(&database.pool, "acct_refresh_margin_unchanged").await;

    repository
        .update_runtime_settings(settings_with_margin(3_600))
        .await
        .expect("update unrelated runtime settings");

    assert_eq!(
        account_refresh_facts(&database.pool, "acct_refresh_margin_unchanged").await,
        before
    );
    database.close().await;
}

async fn insert_refreshable_account(
    pool: &sqlx::PgPool,
    account_id: &str,
    expires_at: DateTime<Utc>,
    next_refresh_at: DateTime<Utc>,
) {
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, upstream_user_id, authentication_kind,
           provider_credentials_json, has_refresh_token, access_token_expires_at,
           next_refresh_at, credential_state, credential_observed_at, created_at, updated_at
         ) values ($1, 'openai', $1, $1, 'oauth', '{}'::jsonb, true, $2, $3,
                   'ready', now(), now(), now())",
    )
    .bind(account_id)
    .bind(expires_at)
    .bind(next_refresh_at)
    .execute(pool)
    .await
    .expect("insert refreshable account");
}

#[derive(Debug, PartialEq, Eq)]
struct AccountRefreshFacts {
    access_token_expires_at: DateTime<Utc>,
    next_refresh_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    credential_revision: i64,
}

async fn account_refresh_facts(pool: &sqlx::PgPool, account_id: &str) -> AccountRefreshFacts {
    let (access_token_expires_at, next_refresh_at, updated_at, credential_revision) =
        sqlx::query_as(
            "select access_token_expires_at, next_refresh_at, updated_at, credential_revision
         from provider_accounts
         where id = $1",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("load account refresh facts");
    AccountRefreshFacts {
        access_token_expires_at,
        next_refresh_at,
        updated_at,
        credential_revision,
    }
}

fn timestamp_micros(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros()).expect("valid test timestamp")
}

#[tokio::test]
async fn retry_after_limit_persists_zero_maximum_and_omission() {
    let Some(database) = TestDatabase::create("retry_after_setting").await else {
        return;
    };
    let repository = PgRuntimeSettingsRepository::new(database.pool.clone());
    assert_eq!(
        repository
            .load_runtime_settings()
            .await
            .unwrap()
            .max_retry_after_seconds,
        240
    );
    for cap in [0, 60, u32::MAX] {
        let mut update = settings_with_margin(3600);
        update.max_retry_after_seconds = Some(cap);
        repository.update_runtime_settings(update).await.unwrap();
        assert_eq!(
            repository
                .load_runtime_settings()
                .await
                .unwrap()
                .max_retry_after_seconds,
            cap
        );
        repository
            .update_runtime_settings(settings_with_margin(1800))
            .await
            .unwrap();
        assert_eq!(
            repository
                .load_runtime_settings()
                .await
                .unwrap()
                .max_retry_after_seconds,
            cap
        );
    }
    database.close().await;
}
