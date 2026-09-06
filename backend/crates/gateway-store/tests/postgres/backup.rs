//! `PgBackupRepository` 的真实 PostgreSQL 集成测试。

use chrono::Utc;
use secrecy::SecretString;

use gateway_admin::model::backup::{
    BackupRecordSeed, BackupSettings, BackupStatus, BackupTriggerKind, UpdateBackupScheduleCommand,
    UpdateBackupStorageCommand,
};
use gateway_admin::model::{MutationActor, MutationContext};
use gateway_admin::ports::backup::{BackupRepository, StatusTransitionUpdate};
use gateway_admin::ports::store::AdminStoreErrorKind;
use gateway_store::postgres::PgBackupRepository;

use super::TestDatabase;

fn seed(
    hex_suffix: &str,
    trigger: BackupTriggerKind,
    scheduled_at: Option<chrono::DateTime<Utc>>,
) -> BackupRecordSeed {
    let id = format!("backup_{hex_suffix}");
    BackupRecordSeed {
        id: id.clone(),
        trigger_kind: trigger,
        scheduled_at,
        object_key: format!("codex/2026/08/01/{id}.dump"),
        expires_at: None,
    }
}

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "store-test".to_owned(),
    }
}

fn storage_command(endpoint: &str) -> UpdateBackupStorageCommand {
    UpdateBackupStorageCommand {
        endpoint: endpoint.to_owned(),
        region: "auto".to_owned(),
        bucket: "backup-bucket".to_owned(),
        access_key_id: "ak".to_owned(),
        secret_access_key: Some(SecretString::from("sk-value")),
        prefix: "codex".to_owned(),
        force_path_style: false,
    }
}

async fn enable_verified_schedule(repository: &PgBackupRepository) -> BackupSettings {
    let (settings, _) = repository
        .update_storage_settings(storage_command("https://one.example.com"), &context())
        .await
        .expect("configure storage");
    repository
        .record_verification(settings.storage_revision, Utc::now())
        .await
        .expect("verify storage");
    repository
        .update_schedule_settings(
            UpdateBackupScheduleCommand {
                schedule_enabled: true,
                cron_expression: "0 2 * * *".to_owned(),
                schedule_timezone: "Asia/Shanghai".to_owned(),
                retention_days: 7,
                retention_count: 5,
            },
            Some(Utc::now() + chrono::Duration::hours(1)),
            &context(),
        )
        .await
        .expect("enable schedule")
}

#[tokio::test]
async fn storage_rotation_pauses_schedule_and_rejects_stale_verification() {
    let Some(db) = TestDatabase::create("backup_rotation").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());
    let before = enable_verified_schedule(&repository).await;
    let mut command = storage_command("https://one.example.com");
    command.secret_access_key = Some(SecretString::from("rotated-secret"));
    let (after, _) = repository
        .update_storage_settings(command, &context())
        .await
        .expect("rotate credentials while schedule is enabled");
    assert_eq!(after.storage_revision, before.storage_revision + 1);
    assert!(!after.schedule_enabled);
    assert!(after.next_run_at.is_none());
    assert!(after.last_verified_at.is_none());
    assert_eq!(after.cron_expression, before.cron_expression);
    assert!(
        !repository
            .record_verification(before.storage_revision, Utc::now())
            .await
            .expect("stale verification is ignored")
    );
    let changed: Vec<String> = sqlx::query_scalar(
        "select changed_fields from admin_audit_events
         where action = 'backup.s3_config_updated' and 'schedule_enabled' = any(changed_fields)",
    )
    .fetch_one(&db.pool)
    .await
    .expect("schedule pause is audited with storage change");
    for field in [
        "secret_access_key",
        "schedule_enabled",
        "next_run_at",
        "last_verified_at",
    ] {
        assert!(
            changed.iter().any(|value| value == field),
            "missing audited field {field}"
        );
    }
    db.close().await;
}

#[tokio::test]
async fn unchanged_storage_preserves_verification_schedule_and_revision() {
    let Some(db) = TestDatabase::create("backup_noop_save").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());
    let before = enable_verified_schedule(&repository).await;
    let revision: i64 = sqlx::query_scalar("select config_revision from runtime_settings")
        .fetch_one(&db.pool)
        .await
        .expect("current config revision");
    let audit_count: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(&db.pool)
        .await
        .expect("current audit count");
    for include_secret in [true, false] {
        let mut command = storage_command("https://one.example.com");
        if !include_secret {
            command.secret_access_key = None;
        }
        let (after, config_revision) = repository
            .update_storage_settings(command, &context())
            .await
            .expect("save unchanged storage");
        assert_eq!(after.storage_revision, before.storage_revision);
        assert_eq!(after.last_verified_at, before.last_verified_at);
        assert_eq!(after.next_run_at, before.next_run_at);
        assert!(after.schedule_enabled);
        assert_eq!(config_revision.get(), u64::try_from(revision).unwrap());
    }
    let current_count: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(&db.pool)
        .await
        .expect("read audit count after no-op");
    assert_eq!(current_count, audit_count);
    db.close().await;
}

#[tokio::test]
async fn backup_lifecycle_transitions_through_deleting_and_hard_delete() {
    let Some(db) = TestDatabase::create("backup_lifecycle").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());

    let record = repository
        .insert_backup_record(seed(&"a".repeat(32), BackupTriggerKind::Manual, None))
        .await
        .expect("insert queued");
    assert_eq!(record.status, BackupStatus::Queued);
    assert_eq!(record.attempt_count, 0);

    let claimed = repository
        .claim_next_queued(Utc::now())
        .await
        .expect("claim")
        .expect("claimed one task");
    assert_eq!(claimed.status, BackupStatus::Dumping);
    assert_eq!(claimed.attempt_count, 1);
    assert!(claimed.started_at.is_some());

    let update = StatusTransitionUpdate {
        size_bytes: Some(10),
        sha256: Some("a".repeat(64)),
        ..Default::default()
    };
    let uploading = repository
        .transition_status(
            &record.id,
            BackupStatus::Dumping,
            BackupStatus::Uploading,
            update,
            Utc::now(),
        )
        .await
        .expect("dump complete")
        .expect("transition matched");
    assert_eq!(uploading.status, BackupStatus::Uploading);
    assert_eq!(uploading.size_bytes, Some(10));

    let completed = repository
        .transition_status(
            &record.id,
            BackupStatus::Uploading,
            BackupStatus::Completed,
            StatusTransitionUpdate::default(),
            Utc::now(),
        )
        .await
        .expect("upload verified")
        .expect("transition matched");
    assert_eq!(completed.status, BackupStatus::Completed);
    assert!(completed.completed_at.is_some());

    let deleting = repository
        .transition_to_deleting(&record.id, Utc::now())
        .await
        .expect("to deleting")
        .expect("transition matched");
    assert_eq!(deleting.status, BackupStatus::Deleting);

    repository
        .delete_record(&record.id)
        .await
        .expect("hard delete");
    assert!(
        repository
            .load_backup_record(&record.id)
            .await
            .expect("load after delete")
            .is_none()
    );

    db.close().await;
}

#[tokio::test]
async fn active_task_unique_index_blocks_second_queued() {
    let Some(db) = TestDatabase::create("backup_active_conflict").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());

    repository
        .insert_backup_record(seed(&"a".repeat(32), BackupTriggerKind::Manual, None))
        .await
        .expect("first queued");
    let error = repository
        .insert_backup_record(seed(&"b".repeat(32), BackupTriggerKind::Manual, None))
        .await
        .expect_err("second queued must conflict");
    assert_eq!(error.kind(), AdminStoreErrorKind::Conflict);

    // 第一个任务完成释放名额后，第二个可以插入。
    let claimed = repository
        .claim_next_queued(Utc::now())
        .await
        .expect("claim")
        .expect("claimed");
    repository
        .transition_status(
            &claimed.id,
            BackupStatus::Dumping,
            BackupStatus::Failed,
            StatusTransitionUpdate {
                error_code: Some("backup.pg_dump_failed".to_owned()),
                error_message: Some("boom".to_owned()),
                completed_at: Some(Utc::now()),
                ..Default::default()
            },
            Utc::now(),
        )
        .await
        .expect("fail task");
    repository
        .insert_backup_record(seed(&"c".repeat(32), BackupTriggerKind::Manual, None))
        .await
        .expect("third queued after terminal");

    db.close().await;
}

#[tokio::test]
async fn scheduled_insert_dedupes_by_logical_time() {
    let Some(db) = TestDatabase::create("backup_scheduled_dedup").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());

    let at = Utc::now();
    let first = repository
        .insert_scheduled_record(seed(
            &"a".repeat(32),
            BackupTriggerKind::Scheduled,
            Some(at),
        ))
        .await
        .expect("first scheduled");
    assert!(first);
    let second = repository
        .insert_scheduled_record(seed(
            &"b".repeat(32),
            BackupTriggerKind::Scheduled,
            Some(at),
        ))
        .await
        .expect("dedup conflict returns false");
    assert!(!second);

    db.close().await;
}

#[tokio::test]
async fn storage_identity_is_locked_once_records_exist() {
    let Some(db) = TestDatabase::create("backup_storage_identity").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());

    repository
        .update_storage_settings(storage_command("https://one.example.com"), &context())
        .await
        .expect("configure storage with no records");

    let changed_endpoint = {
        let mut command = storage_command("https://two.example.com");
        command.endpoint = "https://two.example.com".to_owned();
        command
    };
    // 尚无记录时允许切换 endpoint。
    repository
        .update_storage_settings(changed_endpoint, &context())
        .await
        .expect("endpoint change allowed with no records");

    repository
        .insert_backup_record(seed(&"a".repeat(32), BackupTriggerKind::Manual, None))
        .await
        .expect("insert queued");

    let locked = repository
        .update_storage_settings(storage_command("https://three.example.com"), &context())
        .await
        .expect_err("endpoint change must be rejected with records");
    assert_eq!(locked.kind(), AdminStoreErrorKind::Conflict);

    // 只轮换凭据与修改 prefix 仍然允许。
    let rotation = {
        let mut command = storage_command("https://two.example.com");
        command.access_key_id = "new-ak".to_owned();
        command.secret_access_key = Some(SecretString::from("new-sk"));
        command.prefix = "codex/production".to_owned();
        command
    };
    repository
        .update_storage_settings(rotation, &context())
        .await
        .expect("credential rotation and prefix change allowed");

    db.close().await;
}

#[tokio::test]
async fn schedule_update_persists_cursor_and_clears_on_disable() {
    let Some(db) = TestDatabase::create("backup_schedule_update").await else {
        return;
    };
    let repository = PgBackupRepository::new(db.pool.clone());

    // 启用计划前必须先配置存储并通过连接测试（DB 约束要求）。
    repository
        .update_storage_settings(storage_command("https://one.example.com"), &context())
        .await
        .expect("configure storage");
    let settings = repository.load_settings().await.expect("load settings");
    repository
        .record_verification(settings.storage_revision, Utc::now())
        .await
        .expect("verify storage");

    let enabled = repository
        .update_schedule_settings(
            UpdateBackupScheduleCommand {
                schedule_enabled: true,
                cron_expression: "0 2 * * *".to_owned(),
                schedule_timezone: "Asia/Shanghai".to_owned(),
                retention_days: 7,
                retention_count: 5,
            },
            Some(Utc::now() + chrono::Duration::hours(1)),
            &context(),
        )
        .await
        .expect("enable schedule");
    assert!(enabled.schedule_enabled);
    assert!(enabled.next_run_at.is_some());

    let disabled = repository
        .update_schedule_settings(
            UpdateBackupScheduleCommand {
                schedule_enabled: false,
                cron_expression: "0 2 * * *".to_owned(),
                schedule_timezone: "Asia/Shanghai".to_owned(),
                retention_days: 7,
                retention_count: 5,
            },
            None,
            &context(),
        )
        .await
        .expect("disable schedule");
    assert!(!disabled.schedule_enabled);
    assert!(disabled.next_run_at.is_none());

    db.close().await;
}
