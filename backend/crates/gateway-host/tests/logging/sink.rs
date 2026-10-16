use super::*;
use gateway_core::health::HealthState;
use std::time::Duration;

#[test]
fn application_overload_reports_loss_without_writing_partial_json() {
    if env::var_os(CHILD_PROCESS_ENV).is_some() {
        let directory = PathBuf::from(env::var_os(LOG_DIRECTORY_ENV).unwrap());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let bundle = runtime
            .block_on(gateway_host::initialize(logging_config(directory, false)))
            .unwrap();
        let health = bundle.logging_health_probe();
        let payload = "x".repeat(128 * 1024);
        tracing::info!(target: APPLICATION_LOG_TARGET, payload, "oversized record");
        tracing::info!(target: APPLICATION_LOG_TARGET, "following complete record");
        assert!(
            matches!(runtime.block_on(health.check()), HealthState::Degraded(message) if message.contains("1 records dropped"))
        );
        drop(bundle);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "logging::sink::application_overload_reports_loss_without_writing_partial_json",
        ])
        .env(CHILD_PROCESS_ENV, "1")
        .env(LOG_DIRECTORY_ENV, directory.path())
        .env("RUST_LOG", "info")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = read_log_file_set(directory.path(), APPLICATION_LOG_FILE_PREFIX);
    for line in body.lines() {
        serde_json::from_str::<serde_json::Value>(line).expect("complete JSON event");
    }
    assert!(body.contains("following complete record"));
    assert!(!body.contains("oversized record"));
}

#[test]
fn file_queue_drains_every_record_before_normal_shutdown() {
    if env::var_os(CHILD_PROCESS_ENV).is_some() {
        with_file_logging(
            PathBuf::from(env::var_os(LOG_DIRECTORY_ENV).unwrap()),
            true,
            || {
                let payload = "x".repeat(16 * 1024);
                for sequence in 0..2000 {
                    tracing::info!(target: REQUEST_DUMP_LOG_TARGET, sequence, payload, "queue record");
                }
            },
        );
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "logging::sink::file_queue_drains_every_record_before_normal_shutdown",
        ])
        .env(CHILD_PROCESS_ENV, "1")
        .env(LOG_DIRECTORY_ENV, directory.path())
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = read_log_file_set(directory.path(), REQUEST_DUMP_LOG_FILE_PREFIX);
    let mut sequences = body
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"]["sequence"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, (0..2000).collect::<Vec<_>>());
}

#[test]
fn write_failure_marks_log_completeness_unhealthy_even_after_writes_recover() {
    if env::var_os(CHILD_PROCESS_ENV).is_some() {
        let directory = PathBuf::from(env::var_os(LOG_DIRECTORY_ENV).unwrap());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let bundle = runtime
            .block_on(gateway_host::initialize(logging_config(
                directory.clone(),
                true,
            )))
            .unwrap();
        let health = bundle.logging_health_probe();
        assert_eq!(runtime.block_on(health.check()), HealthState::Healthy);
        let blocked = directory.join(format!(
            "{REQUEST_DUMP_LOG_FILE_PREFIX}{}.1.log",
            chrono::Utc::now().date_naive()
        ));
        fs::create_dir(&blocked).unwrap();
        let payload = "x".repeat(1024 * 1024);
        tracing::info!(target: REQUEST_DUMP_LOG_TARGET, payload, marker="before-failure");
        tracing::info!(target: REQUEST_DUMP_LOG_TARGET, payload, marker="cannot-write");
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if matches!(health.check().await, HealthState::Unhealthy(_)) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        });
        fs::remove_dir(&blocked).unwrap();
        tracing::info!(target: REQUEST_DUMP_LOG_TARGET, marker="after-recovery");
        drop(bundle);
        assert!(matches!(
            runtime.block_on(health.check()),
            HealthState::Unhealthy(_)
        ));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env::current_exe().unwrap())
        .args(["--nocapture", "--exact", "logging::sink::write_failure_marks_log_completeness_unhealthy_even_after_writes_recover"])
        .env(CHILD_PROCESS_ENV, "1").env(LOG_DIRECTORY_ENV, directory.path())
        .env("RUST_LOG", "off").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("log completeness compromised"));
    let body = read_log_file_set(directory.path(), REQUEST_DUMP_LOG_FILE_PREFIX);
    assert!(body.contains("before-failure") && body.contains("after-recovery"));
    assert!(!body.contains("cannot-write"));
}
