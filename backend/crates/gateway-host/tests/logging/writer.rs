use super::*;
use std::time::SystemTime;

#[test]
fn retention_preserves_complete_dates_across_compression_rotation_and_restart() {
    if env::var_os(CHILD_PROCESS_ENV).is_some() {
        with_file_logging(
            PathBuf::from(env::var_os(LOG_DIRECTORY_ENV).unwrap()),
            true,
            || {
                let payload = "x".repeat(1024 * 1024);
                for sequence in 0..25 {
                    tracing::info!(target: REQUEST_DUMP_LOG_TARGET, sequence, payload, "retention record");
                    tracing::info!(target: APPLICATION_LOG_TARGET, sequence, payload, "retention record");
                    tracing::info!(target: OAUTH_RECOVERY_LOG_TARGET, sequence, payload, "retention record");
                }
            },
        );
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let today = chrono::Utc::now().date_naive();
    let mut retained = Vec::new();
    let mut expired = Vec::new();
    for (prefix, days) in [
        (APPLICATION_LOG_FILE_PREFIX, 7),
        (OAUTH_RECOVERY_LOG_FILE_PREFIX, 7),
        (REQUEST_DUMP_LOG_FILE_PREFIX, 1),
    ] {
        let boundary = today - chrono::Days::new(days);
        for segment in 1..=25 {
            let path = directory
                .path()
                .join(format!("{prefix}{boundary}.{segment}.log"));
            seed_log(&path, boundary, &format!("boundary-{segment}\n"));
            retained.push(path);
        }
        let old_date = boundary - chrono::Days::new(1);
        let path = directory.path().join(format!("{prefix}{old_date}.log"));
        seed_log(&path, old_date, "expired date\n");
        expired.push(path);
        // If any old segment was recently written, protect its whole UTC date.
        let restored_date = old_date - chrono::Days::new(1);
        for segment in 0..=1 {
            let path = directory
                .path()
                .join(format!("{prefix}{restored_date}.{segment}.log"));
            seed_log(
                &path,
                if segment == 0 { today } else { restored_date },
                "restored date\n",
            );
            retained.push(path);
        }
    }
    let unrelated = directory.path().join("unmanaged.log");
    fs::write(&unrelated, "unmanaged\n").unwrap();
    // A crash before archive publication leaves the original and possibly a partial temporary file.
    fs::write(
        retained[0].with_extension("log.gz.tmp"),
        "unfinished archive",
    )
    .unwrap();
    for run in 1..=2 {
        let output = Command::new(env::current_exe().unwrap())
            .args(["--exact", "logging::writer::retention_preserves_complete_dates_across_compression_rotation_and_restart"])
            .env(CHILD_PROCESS_ENV, "1").env(LOG_DIRECTORY_ENV, directory.path())
            .env("RUST_LOG", "off,logging_test_application=info").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            retained
                .iter()
                .all(|path| path.with_extension("log.gz").exists())
        );
        assert!(
            expired
                .iter()
                .all(|path| !path.exists() && !path.with_extension("log.gz").exists())
        );
        assert!(unrelated.exists());

        for prefix in [
            APPLICATION_LOG_FILE_PREFIX,
            OAUTH_RECOVERY_LOG_FILE_PREFIX,
            REQUEST_DUMP_LOG_FILE_PREFIX,
        ] {
            let body = read_log_file_set(directory.path(), prefix);
            for sequence in 0..25 {
                assert_eq!(
                    body.matches(&format!(r#""sequence":{sequence},"#)).count(),
                    run
                );
            }
            for segment in 1..=25 {
                assert!(
                    body.contains(&format!("boundary-{segment}\n")),
                    "whole boundary date must survive"
                );
            }
        }
    }
}

fn seed_log(path: &Path, date: chrono::NaiveDate, body: &str) {
    fs::write(path, body).unwrap();
    let modified = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(0, 0, 0).unwrap(),
        chrono::Utc,
    );
    fs::File::open(path)
        .unwrap()
        .set_modified(SystemTime::from(modified))
        .unwrap();
}
