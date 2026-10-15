//! Actual composition root, native workers, SDK subprocess and operating-system signal.
use crate::{
    command_line::credentials::create,
    support::{self, database::Database},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Gateway(Child);
impl Drop for Gateway {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            let _ = Command::new("kill")
                .args(["-KILL", "--", &format!("-{}", self.0.id())])
                .status();
        }
        let _ = self.0.wait();
    }
}
fn binary() -> PathBuf {
    let backend = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .unwrap()
        .to_owned();
    let target =
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| backend.join("target"), PathBuf::from);
    let output = Command::new(env!("CARGO"))
        .current_dir(&backend)
        .args([
            "build",
            "-p",
            "codex-proxy-rs",
            "--bin",
            "codex-proxy-rs",
            "--locked",
            "--target-dir",
        ])
        .arg(&target)
        .env(
            "RUSTUP_TOOLCHAIN",
            option_env!("RUSTUP_TOOLCHAIN").unwrap_or("1.97.0"),
        )
        .env(
            "RUSTC",
            PathBuf::from(env!("CARGO")).with_file_name("rustc"),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "composition root must build: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    target.join("debug/codex-proxy-rs")
}
fn connection(value: &str) -> Value {
    let mut url = url::Url::parse(value).unwrap();
    let password = url.password().unwrap_or_default().to_owned();
    url.set_password(None).unwrap();
    json!({"url":url.as_str(), "password":password})
}
fn logs(directory: &std::path::Path) -> String {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_gateway_sigterm_drains_accepted_sdk_account_commit_before_process_exit() {
    use std::os::unix::process::CommandExt as _;
    let Some((database, database_url, redis_url)) = Database::create().await else {
        return;
    };
    for name in [
        "CPR_DATABASE_URL",
        "CPR_REDIS_URL",
        "CPR_DATABASE_PASSWORD",
        "CPR_REDIS_PASSWORD",
        "CPR_SERVER_HOST",
        "CPR_SERVER_PORT",
        "CPR_WEB_DIST_DIR",
    ] {
        assert!(
            std::env::var_os(name).is_none(),
            "full Host fixture must not inherit deployment overrides"
        );
    }
    let binary = binary();
    let directory = tempfile::tempdir().unwrap();
    let assets = directory.path().join("assets");
    let log_directory = directory.path().join("logs");
    std::fs::create_dir(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<!doctype html><title>Host fixture</title>",
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut settings: Value = config::Config::builder()
        .add_source(config::File::from_str(
            include_str!("../../../../../deploy/config.example.yaml"),
            config::FileFormat::Yaml,
        ))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    settings["host"]["listen"] = json!({"host":"127.0.0.1", "port":port});
    settings["host"]["runtime_data_dir"] = json!(directory.path().join("runtime"));
    settings["host"]["logging"]["stdout"] = json!(false);
    settings["host"]["logging"]["file"]["directory"] = json!(log_directory);
    settings["host"]["drain_timeout_seconds"] = json!(1);
    settings["host"]["worker_shutdown_timeout_seconds"] = json!(5);
    settings["store"]["database"] = connection(&database_url);
    settings["store"]["redis"] = connection(&redis_url);
    settings["admin"]["default_username"] = json!("host-fixture@fixture.test");
    settings["admin"]["default_password"] = json!("host-fixture-admin-password");
    settings["api"]["asset_directory"] = json!(assets);
    std::fs::create_dir(directory.path().join("deploy")).unwrap();
    std::fs::write(
        directory.path().join("deploy/config.yaml"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    drop(settings);
    drop(listener);
    let stderr = directory.path().join("stderr");
    let mut gateway = Gateway(
        Command::new(binary)
            .arg("serve")
            .current_dir(directory.path())
            .process_group(0)
            .env("RUST_LOG", "info")
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .env("HTTP_PROXY", "http://127.0.0.1:9")
            .env("ALL_PROXY", "http://127.0.0.1:9")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let login = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(gateway.0.try_wait().unwrap().is_none(), "gateway startup failed: {}", std::fs::read_to_string(&stderr).unwrap());
            if let Ok(response) = client.post(format!("{base}/api/admin/auth/login"))
                .json(&json!({"username":"host-fixture@fixture.test", "password":"host-fixture-admin-password"}))
                .send().await
                && response.status().is_success() { break response; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("full native gateway must start");
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let artifact: Value = client
        .post(format!("{base}/api/admin/plugins/artifacts"))
        .header("cookie", &cookie)
        .body(support::archive("1.0.0", false).as_ref().to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let installed: Value = client
        .post(format!("{base}/api/admin/plugins"))
        .header("cookie", &cookie)
        .json(&json!({
            "revision":0,"name":"Full Host SDK fixture","artifactSha256":artifact["data"]["sha256"],
            "enabled":true,"trustedProcess":true,"configuration":{},"secrets":{}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = installed["data"]["id"].as_str().unwrap().to_owned();
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await
        .unwrap();
    let revision: i64 =
        sqlx::query_scalar("select config_revision from runtime_settings where id=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut lock = pool.begin().await.unwrap();
    let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("select id from runtime_settings where id=1 for update")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let request = json!({"operation":"models", "mode":"credentials", "method":"host.auth.save", "request":create("full-host-drain")});
    let invocation = tokio::spawn(client.post(format!("{base}/api/admin/plugins/invoke"))
        .header("cookie", cookie).json(&json!({"id":id,"revision":1,"method":"POST","path":"echo", "contentType":"application/json", "body":STANDARD.encode(serde_json::to_vec(&request).unwrap())})).send());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("select exists(select 1 from pg_stat_activity where datname=current_database() and query like 'update runtime_settings%' and $1=any(pg_blocking_pids(pid)))")
                .bind(lock_pid).fetch_one(&pool).await.unwrap();
            if blocked {break;}
            assert!(!invocation.is_finished(), "native SDK save must reach its original transaction");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    let plugin_pids = if cfg!(target_os = "linux") {
        let children = std::fs::read_dir(format!("/proc/{}/task", gateway.0.id()))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path().join("children")).ok())
            .flat_map(|text| {
                text.split_whitespace()
                    .map(|pid| pid.parse::<u32>().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!children.is_empty(), "real SDK subprocess must be present");
        children.into_iter().collect::<Vec<_>>()
    } else {
        vec![]
    };
    assert!(
        Command::new("kill")
            .args(["-TERM", &gateway.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(4), async {
        while !logs(&log_directory).contains("开始等待扩展任务退出") {
            assert!(
                gateway.0.try_wait().unwrap().is_none(),
                "Host must retain accepted native commit"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(gateway.0.try_wait().unwrap().is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from provider_accounts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    lock.commit().await.unwrap();
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = gateway.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("full Host must drain native work and join workers");
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(&stderr).unwrap()
    );
    let _ = invocation.await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from provider_accounts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id=1")
            .fetch_one(&pool)
            .await
            .unwrap(),
        revision + 1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from admin_audit_events where actor_kind='plugin'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    for pid in plugin_pids {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "SDK child must be reaped"
        );
    }
    drop(gateway);
    pool.close().await;
    database.close().await;
}
