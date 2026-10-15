//! Process-local native TLS trust and a strictly owned loopback proxy.
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

pub(crate) struct Trust {
    directory: tempfile::TempDir,
}
impl Trust {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir(root.join("empty-roots")).unwrap();
        std::fs::write(root.join("ca.conf"), "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=ca\n[dn]\nCN=CPR owned offline xAI fixture\n[ca]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n").unwrap();
        std::fs::write(root.join("server.ext"), "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:auth.x.ai\n").unwrap();
        for args in [
            vec![
                "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config",
                "ca.conf", "-keyout", "ca.key", "-out", "ca.pem",
            ],
            vec![
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=auth.x.ai",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
            ],
            vec![
                "x509",
                "-req",
                "-in",
                "server.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-days",
                "1",
                "-extfile",
                "server.ext",
                "-out",
                "server.pem",
            ],
        ] {
            let output = Command::new("openssl")
                .current_dir(root)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "owned fixture PKI failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Self { directory }
    }
    pub async fn run(&self, name: &str) {
        let root = self.directory.path();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env("CPR_NATIVE_XAI_TRUST", root)
            .env("SSL_CERT_FILE", root.join("ca.pem"))
            .env("SSL_CERT_DIR", root.join("empty-roots"))
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(60), child.output())
            .await
            .expect("isolated native xAI case must finish")
            .unwrap();
        assert!(
            output.status.success(),
            "isolated native xAI case failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

pub(crate) struct Proxy {
    child: tokio::process::Child,
    pub url: String,
    log: PathBuf,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}
impl Proxy {
    pub async fn start(root: &Path, subject: &str, inference: &str, mode: &str) -> Self {
        let script = root.join("proxy.py");
        std::fs::write(&script, include_str!("xai.py")).unwrap();
        let log = root.join("proxy.jsonl");
        let config = root.join("proxy.json");
        std::fs::write(
            &config,
            serde_json::to_vec(
                &json!({"root":root,"log":log,"subject":subject,"inference":inference,"mode":mode}),
            )
            .unwrap(),
        )
        .unwrap();
        let mut child = tokio::process::Command::new("python3")
            .args(["-u"])
            .arg(script)
            .arg(config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut ready = String::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            BufReader::new(child.stdout.take().unwrap()).read_line(&mut ready),
        )
        .await
        .unwrap()
        .unwrap();
        let value: Value =
            serde_json::from_str(&ready).expect("owned proxy must publish its loopback address");
        Self {
            child,
            url: value["url"].as_str().unwrap().into(),
            log,
        }
    }
    pub fn events(&self) -> Vec<Value> {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    pub async fn stop(mut self) {
        self.child.kill().await.unwrap();
        let _ = self.child.wait().await.unwrap();
    }
}
