//! Only the actual native OpenAI usage GET reaches this owned loopback server.
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
pub(crate) struct Server {
    pub url: String,
    pub requests: Arc<Mutex<Vec<String>>>,
    pub entered: Arc<Notify>,
    pub release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn new(wait: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/backend-api", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let state = (requests.clone(), entered.clone(), release.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (requests, entered, release) = state.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    loop {
                        let mut chunk = [0; 2048];
                        let count = socket.read(&mut chunk).await.unwrap();
                        assert_ne!(count, 0);
                        bytes.extend_from_slice(&chunk[..count]);
                        assert!(bytes.len() <= 32768);
                        if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    assert!(request.starts_with("GET "));
                    assert!(request.contains("/wham/usage"));
                    assert!(request.to_lowercase().contains("authorization: bearer "));
                    requests.lock().unwrap().push(request);
                    entered.notify_one();
                    if wait {
                        tokio::time::timeout(Duration::from_secs(20), release.notified())
                            .await
                            .unwrap();
                    }
                    let body = json!({"plan_type":"pro", "rate_limit":{"allowed":true,"limit_reached":false,
                        "primary_window":{"used_percent":25.0,"limit_window_seconds":18000,"reset_at":(chrono::Utc::now()+chrono::Duration::hours(5)).timestamp()},
                        "secondary_window":{"used_percent":50.0,"limit_window_seconds":604800,"reset_at":(chrono::Utc::now()+chrono::Duration::days(7)).timestamp()}}}).to_string();
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            url,
            requests,
            entered,
            release,
            task,
        }
    }
}
