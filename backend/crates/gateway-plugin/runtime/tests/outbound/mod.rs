//! Real SDK child processes exercise Host TCP dispatch without model services.
use crate::support::{MemoryStore, actor, archive, request, update};
use gateway_admin::{plugins::PluginManager, ports::plugins::PluginService};
use gateway_plugin_runtime::PluginEngine;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

pub(crate) struct Server {
    pub(crate) url: String,
    pub(crate) requests: Arc<Mutex<Vec<Vec<u8>>>>,
    entered: Arc<tokio::sync::Notify>,
    closed: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(crate) async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let entered = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(AtomicUsize::new(0));
        let (saved, signal, finished) = (requests.clone(), entered.clone(), closed.clone());
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    Some(result)=tasks.join_next()=>{result.unwrap();},
                    accepted=listener.accept()=>{
                        let (mut socket,_)=accepted.unwrap();
                        let (saved,signal,finished)=(saved.clone(),signal.clone(),finished.clone());
                        tasks.spawn(async move {
                            let mut bytes=vec![];
                            let mut buffer=[0;4096];
                            let path=loop {
                                let n=socket.read(&mut buffer).await.unwrap();
                                if n==0 {return;}
                                bytes.extend_from_slice(&buffer[..n]);
                                if let Some(end)=bytes.windows(4).position(|w|w==b"\r\n\r\n") {
                                    let headers=String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                                    let length=headers.lines().find_map(|l|l.strip_prefix("content-length: ")).map(|s|s.parse::<usize>().unwrap()).unwrap_or(0);
                                    if bytes.len()>=end+4+length {break headers.lines().next().unwrap().split_whitespace().nth(1).unwrap().to_owned();}
                                }
                            };
                            saved.lock().unwrap().push(bytes);
                            signal.notify_one();
                            let status=if path=="/error" {500} else {200};
                            let length=if path=="/large" {8*1024*1024+1} else {4};
                            let mut response=format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {length}\r\nConnection: close\r\n").into_bytes();
                            if path=="/opaque" {response.extend_from_slice(b"X-opaque: \xff\r\n");}
                            response.extend_from_slice(b"\r\n");
                            if socket.write_all(&response).await.is_err() {return;}
                            if path=="/stall" {
                                while socket.read(&mut buffer).await.unwrap_or(0)>0 {}
                                finished.fetch_add(1,Ordering::SeqCst);
                                return;
                            }
                            if path=="/slow" {tokio::time::sleep(Duration::from_millis(100)).await;}
                            let body=if path=="/large" {vec![1;length]} else {vec![0,255,1,0]};
                            let _=socket.write_all(&body).await;
                        });
                    }
                }
            }
        });
        Self {
            url,
            requests,
            entered,
            closed,
            task,
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    async fn closed(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.closed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
struct Fixture {
    manager: Arc<PluginManager>,
    id: String,
    artifact: String,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::default());
        let runtime = Arc::new(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        ));
        let manager = Arc::new(PluginManager::new(store, runtime));
        let artifact = manager
            .upload(archive("1.0.0", false), &actor())
            .await
            .unwrap();
        let installed = manager
            .update(None, update(&artifact.sha256, 0, true), &actor())
            .await
            .unwrap();
        Self {
            manager,
            id: installed.id,
            artifact: artifact.sha256,
            _cache: cache,
        }
    }
    async fn call(&self, input: Value) -> Value {
        call(&self.manager, &self.id, input).await
    }
}
async fn call(manager: &PluginManager, id: &str, mut input: Value) -> Value {
    input["operation"] = json!("external");
    let mut req = request("external");
    req.body = serde_json::to_vec(&input).unwrap();
    let response = manager.handle(id, 1, req).await.unwrap();
    serde_json::from_slice(&response.body).unwrap()
}
#[tokio::test]
async fn external_http_unary_sdk_stream_and_cancelled_read_resume_preserve_binary_bytes_and_explicit_headers()
 {
    let f = Fixture::new().await;
    let server = Server::new().await;
    for (mode, path, status) in [
        ("unary", "/binary", 200),
        ("stream", "/binary", 200),
        ("resume", "/slow", 200),
        ("stream", "/error", 500),
    ] {
        let reply = f
            .call(json!({"mode":mode,"url":format!("{}{path}",server.url)}))
            .await;
        assert_eq!(reply, json!({"status":status,"bytes":[0,255,1,0]}));
    }
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for req in requests.iter() {
            let end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let headers = String::from_utf8_lossy(&req[..end]).to_ascii_lowercase();
            assert!(headers.contains("authorization: bearer plugin-explicit"));
            assert!(headers.contains("x-explicit: plugin-value"));
            assert!(!headers.contains("fixture-secret-must-not-leak"));
            assert_eq!(&req[end..], &[0, 255, 1, 0]);
        }
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn external_http_handles_enforce_limits_close_and_free_capacity() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    for mode in ["handles", "capacity"] {
        assert_eq!(
            f.call(json!({"mode":mode,"url":format!("{}/binary",server.url)}))
                .await,
            json!({"checked":true})
        );
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn external_http_stream_is_closed_when_rpc_returns_and_cannot_cross_call_scopes() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    let reply = f
        .call(json!({"mode":"open","url":format!("{}/stall",server.url)}))
        .await;
    server.closed().await;
    let result=f.call(json!({"mode":"raw","method":"host.http.stream_read","params":{"stream":reply["stream"],"maximum_bytes":3}})).await;
    assert_eq!(result["error"]["code"], "permission_denied");
    f.manager.shutdown().await;
}
#[tokio::test]
async fn external_http_explicit_close_releases_unread_socket_and_is_idempotent_in_sdk() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    assert_eq!(
        f.call(json!({"mode":"close","url":format!("{}/stall",server.url)}))
            .await,
        json!({"closed":true})
    );
    server.closed().await;
    f.manager.shutdown().await;
}
#[tokio::test]
async fn external_http_cancel_and_plugin_disable_release_pending_tcp_body() {
    for disable in [false, true] {
        let f = Fixture::new().await;
        let server = Server::new().await;
        let (manager, id, url) = (
            f.manager.clone(),
            f.id.clone(),
            format!("{}/stall", server.url),
        );
        let task =
            tokio::spawn(
                async move { call(&manager, &id, json!({"mode":"stream","url":url})).await },
            );
        server.entered().await;
        if disable {
            f.manager
                .update(Some(&f.id), update(&f.artifact, 1, false), &actor())
                .await
                .unwrap();
            server.closed().await;
        }
        task.abort();
        let _ = task.await;
        server.closed().await;
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn external_http_unary_body_limit_reports_sent_and_requires_streaming() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    let result = f
        .call(json!({"mode":"large","url":format!("{}/large",server.url)}))
        .await;
    assert_eq!(result["error"]["code"], "capacity");
    assert_eq!(result["error"]["send_state"], "sent");
    f.manager.shutdown().await;
}

#[tokio::test]
async fn external_http_stream_has_one_reader_and_close_wakes_pending_read() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    assert_eq!(
        f.call(json!({"mode":"concurrent","url":format!("{}/stall",server.url)}))
            .await,
        json!({"checked":true})
    );
    server.closed().await;
    f.manager.shutdown().await;
}
#[tokio::test]
async fn external_http_inherits_management_origin_cancellation_without_model_services() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    let cancellation = gateway_core::lifecycle::CancellationToken::new();
    let mut req = request("external");
    req.body = serde_json::to_vec(
        &json!({"operation":"external","mode":"stream","url":format!("{}/stall",server.url)}),
    )
    .unwrap();
    req.invocation = Some(gateway_admin::model::plugins::PluginInvocationContext {
        context: gateway_core::engine::middleware::http::Context {
            request_id: "host-request".into(),
            call_id: "host-http".into(),
            parent_call_id: None,
            call_scope: Default::default(),
            model_parent: None,
            cancellation: cancellation.clone(),
            deadline_at: Default::default(),
        },
        http: None,
        services: None,
    });
    let (manager, id) = (f.manager.clone(), f.id.clone());
    let task = tokio::spawn(async move { manager.handle(&id, 1, req).await });
    server.entered().await;
    cancellation.cancel();
    server.closed().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap();
    f.manager.shutdown().await;
}

#[tokio::test]
async fn registration_generic_http_matches_upstream_without_private_state_authority() {
    let server = Server::new().await;
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let runtime = Arc::new(PluginEngine::new(
        cache.path().into(),
        "3.11.2".parse().unwrap(),
        store.clone(),
    ));
    let manager = PluginManager::new(store, runtime);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let mut command = update(&artifact.sha256, 0, true);
    command.configuration = json!({"adapter_mode":"registration-external","adapter_url":format!("{}/binary",server.url)});
    let view = manager.update(None, command, &actor()).await.unwrap();
    assert_eq!(view.status, "ready");
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    manager.shutdown().await;
}

#[tokio::test]
async fn external_http_unrepresentable_response_header_preserves_host_sent_state() {
    let f = Fixture::new().await;
    let server = Server::new().await;
    let reply = f
        .call(json!({"mode":"unary","url":format!("{}/opaque",server.url)}))
        .await;
    assert_eq!(reply["error"]["code"], "fault");
    assert_eq!(reply["error"]["send_state"], "sent");
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
