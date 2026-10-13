mod dispatch;
mod upgrade;
use super::plan::KernelRuntime;
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_admin::{model::plugins::*, plugins::PluginManager, ports::plugins::PluginService};
use gateway_core::{
    engine::middleware::{MiddlewareError, http as core},
    lifecycle::Deadline,
    middleware::compose,
};
use gateway_plugin_runtime::PluginEngine;
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

struct Fixture {
    manager: PluginManager,
    _cache: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::support::MemoryStore::default());
        let runtime = Arc::new(KernelRuntime(PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )));
        let manager = PluginManager::new(store, runtime);
        let artifact = manager
            .upload(
                crate::support::http_archive("1.0.0"),
                &crate::support::actor(),
            )
            .await
            .unwrap();
        let mut command = crate::support::update(&artifact.sha256, 0, true);
        command.middleware = Some(PluginMiddlewareBinding {
            order: 0,
            mounts: vec![PluginMiddlewareMount::Http],
            scope: PluginExecutionScope::All {},
        });
        manager
            .update(None, command, &crate::support::actor())
            .await
            .unwrap();
        Self {
            manager,
            _cache: cache,
        }
    }
    async fn run(
        &self,
        mode: &str,
        body: core::Body,
        context: core::Context,
        next: core::Next,
    ) -> Result<core::Response, MiddlewareError> {
        self.run_with_dispatcher(mode, body, context, next, None)
            .await
    }
    async fn run_with_dispatcher(
        &self,
        mode: &str,
        body: core::Body,
        context: core::Context,
        next: core::Next,
        dispatcher: Option<Arc<dyn core::Dispatcher>>,
    ) -> Result<core::Response, MiddlewareError> {
        let mut request = core::Request::new(body);
        *request.method_mut() = http::Method::PATCH;
        *request.uri_mut() = "/before?raw=%2F".parse().unwrap();
        *request.version_mut() = http::Version::HTTP_2;
        request
            .headers_mut()
            .insert("x-fixture-mode", mode.parse().unwrap());
        request.headers_mut().insert(
            "authorization",
            "Bearer synthetic-http-fixture".parse().unwrap(),
        );
        request
            .headers_mut()
            .append("x-repeat", "one".parse().unwrap());
        request
            .headers_mut()
            .append("x-repeat", "two".parse().unwrap());
        request.extensions_mut().insert(Marker);
        if let Some(dispatcher) = dispatcher {
            request.extensions_mut().insert(dispatcher);
        }
        self.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context)
            .unwrap()
            .run(request, next)
            .await
    }
    async fn stop(self) {
        self.manager.shutdown().await;
    }
    async fn change(&self, enabled: bool) {
        let installed = self.manager.list().await.unwrap().remove(0);
        let mut command =
            crate::support::update(&installed.artifact.sha256, installed.revision, enabled);
        command.middleware = installed.middleware;
        self.manager
            .update(Some(&installed.id), command, &crate::support::actor())
            .await
            .unwrap();
    }
}
fn context() -> core::Context {
    core::Context {
        request_id: "raw-request".into(),
        call_id: "host-call".into(),
        parent_call_id: None,
        call_scope: Default::default(),
        cancellation: Default::default(),
        deadline_at: Deadline::from_timeout(SystemTime::now(), None).unwrap(),
    }
}
#[derive(Clone)]
struct Marker;
struct Source {
    frames: VecDeque<Frame<Bytes>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Body for Source {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.gate.as_mut() {
            if Pin::new(gate).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.gate.take();
        }
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}
fn source(
    data: Vec<Bytes>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
) -> (core::Body, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut trailers = http::HeaderMap::new();
    trailers.append("x-trailer", "one".parse().unwrap());
    trailers.append("x-trailer", "two".parse().unwrap());
    let mut frames: VecDeque<_> = data.into_iter().map(Frame::data).collect();
    frames.push_back(Frame::trailers(trailers));
    (
        Source {
            frames,
            polls: polls.clone(),
            drops: drops.clone(),
            gate,
        }
        .boxed_unsync(),
        polls,
        drops,
    )
}
fn terminal(calls: Arc<AtomicUsize>) -> core::Next {
    compose(vec![], move |request| {
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.method(), http::Method::PATCH);
            assert_eq!(request.uri(), "/after?raw=%2F");
            assert_eq!(request.version(), http::Version::HTTP_2);
            assert_eq!(
                request.headers()["authorization"],
                "Bearer synthetic-http-fixture"
            );
            assert_eq!(
                request
                    .headers()
                    .get_all("x-repeat")
                    .iter()
                    .collect::<Vec<_>>(),
                vec!["one", "two"]
            );
            assert!(request.extensions().get::<Marker>().is_some());
            let (parts, body) = request.into_parts();
            let mut response = core::Response::new(body);
            *response.status_mut() = http::StatusCode::CREATED;
            *response.version_mut() = parts.version;
            *response.extensions_mut() = parts.extensions;
            Ok(response)
        })
    })
}
async fn collect(mut body: core::Body) -> (Vec<u8>, Vec<String>) {
    let mut data = vec![];
    let mut trailers = vec![];
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        match frame.into_data() {
            Ok(bytes) => data.extend_from_slice(&bytes),
            Err(frame) => {
                trailers = frame
                    .into_trailers()
                    .unwrap()
                    .get_all("x-trailer")
                    .iter()
                    .map(|v| v.to_str().unwrap().to_owned())
                    .collect();
            }
        }
    }
    (data, trailers)
}

#[tokio::test]
async fn sdk_http_passthrough_retains_lazy_raw_body_trailers_and_original_response_extensions() {
    let f = Fixture::new().await;
    let (body, polls, drops) = source(vec![Bytes::from_static(b"raw\0bytes")], None);
    let calls = Arc::new(AtomicUsize::new(0));
    let response = f
        .run("pass", body, context(), terminal(calls.clone()))
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(response.status(), 201);
    assert_eq!(response.version(), http::Version::HTTP_2);
    assert!(response.extensions().get::<Marker>().is_some());
    assert_eq!(response.headers()["x-return"], "plugin");
    assert_eq!(
        collect(response.into_body()).await,
        (b"raw\0bytes".to_vec(), vec!["one".into(), "two".into()])
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_upload_pipe_and_streamed_response_keep_scope_open_until_eof() {
    let f = Fixture::new().await;
    let (body, _, drops) = source(
        vec![
            Bytes::from_static(b"mixed\0bytes"),
            Bytes::from_static(b"suffix"),
        ],
        None,
    );
    let response = f
        .run(
            "map",
            body,
            context(),
            terminal(Arc::new(AtomicUsize::new(0))),
        )
        .await
        .unwrap();
    assert!(response.extensions().get::<Marker>().is_some());
    assert_eq!(
        collect(response.into_body()).await,
        (
            b"MIXED\0BYTESSUFFIX".to_vec(),
            vec!["one".into(), "two".into()]
        )
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_invalid_next_does_not_consume_terminal_and_second_valid_next_cannot_repeat_it() {
    let f = Fixture::new().await;
    for mode in ["bad-next", "double"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let response = f
            .run(mode, core::empty_body(), context(), terminal(calls.clone()))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        collect(response.into_body()).await;
    }
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_finished_scope_cannot_reuse_body_handle_in_later_call() {
    let f = Fixture::new().await;
    let (body, polls, drops) = source(vec![Bytes::from_static(b"unread")], None);
    let calls = Arc::new(AtomicUsize::new(0));
    let response = f
        .run("leak", body, context(), terminal(calls.clone()))
        .await
        .unwrap();
    collect(response.into_body()).await;
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let response = f
        .run(
            "stale",
            core::empty_body(),
            context(),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"isolated");
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_body_resources_are_bounded_even_after_each_writer_finishes() {
    let f = Fixture::new().await;
    let response = f
        .run(
            "capacity",
            core::empty_body(),
            context(),
            terminal(Arc::new(AtomicUsize::new(0))),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"bounded");
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_forged_response_and_frame_tag_close_the_generation_until_explicit_restart() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        f.run(
            "forged-response",
            core::empty_body(),
            context(),
            terminal(calls.clone())
        )
        .await
        .is_err()
    );
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_err()
    );
    f.change(true).await;
    let response = f
        .run(
            "invalid-chunks",
            core::empty_body(),
            context(),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    assert!(response.into_body().frame().await.unwrap().is_err());
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.change(true).await;
    let response = f
        .run(
            "pass",
            core::empty_body(),
            context(),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    collect(response.into_body()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_scope_cannot_mint_model_management_authority_or_settings_baseline() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let response = f
        .run(
            "no-authority",
            core::empty_body(),
            context(),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    assert_eq!(collect(response.into_body()).await.0, b"denied");
    assert!(
        f.run(
            "settings-forged",
            core::empty_body(),
            context(),
            terminal(calls.clone())
        )
        .await
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_revocation_during_lazy_request_read_releases_original_body_without_terminal() {
    let f = Fixture::new().await;
    let (send, gate) = tokio::sync::oneshot::channel();
    let (body, polls, drops) = source(vec![], Some(gate));
    let context = context();
    let cancellation = context.cancellation.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut run = Box::pin(f.run("read-wait", body, context, terminal(calls.clone())));
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=async {while polls.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(send.send(()).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_stream_guard_revokes_after_initial_rpc_reply() {
    let f = Fixture::new().await;
    let (send, gate) = tokio::sync::oneshot::channel();
    let (body, polls, drops) = source(vec![Bytes::from_static(b"pending")], Some(gate));
    let context = context();
    let cancellation = context.cancellation.clone();
    let response = f
        .run(
            "stream-wait",
            body,
            context,
            terminal(Arc::new(AtomicUsize::new(0))),
        )
        .await
        .unwrap();
    let mut body = response.into_body();
    let mut read = Box::pin(body.frame());
    tokio::select! {result=&mut read=>panic!("must wait: {}",result.is_some()),()=async {while polls.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(send.send(()).is_err());
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_upload_stops_on_terminal_short_response_and_never_reads_entire_request_ahead() {
    let f = Fixture::new().await;
    let (body, polls, drops) = source(
        (0..2000)
            .map(|_| Bytes::from_static(b"unread frame"))
            .collect(),
        None,
    );
    let next = compose(vec![], |_request| {
        Box::pin(async { Ok(core::Response::new(core::empty_body())) })
    });
    let response = f.run("pipe-short", body, context(), next).await.unwrap();
    collect(response.into_body()).await;
    assert!(polls.load(Ordering::SeqCst) <= 2);
    tokio::time::timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn sdk_http_deadline_closes_waiting_source_before_terminal() {
    let f = Fixture::new().await;
    let (_send, gate) = tokio::sync::oneshot::channel();
    let (body, _, drops) = source(vec![], Some(gate));
    let mut context = context();
    context.deadline_at =
        Deadline::from_timeout(SystemTime::now(), Some(Duration::from_millis(100))).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        f.run("read-wait", body, context, terminal(calls.clone()))
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_partial_raw_read_passes_the_unread_large_frame_and_trailers_without_loss() {
    let f = Fixture::new().await;
    let mut original = b"abc".to_vec();
    original.extend(vec![b'X'; 130_000]);
    let (body, polls, drops) = source(vec![Bytes::from(original)], None);
    let response = f
        .run(
            "prefix",
            body,
            context(),
            terminal(Arc::new(AtomicUsize::new(0))),
        )
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        collect(response.into_body()).await,
        (vec![b'X'; 130_000], vec!["one".into(), "two".into()])
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.stop().await;
}

#[derive(Debug, thiserror::Error)]
#[error("synthetic terminal fact")]
struct OriginalFailure(Arc<()>);

#[tokio::test]
async fn sdk_http_cannot_swallow_or_reclassify_the_original_terminal_error() {
    let f = Fixture::new().await;
    let identity = Arc::new(());
    let original = identity.clone();
    let next = compose(vec![], move |_| {
        Box::pin(async move {
            Err(MiddlewareError::Remote {
                source: Box::new(OriginalFailure(original)),
                rejected: true,
            })
        })
    });
    let error = f
        .run("swallow-next-error", core::empty_body(), context(), next)
        .await
        .err()
        .unwrap();
    assert!(error.is_rejected());
    let MiddlewareError::Remote { source, .. } = error else {
        panic!("original terminal type lost")
    };
    assert!(Arc::ptr_eq(
        &source.downcast::<OriginalFailure>().unwrap().0,
        &identity
    ));
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_ok()
    );
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_preserves_the_original_body_error_when_plugin_attempts_to_swallow_it() {
    let f = Fixture::new().await;
    let identity = Arc::new(());
    let original = identity.clone();
    let body = http_body_util::StreamBody::new(futures::stream::once(async move {
        Err::<Frame<Bytes>, _>(
            Box::new(OriginalFailure(original)) as Box<dyn std::error::Error + Send + Sync>
        )
    }))
    .boxed_unsync();
    let calls = Arc::new(AtomicUsize::new(0));
    let error = f
        .run(
            "swallow-body-error",
            body,
            context(),
            terminal(calls.clone()),
        )
        .await
        .err()
        .unwrap();
    assert!(!error.is_rejected());
    let MiddlewareError::Remote { source, .. } = error else {
        panic!("original body type lost")
    };
    assert!(Arc::ptr_eq(
        &source.downcast::<OriginalFailure>().unwrap().0,
        &identity
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .is_ok()
    );
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_explicit_plugin_rejection_preserves_process_for_later_requests() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        f.run(
            "deny",
            core::empty_body(),
            context(),
            terminal(calls.clone())
        )
        .await
        .err()
        .unwrap()
        .is_rejected()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let response = f
        .run(
            "pass",
            core::empty_body(),
            context(),
            terminal(calls.clone()),
        )
        .await
        .unwrap();
    collect(response.into_body()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}

#[tokio::test]
async fn sdk_http_admin_disable_revokes_streamed_reply_and_closes_waiting_source() {
    let f = Fixture::new().await;
    let (send, gate) = tokio::sync::oneshot::channel();
    let (body, polls, drops) = source(vec![], Some(gate));
    let response = f
        .run(
            "stream-wait",
            body,
            context(),
            terminal(Arc::new(AtomicUsize::new(0))),
        )
        .await
        .unwrap();
    let mut body = response.into_body();
    let mut read = Box::pin(body.frame());
    tokio::select! {result=&mut read=>panic!("must wait: {}",result.is_some()),()=async {while polls.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{}}
    f.change(false).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(send.send(()).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        f.manager
            .middleware_registry()
            .unwrap()
            .freeze_http(context())
            .unwrap()
            .is_empty()
    );
    f.stop().await;
}
