mod upgrade;
use bytes::Bytes;
use futures::{FutureExt, channel::oneshot, executor::block_on};
use gateway_core::{
    engine::middleware::{MiddlewareError, http as core},
    lifecycle::{CancellationToken, Deadline},
    middleware::{Middleware, compose},
};
use http_body::{Body, Frame, SizeHint};
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

struct Source {
    frames: VecDeque<Frame<Bytes>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    gate: Option<oneshot::Receiver<()>>,
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
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn source(
    frames: Vec<Frame<Bytes>>,
    gate: Option<oneshot::Receiver<()>>,
) -> (core::Body, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Source {
            frames: frames.into(),
            polls: polls.clone(),
            drops: drops.clone(),
            gate,
        }
        .boxed_unsync(),
        polls,
        drops,
    )
}
#[derive(Clone)]
struct TransportMarker(Arc<AtomicUsize>);

#[test]
fn http_onion_keeps_lazy_frames_trailers_and_host_transport_extensions() {
    let mut trailers = http::HeaderMap::new();
    trailers.append("x-trailer", "one".parse().unwrap());
    trailers.append("x-trailer", "two".parse().unwrap());
    let (body, polls, drops) = source(
        vec![
            Frame::data(Bytes::from_static(b"raw\0bytes")),
            Frame::trailers(trailers),
        ],
        None,
    );
    let mut request = core::Request::new(body);
    *request.uri_mut() = "/before?raw=%2F".parse().unwrap();
    *request.version_mut() = http::Version::HTTP_2;
    request
        .headers_mut()
        .append("authorization", "Bearer synthetic-fixture".parse().unwrap());
    let terminal_calls = Arc::new(AtomicUsize::new(0));
    request
        .extensions_mut()
        .insert(TransportMarker(terminal_calls.clone()));
    let layers: Vec<Middleware<core::Request, core::Response, MiddlewareError>> = vec![
        Box::new(|mut request, next| {
            Box::pin(async move {
                *request.uri_mut() = "/after?raw=%2F".parse().unwrap();
                request
                    .headers_mut()
                    .append("x-multi", "first".parse().unwrap());
                let mut response = next.run(request).await?;
                response
                    .headers_mut()
                    .append("x-multi", "outer".parse().unwrap());
                Ok(response)
            })
        }),
        Box::new(|mut request, next| {
            Box::pin(async move {
                request
                    .headers_mut()
                    .append("x-multi", "second".parse().unwrap());
                let mut response = next.run(request).await?;
                response
                    .headers_mut()
                    .append("x-multi", "inner".parse().unwrap());
                Ok(response)
            })
        }),
    ];
    let next = compose(layers, |request| {
        Box::pin(async move {
            assert_eq!(request.uri().to_string(), "/after?raw=%2F");
            assert_eq!(request.version(), http::Version::HTTP_2);
            assert_eq!(
                request
                    .headers()
                    .get_all("x-multi")
                    .iter()
                    .map(|v| v.to_str().unwrap())
                    .collect::<Vec<_>>(),
                ["first", "second"]
            );
            assert_eq!(
                request.headers()["authorization"],
                "Bearer synthetic-fixture"
            );
            let (parts, body) = request.into_parts();
            parts
                .extensions
                .get::<TransportMarker>()
                .unwrap()
                .0
                .fetch_add(1, Ordering::SeqCst);
            let mut response = core::Response::new(body);
            *response.extensions_mut() = parts.extensions;
            Ok(response)
        })
    });
    let response = block_on(next.run(request)).unwrap();
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(response.extensions().get::<TransportMarker>().is_some());
    assert_eq!(
        response
            .headers()
            .get_all("x-multi")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["inner", "outer"]
    );
    let mut body = response.into_body();
    assert_eq!(
        block_on(body.frame())
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap(),
        Bytes::from_static(b"raw\0bytes")
    );
    let tail = block_on(body.frame())
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(
        tail.get_all("x-trailer")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn http_short_circuit_releases_unread_request_without_running_terminal() {
    let (body, polls, drops) = source(vec![Frame::data(Bytes::from_static(b"unread"))], None);
    let layers: Vec<Middleware<core::Request, core::Response, MiddlewareError>> =
        vec![Box::new(|request, _| {
            Box::pin(async move {
                drop(request);
                Ok(core::Response::new(core::empty_body()))
            })
        })];
    let next = compose(layers, |_| {
        Box::pin(async { panic!("short circuit must not invoke auth or routing") })
    });
    block_on(next.run(core::Request::new(body))).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn http_revocation_wakes_pending_frame_and_drops_its_original_source_once() {
    let (sender, receiver) = oneshot::channel();
    let (body, polls, drops) = source(vec![], Some(receiver));
    let cancellation = CancellationToken::new();
    let mut body = core::guard_body(body, cancellation.clone(), Deadline::default());
    assert!(body.frame().now_or_never().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    cancellation.cancel();
    assert!(block_on(body.frame()).unwrap().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(sender.send(()).is_err());
    assert!(block_on(body.frame()).is_none());
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn http_deadline_releases_waiting_source_and_preserves_domain_timeout() {
    let (_sender, receiver) = oneshot::channel();
    let (body, _, drops) = source(vec![], Some(receiver));
    let deadline = Deadline::from_timeout(SystemTime::now(), Some(Duration::ZERO)).unwrap();
    let mut body = core::guard_body(body, CancellationToken::new(), deadline);
    let error = block_on(body.frame()).unwrap().unwrap_err();
    assert!(matches!(
        error.downcast_ref::<MiddlewareError>(),
        Some(MiddlewareError::Engine(
            gateway_core::engine::EngineError::Deadline
        ))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn http_frame_error_keeps_original_source_type_and_stops_after_one_error() {
    #[derive(Debug, thiserror::Error)]
    #[error("fixture frame failed")]
    struct FrameError;
    let stream = futures::stream::iter([Err::<Frame<Bytes>, _>(
        Box::new(FrameError) as Box<dyn std::error::Error + Send + Sync>
    )]);
    let body = http_body_util::StreamBody::new(stream).boxed_unsync();
    let mut body = core::guard_body(body, CancellationToken::new(), Deadline::default());
    assert!(
        block_on(body.frame())
            .unwrap()
            .unwrap_err()
            .downcast_ref::<FrameError>()
            .is_some()
    );
    assert!(block_on(body.frame()).is_none());
}
