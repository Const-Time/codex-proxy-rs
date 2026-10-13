mod dispatcher;
pub(crate) mod fixture;
mod websocket;

use crate::openai::middleware::fixture::{Fixture, Mode};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gateway_admin::model::plugins::PluginMiddlewareMount;
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::ServiceExt;

async fn new(mode: Mode) -> Fixture {
    Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Http]).await
}
fn request(path: &str) -> Request<Body> {
    Request::post(path)
        .header("authorization", "Bearer sk_middleware")
        .header("content-type", "application/json")
        .header("x-request-id", "http-ingress-fixture")
        .body(Body::from(
            r#"{"model":"model-a","input":"original input","stream":false}"#,
        ))
        .unwrap()
}
async fn body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
async fn entered(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.plugin.http_seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn http_method_and_uri_are_rewritten_before_route_matching_with_original_authorization_and_charge()
 {
    let f = new(Mode::Rewrite).await;
    let mut input = request("/missing-route");
    *input.method_mut() = http::Method::PUT;
    let response = f.app.clone().oneshot(input).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-http-layer"], "fixture");
    assert!(
        String::from_utf8(body(response).await)
            .unwrap()
            .contains("resp_original")
    );
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(
        f.effects.charges.lock().unwrap()[0].key_id.as_str(),
        "key_api_test"
    );
    assert_eq!(f.plugin.http_seen.lock().unwrap()[0].0, "/missing-route");
    assert_eq!(
        f.plugin.http_seen.lock().unwrap()[0].1.request_id,
        "http-ingress-fixture"
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_header_rewrite_still_uses_normal_key_authentication_before_core_admission() {
    let f = new(Mode::HttpAuthError).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    body(response).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_rejection_short_circuits_before_auth_decode_admission_or_metering() {
    let f = new(Mode::ShortCircuit).await;
    let response = f
        .app
        .clone()
        .oneshot(
            Request::post("/v1/responses")
                .body(Body::from(vec![0, 255]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["error"]["type"], "middleware_error");
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_zero_timeout_prevents_terminal_admission_and_has_the_existing_408_status() {
    let f = new(Mode::ZeroTimeout).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    body(response).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_runtime_overrides_keep_provenance_and_establish_only_the_authenticated_keys_execution_defaults()
 {
    let f = new(Mode::Settings).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    body(response).await;
    f.settled().await;
    {
        let settings = f.effects.settings.lock().unwrap();
        assert_eq!(settings.len(), 1);
        assert_eq!(
            serde_json::to_value(settings[0].values()).unwrap()["request_interval_ms"],
            137
        );
        assert_eq!(
            settings[0].inspect()["overrides"]["request_interval_ms"]["instance_id"],
            "fixture-http"
        );
        assert_eq!(
            settings[0].inspect()["execution"]["client_key_id"],
            "key_api_test"
        );
        assert_eq!(
            settings[0].execution_values().unwrap().fast_mode,
            gateway_core::account::FastMode::Default
        );
    }
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_placement_runs_for_health_routes_as_well_as_model_routes() {
    let f = new(Mode::MessageIdentity).await;
    let response = f
        .app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["x-http-layer"], "fixture");
    body(response).await;
    assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

struct Source {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    frames: std::collections::VecDeque<Frame<bytes::Bytes>>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl HttpBody for Source {
    type Data = bytes::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}
fn source() -> (Body, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut trailers = http::HeaderMap::new();
    trailers.append("x-trailer", "one".parse().unwrap());
    trailers.append("x-trailer", "two".parse().unwrap());
    (
        Body::new(Source {
            polls: polls.clone(),
            drops: drops.clone(),
            frames: [
                Frame::data(bytes::Bytes::from_static(b"raw\0invalid\xff")),
                Frame::trailers(trailers),
            ]
            .into(),
        }),
        polls,
        drops,
    )
}
#[tokio::test]
async fn http_short_circuit_keeps_raw_body_lazy_and_preserves_trailers_without_any_core_execution()
{
    let f = new(Mode::ShortCircuitRaw).await;
    let (input, polls, drops) = source();
    let response = f
        .app
        .clone()
        .oneshot(Request::post("/v1/responses").body(input).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let mut output = response.into_body();
    assert_eq!(
        output.frame().await.unwrap().unwrap().into_data().unwrap(),
        b"raw\0invalid\xff".as_slice()
    );
    let trailers = output
        .frame()
        .await
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(
        trailers.get_all("x-trailer").iter().collect::<Vec<_>>(),
        vec!["one", "two"]
    );
    assert!(output.frame().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_revocation_during_before_next_wait_drops_unread_input_and_never_admits() {
    let f = new(Mode::WaitBeforeNext).await;
    let (input, polls, drops) = source();
    let mut run = Box::pin(
        f.app
            .clone()
            .oneshot(Request::post("/v1/responses").body(input).unwrap()),
    );
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=entered(&f)=>{}}
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_revocation_during_after_next_wait_keeps_the_original_single_settlement() {
    let f = new(Mode::WaitAfterNext).await;
    let mut run = Box::pin(f.app.clone().oneshot(request("/v1/responses")));
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=async {while f.effects.charges.lock().unwrap().is_empty() {tokio::task::yield_now().await;}}=>{}}
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    f.settled().await;
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_post_reply_revocation_blocks_buffered_delivery_after_core_settlement() {
    let f = new(Mode::MessageIdentity).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    f.settled().await;
    f.plugin.access.revoke();
    assert!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .is_err()
    );
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn http_request_and_attempt_settings_share_the_frozen_publication_and_original_key_ledger() {
    for streaming in [false, true] {
        let f = Fixture::with_mounts(
            Mode::Settings,
            vec![
                PluginMiddlewareMount::Http,
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        )
        .await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::OK);
        body(response).await;
        f.settled().await;
        assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
        assert_eq!(f.plugin.seen.lock().unwrap().len(), 2);
        {
            let settings = f.effects.settings.lock().unwrap();
            assert_eq!(settings.len(), 1);
            let sources = settings[0].inspect();
            assert_eq!(
                sources["overrides"]["request_interval_ms"]["instance_id"],
                "fixture-http"
            );
            assert_eq!(sources["execution"]["fast_mode"]["instance_id"], "fixture");
            assert_eq!(sources["execution"]["client_key_id"], "key_api_test");
            assert_eq!(
                settings[0].execution_values().unwrap().timeout_ms,
                Some(60_000)
            );
        }
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn http_revocation_interrupts_stalled_sse_and_releases_original_admission_with_one_finalization()
 {
    use futures::StreamExt;
    let f = new(Mode::MessageIdentity).await;
    *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let response = f.request(true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 1);
    f.plugin.access.revoke();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(bytes) => {
                    assert!(!String::from_utf8_lossy(&bytes).contains("response.completed"))
                }
                Err(_) => break,
            }
        }
        while f.effects.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
    {
        let charges = f.effects.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount_usd, gateway_core::metering::Decimal::ZERO);
    }
    f.manager.shutdown().await;
}
