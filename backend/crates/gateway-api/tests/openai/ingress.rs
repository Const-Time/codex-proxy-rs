use crate::admin::plugin_models::Fixture;
use axum::{
    body::{Body, Bytes},
    http::{Request, StatusCode},
};
use futures::{StreamExt as _, stream};
use tower::ServiceExt as _;

#[tokio::test]
async fn compressed_uploads_charge_actual_growth_and_release_failed_expansion() {
    use std::io::Write as _;
    let fixture = Fixture::with_ingress(
        false,
        gateway_api::IngressConfig {
            memory_budget_mib: 1,
            ..Default::default()
        },
    )
    .await;
    let gzip = |bytes: &[u8]| {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    };
    let request = |bytes: Vec<u8>| {
        Request::post("/v1/responses")
            .header("authorization", "Bearer external")
            .header("content-encoding", "gzip")
            .body(Body::from(bytes))
            .unwrap()
    };
    let expanded = gzip(&vec![b' '; 600 * 1024]);
    assert_eq!(
        fixture
            .app
            .clone()
            .oneshot(request(expanded))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let small = gzip(br#"{"model":"model-a","input":"hello"}"#);
    let results = futures::future::join_all(
        (0..8).map(|_| fixture.app.clone().oneshot(request(small.clone()))),
    )
    .await;
    assert!(
        results
            .into_iter()
            .all(|result| result.unwrap().status() == StatusCode::OK)
    );
}

#[tokio::test]
async fn unauthenticated_uploads_are_rejected_without_polling_the_body() {
    let fixture = Fixture::new(false).await;
    for path in [
        "/v1/responses",
        "/v1/images/generations",
        "/v1/images/edits",
        "/v1/alpha/search",
    ] {
        let body = Body::from_stream(stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
                panic!("unauthenticated body must not be read")
            },
        ));
        let response = fixture
            .app
            .clone()
            .oneshot(Request::post(path).body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn upload_memory_pressure_releases_capacity_for_the_next_request() {
    let fixture = Fixture::with_ingress(
        false,
        gateway_api::IngressConfig {
            memory_budget_mib: 1,
            ..Default::default()
        },
    )
    .await;
    let oversized = Request::post("/v1/responses")
        .header("authorization", "Bearer external")
        .body(Body::from(vec![b' '; 600 * 1024]))
        .unwrap();
    let response = fixture.app.clone().oneshot(oversized).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/v1/responses")
                .header("authorization", "Bearer external")
                .body(Body::from(r#"{"model":"model-a","input":"hello"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_stalled_upload_times_out_and_releases_its_slot() {
    let fixture = Fixture::with_ingress(
        false,
        gateway_api::IngressConfig {
            maximum_uploads: 1,
            upload_timeout_seconds: 1,
            ..Default::default()
        },
    )
    .await;
    let body = Body::from_stream(
        stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"{")) })
            .chain(stream::pending()),
    );
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/v1/responses")
                .header("authorization", "Bearer external")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    let response = fixture
        .app
        .clone()
        .oneshot(Request::post("/v1/responses").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
