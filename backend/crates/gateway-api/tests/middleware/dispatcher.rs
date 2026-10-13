use super::*;
use crate::openai::middleware::fixture::Plugin;
use gateway_core::engine::middleware::{MiddlewareError, http as core};
use http_body_util::Full;

pub(super) async fn handle(
    plugin: &Plugin,
    context: core::Context,
    request: core::Request,
) -> Result<core::Response, MiddlewareError> {
    let dispatcher = request
        .extensions()
        .get::<Arc<dyn core::Dispatcher>>()
        .unwrap();
    if matches!(plugin.mode, Mode::HttpDispatchBudget) {
        let mut held = vec![];
        for _ in 0..16 {
            held.push(
                dispatcher
                    .dispatch(
                        context.child("fixture".into()).unwrap(),
                        child(plugin.mode, None),
                    )
                    .await?,
            );
        }
        // All model executions have already settled; unread HTTP responses still
        // reserve the same graph permits, until transport consumption or drop.
        assert!(context.child("fixture".into()).is_err());
        let response = held.pop().unwrap();
        response.into_body().collect().await.unwrap();
        let response = dispatcher
            .dispatch(
                context.child("fixture".into()).unwrap(),
                child(plugin.mode, None),
            )
            .await?;
        response.into_body().collect().await.unwrap();
        drop(held);
        return Ok(core::Response::new(core::empty_body()));
    }
    let mut child_context = context.child("fixture".into()).unwrap();
    if matches!(plugin.mode, Mode::HttpDispatchForged) {
        child_context.parent_call_id = Some("wire-forged-parent".into());
        assert!(matches!(
            dispatcher
                .dispatch(child_context, child(plugin.mode, None))
                .await,
            Err(MiddlewareError::InvalidState)
        ));
        return Ok(core::Response::new(core::empty_body()));
    }
    let settings = if matches!(plugin.mode, Mode::HttpDispatchSettings) {
        let baseline = request
            .extensions()
            .get::<core::Settings>()
            .unwrap()
            .runtime
            .as_ref()
            .unwrap();
        let mut values = serde_json::to_value(baseline.values()).unwrap();
        values["request_interval_ms"] = serde_json::json!(173);
        Some(core::Settings {
            timeout: None,
            runtime: Some(
                baseline
                    .replace(
                        serde_json::from_value(values).unwrap(),
                        "fixture-http-child",
                    )
                    .unwrap(),
            ),
        })
    } else {
        None
    };
    context.call_scope.observe_http_dispatch();
    dispatcher
        .dispatch(child_context, child(plugin.mode, settings))
        .await
}
fn child(mode: Mode, settings: Option<core::Settings>) -> core::Request {
    let mut request = core::Request::new(Full::new(bytes::Bytes::from(serde_json::json!({"model":"model-a", "input":"original input", "stream":matches!(mode, Mode::HttpDispatchStream)}).to_string())).map_err(|never| match never {}).boxed_unsync());
    *request.method_mut() = http::Method::POST;
    *request.uri_mut() = if matches!(mode, Mode::HttpDispatchAdmin) {
        "/api/admin/plugins"
    } else {
        "/v1/responses"
    }
    .parse()
    .unwrap();
    request
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    request
        .headers_mut()
        .insert("x-custom", "preserved".parse().unwrap());
    if !matches!(
        mode,
        Mode::HttpDispatchUnauthorized | Mode::HttpDispatchAdmin
    ) {
        request
            .headers_mut()
            .insert("authorization", "Bearer sk_middleware".parse().unwrap());
    }
    if let Some(settings) = settings {
        request.extensions_mut().insert(settings);
    }
    request
}
#[tokio::test]
async fn child_http_model_authenticates_the_real_key_skips_all_initiating_mounts_and_charges_once()
{
    let f = Fixture::with_mounts(
        Mode::HttpDispatchModel,
        vec![
            PluginMiddlewareMount::Http,
            PluginMiddlewareMount::Request,
            PluginMiddlewareMount::Attempt,
        ],
    )
    .await;
    let response = f
        .app
        .clone()
        .oneshot(request("/virtual-plugin-route"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        String::from_utf8(body(response).await)
            .unwrap()
            .contains("resp_original")
    );
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.requests.lock().unwrap()[0].1, "model-a");
    assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
    assert!(f.plugin.seen.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_dispatch_cannot_synthesize_key_or_admin_authority() {
    for mode in [Mode::HttpDispatchUnauthorized, Mode::HttpDispatchAdmin] {
        let f = new(mode).await;
        let response = f
            .app
            .clone()
            .oneshot(request("/virtual-plugin-route"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        body(response).await;
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
        assert!(f.effects.charges.lock().unwrap().is_empty());
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn forged_parent_call_is_rejected_before_router_authentication_or_execution() {
    let f = new(Mode::HttpDispatchForged).await;
    body(
        f.app
            .clone()
            .oneshot(request("/virtual-plugin-route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn child_runtime_settings_rebase_to_authenticated_key_and_keep_override_provenance() {
    let f = new(Mode::HttpDispatchSettings).await;
    body(
        f.app
            .clone()
            .oneshot(request("/virtual-plugin-route"))
            .await
            .unwrap(),
    )
    .await;
    f.settled().await;
    {
        let settings = f.effects.settings.lock().unwrap();
        assert_eq!(
            serde_json::to_value(settings[0].values()).unwrap()["request_interval_ms"],
            173
        );
        assert_eq!(
            settings[0].inspect()["overrides"]["request_interval_ms"]["instance_id"],
            "fixture-http-child"
        );
        assert_eq!(
            settings[0].inspect()["execution"]["client_key_id"],
            "key_api_test"
        );
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn buffered_child_http_responses_keep_graph_capacity_after_core_settlement_and_return_it_at_eof()
 {
    let f = new(Mode::HttpDispatchBudget).await;
    body(
        f.app
            .clone()
            .oneshot(request("/virtual-plugin-route"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 17);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 17);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 17);
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn revocation_interrupts_child_sse_after_reply_and_finalizes_original_child_ledger_once() {
    use futures::StreamExt;
    let f = new(Mode::HttpDispatchStream).await;
    *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let response = f
        .app
        .clone()
        .oneshot(request("/virtual-plugin-route"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.plugin.access.revoke();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = stream.next().await {
            if frame.is_err() {
                break;
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
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
