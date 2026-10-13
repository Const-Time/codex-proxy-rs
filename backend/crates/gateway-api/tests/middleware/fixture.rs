//! HTTP placement is tested through the real router, publication and Core ledger.
use crate::openai::middleware::fixture::{Mode, Plugin};
use gateway_core::engine::middleware::{MiddlewareError, http as core};
use serde_json::json;
use std::sync::Arc;

pub(crate) async fn handle(
    plugin: &Plugin,
    context: core::Context,
    mut request: core::Request,
    next: core::Next,
) -> Result<core::Response, MiddlewareError> {
    plugin
        .http_seen
        .lock()
        .unwrap()
        .push((request.uri().to_string(), context.clone()));
    match plugin.mode {
        Mode::HttpDispatchModel
        | Mode::HttpDispatchStream
        | Mode::HttpDispatchUnauthorized
        | Mode::HttpDispatchAdmin
        | Mode::HttpDispatchBudget
        | Mode::HttpDispatchForged
        | Mode::HttpDispatchSettings => {
            return super::dispatcher::handle(plugin, context, request).await;
        }
        Mode::WaitBeforeNext => return futures::future::pending().await,
        Mode::Rewrite => {
            *request.uri_mut() = "/v1/responses?raw=%2F".parse().unwrap();
            *request.method_mut() = http::Method::POST;
        }
        Mode::HttpAuthError => {
            request
                .headers_mut()
                .insert("authorization", "Bearer invalid-key".parse().unwrap());
        }
        Mode::ShortCircuit => return Err(MiddlewareError::Rejected),
        Mode::ShortCircuitRaw => return Ok(core::Response::new(request.into_body())),
        Mode::Settings => {
            let settings = request
                .extensions_mut()
                .get_mut::<core::Settings>()
                .unwrap();
            let baseline = settings.runtime.as_ref().unwrap();
            assert!(baseline.execution_values().is_none());
            let mut values = serde_json::to_value(baseline.values()).unwrap();
            values["request_interval_ms"] = json!(137);
            settings.runtime = Some(
                baseline
                    .replace(serde_json::from_value(values).unwrap(), "fixture-http")
                    .unwrap(),
            );
        }
        Mode::ZeroTimeout => {
            request
                .extensions_mut()
                .get_mut::<core::Settings>()
                .unwrap()
                .timeout = Some(std::time::Duration::ZERO)
        }
        Mode::HttpUpgrade | Mode::HttpUpgradeBadResponse | Mode::HttpUpgradeDiscard => {
            let (mut parts, _) = request.into_parts();
            let upgrade = parts
                .extensions
                .remove::<Arc<dyn core::upgrade::WebSocketUpgrade>>()
                .unwrap();
            let (mut response, pending) = upgrade.accept(parts, vec!["fixture".into()]).await?;
            let task = Box::pin(async move {
                let socket = pending.await.map_err(|error| MiddlewareError::Remote {
                    rejected: error.is_rejected(),
                    source: Box::new(error),
                })?;
                while let Some(message) = socket.receive().await? {
                    socket.send(message).await?;
                }
                Ok(())
            });
            let status = response.status();
            response
                .extensions_mut()
                .insert(core::upgrade::Upgraded::new(status, task)?);
            if matches!(plugin.mode, Mode::HttpUpgradeBadResponse) {
                *response.status_mut() = http::StatusCode::FORBIDDEN;
            }
            if matches!(plugin.mode, Mode::HttpUpgradeDiscard) {
                return Ok(http::Response::builder()
                    .status(403)
                    .body(core::empty_body())
                    .unwrap());
            }
            return Ok(response);
        }
        _ => {}
    }
    let mut response = next.run(request).await?;
    if matches!(plugin.mode, Mode::WaitAfterNext) {
        return futures::future::pending().await;
    }
    if matches!(plugin.mode, Mode::BadResponse) {
        *response.status_mut() = http::StatusCode::FORBIDDEN;
    }
    if matches!(plugin.mode, Mode::Head) {
        return Ok(http::Response::builder()
            .status(403)
            .body(core::empty_body())
            .unwrap());
    }
    response
        .headers_mut()
        .insert("x-http-layer", "fixture".parse().unwrap());
    Ok(response)
}
