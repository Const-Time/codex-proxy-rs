//! Administrator-owned diagnostics use the native account probe, never a customer's Key.
use super::{Scope, invalid};
use crate::RpcReply;
use gateway_core::{
    account::ProviderAccountId,
    engine::probe::{AccountProbe, AccountProbeRequest},
    identity::ProviderKind,
    operation::{GenerateRequest, Operation, ProtocolPayload},
    routing::UpstreamModelId,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, SendState, Stage};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

pub(crate) struct AccountProbes {
    native: Arc<dyn AccountProbe>,
    capacity: tokio::sync::Semaphore,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    account_id: String,
    model: String,
    #[serde(default)]
    mode: ProbeMode,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProbeMode {
    #[default]
    Short,
    ModelDetection,
}
impl AccountProbes {
    pub(super) fn capabilities(&self, scope: &Scope) -> Result<RpcReply, PluginFault> {
        if !scope
            .account_probe
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(PluginFault::new(
                ErrorCode::PermissionDenied,
                "account diagnostics are not authorized",
            ));
        }
        super::encode(
            json!({"scheduled_detection":true,"maximum_output_tokens":4096,"timeout_seconds":90}),
        )
    }
    pub(crate) fn new(native: Arc<dyn AccountProbe>) -> Arc<Self> {
        Arc::new(Self {
            native,
            capacity: tokio::sync::Semaphore::new(1),
        })
    }
    pub(super) async fn execute(
        &self,
        scope: &Scope,
        params: Value,
        payload: Vec<u8>,
    ) -> Result<RpcReply, PluginFault> {
        if !scope
            .account_probe
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(PluginFault::new(
                ErrorCode::PermissionDenied,
                "account probes require an administrator or published maintenance root",
            ));
        }
        let query: Request = serde_json::from_value(params).map_err(|_| invalid())?;
        if payload.len() > 64 * 1024 {
            return Err(invalid());
        }
        let Value::Object(mut body) = serde_json::from_slice(&payload).map_err(|_| invalid())?
        else {
            return Err(invalid());
        };
        if body.get("model").and_then(Value::as_str) != Some(&query.model)
            || body.contains_key("previous_response_id")
            || body.contains_key("tools")
        {
            return Err(invalid());
        }
        body.insert("stream".into(), json!(true));
        body.insert("store".into(), json!(false));
        // Bound requested output where supported; Codex OAuth strips this unsupported field.
        // The timeout and semaphore apply independently of provider token-limit support.
        let short_maintenance =
            scope.context.stage == Stage::Maintenance && matches!(query.mode, ProbeMode::Short);
        let limit = if short_maintenance { 128 } else { 4096 };
        let maximum = body
            .get("max_output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(limit)
            .min(limit);
        body.insert("max_output_tokens".into(), json!(maximum));
        let request = AccountProbeRequest {
            account_id: ProviderAccountId::new(query.account_id).map_err(|_| invalid())?,
            provider_kind: ProviderKind::new("openai").map_err(|_| invalid())?,
            upstream_model: UpstreamModelId::new(query.model).map_err(|_| invalid())?,
            operation: Operation::Generate(GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body).map_err(|_| invalid())?,
            )),
        };
        let _permit = self.capacity.try_acquire().map_err(|_| {
            PluginFault::new(ErrorCode::Capacity, "another account diagnostic is running")
        })?;
        let timeout = Duration::from_secs(if short_maintenance { 20 } else { 90 });
        let result = tokio::select! { biased;
            () = scope.cancellation.cancelled() => return Err(uncertain(ErrorCode::Cancelled, "account diagnostic cancelled")),
            result = tokio::time::timeout(timeout, self.native.probe(request)) => result,
        }.map_err(|_| uncertain(ErrorCode::Timeout, "account diagnostic timed out"))?;
        match result {
            Ok(result) => {
                let text = result.text.concat();
                if text.len() > 64 * 1024 {
                    return Err(uncertain(
                        ErrorCode::Capacity,
                        "account diagnostic output exceeded limit",
                    ));
                }
                super::encode(json!({"text":text,"complete":result.complete,"status":"ok"}))
            }
            Err(error) => {
                let status = error.upstream_response().map(|r| r.status());
                // Raw upstream bodies can contain credentials; retain only bounded, stable facts.
                let mut fault = PluginFault::new(ErrorCode::Upstream, "account diagnostic failed");
                fault.http_status = status;
                fault.send_state = match error.send_state() {
                    Some(gateway_core::upstream::UpstreamSendState::NotSent) => SendState::NotSent,
                    Some(gateway_core::upstream::UpstreamSendState::Sent) => SendState::Sent,
                    _ => SendState::Ambiguous,
                };
                let code = error.client_error_code().filter(|code| {
                    matches!(
                        *code,
                        "account_deactivated" | "account_suspended" | "account_banned"
                    )
                });
                fault.details = Some(json!({"source":error.source().as_str(),"code":code}));
                Err(fault)
            }
        }
    }
}
fn uncertain(code: ErrorCode, message: &str) -> PluginFault {
    let mut fault = PluginFault::new(code, message);
    fault.send_state = SendState::Ambiguous;
    fault
}
