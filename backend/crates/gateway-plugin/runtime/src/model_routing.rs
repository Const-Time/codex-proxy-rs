//! Decode the SDK routing decision; Core revalidates the actual route.
use crate::{RpcSession, models::Models};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::{
    engine::model_routing::*, identity::ProviderKind, operation::Operation, routing::PublicModelId,
};
use gateway_plugin_sdk::{
    Capability, Stage,
    call::policy::{ModelRouteDecision as WireDecision, ModelRouteRequest, PolicyHeader},
};
use std::{sync::Arc, time::Duration};

struct ModelScope {
    models: Option<Arc<Models>>,
    context: gateway_plugin_sdk::CallContext,
}
impl Drop for ModelScope {
    fn drop(&mut self) {
        if let Some(models) = &self.models {
            models.finish(&self.context);
        }
    }
}
pub(crate) async fn invoke(
    session: &Arc<RpcSession>,
    models: Option<Arc<Models>>,
    outbound: Arc<crate::outbound::Registry>,
    services: Arc<crate::services::Registry>,
    input: ModelRouteInput,
) -> Result<ModelRouteDecision, ModelRoutingFault> {
    if session.capability_version(Capability::ModelRouter) != Some(1)
        || !session.capability_supports_stage(Capability::ModelRouter, Stage::Routing)
    {
        return Err(ModelRoutingFault);
    }
    let Operation::Generate(generate) = input.operation() else {
        return Err(ModelRoutingFault);
    };
    let payload = generate.protocol_payload();
    let body = serde_json::to_vec(payload.body()).map_err(|_| ModelRoutingFault)?;
    if body.len() > 8 * 1024 * 1024 {
        return Err(ModelRoutingFault);
    }
    let headers = headers(payload.context())?;
    let timeout = input
        .deadline()
        .bounded(Duration::from_secs(2).min(session.maximum_call_timeout()));
    if timeout.is_zero() || input.cancellation().is_cancelled() {
        return Err(ModelRoutingFault);
    }
    let mut context = session.context(Stage::Routing, timeout);
    context.request_id = Some(input.request_id().as_str().into());
    context.request = crate::request_context::wire(input.nested_context().scope());
    outbound
        .bind(
            &context,
            input.cancellation(),
            Some(input.nested_context().scope().clone()),
        )
        .map_err(|_| ModelRoutingFault)?;
    let _network = outbound.guard(&context);
    let _services = services
        .bind_parent(&context, &input.nested_context(), input.cancellation())
        .map_err(|_| ModelRoutingFault)?;
    if let Some(models) = &models {
        models
            .bind(
                &context,
                Some(input.nested_context()),
                None,
                input.cancellation(),
                None,
                None,
            )
            .map_err(|_| ModelRoutingFault)?;
    }
    let _scope = ModelScope {
        models,
        context: context.clone(),
    };
    let request = ModelRouteRequest {
        request_id: input.request_id().as_str().into(),
        operation: input.operation().kind().as_str().into(),
        protocol: payload.protocol().into(),
        model: input.requested_model().as_str().into(),
        available_providers: input
            .available_providers()
            .iter()
            .map(|p| p.as_str().into())
            .collect(),
        headers,
    };
    let reply = session
        .call(
            "policy.route_model",
            context,
            serde_json::to_value(request).map_err(|_| ModelRoutingFault)?,
            body,
        )
        .await
        .map_err(|_| ModelRoutingFault)?;
    if !reply.payload.is_empty() {
        session.invalid_response(Stage::Routing);
        return Err(ModelRoutingFault);
    }
    // Unit variants need an explicit shape check: Serde otherwise ignores
    // additional fields on internally tagged unhandled/reject decisions.
    let valid_shape = reply.result.as_object().is_some_and(|fields| {
        let route = fields.get("decision").and_then(serde_json::Value::as_str) == Some("route");
        if route {
            fields
                .keys()
                .all(|key| matches!(key.as_str(), "decision" | "provider" | "model"))
        } else {
            fields.len() == 1 && fields.contains_key("decision")
        }
    });
    if !valid_shape {
        session.invalid_response(Stage::Routing);
        return Err(ModelRoutingFault);
    }
    let decision: WireDecision = session
        .decode_response(Stage::Routing, reply.result)
        .map_err(|_| ModelRoutingFault)?;
    let result = (|| match decision {
        WireDecision::Unhandled => Ok(ModelRouteDecision::Unhandled),
        WireDecision::Reject => Ok(ModelRouteDecision::Reject),
        WireDecision::Route { provider, model } => {
            if provider.is_none() && model.is_none() {
                return Err(ModelRoutingFault);
            }
            let provider = provider
                .map(|p| ProviderKind::new(p).map_err(|_| ModelRoutingFault))
                .transpose()?;
            let model = model
                .map(|m| PublicModelId::new(m).map_err(|_| ModelRoutingFault))
                .transpose()?;
            Ok(ModelRouteDecision::Route { provider, model })
        }
    })();
    if result.is_err() {
        session.invalid_response(Stage::Routing);
    }
    result
}
pub(crate) fn headers(
    context: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<PolicyHeader>, ModelRoutingFault> {
    let Some(value) = context.get("opaque_request_headers") else {
        return Ok(vec![]);
    };
    let entries = value
        .as_array()
        .filter(|h| h.len() <= 128)
        .ok_or(ModelRoutingFault)?;
    let mut headers = Vec::with_capacity(entries.len());
    let mut total = 0_usize;
    for entry in entries {
        let pair = entry
            .as_array()
            .filter(|p| p.len() == 2)
            .ok_or(ModelRoutingFault)?;
        let name = pair[0]
            .as_str()
            .ok_or(ModelRoutingFault)?
            .to_ascii_lowercase();
        let encoded = pair[1]
            .as_str()
            .filter(|v| v.len() <= 16 * 1024)
            .ok_or(ModelRoutingFault)?;
        let value = STANDARD.decode(encoded).map_err(|_| ModelRoutingFault)?;
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            || value
                .iter()
                .any(|b| *b != b'\t' && (*b < b' ' || *b == 0x7f))
        {
            return Err(ModelRoutingFault);
        }
        total = total
            .checked_add(name.len())
            .and_then(|n| n.checked_add(encoded.len()))
            .ok_or(ModelRoutingFault)?;
        if total > 64 * 1024 {
            return Err(ModelRoutingFault);
        }
        headers.push(PolicyHeader {
            name,
            value_base64: encoded.into(),
        });
    }
    Ok(headers)
}
