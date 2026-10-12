//! SDK model metadata is decoded here; Core retains routing and continuation authority.
use super::invalid;
use bytes::Bytes;
use gateway_core::{
    account::ProviderAccountId,
    engine::{
        continuation::PreviousResponseId,
        execution::{ClientTransport, ExecutionRequestMetadata, PreparedExecutionRequest},
        nested::ModelExecutionTarget,
    },
    identity::ProviderKind,
    operation::{
        GenerateRequest, ImageRequest, ImageRequestKind, Operation, ProtocolPayload,
        RawJsonPayload, StandaloneSearchRequest,
    },
    routing::PublicModelId,
};
use gateway_plugin_sdk::{
    CallContext,
    call::host::{ModelExecuteRequest, ModelOperation},
};
use serde_json::Value;

pub(super) struct Request {
    pub request: PreparedExecutionRequest,
    pub target: ModelExecutionTarget,
    pub endpoint_provider: Option<ProviderKind>,
}
pub(super) fn decode(
    context: &CallContext,
    metadata: &ModelExecuteRequest,
    payload: Vec<u8>,
    streaming: bool,
) -> Result<Request, gateway_plugin_sdk::PluginFault> {
    if metadata.protocol != "openai" {
        return Err(gateway_plugin_sdk::PluginFault::new(
            gateway_plugin_sdk::ErrorCode::Unsupported,
            "host model protocol is unsupported",
        ));
    }
    if payload.len() > super::MAXIMUM_BYTES {
        return Err(super::capacity());
    }
    let Value::Object(body) = serde_json::from_slice(&payload).map_err(|_| invalid())? else {
        return Err(invalid());
    };
    let public_model =
        PublicModelId::from_client_wire(metadata.model.clone()).map_err(|_| invalid())?;
    if body
        .get("model")
        .is_some_and(|m| m.as_str() != Some(&metadata.model))
        || (metadata.operation == ModelOperation::Generate && !body.contains_key("model"))
        || body.get("stream").is_some_and(|s| s.as_bool().is_none())
        || body
            .get("previous_response_id")
            .is_some_and(|p| !p.is_null() && p.as_str().is_none())
    {
        return Err(invalid());
    }
    let previous = body.get("previous_response_id").and_then(Value::as_str);
    if previous != metadata.previous_response_id.as_deref() {
        return Err(invalid());
    }
    if metadata.operation != ModelOperation::Generate && previous.is_some() {
        return Err(invalid());
    }
    let provider = metadata
        .provider
        .clone()
        .map(ProviderKind::new)
        .transpose()
        .map_err(|_| invalid())?;
    let account = metadata
        .account_id
        .clone()
        .map(ProviderAccountId::new)
        .transpose()
        .map_err(|_| invalid())?;
    let parent_account = context
        .account_id
        .clone()
        .map(ProviderAccountId::new)
        .transpose()
        .map_err(|_| invalid())?;
    let raw = || RawJsonPayload::new("openai", Bytes::from(payload.clone())).map_err(|_| invalid());
    let (operation, endpoint, endpoint_provider) = match metadata.operation {
        ModelOperation::Generate => (
            Operation::Generate(GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body).map_err(|_| invalid())?,
            )),
            "/v1/responses",
            None,
        ),
        ModelOperation::GenerateImage => (
            Operation::GenerateImage(ImageRequest::from_raw_json(
                ImageRequestKind::Generation,
                raw()?,
            )),
            "/v1/images/generations",
            Some(ProviderKind::new("openai").map_err(|_| invalid())?),
        ),
        ModelOperation::EditImage => (
            Operation::GenerateImage(ImageRequest::from_raw_json(ImageRequestKind::Edit, raw()?)),
            "/v1/images/edits",
            Some(ProviderKind::new("openai").map_err(|_| invalid())?),
        ),
        ModelOperation::Search => (
            Operation::Search(StandaloneSearchRequest::from_raw_json(raw()?)),
            "/v1/alpha/search",
            Some(ProviderKind::new("openai").map_err(|_| invalid())?),
        ),
    };
    if endpoint_provider
        .as_ref()
        .zip(provider.as_ref())
        .is_some_and(|(endpoint, explicit)| endpoint != explicit)
    {
        return Err(invalid());
    }
    let target = ModelExecutionTarget {
        provider: provider.or_else(|| endpoint_provider.clone()),
        account,
        parent_account,
    };
    Ok(Request {
        request: PreparedExecutionRequest {
            public_model,
            operation,
            metadata: ExecutionRequestMetadata {
                protocol: metadata.protocol.clone(),
                endpoint: endpoint.into(),
                transport: if streaming && metadata.operation == ModelOperation::Generate {
                    ClientTransport::HttpSse
                } else {
                    ClientTransport::HttpJson
                },
                stream: streaming,
                client_ip: None,
                user_agent: None,
                previous_response_id: metadata
                    .previous_response_id
                    .clone()
                    .map(PreviousResponseId::new),
            },
        },
        target,
        endpoint_provider,
    })
}
