//! Images and standalone Search retain their raw JSON and fixed provider route.
use crate::openai::{
    error::{gateway_error_response, runtime_unavailable_response},
    responses::request::forwarded_request_headers,
    service::OpenAiService,
};
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    engine::{
        execution::{AuthenticatedClient, ClientTransport},
        middleware::*,
    },
    operation::{Operation, RawJsonPayload},
};

pub(in crate::openai) struct Ingress {
    pub client: AuthenticatedClient,
    pub operation: Operation,
    pub headers: HeaderMap,
    pub client_ip: Option<std::net::IpAddr>,
    pub user_agent: Option<String>,
    pub endpoint: &'static str,
}
pub(in crate::openai) async fn respond(
    service: OpenAiService,
    registry: PluginMiddlewareRegistry,
    input: Ingress,
) -> Response {
    let prepared = match service.prepare_execution(input.client) {
        Ok(value) => value,
        Err(error) => return gateway_error_response(&error),
    };
    let context = super::root::context(
        &prepared,
        &input.operation,
        input.endpoint,
        ClientTransport::HttpJson,
        None,
    );
    let root = match super::root::bind(prepared, registry, context) {
        Ok(root) => root,
        Err(_) => return runtime_unavailable_response().into_response(),
    };
    let super::root::RootPlan {
        prepared,
        plan,
        has_attempts,
        revocation,
    } = root;
    let request_id = prepared.request_id().as_str().to_owned();
    let access = prepared
        .client()
        .bound_access()
        .map(|guard| guard.with_revocation(revocation));
    let mut response = if plan.is_empty() && !has_attempts {
        match service
            .start_prepared_provider_endpoint(
                prepared,
                input.operation,
                input.client_ip,
                input.user_agent,
                input.endpoint,
            )
            .await
        {
            Ok(started) => crate::openai::endpoint::collect_raw_json_response(started).await,
            Err(error) => gateway_error_response(&error),
        }
    } else {
        let request = match request(&input.operation, &input.headers) {
            Ok(request) => request,
            Err(error) => return super::delivery::error_response(&error),
        };
        let next = gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
            Box::pin(async move {
                let operation = rewrite(&input.operation, &request)?;
                let started = service
                    .start_prepared_provider_endpoint(
                        prepared,
                        operation,
                        input.client_ip,
                        input.user_agent,
                        input.endpoint,
                    )
                    .await?;
                super::body::ExecutionBody::response(started.session, super::WireFormat::RawJson)
                    .await
            })
        });
        match plan.run(request, next).await {
            Ok(response) => super::deliver_middleware_response(response, false, None).await,
            Err(error) => super::delivery::error_response(&error),
        }
    };
    if let Ok(id) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-gateway-request-id", id);
    }
    if let Some(access) = access {
        response = crate::openai::revocation::protect(response, access);
    }
    response
}
pub(super) fn payload(operation: &Operation) -> Result<&RawJsonPayload, MiddlewareError> {
    let payload = match operation {
        Operation::GenerateImage(image) => image.payload(),
        Operation::Search(search) => search.payload(),
        _ => return Err(MiddlewareError::InvalidState),
    };
    if payload.protocol() != "openai" {
        return Err(MiddlewareError::InvalidState);
    }
    Ok(payload)
}
pub(super) fn request(
    operation: &Operation,
    headers: &HeaderMap,
) -> Result<MiddlewareRequest, MiddlewareError> {
    Ok(MiddlewareRequest::new(
        "openai",
        forwarded_request_headers(headers)
            .map(|(name, value)| {
                MiddlewareHeader::new(name.as_str(), Bytes::copy_from_slice(value.as_bytes()))
            })
            .collect(),
        payload(operation)?.body().clone(),
    ))
}
pub(super) fn rewrite(
    original: &Operation,
    request: &MiddlewareRequest,
) -> Result<Operation, MiddlewareError> {
    if request.protocol() != "openai" || request.has_capability_declaration() {
        return Err(MiddlewareError::InvalidState);
    }
    let headers = super::headers::filtered_headers(request.headers())?;
    // Endpoint kind is host-owned; body.model never becomes a text-routing fact.
    match original {
        Operation::GenerateImage(image) => {
            crate::openai::images::image_operation(request.body().clone(), &headers, image.kind())
                .map_err(Into::into)
        }
        Operation::Search(_) => {
            crate::openai::search::search_operation(request.body().clone(), &headers)
                .map_err(Into::into)
        }
        _ => Err(MiddlewareError::InvalidState),
    }
}

// Raw endpoints have a deliberately smaller header contract than Responses.
// Reconstruct only the semantic headers their Provider already consumes.
pub(super) fn context_headers(payload: &RawJsonPayload) -> Result<HeaderMap, MiddlewareError> {
    let mut headers = HeaderMap::new();
    for (field, name) in [
        ("session_id", "session-id"),
        ("thread_id", "thread-id"),
        ("turn_metadata", "x-codex-turn-metadata"),
        ("image_turn_id", "x-codex-image-turn-id"),
    ] {
        if let Some(value) = payload.context().get(field) {
            let value = value.as_str().ok_or(MiddlewareError::InvalidState)?;
            headers.insert(
                name,
                HeaderValue::from_str(value).map_err(|_| MiddlewareError::InvalidState)?,
            );
        }
    }
    Ok(headers)
}
