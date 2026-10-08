use super::super::{
    DecodedResponsesRequest, decode_request_with_headers, request::forwarded_request_headers,
};
use crate::openai::{
    error::{gateway_error_response, runtime_unavailable_response},
    service::OpenAiService,
};
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    engine::{
        execution::{AuthenticatedClient, ClientTransport},
        middleware::*,
    },
    error::{GatewayError, GatewayErrorKind},
    lifecycle::ConnectionGuard,
};

pub(in crate::openai::responses) struct Ingress {
    pub client: AuthenticatedClient,
    pub decoded: DecodedResponsesRequest,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub endpoint: &'static str,
    pub connection: Option<Box<dyn ConnectionGuard>>,
    pub ingress_id: Option<String>,
}
pub(in crate::openai::responses) async fn respond(
    service: OpenAiService,
    registry: PluginMiddlewareRegistry,
    input: Ingress,
) -> Response {
    let prepared = match service.prepare_execution(input.client) {
        Ok(value) => value,
        Err(error) => return gateway_error_response(&error),
    };
    let streaming = input.decoded.metadata().stream();
    let transport = if streaming {
        ClientTransport::HttpSse
    } else {
        ClientTransport::HttpJson
    };
    let context = MiddlewareContext {
        request_id: prepared.request_id().clone(),
        client_key_id: prepared.client().policy().key_id().clone(),
        account_group_ids: prepared
            .client()
            .policy()
            .account_scope()
            .routing_snapshot()
            .groups_snapshot()
            .iter()
            .map(|group| group.id().clone())
            .collect(),
        mount: MiddlewareMount::Request,
        attempt_index: None,
        operation: input.decoded.operation().kind(),
        endpoint: input.endpoint.into(),
        transport,
        provider: None,
        model: Some(input.decoded.metadata().requested_model().into()),
        account_id: None,
        cancellation: prepared.cancellation(),
        deadline_at: prepared.deadline_at(),
    };
    let registry = std::sync::Arc::new(registry);
    let attempt_context = MiddlewareContext {
        mount: MiddlewareMount::Attempt,
        ..context.clone()
    };
    let attempts = match registry.freeze(attempt_context.clone()) {
        Ok(plan) => plan,
        Err(_) => return runtime_unavailable_response().into_response(),
    };
    let has_attempts = !attempts.is_empty();
    let prepared = prepared.with_revocation(attempts.cancellation());
    let prepared = if has_attempts {
        prepared.with_attempt_middleware(super::responses_attempt_middleware(
            registry.clone(),
            attempt_context,
        ))
    } else {
        prepared
    };
    let context = MiddlewareContext {
        cancellation: prepared.cancellation(),
        ..context
    };
    let plan = match registry.freeze(context) {
        Ok(plan) => plan,
        Err(_) => return runtime_unavailable_response().into_response(),
    };
    let cancellation = gateway_core::lifecycle::CancellationToken::linked([
        plan.cancellation(),
        attempts.cancellation(),
    ]);
    let access = prepared
        .client()
        .bound_access()
        .map(|guard| guard.with_revocation(cancellation.clone()));
    let prepared = prepared.with_revocation(cancellation);
    let request_id = prepared.request_id().as_str().to_owned();
    // Empty plans keep the existing transparent response path and its size contract.
    let mut response = if plan.is_empty() && !has_attempts {
        match service
            .start_prepared_response(prepared, input.decoded, transport, input.endpoint)
            .await
        {
            Ok(started) => {
                started.session.trace().headers(
                    "client.request",
                    serde_json::json!({"ingressRequestId":input.ingress_id}),
                    input
                        .headers
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_bytes())),
                );
                started
                    .session
                    .trace()
                    .capture("client.request.body", &input.body);
                if streaming {
                    super::super::stream_execution_response(started.session, input.connection).await
                } else {
                    super::super::collect_execution_response(started.session).await
                }
            }
            Err(error) => gateway_error_response(&error),
        }
    } else {
        let body = match super::super::request::decompress_request_body(&input.body, &input.headers)
        {
            Ok(body) => Bytes::copy_from_slice(&body),
            Err(error) => {
                return crate::openai::error::protocol_error_response(
                    StatusCode::BAD_REQUEST,
                    error.protocol_body(),
                );
            }
        };
        let request = MiddlewareRequest::new(
            "openai",
            forwarded_request_headers(&input.headers)
                .map(|(name, value)| {
                    MiddlewareHeader::new(name.as_str(), Bytes::copy_from_slice(value.as_bytes()))
                })
                .collect(),
            body,
        );
        let ip = input.decoded.metadata().client_ip();
        let agent = input.decoded.metadata().user_agent().map(str::to_owned);
        let next = gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
            Box::pin(async move {
                if request.protocol() != "openai" {
                    return Err(MiddlewareError::InvalidState);
                }
                let headers = filtered_headers(request.headers())?;
                let decoded = decode_request_with_headers(request.body(), &headers)
                    .map_err(|_| {
                        MiddlewareError::Gateway(GatewayError::new(
                            GatewayErrorKind::InvalidRequest,
                            "middleware request is invalid",
                        ))
                    })?
                    .with_client_context(ip, agent);
                if decoded.metadata().stream() != streaming {
                    return Err(MiddlewareError::Gateway(GatewayError::new(
                        GatewayErrorKind::InvalidRequest,
                        "middleware cannot change the client transport",
                    )));
                }
                let started = service
                    .start_prepared_response(prepared, decoded, transport, input.endpoint)
                    .await?;
                started.session.trace().headers(
                    "client.request",
                    serde_json::json!({"ingressRequestId":input.ingress_id}),
                    input
                        .headers
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_bytes())),
                );
                started
                    .session
                    .trace()
                    .capture("client.request.body", &input.body);
                super::execution_middleware_response(started.session, streaming).await
            })
        });
        match plan.run(request, next).await {
            Ok(response) => {
                super::deliver_middleware_response(response, streaming, input.connection).await
            }
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

pub(super) fn filtered_headers(input: &[MiddlewareHeader]) -> Result<HeaderMap, MiddlewareError> {
    let mut headers = HeaderMap::new();
    for header in input {
        let name = HeaderName::from_bytes(header.name().as_bytes())
            .map_err(|_| MiddlewareError::InvalidState)?;
        let value =
            HeaderValue::from_bytes(header.value()).map_err(|_| MiddlewareError::InvalidState)?;
        headers.append(name, value);
    }
    Ok(forwarded_request_headers(&headers)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}
