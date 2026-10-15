use crate::openai::responses::{
    DecodedResponsesRequest, decode_request_with_headers, request::forwarded_request_headers,
};
use crate::openai::{
    error::{gateway_error_response, runtime_unavailable_response},
    service::OpenAiService,
};
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
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

pub(in crate::openai) struct Ingress {
    pub client: AuthenticatedClient,
    pub decoded: DecodedResponsesRequest,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub endpoint: &'static str,
    pub connection: Option<Box<dyn ConnectionGuard>>,
    pub ingress_id: Option<String>,
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
    let streaming = input.decoded.metadata().stream();
    let transport = if streaming {
        ClientTransport::HttpSse
    } else {
        ClientTransport::HttpJson
    };
    let context = super::root::context(
        &prepared,
        input.decoded.operation(),
        input.endpoint,
        transport,
        Some(input.decoded.metadata().requested_model().into()),
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
    let access = prepared
        .client()
        .bound_access()
        .map(|guard| guard.with_revocation(revocation));
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
                    crate::openai::responses::stream_execution_response(
                        started.session,
                        input.connection,
                    )
                    .await
                } else {
                    crate::openai::responses::collect_execution_response(started.session).await
                }
            }
            Err(error) => gateway_error_response(&error),
        }
    } else {
        let body = match crate::openai::responses::request::decompress_request_body(
            &input.body,
            &input.headers,
        ) {
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
        )
        .with_settings(prepared.settings().clone())
        .with_execution_context(prepared.nested_context());
        let ip = input.decoded.metadata().client_ip();
        let agent = input.decoded.metadata().user_agent().map(str::to_owned);
        let (admitted, keepalive) = tokio::sync::oneshot::channel();
        let next = gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
            Box::pin(async move {
                if request.protocol() != "openai" {
                    return Err(MiddlewareError::InvalidState);
                }
                let prepared = prepared.with_settings(
                    request
                        .settings()
                        .ok_or(MiddlewareError::InvalidState)?
                        .clone(),
                )?;
                let headers = super::headers::filtered_headers(request.headers())?;
                let decoded = decode_request_with_headers(request.body(), &headers)
                    .map_err(|_| {
                        MiddlewareError::Gateway(GatewayError::new(
                            GatewayErrorKind::InvalidRequest,
                            "middleware request is invalid",
                        ))
                    })?
                    .with_client_context(ip, agent)
                    .with_middleware_capabilities(&request)?;
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
                let _ = admitted.send(started.session.response_commit());
                super::execution_middleware_response(started.session, transport).await
            })
        });
        let response = async move {
            match plan.run(request, next).await {
                Ok(response) => {
                    super::deliver_middleware_response(response, streaming, input.connection).await
                }
                Err(error) => super::delivery::error_response(&error),
            }
        };
        if streaming {
            crate::openai::responses::keepalive::response(response, keepalive).await
        } else {
            response.await
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
