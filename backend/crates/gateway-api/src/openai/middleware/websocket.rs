//! WebSocket request middleware shares Core's body, delivery barrier and control pump.
use crate::openai::{
    responses::{
        DecodedResponsesRequest,
        websocket::{
            connection::ResponsesWebSocketConnection,
            forward::{self, ConnectionReplaySnapshot, ForwardOutcome},
            protocol::response_metadata_event,
        },
    },
    service::OpenAiService,
};
use bytes::Bytes;
use gateway_admin::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::{
    diagnostics::TraceContext,
    engine::{
        execution::{AuthenticatedClient, ClientTransport},
        middleware::*,
        response_control::ResponseControl,
    },
    error::{GatewayError, GatewayErrorKind},
    event::ProviderResponseHeader,
    operation::Operation,
};
use std::sync::{Arc, Mutex};

pub(in crate::openai) struct Ingress {
    pub client: AuthenticatedClient,
    pub decoded: DecodedResponsesRequest,
    pub payload: String,
    pub correlation_id: Arc<str>,
    pub request_index: u64,
}

pub(in crate::openai) async fn respond(
    connection: &mut ResponsesWebSocketConnection,
    replay: &mut ConnectionReplaySnapshot,
    service: OpenAiService,
    registry: PluginMiddlewareRegistry,
    input: Ingress,
) -> ForwardOutcome {
    let prepared = match service.prepare_execution(input.client) {
        Ok(prepared) => prepared,
        Err(error) => {
            return forward::send_gateway_error(connection, &error, &input.correlation_id).await;
        }
    };
    let context = super::root::context(
        &prepared,
        input.decoded.operation(),
        "/v1/responses",
        ClientTransport::WebSocket,
        Some(input.decoded.metadata().requested_model().into()),
    );
    let root = match super::root::bind(prepared, registry, context) {
        Ok(root) => root,
        Err(_) => {
            return send_error(connection, MiddlewareError::Fault, &input.correlation_id).await;
        }
    };
    let request_id: Arc<str> = root.prepared.request_id().to_string().into();
    let trace = TraceContext::new(&request_id);
    trace.record("client.connection", serde_json::json!({
        "transport":"websocket", "connectionId":connection.id(), "correlationId":input.correlation_id.as_ref(), "requestIndex":input.request_index,
    }));
    trace.capture("client.request.body", input.payload.as_bytes());
    let revocation = root.revocation.clone();
    let work = async {
        if root.plan.is_empty() {
            let started = forward::await_active(
                connection,
                service.start_prepared_response(
                    root.prepared,
                    input.decoded,
                    ClientTransport::WebSocket,
                    "/v1/responses",
                ),
                || None,
                &trace,
                &request_id,
            )
            .await;
            return match started {
                Some(Ok(started)) => {
                    forward::forward_execution(connection, started, replay, Some(root.revocation))
                        .await
                }
                Some(Err(error)) => {
                    forward::send_gateway_error(connection, &error, &request_id).await
                }
                None => ForwardOutcome::Disconnect,
            };
        }
        let control = Arc::new(Mutex::new(None::<ResponseControl>));
        let next_control = control.clone();
        let snapshot = replay.clone();
        let Operation::Generate(generate) = input.decoded.operation() else {
            return send_error(connection, MiddlewareError::InvalidState, &request_id).await;
        };
        let headers = match super::headers::from_context(generate.protocol_payload().context()) {
            Ok(headers) => headers,
            Err(error) => return send_error(connection, error, &request_id).await,
        };
        let body = match serde_json::to_vec(generate.protocol_payload().body()) {
            Ok(body) => Bytes::from(body),
            Err(_) => {
                return send_error(connection, MiddlewareError::InvalidState, &request_id).await;
            }
        };
        let request = MiddlewareRequest::new(
            "openai",
            headers
                .iter()
                .map(|(name, value)| {
                    MiddlewareHeader::new(name.as_str(), Bytes::copy_from_slice(value.as_bytes()))
                })
                .collect(),
            body,
        );
        let next = gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
            Box::pin(async move {
                if request.protocol() != "openai" {
                    return Err(MiddlewareError::InvalidState);
                }
                let headers = super::headers::filtered_headers(request.headers())?;
                let decoded = super::attempt::request::decode(
                    input.decoded.operation(),
                    request.body(),
                    &headers,
                    super::WireFormat::WebSocket,
                )?
                .with_client_context(
                    input.decoded.metadata().client_ip(),
                    input.decoded.metadata().user_agent().map(str::to_owned),
                );
                // Select private replay state using the rewritten continuation, never plugin bytes.
                let decoded = snapshot.prepare(decoded.with_middleware_capabilities(&request)?);
                let started = service
                    .start_prepared_response(
                        root.prepared,
                        decoded,
                        ClientTransport::WebSocket,
                        "/v1/responses",
                    )
                    .await?;
                *next_control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    started.session.response_control();
                super::execution_middleware_response(started.session, ClientTransport::WebSocket)
                    .await
            })
        });
        let control_ref = || {
            control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        };
        let response = match forward::await_active(
            connection,
            root.plan.run(request, next),
            control_ref,
            &trace,
            &request_id,
        )
        .await
        {
            Some(Ok(response)) => response,
            Some(Err(error)) => return send_error(connection, error, &request_id).await,
            None => return ForwardOutcome::Disconnect,
        };
        let (mut delivery, headers) =
            super::delivery::Delivery::new(response, super::WireFormat::WebSocket, None);
        if let Err(error) = delivery.validate() {
            return fail_delivery(connection, &mut delivery, error, &request_id).await;
        }
        let status = delivery.status();
        let headers: Vec<_> = headers
            .into_iter()
            .map(|h| {
                let (name, value) = h.into_parts();
                ProviderResponseHeader::new(name, value)
            })
            .collect();
        let mut first_written = false;
        loop {
            let result = forward::await_active(
                connection,
                async {
                    if !(200..300).contains(&status) {
                        let document = delivery.document().await?;
                        let mut value: serde_json::Map<String, serde_json::Value> =
                            serde_json::from_slice(&document)
                                .map_err(|_| MiddlewareError::InvalidState)?;
                        value.insert("type".into(), "error".into());
                        value.insert("status".into(), status.into());
                        return Ok((vec![serde_json::Value::Object(value).to_string()], true));
                    }
                    delivery.advance().await?;
                    let (frames, terminal) = delivery.take_output();
                    let messages = frames
                        .into_iter()
                        .map(|bytes| {
                            String::from_utf8(bytes.to_vec())
                                .map_err(|_| MiddlewareError::InvalidState)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((messages, terminal))
                },
                control_ref,
                &trace,
                &request_id,
            )
            .await;
            let (messages, terminal) = match result {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    return fail_delivery(connection, &mut delivery, error, &request_id).await;
                }
                None => return ForwardOutcome::Disconnect,
            };
            if !first_written && messages.is_empty() {
                return fail_delivery(
                    connection,
                    &mut delivery,
                    MiddlewareError::InvalidState,
                    &request_id,
                )
                .await;
            }
            let terminal_at = tokio::time::Instant::now();
            if terminal && let Some((id, state)) = delivery.replay() {
                replay.commit(id, state);
            }
            let success = (first_written
                || forward::send_metadata(
                    connection,
                    &request_id,
                    response_metadata_event(&request_id, &headers),
                )
                .await)
                && forward::send_messages(
                    connection,
                    messages,
                    &request_id,
                    &mut first_written,
                    terminal,
                )
                .await;
            if !success {
                if terminal {
                    forward::log_terminal_write_failure(
                        connection,
                        &request_id,
                        terminal_at.elapsed(),
                    );
                }
                return ForwardOutcome::Disconnect;
            }
            if terminal {
                forward::log_terminal_write_success(connection, &request_id, terminal_at.elapsed());
                return ForwardOutcome::Continue;
            }
        }
    };
    let result = tokio::select! { biased;
        () = revocation.cancelled() => None,
        result = work => Some(result),
    };
    match result {
        Some(result) => result,
        None => send_error(connection, MiddlewareError::Fault, &request_id).await,
    }
}

async fn send_error(
    connection: &mut ResponsesWebSocketConnection,
    error: MiddlewareError,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    match error {
        MiddlewareError::Engine(error) => {
            forward::send_engine_error(connection, &error, &[], request_id).await
        }
        error => forward::send_gateway_error(connection, &gateway_error(error), request_id).await,
    }
}

fn gateway_error(error: MiddlewareError) -> GatewayError {
    match error {
        MiddlewareError::Gateway(error) => error,
        MiddlewareError::Engine(error) => crate::openai::error::gateway_error_from_engine(&error),
        _ => GatewayError::new(GatewayErrorKind::Internal, "plugin middleware failed"),
    }
}

async fn fail_delivery(
    connection: &mut ResponsesWebSocketConnection,
    delivery: &mut super::delivery::Delivery,
    error: MiddlewareError,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    let status = match &error {
        MiddlewareError::Engine(error) => forward::engine_error_status(error),
        MiddlewareError::Gateway(error) => {
            crate::openai::error::gateway_error_contract(error.kind())
                .0
                .as_u16()
        }
        _ => 500,
    };
    delivery.record_client_status(status).await;
    delivery.close().await;
    send_error(connection, error, request_id).await
}
