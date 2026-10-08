//! 已启动 Responses execution 到客户端 WebSocket wire event 的串行转发。

use std::sync::Arc;

use axum::http::StatusCode;
use gateway_core::engine::execution::StartedExecution;
use gateway_core::engine::{CommitRequirement, CoordinatedEvent, EngineError};
use gateway_core::error::{ContinuationRecoveryDisposition, GatewayError, GatewayErrorKind};
use gateway_core::operation::ProviderSessionState;
use tokio::time::Instant;

use crate::openai::error::{gateway_error_contract, gateway_error_from_engine};

use super::{
    super::{DecodedResponsesRequest, OpenAiResponsesEncoder, PendingExecution, ProtocolErrorBody},
    connection::{ConnectionEvent, FramePhase, ResponsesWebSocketConnection, WriteContext},
    protocol::{decode_response_interrupt, error_event, response_metadata_event},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::openai) enum ForwardOutcome {
    Continue,
    Disconnect,
}

#[derive(Clone, Default)]
pub(in crate::openai) struct ConnectionReplaySnapshot {
    last_response_id: Option<String>,
    provider_state: Option<ProviderSessionState>,
}

impl ConnectionReplaySnapshot {
    pub(in crate::openai) fn prepare(
        &self,
        request: DecodedResponsesRequest,
    ) -> DecodedResponsesRequest {
        match (
            request.metadata().continuation().previous_response_id(),
            self.last_response_id.as_deref(),
            self.provider_state.as_ref(),
        ) {
            (Some(previous), Some(last), Some(state)) if previous == last => {
                request.with_provider_session_state(state.clone())
            }
            _ => request,
        }
    }

    pub(in crate::openai) fn commit(
        &mut self,
        response_id: String,
        provider_state: Option<ProviderSessionState>,
    ) {
        self.last_response_id = Some(response_id);
        self.provider_state = provider_state;
    }
}

pub(in crate::openai) async fn forward_execution(
    connection: &mut ResponsesWebSocketConnection,
    started: StartedExecution,
    replay: &mut ConnectionReplaySnapshot,
    revocation: Option<gateway_core::lifecycle::CancellationToken>,
) -> ForwardOutcome {
    let request_id = Arc::<str>::from(started.request_id.to_string());
    let streaming = started.stream;
    // Construct cleanup ownership before a ready revocation can win the select.
    let execution = PendingExecution::new(started.session);
    let Some(revocation) = revocation else {
        return forward_execution_inner(connection, request_id, streaming, execution, replay).await;
    };
    // Revocation also covers a completed Core response waiting on a WS write.
    // Dropping the inner forwarder retains PendingExecution's original cleanup.
    let outcome = tokio::select! { biased;
        () = revocation.cancelled() => None,
        outcome = forward_execution_inner(connection, request_id.clone(), streaming, execution, replay) => Some(outcome),
    };
    match outcome {
        Some(outcome) => outcome,
        None => {
            send_gateway_error(
                connection,
                &GatewayError::new(GatewayErrorKind::Internal, "plugin middleware was revoked"),
                &request_id,
            )
            .await
        }
    }
}

async fn forward_execution_inner(
    connection: &mut ResponsesWebSocketConnection,
    request_id: Arc<str>,
    streaming: bool,
    mut execution: PendingExecution,
    replay: &mut ConnectionReplaySnapshot,
) -> ForwardOutcome {
    if !streaming {
        let error = GatewayError::new(
            GatewayErrorKind::Internal,
            "WebSocket execution was not initialized as a stream",
        );
        return send_gateway_error(connection, &error, &request_id).await;
    }
    let first = match next_active_input(connection, &mut execution, &request_id).await {
        ActiveInput::Event(Ok(Some(event))) => event,
        ActiveInput::Event(Ok(None)) => {
            let error = GatewayError::new(
                GatewayErrorKind::Internal,
                "gateway response ended before its first event",
            );
            return send_gateway_error(connection, &error, &request_id).await;
        }
        ActiveInput::Event(Err(error)) => {
            return send_initial_engine_error(connection, &mut execution, &error, &request_id)
                .await;
        }
        ActiveInput::Disconnect => return ForwardOutcome::Disconnect,
    };
    let requirement = first.commit_requirement();
    let mut first = first.into_provider_events();
    if requirement != CommitRequirement::CommitBeforeDelivery {
        let error = GatewayError::new(
            GatewayErrorKind::Internal,
            "gateway first event did not require commit",
        );
        return send_gateway_error(connection, &error, &request_id).await;
    }
    let mut encoder = OpenAiResponsesEncoder::new();
    let mut provider_state = None;
    let mut first_messages = Vec::new();
    for event in &mut first {
        if let Some(update) = event.take_session_update() {
            provider_state = Some(update);
        }
        first_messages.extend(encoder.push_websocket(event));
    }
    if first_messages.is_empty() {
        let error = GatewayError::new(
            GatewayErrorKind::Internal,
            "gateway commit batch encoded no output",
        );
        return send_gateway_error(connection, &error, &request_id).await;
    }
    let Some(response_session) = execution.session_mut() else {
        return ForwardOutcome::Disconnect;
    };
    let response_headers = response_session.response_headers().to_vec();
    let projected_error_status = response_session
        .middleware_response_head()
        .map(|head| head.status_code)
        .filter(|status| !(200..300).contains(status));
    if let Err(error) = response_session
        .commit_downstream(projected_error_status)
        .await
    {
        let error = gateway_error_from_engine(&error);
        return send_gateway_error(connection, &error, &request_id).await;
    }
    let mut first_frame_written = false;
    if encoder.is_completed() {
        if let Err(outcome) =
            confirm_completed_execution(connection, &mut execution, &request_id).await
        {
            return outcome;
        }
        let provider_terminal_at = Instant::now();
        commit_connection_replay(replay, &encoder, provider_state.take());
        let writes_succeeded = send_metadata(
            connection,
            &request_id,
            response_metadata_event(&request_id, &response_headers),
        )
        .await
            && send_messages(
                connection,
                first_messages,
                &request_id,
                &mut first_frame_written,
                true,
            )
            .await;
        if !writes_succeeded {
            log_terminal_write_failure(connection, &request_id, provider_terminal_at.elapsed());
            return ForwardOutcome::Disconnect;
        }
        log_terminal_write_success(connection, &request_id, provider_terminal_at.elapsed());
        execution.disarm();
        return ForwardOutcome::Continue;
    }
    if !send_metadata(
        connection,
        &request_id,
        response_metadata_event(&request_id, &response_headers),
    )
    .await
        || !send_messages(
            connection,
            first_messages,
            &request_id,
            &mut first_frame_written,
            encoder.has_wire_failure(),
        )
        .await
    {
        return ForwardOutcome::Disconnect;
    }

    loop {
        match next_active_input(connection, &mut execution, &request_id).await {
            ActiveInput::Event(Ok(Some(delivery))) => {
                let requirement = delivery.commit_requirement();
                let mut events = delivery.into_provider_events();
                if requirement != CommitRequirement::AlreadyCommitted {
                    let error = GatewayError::new(
                        GatewayErrorKind::Internal,
                        "gateway requested another downstream commit",
                    );
                    return send_gateway_error(connection, &error, &request_id).await;
                }
                let mut messages = Vec::new();
                for event in &mut events {
                    if let Some(update) = event.take_session_update() {
                        provider_state = Some(update);
                    }
                    messages.extend(encoder.push_websocket(event));
                }
                if encoder.is_completed() {
                    if let Err(outcome) =
                        confirm_completed_execution(connection, &mut execution, &request_id).await
                    {
                        return outcome;
                    }
                    let provider_terminal_at = Instant::now();
                    commit_connection_replay(replay, &encoder, provider_state.take());
                    if !send_messages(
                        connection,
                        messages,
                        &request_id,
                        &mut first_frame_written,
                        true,
                    )
                    .await
                    {
                        log_terminal_write_failure(
                            connection,
                            &request_id,
                            provider_terminal_at.elapsed(),
                        );
                        return ForwardOutcome::Disconnect;
                    }
                    log_terminal_write_success(
                        connection,
                        &request_id,
                        provider_terminal_at.elapsed(),
                    );
                    execution.disarm();
                    return ForwardOutcome::Continue;
                }
                let terminal_batch = encoder.has_wire_failure();
                if !send_messages(
                    connection,
                    messages,
                    &request_id,
                    &mut first_frame_written,
                    terminal_batch,
                )
                .await
                {
                    return ForwardOutcome::Disconnect;
                }
            }
            ActiveInput::Event(Ok(None)) => {
                if execution
                    .session_mut()
                    .is_some_and(|session| session.is_finalized())
                {
                    commit_connection_replay(replay, &encoder, provider_state.take());
                    execution.disarm();
                    return ForwardOutcome::Continue;
                }
                let error = GatewayError::new(
                    GatewayErrorKind::Internal,
                    "gateway response ended without finalizing the execution",
                );
                return send_gateway_error(connection, &error, &request_id).await;
            }
            ActiveInput::Event(Err(error)) => {
                if encoder.has_wire_failure() {
                    execution.disarm();
                    return ForwardOutcome::Continue;
                }
                let error = gateway_error_from_engine(&error);
                return send_gateway_error(connection, &error, &request_id).await;
            }
            ActiveInput::Disconnect => return ForwardOutcome::Disconnect,
        }
    }
}

async fn send_initial_engine_error(
    connection: &mut ResponsesWebSocketConnection,
    execution: &mut PendingExecution,
    error: &EngineError,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    let headers = execution
        .session_mut()
        .map(|session| session.response_headers().to_vec())
        .unwrap_or_default();
    send_engine_error(connection, error, &headers, request_id).await
}

pub(in crate::openai) async fn send_engine_error(
    connection: &mut ResponsesWebSocketConnection,
    error: &EngineError,
    response_headers: &[gateway_core::event::ProviderResponseHeader],
    request_id: &Arc<str>,
) -> ForwardOutcome {
    // A safe continuation replay is a client projection, not a replacement of the
    // original quota failure used for isolation and accounting. Use its status
    // and headers instead of re-deriving 429 from the underlying error kind.
    if let EngineError::Provider(provider) = error
        && provider.continuation_recovery_disposition()
            == Some(ContinuationRecoveryDisposition::ClientReplayRequired)
        && let Some(projected) = provider.client_visible_upstream_response()
        && let Some(detail) = provider.client_visible_upstream_error()
    {
        let metadata: serde_json::Value =
            serde_json::from_str(&response_metadata_event(request_id, projected.headers()))
                .expect("metadata encoder produces JSON");
        let frame = serde_json::json!({
            "type": "error",
            "status": projected.status(),
            "error": {
                "type": detail.error_type(),
                "code": detail.code(),
                "message": detail.message(),
            },
            "headers": metadata["headers"],
        });
        return if connection
            .send_text(
                frame.to_string(),
                WriteContext::request(request_id, FramePhase::Error),
            )
            .await
            .is_ok()
        {
            ForwardOutcome::Continue
        } else {
            ForwardOutcome::Disconnect
        };
    }
    if !response_headers.is_empty()
        && !send_metadata(
            connection,
            request_id,
            response_metadata_event(request_id, response_headers),
        )
        .await
    {
        return ForwardOutcome::Disconnect;
    }
    let status = StatusCode::from_u16(engine_error_status(error))
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let error = gateway_error_from_engine(error);
    let (_, default_type, default_code) = gateway_error_contract(error.kind());
    send_error(
        connection,
        status,
        error.client_error_type().unwrap_or(default_type),
        error.client_error_code().unwrap_or(default_code),
        error.client_message(),
        None,
        request_id,
    )
    .await
}

fn commit_connection_replay(
    replay: &mut ConnectionReplaySnapshot,
    encoder: &OpenAiResponsesEncoder,
    provider_state: Option<ProviderSessionState>,
) {
    if let Some(response_id) = encoder.response_id() {
        replay.commit(response_id.to_owned(), provider_state);
    }
}

async fn confirm_completed_execution(
    connection: &mut ResponsesWebSocketConnection,
    execution: &mut PendingExecution,
    request_id: &Arc<str>,
) -> Result<(), ForwardOutcome> {
    match next_active_input(connection, execution, request_id).await {
        ActiveInput::Event(Ok(None))
            if execution
                .session_mut()
                .is_some_and(|session| session.is_finalized()) =>
        {
            Ok(())
        }
        ActiveInput::Event(Ok(None)) => {
            let error = GatewayError::new(
                GatewayErrorKind::Internal,
                "gateway response was not finalized after its terminal event",
            );
            Err(send_gateway_error(connection, &error, request_id).await)
        }
        ActiveInput::Event(Ok(Some(_))) => {
            let error = GatewayError::new(
                GatewayErrorKind::Internal,
                "gateway response continued after its terminal event",
            );
            Err(send_gateway_error(connection, &error, request_id).await)
        }
        ActiveInput::Event(Err(error)) => {
            let error = gateway_error_from_engine(&error);
            Err(send_gateway_error(connection, &error, request_id).await)
        }
        ActiveInput::Disconnect => Err(ForwardOutcome::Disconnect),
    }
}

enum ActiveInput {
    Event(Result<Option<CoordinatedEvent>, EngineError>),
    Disconnect,
}

async fn next_active_input(
    connection: &mut ResponsesWebSocketConnection,
    execution: &mut PendingExecution,
    request_id: &Arc<str>,
) -> ActiveInput {
    let Some(session) = execution.session_mut() else {
        return ActiveInput::Disconnect;
    };
    let control = session.response_control();
    let trace = session.trace();
    match await_active(
        connection,
        session.next_event(),
        || control.clone(),
        &trace,
        request_id,
    )
    .await
    {
        Some(event) => ActiveInput::Event(event),
        None => ActiveInput::Disconnect,
    }
}

/// Keep one future pinned while servicing controls and retaining queued create permits.
pub(in crate::openai) async fn await_active<T>(
    connection: &mut ResponsesWebSocketConnection,
    future: impl std::future::Future<Output = T>,
    control: impl Fn() -> Option<gateway_core::engine::response_control::ResponseControl>,
    trace: &gateway_core::diagnostics::TraceContext,
    request_id: &Arc<str>,
) -> Option<T> {
    // Keep the same execution future alive while processing control frames.
    // Deferred create frames retain their bounded queue permits until consumed.
    let next = future;
    tokio::pin!(next);
    let outcome = loop {
        tokio::select! {
            event = connection.next_active_event() => {
                let Some(event) = event else { break None; };
                match &event.event {
                    ConnectionEvent::Text(payload) => {
                        let error = match decode_response_interrupt(payload) {
                            Ok(Some(response_id)) => {
                                let result = control().as_ref().ok_or(gateway_core::engine::response_control::ResponseInterruptError::Unavailable).and_then(|control| control.interrupt(&response_id));
                                result.err().map(|_| super::super::RequestDecodeError::InvalidValue { field: "response_id".to_owned() }.protocol_body())
                            }
                            Ok(None) => { connection.defer(event); continue; }
                            Err(error) => Some(error.protocol_body()),
                        };
                        if let Some(error) = error
                            && send_protocol_error(connection, StatusCode::BAD_REQUEST, error, request_id).await == ForwardOutcome::Disconnect
                        { break None; }
                    }
                    ConnectionEvent::Expired => {},
                    ConnectionEvent::Binary => connection.defer(event),
                    ConnectionEvent::Exited(_) => break None,
                }
            }
            event = &mut next => break Some(event),
        }
    };
    if outcome.is_none() {
        trace.record(
            "downstream.cancelled",
            serde_json::json!({
                "reason": "websocket_connection_exit", "connectionId": connection.id(),
            }),
        );
    }
    outcome
}

pub(in crate::openai) async fn send_gateway_error(
    connection: &mut ResponsesWebSocketConnection,
    error: &GatewayError,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    let (status, default_type, default_code) = gateway_error_contract(error.kind());
    send_error(
        connection,
        status,
        error.client_error_type().unwrap_or(default_type),
        error.client_error_code().unwrap_or(default_code),
        error.client_message(),
        None,
        request_id,
    )
    .await
}

pub(in crate::openai) async fn send_protocol_error(
    connection: &mut ResponsesWebSocketConnection,
    status: StatusCode,
    body: ProtocolErrorBody,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    let error = body.error;
    send_error(
        connection,
        status,
        error.kind,
        error.code,
        &error.message,
        error.param.as_deref(),
        request_id,
    )
    .await
}

async fn send_error(
    connection: &mut ResponsesWebSocketConnection,
    status: StatusCode,
    error_type: &str,
    code: &str,
    message: &str,
    param: Option<&str>,
    request_id: &Arc<str>,
) -> ForwardOutcome {
    if connection
        .send_text(
            error_event(status, error_type, code, message, param, Some(request_id)),
            WriteContext::request(request_id, FramePhase::Error),
        )
        .await
        .is_ok()
    {
        ForwardOutcome::Continue
    } else {
        ForwardOutcome::Disconnect
    }
}

pub(in crate::openai) async fn send_metadata(
    connection: &mut ResponsesWebSocketConnection,
    request_id: &Arc<str>,
    message: String,
) -> bool {
    connection
        .send_text(
            message,
            WriteContext::request(request_id, FramePhase::Metadata),
        )
        .await
        .is_ok()
}

pub(in crate::openai) async fn send_messages(
    connection: &mut ResponsesWebSocketConnection,
    messages: Vec<String>,
    request_id: &Arc<str>,
    first_frame_written: &mut bool,
    terminal_batch: bool,
) -> bool {
    let message_count = messages.len();
    for (index, message) in messages.into_iter().enumerate() {
        let is_terminal = terminal_batch && index + 1 == message_count;
        let phase = match (*first_frame_written, is_terminal) {
            (false, true) => FramePhase::FirstAndTerminal,
            (false, false) => FramePhase::First,
            (true, true) => FramePhase::Terminal,
            (true, false) => FramePhase::Data,
        };
        if connection
            .send_text(message, WriteContext::request(request_id, phase))
            .await
            .is_err()
        {
            return false;
        }
        *first_frame_written = true;
    }
    true
}

pub(in crate::openai) fn log_terminal_write_success(
    connection: &ResponsesWebSocketConnection,
    request_id: &Arc<str>,
    provider_terminal_to_write: std::time::Duration,
) {
    tracing::info!(
        target: "request_trace", stage = "downstream.terminal.written",
        websocket_connection_id = connection.id(),
        request_id = %request_id,
        provider_terminal_to_terminal_write_ms = provider_terminal_to_write.as_millis(),
        terminal_frame_written = true,
        "Provider execution terminal was written to the WebSocket transport"
    );
}

pub(in crate::openai) fn log_terminal_write_failure(
    connection: &ResponsesWebSocketConnection,
    request_id: &Arc<str>,
    provider_terminal_to_write: std::time::Duration,
) {
    tracing::info!(
        target: "request_trace", stage = "downstream.terminal.write_failed",
        websocket_connection_id = connection.id(),
        request_id = %request_id,
        provider_terminal_to_terminal_write_ms = provider_terminal_to_write.as_millis(),
        provider_succeeded_but_terminal_write_failed = true,
        "Provider execution succeeded before the terminal WebSocket frame write failed"
    );
}

pub(in crate::openai) fn engine_error_status(error: &EngineError) -> u16 {
    if let EngineError::Provider(provider) = error {
        if provider.continuation_recovery_disposition()
            == Some(ContinuationRecoveryDisposition::ClientReplayRequired)
            && provider.client_visible_upstream_error().is_some()
            && let Some(projected) = provider.client_visible_upstream_response()
        {
            return projected.status();
        }
        if provider.kind() == gateway_core::error::ProviderErrorKind::UpstreamCapacityUnavailable {
            return 503;
        }
    }
    gateway_error_contract(gateway_error_from_engine(error).kind())
        .0
        .as_u16()
}
