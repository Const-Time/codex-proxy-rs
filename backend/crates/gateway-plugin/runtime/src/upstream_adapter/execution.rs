//! 执行插件上游适配调用，管理事件流、续接状态与调用生命周期

use std::sync::Arc;

use futures::stream;
use gateway_core::{
    engine::{provider::EventStream, upstream_adapter::UpstreamAdapterInvocation},
    error::{ProviderError, ProviderErrorKind},
    operation::{ExtensionSessionOwner, ProviderSessionState},
    upstream::UpstreamSendState,
};
use gateway_plugin_sdk::{
    CallContext, ErrorCode, Stage,
    call::upstream_adapter::{
        ContinuationScope, EXECUTE_METHOD, UpstreamAdapterRequest, UpstreamContinuation,
        UpstreamTransport,
    },
};
use serde::{Deserialize, Serialize};

use super::{
    PluginUpstreamAdapter,
    event::{DecodedEvent, EventDecodeError, EventDecoder},
    failure::{continuation_unavailable, fault_error, invalid, rpc_error},
};
use super::{
    callbacks::Binding,
    network::{CallOptions, ManagedCall},
    sessions::ConnectionOwner,
};
use crate::RpcStream;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::lifecycle::CancellationToken;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredContinuation {
    continuation: UpstreamContinuation,
    client_key_id: String,
    account_id: String,
    credential_revision: u64,
    connection: Option<String>,
}

struct Execution {
    adapter: Arc<PluginUpstreamAdapter>,
    invocation: UpstreamAdapterInvocation,
    active: Option<ActiveCall>,
    decoder: EventDecoder,
    ended: bool,
    cancellation: CancellationToken,
}

struct ActiveCall {
    context: CallContext,
    stream: RpcStream,
    managed: Arc<ManagedCall>,
    _scope: Binding,
}

pub(super) fn execute(
    adapter: Arc<PluginUpstreamAdapter>,
    invocation: UpstreamAdapterInvocation,
) -> EventStream {
    let cancellation = CancellationToken::linked([
        invocation.context.cancellation().clone(),
        adapter.cancellation.clone(),
    ]);
    let state = Execution {
        cancellation,
        adapter,
        invocation,
        active: None,
        decoder: EventDecoder::default(),
        ended: false,
    };
    // try_unfold 首次 poll 才运行闭包；Core 在此前登记 attempt 并持有账号租约
    Box::pin(stream::try_unfold(state, |mut state| async move {
        if state.ended {
            return Ok(None);
        }
        let result = state.next().await;
        match result {
            Ok(event) => Ok(Some((event, state))),
            Err(error) => Err(state.invocation.account.record_failure(error).await),
        }
    }))
}

impl Execution {
    async fn next(&mut self) -> Result<gateway_core::event::ProviderEvent, ProviderError> {
        if self.active.is_none() {
            self.active = Some(self.begin().await?);
        }
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| invalid(UpstreamSendState::NotSent))?;
        let cancellation = self.cancellation.clone();
        let chunk = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, active.managed.send_state.get())),
            result = active.stream.next() => result.map_err(|error| rpc_error(error, active.managed.send_state.get()))?,
        }.ok_or_else(|| {
            self.adapter.session.invalid_response(Stage::Upstream);
            invalid(active.managed.send_state.get())
        })?;
        let mut decoded = self
            .decoder
            .decode(
                &chunk,
                &self.adapter.registered.declaration.protocol,
                self.adapter.registered.declaration.transport.as_str(),
                self.invocation.account.as_ref(),
                active.managed.send_state.get(),
            )
            .map_err(|error| match error {
                EventDecodeError::Invalid(error) => {
                    self.adapter.session.invalid_response(Stage::Upstream);
                    error
                }
                EventDecodeError::Upstream(error) => error,
            })?;
        if self.decoder.completed {
            // 先确认 RPC 正常终结，再发布唯一 Completed；否则 Core 不会继续 poll 尾部错误
            let terminal = async {
                let tail = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, active.managed.send_state.get())),
                    result = active.stream.next() => result.map_err(|error| rpc_error(error, active.managed.send_state.get()))?,
                };
                if tail.is_some() {
                    self.adapter.session.invalid_response(Stage::Upstream);
                    return Err(invalid(active.managed.send_state.get()));
                }
                self.decoder.finish(active.managed.send_state.get())?;
                attach_continuation(&self.adapter, &self.invocation, active, &mut decoded).await
            }.await;
            if let Err(error) = terminal {
                // 尾部故障不得交付成功 wire，但已经解析的计量事实仍归 Core 结算
                let metering = decoded
                    .event
                    .canonical_facts()
                    .iter()
                    .filter(|fact| {
                        matches!(
                            fact,
                            gateway_core::event::GatewayEvent::Usage(_)
                                | gateway_core::event::GatewayEvent::CalculatedCost(_)
                        )
                    })
                    .cloned()
                    .map(gateway_core::event::ProviderEvent::canonical)
                    .collect();
                return Err(error.with_atomic_client_events(metering));
            }
            self.ended = true;
        }
        Ok(decoded.event)
    }

    async fn begin(&self) -> Result<ActiveCall, ProviderError> {
        let invalid = || invalid(UpstreamSendState::NotSent);
        let declaration = &self.adapter.registered.declaration;
        let invocation = &self.invocation;
        let gateway_core::operation::Operation::Generate(generate) = &invocation.operation else {
            return Err(invalid());
        };
        let protocol = generate.protocol_payload();
        if !self
            .adapter
            .active
            .load(std::sync::atomic::Ordering::Acquire)
            || !self.adapter.session.is_ready()
        {
            return Err(ProviderError::new(
                ProviderErrorKind::Unavailable,
                UpstreamSendState::NotSent,
            ));
        }
        if protocol.protocol() != declaration.protocol
            || invocation.metadata.provider().as_str() != declaration.provider.as_str()
            || invocation.metadata.provider_account_id() != invocation.account.account_id()
            || !declaration
                .authentication_kinds
                .iter()
                .any(|kind| kind == invocation.account.authentication_kind())
        {
            return Err(invalid());
        }
        let timeout = invocation
            .context
            .deadline()
            .bounded(self.adapter.session.maximum_call_timeout());
        if timeout.is_zero() || self.cancellation.is_cancelled() {
            return Err(ProviderError::new(
                ProviderErrorKind::Cancelled,
                UpstreamSendState::NotSent,
            ));
        }
        let mut context = self.adapter.session.context(Stage::Upstream, timeout);
        context.request_id = Some(invocation.context.request_id().as_str().to_owned());
        context.attempt_id = Some(format!(
            "{}:{}",
            invocation.context.request_id().as_str(),
            invocation.context.attempt_index()
        ));
        context.account_id = Some(invocation.account.account_id().as_str().to_owned());
        context.credential_revision = Some(invocation.account.credential_revision().get());
        let owner = ConnectionOwner {
            client_key_id: invocation.context.client_api_key_ref().as_str().to_owned(),
            account_id: invocation.account.account_id().as_str().to_owned(),
            credential_revision: invocation.account.credential_revision().get(),
        };
        let previous = previous_continuation(&self.adapter, invocation, &context, &owner)?;
        let connection = previous.as_ref().and_then(|state| state.connection.clone());
        let resumes_connection = connection.is_some();
        let managed = ManagedCall::new(
            Arc::clone(&invocation.account),
            CallOptions {
                target: self.adapter.registered.target.clone(),
                effects: invocation
                    .context
                    .nested_context()
                    .map(|parent| parent.scope().clone()),
                cancellation: self.cancellation.clone(),
                timeout,
                pool: self.adapter.connections.clone(),
                owner,
                connection,
                websocket_allowed: declaration.transport == UpstreamTransport::WebSocket,
            },
        )
        .map_err(|error| {
            if resumes_connection && error.code == ErrorCode::Rejected {
                continuation_unavailable()
            } else {
                fault_error(error, UpstreamSendState::NotSent)
            }
        })?;
        let scope = self.adapter.callbacks.bind(
            &context,
            managed.clone(),
            invocation.context.nested_context().cloned(),
        )?;
        let headers = crate::model_routing::headers(protocol.context())
            .map_err(|_| invalid())?
            .into_iter()
            .map(|header| {
                STANDARD
                    .decode(header.value_base64)
                    .map(|value| (header.name, value))
                    .map_err(|_| invalid())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = UpstreamAdapterRequest {
            adapter_id: declaration.id.clone(),
            provider: declaration.provider,
            upstream_model: invocation
                .metadata
                .upstream_model()
                .ok_or_else(invalid)?
                .as_str()
                .to_owned(),
            client_key_id: invocation.context.client_api_key_ref().as_str().to_owned(),
            account_id: invocation.account.account_id().as_str().to_owned(),
            credential_revision: invocation.account.credential_revision().get(),
            protocol: protocol.protocol().to_owned(),
            client_transport: invocation
                .context
                .client_transport()
                .ok_or_else(invalid)?
                .to_owned(),
            fast_mode: invocation.context.fast_mode().as_str().to_owned(),
            headers,
            continuation: previous.map(|state| state.continuation),
        };
        let body = serde_json::to_vec(protocol.body()).map_err(|source| {
            invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                source.to_string(),
            ))
        })?;
        let payload = if self
            .adapter
            .session
            .capability_version(gateway_plugin_sdk::Capability::UpstreamAdapter)
            == Some(1)
        {
            let mut metadata = serde_json::to_value(&request).map_err(|source| {
                invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                    source.to_string(),
                ))
            })?;
            metadata
                .as_object_mut()
                .ok_or_else(invalid)?
                .remove("fast_mode");
            metadata["disable_fast"] = serde_json::json!(
                invocation.context.fast_mode() == gateway_core::account::FastMode::Disabled
            );
            gateway_plugin_sdk::call::upstream_adapter::encode_request_metadata(&metadata, body)
        } else {
            request.encode(body)
        }
        .map_err(|source| {
            invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                source.to_string(),
            ))
        })?;
        let stream = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, managed.send_state.get())),
            result = self.adapter.session.call_stream(EXECUTE_METHOD, context.clone(), serde_json::json!({}), payload) => result.map_err(|error| rpc_error(error, managed.send_state.get()))?,
        };
        if stream.initial.result != serde_json::json!({}) || !stream.initial.payload.is_empty() {
            self.adapter.session.invalid_response(Stage::Upstream);
            return Err(super::failure::invalid(managed.send_state.get()));
        }
        Ok(ActiveCall {
            context,
            stream,
            managed,
            _scope: scope,
        })
    }
}

fn previous_continuation(
    adapter: &PluginUpstreamAdapter,
    invocation: &UpstreamAdapterInvocation,
    context: &CallContext,
    owner: &ConnectionOwner,
) -> Result<Option<StoredContinuation>, ProviderError> {
    let invalid = continuation_unavailable;
    let Some(state) = invocation
        .operation
        .provider_session_state(adapter.registered.declaration.provider.as_str())
    else {
        return if invocation.context.continuation().is_some() {
            Err(invalid())
        } else {
            Ok(None)
        };
    };
    let expected = extension_owner(
        adapter,
        context,
        state
            .extension_owner()
            .is_some_and(|owner| owner.connection_local),
    );
    if state.extension_owner() != Some(&expected) {
        return Err(invalid());
    }
    let stored: StoredContinuation = serde_json::from_value(serde_json::Value::Object(
        state.payload().clone(),
    ))
    .map_err(|source| {
        invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
            source.to_string(),
        ))
    })?;
    if stored.client_key_id != owner.client_key_id
        || stored.account_id != owner.account_id
        || stored.credential_revision != owner.credential_revision
        || (stored.continuation.scope == ContinuationScope::ConnectionLocal)
            != stored.connection.is_some()
        || expected.connection_local != stored.connection.is_some()
    {
        return Err(invalid());
    }
    Ok(Some(stored))
}

async fn attach_continuation(
    adapter: &PluginUpstreamAdapter,
    invocation: &UpstreamAdapterInvocation,
    active: &ActiveCall,
    decoded: &mut DecodedEvent,
) -> Result<(), ProviderError> {
    let invalid = || invalid(active.managed.send_state.get());
    let Some(continuation) = decoded.continuation.take() else {
        if invocation
            .operation
            .provider_session_state(adapter.registered.declaration.provider.as_str())
            .is_some()
        {
            return Err(invalid());
        }
        return Ok(());
    };
    if continuation.upstream_response_id.is_empty()
        || continuation.upstream_response_id.len() > 512
        || serde_json::to_vec(&continuation)
            .map_err(|source| {
                invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                    source.to_string(),
                ))
            })?
            .len()
            > 32 * 1024
    {
        return Err(invalid());
    }
    let local = continuation.scope == ContinuationScope::ConnectionLocal;
    let connection = if local {
        Some(
            active
                .managed
                .retain_connection()
                .await
                .map_err(|error| fault_error(error, active.managed.send_state.get()))?,
        )
    } else {
        None
    };
    let state = StoredContinuation {
        continuation,
        connection,
        client_key_id: invocation.context.client_api_key_ref().as_str().to_owned(),
        account_id: invocation.account.account_id().as_str().to_owned(),
        credential_revision: invocation.account.credential_revision().get(),
    };
    let serde_json::Value::Object(payload) = serde_json::to_value(state).map_err(|source| {
        invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
            source.to_string(),
        ))
    })?
    else {
        return Err(invalid());
    };
    let state =
        ProviderSessionState::new(adapter.registered.declaration.provider.as_str(), payload)
            .map_err(|source| {
                invalid().with_raw_upstream_error(gateway_core::error::RawUpstreamError::new(
                    source.to_string(),
                ))
            })?
            .with_extension_owner(extension_owner(adapter, &active.context, local));
    decoded.event.attach_session_update(state);
    Ok(())
}

fn extension_owner(
    adapter: &PluginUpstreamAdapter,
    context: &CallContext,
    connection_local: bool,
) -> ExtensionSessionOwner {
    ExtensionSessionOwner {
        instance_id: adapter.instance_id.clone(),
        contribution_id: adapter.contribution_id.clone(),
        adapter_id: adapter.registered.declaration.id.clone(),
        generation: context.generation,
        incarnation: context.incarnation.clone(),
        connection_local,
    }
}
