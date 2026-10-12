//! Native SDK callbacks reuse Core execution, not a plugin-specific ledger or scheduler.
mod request;
mod session;

use crate::RpcReply;
use futures::future::BoxFuture;
use gateway_admin::{
    ClientKeyService,
    model::{MutationActor, MutationContext, client_keys::*},
};
use gateway_core::{
    engine::{
        EngineError,
        execution::{AuthenticatedClient, ExecutionService, gateway_error_from_engine},
        nested::{ExtensionCallScope, NestedExecutionContext},
    },
    error::{GatewayError, GatewayErrorKind},
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    routing::PublicModelDescriptor,
};
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, SendState, call::host::*};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

pub(super) const MAXIMUM_BYTES: usize = 8 * 1024 * 1024;
pub(crate) fn is_method(method: &str) -> bool {
    matches!(
        method,
        "host.keys.list"
            | "host.models.list"
            | "host.model.execute"
            | "host.model.execute_stream"
            | "host.model.stream_read"
            | "host.model.stream_close"
    )
}
pub(crate) struct ModelServices {
    execution: Weak<dyn ExecutionService>,
    keys: Arc<dyn ClientKeyService>,
}
impl ModelServices {
    pub(crate) fn new(
        execution: Arc<dyn ExecutionService>,
        keys: Arc<dyn ClientKeyService>,
    ) -> Arc<Self> {
        Arc::new(Self {
            execution: Arc::downgrade(&execution),
            keys,
        })
    }
}
struct Scope {
    context: CallContext,
    owner: Option<String>,
    parent: Option<NestedExecutionContext>,
    graph: ExtensionCallScope,
    cancellation: CancellationToken,
    permits: Arc<tokio::sync::Semaphore>,
    streams: Mutex<BTreeMap<String, Arc<session::ModelStream>>>,
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.close();
    }
}
impl Scope {
    fn close(&self) {
        self.cancellation.cancel();
        for stream in self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
        {
            stream.close();
        }
    }
    fn validate(&self) -> Result<(), PluginFault> {
        if self.cancellation.is_cancelled() {
            return Err(PluginFault::new(
                ErrorCode::PermissionDenied,
                "model callback scope has ended",
            ));
        }
        Ok(())
    }
}
pub(crate) struct Models {
    services: Arc<ModelServices>,
    cancellation: CancellationToken,
    scopes: Mutex<BTreeMap<String, Arc<Scope>>>,
}
impl Models {
    pub(crate) fn new(services: Arc<ModelServices>, cancellation: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            services,
            cancellation,
            scopes: Mutex::default(),
        })
    }
    pub(crate) fn bind(
        &self,
        context: &CallContext,
        parent: Option<NestedExecutionContext>,
        actor: Option<MutationContext>,
        revocation: CancellationToken,
    ) -> Result<(), PluginFault> {
        let mut scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scopes.len() >= 32 || scopes.contains_key(&context.resource_scope_id) {
            return Err(capacity());
        }
        let owner = actor.and_then(|a| match a.actor {
            MutationActor::AdminSession { admin_user_id } => Some(admin_user_id),
            _ => None,
        });
        let cancellation = CancellationToken::linked([
            self.cancellation.clone(),
            revocation,
            parent
                .as_ref()
                .map_or_else(CancellationToken::new, NestedExecutionContext::cancellation),
        ]);
        scopes.insert(
            context.resource_scope_id.clone(),
            Arc::new(Scope {
                context: context.clone(),
                owner,
                parent,
                graph: ExtensionCallScope::default(),
                cancellation,
                permits: Arc::new(tokio::sync::Semaphore::new(16)),
                streams: Mutex::default(),
            }),
        );
        Ok(())
    }
    pub(crate) fn finish(&self, context: &CallContext) {
        let scope = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id);
        if let Some(scope) = scope {
            scope.close();
        }
    }
    pub(crate) fn call(
        &self,
        context: CallContext,
        method: String,
        params: Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        let scope = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|scope| {
                // RpcSession assigns these two fields when registering the host call,
                // after middleware binds its immutable authority and resource scope.
                let mut expected = scope.context.clone();
                expected.call_id = context.call_id;
                expected.resource_stream = context.resource_stream;
                expected == context
            })
            .cloned();
        let services = self.services.clone();
        Box::pin(async move {
            let scope = scope.ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "model callback is outside its host scope",
                )
            })?;
            scope.validate()?;
            let reply = call(services, &scope, &method, params, payload).await?;
            scope.validate().map_err(|mut fault| {
                if matches!(
                    method.as_str(),
                    "host.model.execute"
                        | "host.model.execute_stream"
                        | "host.model.stream_read"
                        | "host.model.stream_close"
                ) {
                    // The operation already returned. Revocation cannot prove
                    // that this execution was NotSent or roll back its effects.
                    fault.send_state = SendState::Ambiguous;
                }
                fault
            })?;
            Ok(reply)
        })
    }
}
fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, PluginFault> {
    serde_json::from_value(value).map_err(|_| invalid())
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid host model request")
}
fn capacity() -> PluginFault {
    PluginFault::new(ErrorCode::Capacity, "host model callback capacity exceeded")
}
fn encode(value: impl serde::Serialize) -> Result<RpcReply, PluginFault> {
    Ok(RpcReply {
        result: serde_json::to_value(value).map_err(|_| invalid())?,
        payload: vec![],
    })
}
fn gateway_fault(error: GatewayError) -> PluginFault {
    let code = match error.kind() {
        GatewayErrorKind::InvalidRequest | GatewayErrorKind::ModelNotFound => {
            ErrorCode::InvalidInput
        }
        GatewayErrorKind::Unsupported => ErrorCode::Unsupported,
        GatewayErrorKind::Unauthorized | GatewayErrorKind::PolicyDenied => {
            ErrorCode::PermissionDenied
        }
        GatewayErrorKind::Timeout => ErrorCode::Timeout,
        GatewayErrorKind::Cancelled => ErrorCode::Cancelled,
        GatewayErrorKind::RateLimited
        | GatewayErrorKind::AccountCapacityUnavailable
        | GatewayErrorKind::MessageTooBig => ErrorCode::Capacity,
        GatewayErrorKind::Internal => ErrorCode::Fault,
        _ => ErrorCode::Upstream,
    };
    PluginFault::new(code, error.safe_message())
}
fn engine_fault(error: EngineError) -> PluginFault {
    let mut fault = gateway_fault(gateway_error_from_engine(&error));
    if let EngineError::Provider(error) = error {
        fault.send_state = match error.send_state() {
            gateway_core::upstream::UpstreamSendState::NotSent => SendState::NotSent,
            gateway_core::upstream::UpstreamSendState::Sent => SendState::Sent,
            gateway_core::upstream::UpstreamSendState::Ambiguous => SendState::Ambiguous,
        };
        fault.http_status = error.upstream_status();
    }
    fault
}
async fn client(
    services: &ModelServices,
    execution: &dyn ExecutionService,
    scope: &Scope,
    key: &str,
) -> Result<AuthenticatedClient, PluginFault> {
    let key = ClientApiKeyId::new(key).map_err(|_| invalid())?;
    if let Some(owner) = &scope.owner {
        let secret = services.keys.reveal(owner, &key).await.map_err(|_| {
            PluginFault::new(
                ErrorCode::PermissionDenied,
                "selected Key is not available to the caller",
            )
        })?;
        scope.validate()?;
        execution.authenticate_revocable(secret.expose_for_response(), scope.cancellation.clone())
    } else if scope.parent.is_some() {
        execution.authenticate_bound_key(&key, scope.cancellation.clone())
    } else {
        return Err(PluginFault::new(
            ErrorCode::PermissionDenied,
            "model callbacks require a personally authenticated caller",
        ));
    }
    .map_err(|_| {
        PluginFault::new(
            ErrorCode::PermissionDenied,
            "selected Key authorization is unavailable",
        )
    })
}
async fn call(
    services: Arc<ModelServices>,
    scope: &Arc<Scope>,
    method: &str,
    params: Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if method != "host.model.execute"
        && method != "host.model.execute_stream"
        && !payload.is_empty()
    {
        return Err(invalid());
    }
    match method {
        "host.keys.list" => {
            let request: KeyListRequest = parse(params)?;
            let owner = scope.owner.as_deref().ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "Key listing requires a personally authenticated caller",
                )
            })?;
            let sort = ClientKeySort {
                field: ClientKeySortField::Name,
                direction: SortDirection::Asc,
            };
            let cursor = request
                .cursor
                .map(|cursor| {
                    if cursor.len() > 4096 {
                        return Err(invalid());
                    }
                    let (name, id): (String, String) =
                        serde_json::from_slice(&hex::decode(cursor).map_err(|_| invalid())?)
                            .map_err(|_| invalid())?;
                    Ok(ClientKeyCursor {
                        sort,
                        value: ClientKeyCursorValue::Name(name),
                        id: ClientApiKeyId::new(id).map_err(|_| invalid())?,
                    })
                })
                .transpose()?;
            let page = services
                .keys
                .list(
                    owner,
                    ClientKeyListQuery {
                        cursor,
                        page_size: ClientKeyPageSize::new(request.limit).map_err(|_| invalid())?,
                        search: None,
                        sort,
                    },
                )
                .await
                .map_err(|_| PluginFault::new(ErrorCode::Fault, "Key listing failed"))?;
            let next_cursor = page
                .next_cursor
                .map(|c| {
                    let ClientKeyCursorValue::Name(name) = c.value else {
                        return Err(invalid());
                    };
                    Ok(hex::encode(
                        serde_json::to_vec(&(name, c.id.as_str())).map_err(|_| invalid())?,
                    ))
                })
                .transpose()?;
            encode(KeyListResult {
                keys: page
                    .items
                    .into_iter()
                    .map(|k| ClientKey {
                        id: k.id.as_str().into(),
                        name: k.name,
                        enabled: k.enabled,
                    })
                    .collect(),
                next_cursor,
            })
        }
        "host.models.list" => {
            let request: ModelListRequest = parse(params)?;
            if request.protocol != "openai" {
                return Err(PluginFault::new(
                    ErrorCode::Unsupported,
                    "host model protocol is unsupported",
                ));
            }
            let execution = services
                .execution
                .upgrade()
                .ok_or_else(|| PluginFault::new(ErrorCode::Fault, "model service stopped"))?;
            let client =
                client(&services, execution.as_ref(), scope, &request.client_key_id).await?;
            let catalog = execution
                .client_model_catalog(&client, &request.protocol, &request.client_version)
                .await;
            if client
                .bound_access()
                .is_none_or(|access| access.cancellation().is_cancelled())
            {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "selected Key authorization was revoked",
                ));
            }
            let catalog = catalog.map_err(|_| {
                PluginFault::new(ErrorCode::Upstream, "model catalog is unavailable")
            })?;
            let mut models = catalog
                .into_iter()
                .map(|model| match model {
                    PublicModelDescriptor::Native { model, .. } => model.as_str().into(),
                    PublicModelDescriptor::Adapted(profile) => profile.model().as_str().into(),
                })
                .collect::<Vec<String>>();
            models.sort();
            models.dedup();
            encode(ModelListResult { models })
        }
        "host.model.execute" | "host.model.execute_stream" => {
            let metadata: ModelExecuteRequest = parse(params)?;
            let streaming = method == "host.model.execute_stream";
            let request = request::decode(&scope.context, &metadata, payload, streaming)?;
            let execution = services
                .execution
                .upgrade()
                .ok_or_else(|| PluginFault::new(ErrorCode::Fault, "model service stopped"))?;
            let selected = match metadata.client_key_id.as_deref() {
                Some(key) => Some(client(&services, execution.as_ref(), scope, key).await?),
                None if scope.parent.is_some() => None,
                None => return Err(invalid()),
            };
            let stop = CancellationToken::linked([scope.cancellation.clone()]);
            let cancel = session::CancelOnDrop::new(stop.clone());
            let permit = scope
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?;
            let prepared = match &scope.parent {
                Some(parent) => execution.prepare_nested(
                    parent.clone(),
                    scope.context.instance_id.clone(),
                    selected,
                ),
                None => execution.prepare_plugin_execution(
                    selected.ok_or_else(invalid)?,
                    scope.context.instance_id.clone(),
                    scope.graph.clone(),
                    scope.cancellation.clone(),
                ),
            }
            .map_err(gateway_fault)?
            .with_revocation(stop.clone())
            .with_model_target(request.target.clone())
            .map_err(gateway_fault)?;
            let owner = session::open(execution, prepared, request, stop, permit).await?;
            if streaming {
                let request_id = owner.request_id.clone();
                let stream = uuid::Uuid::new_v4().to_string();
                let resource = Arc::new(session::ModelStream::new(owner));
                let reply = encode(ModelStreamResult {
                    request_id,
                    stream: stream.clone(),
                })?;
                scope.validate()?;
                scope
                    .streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(stream, resource);
                cancel.disarm();
                Ok(reply)
            } else {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                drop(tokio::spawn(async move {
                    let result = session::execute(owner).await;
                    let _ = sender.send(result);
                }));
                receiver.await.map_err(|_| {
                    PluginFault::new(ErrorCode::Fault, "model execution worker stopped")
                })?
            }
        }
        "host.model.stream_read" => {
            let request: ModelStreamReadRequest = parse(params)?;
            let stream = scope
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&request.stream)
                .cloned()
                .ok_or_else(invalid)?;
            let result = stream.read(request.maximum_bytes).await?;
            if result.result.get("end").and_then(Value::as_bool) == Some(true) {
                scope
                    .streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.stream);
            }
            Ok(result)
        }
        "host.model.stream_close" => {
            let request: ModelStreamCloseRequest = parse(params)?;
            let stream = scope
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&request.stream)
                .ok_or_else(invalid)?;
            stream.close();
            encode(serde_json::json!({}))
        }
        _ => Err(PluginFault::new(
            ErrorCode::Unsupported,
            "unsupported host model callback",
        )),
    }
}
