//! Call scopes authorize callbacks; finishing RPC revokes handles without
//! dropping the body still owned by the outer delivery adapter.
use super::MiddlewareInvocation;
use crate::{CallbackHandler, RpcReply, RpcSession};
use futures::future::BoxFuture;
use gateway_core::engine::middleware::{
    MiddlewareContext, MiddlewareError, MiddlewareNext, MiddlewareRequest, MiddlewareResponse,
};
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, Stage, call::middleware::*};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
enum Invocation {
    Service(Arc<super::service::Invocation>),
    WebSocket(Arc<super::websocket::Invocation>),
    Execution(Arc<MiddlewareInvocation>),
    Http(Arc<super::http::Invocation>),
}
impl Invocation {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Service(a), Self::Service(b)) => Arc::ptr_eq(a, b),
            (Self::WebSocket(a), Self::WebSocket(b)) => Arc::ptr_eq(a, b),
            (Self::Execution(a), Self::Execution(b)) => Arc::ptr_eq(a, b),
            (Self::Http(a), Self::Http(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    fn close(&self) {
        if let Self::Service(invocation) = self {
            invocation.closed.cancel();
        }
        if let Self::WebSocket(invocation) = self {
            invocation.closed.cancel();
        }
        if let Self::Http(invocation) = self {
            invocation.close_callbacks();
        }
    }
}
struct Entry {
    _services: Option<crate::services::Binding>,
    context: CallContext,
    invocation: Invocation,
}
pub struct MiddlewareCallbacks {
    services: Option<Arc<crate::services::Registry>>,
    outbound: Option<Arc<crate::outbound::Registry>>,
    models: Option<Arc<crate::models::Models>>,
    inner: Arc<dyn CallbackHandler>,
    calls: Mutex<BTreeMap<String, Entry>>,
}
impl MiddlewareCallbacks {
    pub(super) fn service_caller(
        &self,
    ) -> Option<gateway_core::engine::middleware::service::Caller> {
        self.services.as_ref().map(|services| services.caller())
    }
    pub async fn invoke_service(
        self: &Arc<Self>,
        session: Arc<RpcSession>,
        context: gateway_core::engine::middleware::service::Context,
        input: serde_json::Value,
        next: gateway_core::engine::middleware::service::Next,
    ) -> Result<serde_json::Value, gateway_core::engine::middleware::service::Error> {
        super::service::invoke(self.clone(), session, context, input, next).await
    }
    pub(super) fn bind_service(
        self: &Arc<Self>,
        context: &CallContext,
        invocation: Arc<super::service::Invocation>,
        graph: gateway_core::engine::nested::ExtensionCallScope,
    ) -> Result<Binding, MiddlewareError> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if calls.len() >= 32 || calls.contains_key(&context.resource_scope_id) {
            return Err(MiddlewareError::Fault);
        }
        self.bind_outbound(context, invocation.cancellation.clone(), Some(graph))?;
        let invocation = Invocation::Service(invocation);
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
                _services: None,
                context: context.clone(),
                invocation: invocation.clone(),
            },
        );
        Ok(Binding {
            callbacks: self.clone(),
            scope: context.resource_scope_id.clone(),
            invocation,
        })
    }
    pub async fn invoke_websocket(
        self: &Arc<Self>,
        session: Arc<RpcSession>,
        context: gateway_core::engine::middleware::websocket::Context,
        message: gateway_core::engine::middleware::websocket::Message,
        next: gateway_core::engine::middleware::websocket::Next,
    ) -> Result<Option<gateway_core::engine::middleware::websocket::Message>, MiddlewareError> {
        super::websocket_execution::invoke(self.clone(), session, context, message, next).await
    }
    pub(super) fn bind_websocket(
        self: &Arc<Self>,
        context: &CallContext,
        invocation: Arc<super::websocket::Invocation>,
        mut origin: gateway_core::engine::middleware::service::Origin,
    ) -> Result<Binding, MiddlewareError> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if calls.len() >= 32 || calls.contains_key(&context.resource_scope_id) {
            return Err(MiddlewareError::Fault);
        }
        origin.context.cancellation = gateway_core::lifecycle::CancellationToken::linked([
            origin.context.cancellation,
            invocation.cancellation.clone(),
            invocation.closed.clone(),
        ]);
        let graph = origin.context.call_scope.clone();
        let service_binding = self
            .services
            .as_ref()
            .map(|services| services.bind_origin(context, origin))
            .transpose()
            .map_err(|_| MiddlewareError::Fault)?;
        self.bind_outbound(context, invocation.cancellation.clone(), Some(graph))?;
        let invocation = Invocation::WebSocket(invocation);
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
                _services: service_binding,
                context: context.clone(),
                invocation: invocation.clone(),
            },
        );
        Ok(Binding {
            callbacks: self.clone(),
            scope: context.resource_scope_id.clone(),
            invocation,
        })
    }
    pub fn new(inner: Arc<dyn CallbackHandler>) -> Arc<Self> {
        Arc::new(Self {
            services: None,
            outbound: None,
            models: None,
            inner,
            calls: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) fn with_services(
        mut self: Arc<Self>,
        services: Arc<crate::services::Registry>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("callbacks must be composed before publication")
            .services = Some(services);
        self
    }
    pub(crate) fn with_models(
        mut self: Arc<Self>,
        models: Option<Arc<crate::models::Models>>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("callbacks must be composed before publication")
            .models = models;
        self
    }
    pub(crate) fn with_outbound(
        mut self: Arc<Self>,
        outbound: Arc<crate::outbound::Registry>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("callbacks must be composed before publication")
            .outbound = Some(outbound);
        self
    }
    fn bind_outbound(
        &self,
        context: &CallContext,
        cancellation: gateway_core::lifecycle::CancellationToken,
        effects: Option<gateway_core::engine::nested::ExtensionCallScope>,
    ) -> Result<(), MiddlewareError> {
        if let Some(outbound) = &self.outbound {
            outbound
                .bind(context, cancellation, effects)
                .map_err(|_| MiddlewareError::Fault)?;
        }
        Ok(())
    }
    /// The caller freezes the generation and the Core terminal before entering.
    /// This primitive does not enable a manifest capability or publish a binding.
    pub async fn invoke(
        self: &Arc<Self>,
        session: Arc<RpcSession>,
        context: MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> Result<MiddlewareResponse, MiddlewareError> {
        super::execution::invoke(self.clone(), session, context, request, next).await
    }
    pub async fn invoke_http(
        self: &Arc<Self>,
        session: Arc<RpcSession>,
        context: gateway_core::engine::middleware::http::Context,
        request: gateway_core::engine::middleware::http::Request,
        next: gateway_core::engine::middleware::http::Next,
    ) -> Result<gateway_core::engine::middleware::http::Response, MiddlewareError> {
        super::http::invoke(self.clone(), session, context, request, next).await
    }
    pub(super) fn bind_http(
        self: &Arc<Self>,
        context: &CallContext,
        invocation: Arc<super::http::Invocation>,
    ) -> Result<Binding, MiddlewareError> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if calls.len() >= 32 || calls.contains_key(&context.resource_scope_id) {
            return Err(MiddlewareError::Fault);
        }
        let service_binding = if let Some(services) = &self.services {
            let mut origin = invocation
                .context
                .service_context()
                .map_err(|_| MiddlewareError::Fault)?;
            origin.cancellation = gateway_core::lifecycle::CancellationToken::linked([
                origin.cancellation,
                invocation.cancellation.clone(),
                invocation.callbacks_closed.clone(),
            ]);
            let factory = match &invocation.context.model_parent {
                Some(parent) => parent.service_factory(),
                None => invocation
                    .dispatcher
                    .as_ref()
                    .and_then(|dispatcher| dispatcher.service_factory()),
            };
            Some(
                services
                    .bind(context, factory, origin)
                    .map_err(|_| MiddlewareError::Fault)?,
            )
        } else {
            None
        };
        self.bind_outbound(
            context,
            invocation.cancellation.clone(),
            Some(invocation.context.call_scope.clone()),
        )?;
        let invocation = Invocation::Http(invocation);
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
                _services: service_binding,
                context: context.clone(),
                invocation: invocation.clone(),
            },
        );
        Ok(Binding {
            callbacks: self.clone(),
            scope: context.resource_scope_id.clone(),
            invocation,
        })
    }
    pub(super) fn bind(
        self: &Arc<Self>,
        context: &CallContext,
        invocation: Arc<MiddlewareInvocation>,
    ) -> Result<Binding, MiddlewareError> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if calls.len() >= 32 || calls.contains_key(&context.resource_scope_id) {
            return Err(MiddlewareError::Fault);
        }
        let parent = invocation
            .nested_context()
            .map_err(|_| MiddlewareError::Fault)?;
        let service_binding = match (&self.services, parent.as_ref()) {
            (Some(services), Some(parent)) => Some(
                services
                    .bind_parent(context, parent, invocation.cancellation.clone())
                    .map_err(|_| MiddlewareError::Fault)?,
            ),
            _ => None,
        };
        self.bind_outbound(
            context,
            invocation.cancellation.clone(),
            parent.as_ref().map(|p| p.scope().clone()),
        )?;
        if let (Some(models), Some(parent)) = (&self.models, parent) {
            // A wire CallContext cannot construct or substitute Core authority.
            if models
                .bind(
                    context,
                    Some(parent),
                    None,
                    invocation.cancellation.clone(),
                    None,
                    None,
                )
                .is_err()
            {
                if let Some(outbound) = &self.outbound {
                    outbound.finish(context);
                }
                return Err(MiddlewareError::Fault);
            }
        }
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
                _services: service_binding,
                context: context.clone(),
                invocation: Invocation::Execution(invocation.clone()),
            },
        );
        Ok(Binding {
            callbacks: self.clone(),
            scope: context.resource_scope_id.clone(),
            invocation: Invocation::Execution(invocation),
        })
    }
}
pub(super) struct Binding {
    callbacks: Arc<MiddlewareCallbacks>,
    scope: String,
    invocation: Invocation,
}
impl Drop for Binding {
    fn drop(&mut self) {
        let mut calls = self
            .callbacks
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if calls
            .get(&self.scope)
            .is_some_and(|e| e.invocation.same(&self.invocation))
            && let Some(entry) = calls.remove(&self.scope)
        {
            entry.invocation.close();
            if let Some(outbound) = &self.callbacks.outbound {
                outbound.finish(&entry.context);
            }
            if matches!(entry.invocation, Invocation::Execution(_))
                && let Some(models) = &self.callbacks.models
            {
                models.finish(&entry.context);
            }
        }
    }
}
impl CallbackHandler for MiddlewareCallbacks {
    fn begin(&self, context: &CallContext) {
        self.inner.begin(context);
    }
    fn streaming(&self, context: &CallContext) {
        self.inner.streaming(context);
    }
    fn finish(&self, context: &CallContext) {
        if let Some(entry) = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id)
        {
            entry.invocation.close();
        }
        self.inner.finish(context);
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        let model_method = crate::models::is_method(&method)
            || (method.starts_with("host.http.")
                && !crate::outbound::is_method(&method)
                && context.stage != Stage::Http);
        let service_method = method == gateway_plugin_sdk::call::services::CALL_METHOD
            && context.stage == Stage::Service;
        if model_method && !matches!(context.stage, Stage::Request | Stage::Attempt) {
            return self.inner.call(context, method, params, payload);
        }
        if !model_method
            && !service_method
            && !super::http::is_method(&method)
            && !matches!(
                method.as_str(),
                NEXT_METHOD | BODY_READ_METHOD | BODY_FACTS_METHOD | BODY_CLOSE_METHOD
            )
        {
            return self.inner.call(context, method, params, payload);
        }
        let invocation = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|entry| {
                matches!(
                    context.stage,
                    Stage::Http
                        | Stage::WebSocket
                        | Stage::Service
                        | Stage::Request
                        | Stage::Attempt
                ) && entry.context.resource_stream == context.resource_stream
                    && entry.context.stage == context.stage
                    && entry.context.instance_id == context.instance_id
                    && entry.context.generation == context.generation
                    && entry.context.incarnation == context.incarnation
                    && entry.context.request_id == context.request_id
                    && entry.context.attempt_id == context.attempt_id
                    && entry.context.account_id == context.account_id
                    && entry.context.credential_revision == context.credential_revision
            })
            .map(|entry| entry.invocation.clone());
        let models = self.models.clone();
        Box::pin(async move {
            let invocation = invocation.ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "middleware invocation is no longer active",
                )
            })?;
            if let Invocation::Service(invocation) = invocation {
                return tokio::select! {biased;
                    ()=invocation.cancellation.cancelled()=>Err(if service_method {super::service_cancelled()}else{PluginFault::new(ErrorCode::Cancelled,"service invocation cancelled")}),
                    ()=invocation.closed.cancelled()=>Err(if service_method {super::service_cancelled()}else{PluginFault::new(ErrorCode::PermissionDenied,"service callback scope ended")}),
                    result=invocation.clone().call(method,params,payload)=>result,
                };
            }
            if let Invocation::WebSocket(invocation) = invocation {
                return tokio::select! {biased;
                    ()=invocation.cancellation.cancelled()=>Err(PluginFault::new(ErrorCode::Cancelled, "WebSocket invocation cancelled")),
                    ()=invocation.closed.cancelled()=>Err(PluginFault::new(ErrorCode::PermissionDenied, "WebSocket callback scope ended")),
                    result=invocation.clone().call(method, params, payload, 8*1024*1024)=>result,
                };
            }
            if let Invocation::Http(invocation) = invocation {
                if model_method {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "HTTP scope has no model execution authority",
                    ));
                }
                let dispatching =
                    method == gateway_plugin_sdk::call::middleware::http::DISPATCH_METHOD;
                let cancelled = |code, message| {
                    let mut fault = PluginFault::new(code, message);
                    if dispatching {
                        fault.send_state = gateway_plugin_sdk::SendState::Ambiguous;
                    }
                    fault
                };
                return tokio::select! {
                    biased;
                    ()=invocation.cancellation.cancelled()=>Err(cancelled(ErrorCode::Cancelled,"HTTP invocation cancelled")),
                    ()=invocation.callbacks_closed.cancelled()=>Err(cancelled(ErrorCode::PermissionDenied,"HTTP callback scope ended")),
                    result=invocation.clone().call(method,params,payload,8*1024*1024)=>result,
                };
            }
            let Invocation::Execution(invocation) = invocation else {
                unreachable!()
            };
            if model_method {
                let models = models.ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::Unsupported,
                        "host model service is not configured",
                    )
                })?;
                return models.call(context, method, params, payload).await;
            }
            invocation
                .call(&method, params, payload, 8 * 1024 * 1024)
                .await
        })
    }
}
