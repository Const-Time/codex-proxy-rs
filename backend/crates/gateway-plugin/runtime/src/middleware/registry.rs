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
    Execution(Arc<MiddlewareInvocation>),
    Http(Arc<super::http::Invocation>),
}
impl Invocation {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Execution(a), Self::Execution(b)) => Arc::ptr_eq(a, b),
            (Self::Http(a), Self::Http(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    fn close(&self) {
        if let Self::Http(invocation) = self {
            invocation.close_callbacks();
        }
    }
}
struct Entry {
    context: CallContext,
    invocation: Invocation,
}
pub struct MiddlewareCallbacks {
    models: Option<Arc<crate::models::Models>>,
    inner: Arc<dyn CallbackHandler>,
    calls: Mutex<BTreeMap<String, Entry>>,
}
impl MiddlewareCallbacks {
    pub fn new(inner: Arc<dyn CallbackHandler>) -> Arc<Self> {
        Arc::new(Self {
            models: None,
            inner,
            calls: Mutex::new(BTreeMap::new()),
        })
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
        let invocation = Invocation::Http(invocation);
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
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
        if let Some(models) = &self.models {
            let parent = invocation
                .nested_context()
                .map_err(|_| MiddlewareError::Fault)?;
            // A wire CallContext cannot construct or substitute Core authority.
            if let Some(parent) = parent {
                models
                    .bind(context, Some(parent), None, invocation.cancellation.clone())
                    .map_err(|_| MiddlewareError::Fault)?;
            }
        }
        calls.insert(
            context.resource_scope_id.clone(),
            Entry {
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
        let model_method = crate::models::is_method(&method);
        if method.starts_with("host.http.") && context.stage != Stage::Http {
            return Box::pin(async {
                Err(PluginFault::new(
                    ErrorCode::Unsupported,
                    "host HTTP dispatch is not connected for this stage",
                ))
            });
        }
        if model_method && !matches!(context.stage, Stage::Request | Stage::Attempt) {
            return self.inner.call(context, method, params, payload);
        }
        if !model_method
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
                matches!(context.stage, Stage::Http | Stage::Request | Stage::Attempt)
                    && entry.context.resource_stream == context.resource_stream
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
            if let Invocation::Http(invocation) = invocation {
                if model_method {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "HTTP scope has no model execution authority",
                    ));
                }
                return tokio::select! {
                    biased;
                    ()=invocation.cancellation.cancelled()=>Err(PluginFault::new(ErrorCode::Cancelled,"HTTP invocation cancelled")),
                    ()=invocation.callbacks_closed.cancelled()=>Err(PluginFault::new(ErrorCode::PermissionDenied,"HTTP callback scope ended")),
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
