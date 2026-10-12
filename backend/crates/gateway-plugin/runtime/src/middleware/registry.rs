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

struct Entry {
    context: CallContext,
    invocation: Arc<MiddlewareInvocation>,
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
                invocation: invocation.clone(),
            },
        );
        Ok(Binding {
            callbacks: self.clone(),
            scope: context.resource_scope_id.clone(),
            invocation,
        })
    }
}
pub(super) struct Binding {
    callbacks: Arc<MiddlewareCallbacks>,
    scope: String,
    invocation: Arc<MiddlewareInvocation>,
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
            .is_some_and(|e| Arc::ptr_eq(&e.invocation, &self.invocation))
            && let Some(entry) = calls.remove(&self.scope)
            && let Some(models) = &self.callbacks.models
        {
            models.finish(&entry.context);
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
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id);
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
        if model_method && !matches!(context.stage, Stage::Request | Stage::Attempt) {
            return self.inner.call(context, method, params, payload);
        }
        if !model_method
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
                matches!(context.stage, Stage::Request | Stage::Attempt)
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
