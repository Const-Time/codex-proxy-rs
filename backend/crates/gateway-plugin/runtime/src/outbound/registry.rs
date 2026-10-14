//! Only host-bound RPC scopes authorize external HTTP callbacks.
use super::{HttpScope, SharedClient, is_method};
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_core::{engine::nested::ExtensionCallScope, lifecycle::CancellationToken};
use gateway_host::outbound::HttpClient;
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) struct Registry {
    client: SharedClient,
    cancellation: CancellationToken,
    scopes: Mutex<BTreeMap<String, (CallContext, Arc<HttpScope>)>>,
}
impl Registry {
    pub(crate) fn new(client: SharedClient, cancellation: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            client,
            cancellation,
            scopes: Mutex::default(),
        })
    }
    pub(crate) fn guard(self: &Arc<Self>, context: &CallContext) -> Binding {
        Binding {
            registry: self.clone(),
            context: context.clone(),
        }
    }
    pub(crate) fn wrap(
        self: &Arc<Self>,
        inner: Arc<dyn CallbackHandler>,
    ) -> Arc<dyn CallbackHandler> {
        Arc::new(Callbacks {
            registry: self.clone(),
            inner,
        })
    }
    pub(crate) fn bind(
        &self,
        context: &CallContext,
        cancellation: CancellationToken,
        effects: Option<ExtensionCallScope>,
    ) -> Result<(), PluginFault> {
        let scope = HttpScope::new(
            CancellationToken::linked([self.cancellation.clone(), cancellation]),
            Duration::from_millis(context.timeout_ms),
            effects,
            None,
            format!(
                "{}:{}:{}",
                context.instance_id, context.generation, context.incarnation
            ),
        );
        self.bind_scope(context, scope)
    }
    pub(crate) fn bind_scope(
        &self,
        context: &CallContext,
        scope: Arc<HttpScope>,
    ) -> Result<(), PluginFault> {
        let mut scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scopes.len() >= 32 || scopes.contains_key(&context.resource_scope_id) {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "external HTTP callback scope unavailable",
            ));
        }
        scopes.insert(context.resource_scope_id.clone(), (context.clone(), scope));
        Ok(())
    }
    fn begin(&self, context: &CallContext, budget: &gateway_plugin_sdk::client::CallDeadline) {
        let mut scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scopes.len() < 32 {
            scopes
                .entry(context.resource_scope_id.clone())
                .or_insert_with(|| {
                    (
                        context.clone(),
                        HttpScope::new(
                            self.cancellation.clone(),
                            Duration::from_millis(context.timeout_ms),
                            None,
                            None,
                            format!(
                                "{}:{}:{}",
                                context.instance_id, context.generation, context.incarnation
                            ),
                        ),
                    )
                });
        }
        if let Some((_, scope)) = scopes
            .get(&context.resource_scope_id)
            .filter(|(expected, _)| {
                let mut expected = expected.clone();
                expected.call_id = context.call_id;
                expected.resource_stream = context.resource_stream;
                expected == *context
            })
        {
            scope.deadline.bind(budget);
        }
    }
    fn streaming(&self, context: &CallContext) {
        if let Some((_, scope)) = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
        {
            scope.streaming(context.resource_stream);
        }
    }
    pub(crate) fn finish(&self, context: &CallContext) {
        if let Some((_, scope)) = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id)
        {
            scope.finish();
        }
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        let scope = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|(expected, _)| {
                let mut expected = expected.clone();
                expected.call_id = context.call_id;
                expected.resource_stream = context.resource_stream;
                expected == context
            })
            .map(|(_, scope)| scope.clone());
        let client = self.client.clone();
        let cancellation = self.cancellation.clone();
        Box::pin(async move {
            let scope = scope.ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "external HTTP call is outside its host scope",
                )
            })?;
            let client = client.get_or_init(HttpClient::new).as_ref().map_err(|_| {
                PluginFault::new(ErrorCode::Fault, "managed network initialization failed")
            })?;
            tokio::select! {biased;
                ()=cancellation.cancelled()=>Err(scope.fault(ErrorCode::Cancelled, "plugin access ended")),
                ()=scope.closed.cancelled()=>Err(scope.fault(ErrorCode::Cancelled, "network callback scope ended")),
                ()=scope.cancellation.cancelled()=>Err(scope.fault(ErrorCode::Cancelled, "network invocation cancelled")),
                result=scope.dispatch(client, &method, params, payload)=>result,
            }
        })
    }
}
struct Callbacks {
    registry: Arc<Registry>,
    inner: Arc<dyn CallbackHandler>,
}
impl CallbackHandler for Callbacks {
    fn begin(&self, context: &CallContext, _deadline: &gateway_plugin_sdk::client::CallDeadline) {
        self.inner.begin(context, _deadline);
        self.registry.begin(context, _deadline);
    }
    fn streaming(&self, context: &CallContext) {
        self.inner.streaming(context);
        self.registry.streaming(context);
    }
    fn finish(&self, context: &CallContext) {
        self.inner.finish(context);
        self.registry.finish(context);
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        if is_method(&method) {
            self.registry.call(context, method, params, payload)
        } else {
            self.inner.call(context, method, params, payload)
        }
    }
}

pub(crate) struct Binding {
    registry: Arc<Registry>,
    context: CallContext,
}
impl Drop for Binding {
    fn drop(&mut self) {
        self.registry.finish(&self.context);
    }
}
