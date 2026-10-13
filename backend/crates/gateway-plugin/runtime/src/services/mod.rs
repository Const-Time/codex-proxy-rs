//! Active services use host-issued process leases and original Core call scopes.
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_core::{
    engine::{middleware::service as core, nested::NestedExecutionContext},
    lifecycle::CancellationToken,
};
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, Stage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

struct Scope {
    context: CallContext,
    dispatcher: Option<Arc<dyn core::Dispatcher>>,
    cancellation: CancellationToken,
    deadline: gateway_core::lifecycle::Deadline,
    closed: CancellationToken,
}
pub(crate) struct Registry {
    caller: core::Caller,
    scopes: Mutex<BTreeMap<String, Arc<Scope>>>,
}
impl Registry {
    pub(crate) fn new(caller: core::Caller) -> Arc<Self> {
        Arc::new(Self {
            caller,
            scopes: Mutex::default(),
        })
    }
    pub(crate) fn caller(&self) -> core::Caller {
        self.caller.clone()
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
    pub(crate) fn bind_parent(
        self: &Arc<Self>,
        context: &CallContext,
        parent: &NestedExecutionContext,
        cancellation: CancellationToken,
    ) -> Result<Binding, PluginFault> {
        let origin = parent
            .service_context(context.resource_scope_id.clone(), cancellation)
            .map_err(|_| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "service parent execution ended",
                )
            })?;
        self.bind(context, parent.service_factory(), origin)
    }
    pub(crate) fn bind(
        self: &Arc<Self>,
        context: &CallContext,
        factory: Option<Arc<dyn core::Factory>>,
        mut origin: core::Context,
    ) -> Result<Binding, PluginFault> {
        if context.instance_id != self.caller.instance_id()
            || context.generation != self.caller.revision().get()
        {
            return Err(PluginFault::new(
                ErrorCode::PermissionDenied,
                "service process identity mismatch",
            ));
        }
        let closed = CancellationToken::new();
        origin.cancellation = CancellationToken::linked([
            origin.cancellation,
            self.caller.cancellation(),
            closed.clone(),
        ]);
        origin.deadline_at = origin
            .deadline_at
            .cap(SystemTime::now() + Duration::from_millis(context.timeout_ms));
        let dispatcher = factory
            .map(|factory| factory.bind(&origin, self.caller.clone()))
            .transpose()
            .map_err(|_| {
                PluginFault::new(ErrorCode::PermissionDenied, "service authority unavailable")
            })?;
        let scope = Arc::new(Scope {
            context: context.clone(),
            dispatcher,
            cancellation: origin.cancellation,
            deadline: origin.deadline_at,
            closed,
        });
        let mut scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scopes.len() >= 32 || scopes.contains_key(&context.resource_scope_id) {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "service callback scope unavailable",
            ));
        }
        scopes.insert(context.resource_scope_id.clone(), scope.clone());
        Ok(Binding {
            registry: self.clone(),
            id: context.resource_scope_id.clone(),
            scope,
        })
    }
    fn finish(&self, context: &CallContext) {
        if let Some(scope) = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id)
        {
            scope.closed.cancel();
        }
    }
    fn call(
        &self,
        context: CallContext,
        params: core::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        let scope = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|scope| {
                let mut expected = scope.context.clone();
                expected.call_id = context.call_id;
                expected.resource_stream = context.resource_stream;
                expected == context
            })
            .cloned();
        Box::pin(async move {
            let scope = scope.ok_or_else(|| {
                PluginFault::new(ErrorCode::PermissionDenied, "service callback scope ended")
            })?;
            let dispatcher = scope.dispatcher.clone().ok_or_else(|| {
                PluginFault::new(ErrorCode::Unsupported, "host service is not configured")
            })?;
            tokio::select! { biased;
                () = scope.cancellation.cancelled() => Err(crate::middleware::service_cancelled()),
                () = scope.closed.cancelled() => Err(crate::middleware::service_cancelled()),
                () = scope.deadline.wait() => Err(crate::middleware::service_cancelled()),
                reply = crate::middleware::call_service(dispatcher, scope.context.instance_id.clone(), params, payload) => reply,
            }
        })
    }
}
pub(crate) struct Binding {
    registry: Arc<Registry>,
    id: String,
    scope: Arc<Scope>,
}
impl Drop for Binding {
    fn drop(&mut self) {
        self.scope.closed.cancel();
        let mut scopes = self
            .registry
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scopes
            .get(&self.id)
            .is_some_and(|scope| Arc::ptr_eq(scope, &self.scope))
        {
            scopes.remove(&self.id);
        }
    }
}
struct Callbacks {
    registry: Arc<Registry>,
    inner: Arc<dyn CallbackHandler>,
}
impl CallbackHandler for Callbacks {
    fn begin(&self, context: &CallContext) {
        self.inner.begin(context);
    }
    fn streaming(&self, context: &CallContext) {
        self.inner.streaming(context);
    }
    fn finish(&self, context: &CallContext) {
        self.inner.finish(context);
        self.registry.finish(context);
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: core::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        if method == gateway_plugin_sdk::call::services::CALL_METHOD
            && matches!(
                context.stage,
                Stage::Request
                    | Stage::Attempt
                    | Stage::Routing
                    | Stage::Scheduling
                    | Stage::Retry
                    | Stage::Upstream
            )
        {
            self.registry.call(context, params, payload)
        } else {
            self.inner.call(context, method, params, payload)
        }
    }
}
