//! Request-scoped managed network resources share the existing RPC lifecycle.
use super::{failure::invalid, network::ManagedCall};
use crate::outbound::SharedClient;
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_core::{
    engine::nested::NestedExecutionContext, error::ProviderError, lifecycle::CancellationToken,
    upstream::UpstreamSendState,
};
use gateway_host::outbound::HttpClient;
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, Stage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct Callbacks {
    inner: Arc<dyn CallbackHandler>,
    credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    outbound: Arc<crate::outbound::Registry>,
    services: Arc<crate::services::Registry>,
    models: Option<Arc<crate::models::Models>>,
    pub(super) client: SharedClient,
    calls: Mutex<BTreeMap<String, (CallContext, Arc<ManagedCall>)>>,
}
pub(super) struct Binding {
    _services: Option<crate::services::Binding>,
    callbacks: Arc<Callbacks>,
    context: CallContext,
}
impl Drop for Binding {
    fn drop(&mut self) {
        self.callbacks.finish(&self.context);
    }
}
impl Callbacks {
    pub(crate) fn new(
        inner: Arc<dyn CallbackHandler>,
        client: SharedClient,
        models: Option<Arc<crate::models::Models>>,
        outbound: Arc<crate::outbound::Registry>,
        services: Arc<crate::services::Registry>,
        credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            credentials,
            outbound,
            services,
            models,
            client,
            calls: Mutex::default(),
        })
    }
    pub(super) fn bind(
        self: &Arc<Self>,
        context: &CallContext,
        call: Arc<ManagedCall>,
        parent: Option<NestedExecutionContext>,
    ) -> Result<Binding, ProviderError> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if context.stage != Stage::Upstream
            || calls.len() >= 32
            || calls.contains_key(&context.resource_scope_id)
        {
            return Err(invalid(UpstreamSendState::NotSent));
        }
        let service_binding = parent
            .as_ref()
            .map(|parent| {
                self.services.bind_parent(
                    context,
                    parent,
                    CancellationToken::linked([call.cancellation.clone(), call.closed.clone()]),
                )
            })
            .transpose()
            .map_err(|error| super::failure::fault_error(error, UpstreamSendState::NotSent))?;
        if let (Some(models), Some(parent)) = (&self.models, parent) {
            models
                .bind(
                    context,
                    Some(parent),
                    None,
                    CancellationToken::linked([call.cancellation.clone(), call.closed.clone()]),
                    None,
                    None,
                )
                .map_err(|error| super::failure::fault_error(error, UpstreamSendState::NotSent))?;
        }
        if let Err(error) = self.outbound.bind_scope(context, call.http.clone()) {
            if let Some(models) = &self.models {
                models.finish(context);
            }
            return Err(super::failure::fault_error(
                error,
                UpstreamSendState::NotSent,
            ));
        }
        calls.insert(context.resource_scope_id.clone(), (context.clone(), call));
        Ok(Binding {
            _services: service_binding,
            callbacks: self.clone(),
            context: context.clone(),
        })
    }
}
impl CallbackHandler for Callbacks {
    fn begin(&self, context: &CallContext) {
        self.inner.begin(context);
    }
    fn streaming(&self, context: &CallContext) {
        self.inner.streaming(context);
    }
    fn finish(&self, context: &CallContext) {
        if let Some((_, call)) = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&context.resource_scope_id)
        {
            call.finish();
        }
        if let Some(models) = &self.models {
            models.finish(context);
        }
        self.inner.finish(context);
        self.outbound.finish(context);
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        if context.stage != Stage::Upstream && !method.starts_with("host.upstream.") {
            return self.inner.call(context, method, params, payload);
        }
        let call = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|(expected, _)| {
                // RPC assigns these fields; all authority comes from the original call.
                let mut expected = expected.clone();
                expected.call_id = context.call_id;
                expected.resource_stream = context.resource_stream;
                expected == context
            })
            .map(|(_, call)| call.clone());
        let client = self.client.clone();
        let models = self.models.clone();
        let inner = self.inner.clone();
        let credentials = self.credentials.clone();
        Box::pin(async move {
            let call = call.ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "upstream call is no longer active",
                )
            })?;
            if !method.starts_with("host.upstream.") {
                let callback = if crate::credentials::is_method(&method) {
                    let service = credentials.ok_or_else(|| {
                        PluginFault::new(
                            ErrorCode::Unsupported,
                            "native account credentials are not configured",
                        )
                    })?;
                    Box::pin(super::credentials::read(
                        service,
                        context,
                        call.account.clone(),
                        &method,
                        params,
                        payload,
                    )) as BoxFuture<'_, Result<RpcReply, PluginFault>>
                } else if crate::models::is_method(&method) || method.starts_with("host.http.") {
                    models
                        .ok_or_else(|| {
                            PluginFault::new(
                                ErrorCode::Unsupported,
                                "host model service is not configured",
                            )
                        })?
                        .call(context, method, params, payload)
                } else {
                    inner.call(context, method, params, payload)
                };
                return tokio::select! {biased;
                    ()=call.closed.cancelled()=>Err(call.fault(ErrorCode::Cancelled,"upstream callback scope ended")),
                    ()=call.cancellation.cancelled()=>Err(call.fault(ErrorCode::Cancelled,"upstream invocation cancelled")),
                    result=callback=>result,
                };
            }
            let client = client.get_or_init(HttpClient::new).as_ref().map_err(|_| {
                PluginFault::new(ErrorCode::Fault, "managed network initialization failed")
            })?;
            tokio::select! {biased;
                ()=call.closed.cancelled()=>Err(call.fault(ErrorCode::Cancelled,"upstream callback scope ended")),
                ()=call.cancellation.cancelled()=>Err(call.fault(ErrorCode::Cancelled,"upstream invocation cancelled")),
                result=call.dispatch(client,&method,params,payload)=>result,
            }
        })
    }
}
