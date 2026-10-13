//! Request-scoped managed network resources share the existing RPC lifecycle.
use super::{failure::invalid, network::ManagedCall};
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_core::{error::ProviderError, upstream::UpstreamSendState};
use gateway_host::outbound::{HttpClient, HttpError};
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, Stage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};

pub(crate) type SharedClient = Arc<OnceLock<Result<HttpClient, HttpError>>>;
pub(crate) struct Callbacks {
    inner: Arc<dyn CallbackHandler>,
    pub(super) client: SharedClient,
    calls: Mutex<BTreeMap<String, (CallContext, Arc<ManagedCall>)>>,
}
pub(super) struct Binding {
    callbacks: Arc<Callbacks>,
    context: CallContext,
}
impl Drop for Binding {
    fn drop(&mut self) {
        self.callbacks.finish(&self.context);
    }
}
impl Callbacks {
    pub(crate) fn new(inner: Arc<dyn CallbackHandler>, client: SharedClient) -> Arc<Self> {
        Arc::new(Self {
            inner,
            client,
            calls: Mutex::default(),
        })
    }
    pub(super) fn bind(
        self: &Arc<Self>,
        context: &CallContext,
        call: Arc<ManagedCall>,
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
        calls.insert(context.resource_scope_id.clone(), (context.clone(), call));
        Ok(Binding {
            callbacks: self.clone(),
            context: context.clone(),
        })
    }
}
impl CallbackHandler for Callbacks {
    fn begin(&self, context: &CallContext) {
        self.inner.begin(context);
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
        self.inner.finish(context);
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: serde_json::Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        if !method.starts_with("host.upstream.") {
            return self.inner.call(context, method, params, payload);
        }
        let call = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&context.resource_scope_id)
            .filter(|(expected, _)| {
                context.stage == Stage::Upstream
                    && expected.resource_stream == context.resource_stream
                    && expected.instance_id == context.instance_id
                    && expected.generation == context.generation
                    && expected.incarnation == context.incarnation
                    && expected.request_id == context.request_id
                    && expected.attempt_id == context.attempt_id
                    && expected.account_id == context.account_id
                    && expected.credential_revision == context.credential_revision
            })
            .map(|(_, call)| call.clone());
        let client = self.client.clone();
        Box::pin(async move {
            let call = call.ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "upstream call is no longer active",
                )
            })?;
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
