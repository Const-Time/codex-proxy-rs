//! 状态回调使用宿主保存的实例/revision，不接受插件指定另一个 owner。
mod log;
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_admin::{
    model::{AdminErrorKind, plugins::*},
    ports::plugins::PluginStore,
};
use gateway_plugin_sdk::{
    CallContext, ErrorCode, Manifest, PluginFault, Stage, StateNamespace, call::host::*,
};
pub(crate) use log::PluginLog;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

struct Namespace {
    declaration: StateNamespace,
    schema: jsonschema::Validator,
}
pub(crate) struct Callbacks {
    logs: Arc<PluginLog>,
    credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    outbound: Arc<crate::outbound::Registry>,
    models: Option<Arc<crate::models::Models>>,
    quota: Option<Arc<dyn gateway_admin::ports::plugins::PluginQuotaService>>,
    resources: Option<Arc<dyn gateway_admin::ports::plugins::PluginResourceService>>,
    pub(crate) calls: Arc<crate::resources::InvocationCalls>,
    store: Arc<dyn PluginStore>,
    id: String,
    revision: u64,
    active: Arc<AtomicBool>,
    namespaces: Arc<BTreeMap<String, Namespace>>,
    data: Option<Arc<dyn gateway_admin::ports::plugins::PluginDataService>>,
}
impl Callbacks {
    pub(crate) fn with_credentials(
        mut self,
        credentials: Option<Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    ) -> Self {
        self.credentials = credentials;
        self
    }
    pub(crate) fn with_models(mut self, models: Option<Arc<crate::models::Models>>) -> Self {
        self.models = models;
        self
    }
    pub(crate) fn with_quota(
        mut self,
        service: Option<Arc<dyn gateway_admin::ports::plugins::PluginQuotaService>>,
    ) -> Self {
        self.quota = service;
        self
    }
    pub(crate) fn with_resources(
        mut self,
        resources: Option<Arc<dyn gateway_admin::ports::plugins::PluginResourceService>>,
    ) -> Self {
        self.resources = resources;
        self
    }
    pub(crate) fn new(
        store: Arc<dyn PluginStore>,
        record: &PluginRecord,
        manifest: &Manifest,
        active: Arc<AtomicBool>,
        data: Option<Arc<dyn gateway_admin::ports::plugins::PluginDataService>>,
        outbound: Arc<crate::outbound::Registry>,
        logs: Arc<PluginLog>,
    ) -> Result<Self, gateway_admin::model::AdminError> {
        let namespaces = manifest
            .state
            .iter()
            .map(|s| {
                Ok((
                    s.namespace.clone(),
                    Namespace {
                        declaration: s.clone(),
                        schema: crate::engine::schema(&s.schema)?,
                    },
                ))
            })
            .collect::<Result<_, gateway_admin::model::AdminError>>()?;
        Ok(Self {
            logs,
            credentials: None,
            outbound,
            models: None,
            quota: None,
            resources: None,
            calls: Arc::default(),
            store,
            id: record.id.clone(),
            revision: record.revision,
            active,
            namespaces: Arc::new(namespaces),
            data,
        })
    }
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid plugin state input")
}
fn encode(value: impl serde::Serialize) -> Result<RpcReply, PluginFault> {
    Ok(RpcReply {
        result: serde_json::to_value(value).map_err(|_| invalid())?,
        payload: vec![],
    })
}
fn store_error(error: gateway_admin::model::AdminError) -> PluginFault {
    PluginFault::new(
        match error.kind() {
            AdminErrorKind::Conflict => ErrorCode::Conflict,
            AdminErrorKind::Invalid => ErrorCode::Capacity,
            _ => ErrorCode::Fault,
        },
        "plugin state operation failed",
    )
}
impl CallbackHandler for Callbacks {
    fn begin(&self, context: &CallContext, _deadline: &gateway_plugin_sdk::client::CallDeadline) {
        self.calls.bind_budget(context, _deadline);
        if context.stage == Stage::Management
            && let Some(caller) = self.calls.caller(&context.resource_scope_id)
        {
            let _ = self.outbound.bind(
                context,
                caller.cancellation.clone(),
                Some(caller.graph.clone()),
            );
        }
        if context.stage == Stage::Management
            && let Some(models) = &self.models
        {
            // LivePlugin registers the authenticated actor before the RPC begins.
            let _ = models.bind(
                context,
                None,
                self.calls.actor(&context.resource_scope_id),
                gateway_core::lifecycle::CancellationToken::new(),
                self.calls
                    .caller(&context.resource_scope_id)
                    .and_then(|caller| caller.invocation.clone()),
                self.calls
                    .caller(&context.resource_scope_id)
                    .map(|caller| caller.graph.clone()),
            );
        }
    }
    fn finish(&self, context: &CallContext) {
        if let Some(models) = &self.models {
            models.finish(context);
        }
    }
    fn call(
        &self,
        context: CallContext,
        method: String,
        params: Value,
        payload: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcReply, PluginFault>> {
        let store = self.store.clone();
        let id = self.id.clone();
        let revision = self.revision;
        let active = self.active.clone();
        let namespaces = self.namespaces.clone();
        let data = self.data.clone();
        let resources = self.resources.clone();
        let calls = self.calls.clone();
        let quota = self.quota.clone();
        let credentials = self.credentials.clone();
        let models = self.models.clone();
        let logs = self.logs.clone();
        Box::pin(async move {
            if method == "host.log" {
                if (context.stage != Stage::Registration && !active.load(Ordering::Acquire))
                    || context.instance_id != id
                    || context.generation != revision
                    || calls
                        .caller(&context.resource_scope_id)
                        .is_some_and(|caller| !caller.authorizes(&context))
                {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "plugin log scope ended",
                    ));
                }
                return logs.record(&context, params, &payload);
            }
            if !active.load(Ordering::Acquire)
                || context.instance_id != id
                || context.generation != revision
                || !matches!(
                    context.stage,
                    Stage::Management
                        | Stage::CommandLine
                        | Stage::Maintenance
                        | Stage::Observation
                        | Stage::Authentication
                        | Stage::Routing
                        | Stage::Scheduling
                        | Stage::Retry
                        | Stage::Upstream
                )
            {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "plugin generation is not active",
                ));
            }
            if matches!(
                context.stage,
                Stage::Maintenance | Stage::Observation | Stage::CommandLine
            ) && calls
                .caller(&context.resource_scope_id)
                .is_none_or(|caller| caller.cancellation.is_cancelled())
            {
                return Err(PluginFault::new(
                    ErrorCode::Cancelled,
                    "background invocation scope ended",
                ));
            }
            if crate::credentials::is_method(&method)
                || method == gateway_plugin_sdk::call::data::QUOTA_REFRESH
            {
                let caller = calls
                    .caller(&context.resource_scope_id)
                    .filter(|caller| caller.authorizes(&context))
                    .ok_or_else(|| {
                        PluginFault::new(
                            ErrorCode::PermissionDenied,
                            "credential invocation is not active",
                        )
                    })?;
                let permitted = match context.stage {
                    Stage::Management => matches!(
                        caller.actor().actor,
                        gateway_admin::model::MutationActor::AdminSession { .. }
                            | gateway_admin::model::MutationActor::AdminApiKey
                    ),
                    Stage::CommandLine => {
                        caller.graph.is_root()
                            && matches!(
                                caller.actor().actor,
                                gateway_admin::model::MutationActor::System
                            )
                    }
                    Stage::Maintenance => {
                        matches!(&caller.actor().actor, gateway_admin::model::MutationActor::Plugin { instance_id, revision: actual } if instance_id == &id && actual.get() == revision)
                    }
                    _ => false,
                };
                if !permitted {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "this invocation has no account credential authority",
                    ));
                }
                let scope = PluginMutationScope {
                    instance_id: id,
                    revision,
                    actor: gateway_admin::model::MutationContext {
                        actor: gateway_admin::model::MutationActor::Plugin {
                            instance_id: context.instance_id,
                            revision: std::num::NonZeroU64::new(revision).ok_or_else(invalid)?,
                        },
                        request_id: caller.actor().request_id.clone(),
                    },
                };
                if method == gateway_plugin_sdk::call::data::QUOTA_REFRESH {
                    let service = data.ok_or_else(|| {
                        PluginFault::new(
                            ErrorCode::Unsupported,
                            "native quota refresh is not configured",
                        )
                    })?;
                    return crate::data::refresh(service, scope, caller, params, payload).await;
                }
                let service = credentials.ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::Unsupported,
                        "native account credentials are not configured",
                    )
                })?;
                return crate::credentials::call(service, scope, caller, &method, params, payload)
                    .await;
            }
            if matches!(context.stage, Stage::Routing | Stage::Scheduling)
                && !crate::models::is_method(&method)
                && !matches!(
                    method.as_str(),
                    "host.state.get" | "host.state.put" | "host.state.delete"
                )
            {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "policy callbacks cannot access management capabilities",
                ));
            }
            if matches!(
                context.stage,
                Stage::Authentication | Stage::Retry | Stage::Upstream
            ) && !matches!(
                method.as_str(),
                "host.state.get" | "host.state.put" | "host.state.delete"
            ) {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "this policy stage has no management authority",
                ));
            }
            if method == gateway_plugin_sdk::call::services::CALL_METHOD {
                let caller = calls.caller(&context.resource_scope_id).ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "service caller is no longer active",
                    )
                })?;
                if caller.cancellation.is_cancelled() {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "service caller has ended",
                    ));
                }
                let dispatcher = caller
                    .invocation
                    .as_ref()
                    .and_then(|origin| origin.services.clone())
                    .ok_or_else(|| {
                        PluginFault::new(
                            ErrorCode::PermissionDenied,
                            "service authority is not bound",
                        )
                    })?;
                return tokio::select! {biased;
                    ()=caller.cancellation.cancelled()=>Err(crate::middleware::service_cancelled()),
                    result=crate::middleware::call_service(dispatcher,id,params,payload)=>result,
                };
            }
            if crate::models::is_method(&method)
                || (matches!(context.stage, Stage::Management | Stage::CommandLine)
                    && method.starts_with("host.http."))
            {
                let models = models.ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::Unsupported,
                        "host model service is not configured",
                    )
                })?;
                return models.call(context, method, params, payload).await;
            }
            if matches!(
                method.as_str(),
                gateway_plugin_sdk::call::shared_budgets::GET
                    | gateway_plugin_sdk::call::shared_budgets::UPDATE_GROUP_LIMITS
                    | gateway_plugin_sdk::call::shared_budgets::RESET
            ) {
                let actor = calls.actor(&context.resource_scope_id).ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "management caller is no longer active",
                    )
                })?;
                let service = quota.ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::Unsupported,
                        "shared quota service is not configured",
                    )
                })?;
                return crate::quota::call(
                    service.as_ref(),
                    PluginMutationScope {
                        instance_id: id,
                        revision,
                        actor,
                    },
                    &method,
                    params,
                    &payload,
                )
                .await;
            }
            if matches!(
                method.as_str(),
                gateway_plugin_sdk::call::resources::GROUP_ENSURE
                    | gateway_plugin_sdk::call::resources::KEY_ENSURE
                    | gateway_plugin_sdk::call::resources::GROUP_MEMBERS
            ) {
                let actor = calls.actor(&context.resource_scope_id).ok_or_else(|| {
                    PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "management caller is no longer active",
                    )
                })?;
                let resources = resources.ok_or_else(|| {
                    PluginFault::new(ErrorCode::Unsupported, "resource service is not configured")
                })?;
                return crate::resources::call(
                    resources.as_ref(),
                    PluginMutationScope {
                        instance_id: id,
                        revision,
                        actor,
                    },
                    &method,
                    params,
                    &payload,
                )
                .await;
            }
            if matches!(
                method.as_str(),
                gateway_plugin_sdk::call::data::ACCOUNTS_LIST
                    | gateway_plugin_sdk::call::data::KEYS_GET
                    | gateway_plugin_sdk::call::data::QUOTA_GET
            ) {
                let data = data.ok_or_else(|| {
                    PluginFault::new(ErrorCode::Unsupported, "plugin facts are not configured")
                })?;
                let reply = crate::data::call(data.as_ref(), &method, params, &payload).await?;
                if !active.load(Ordering::Acquire) {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "plugin generation is not active",
                    ));
                }
                return Ok(reply);
            }
            if !matches!(
                method.as_str(),
                "host.state.get" | "host.state.put" | "host.state.delete"
            ) {
                return Err(PluginFault::new(
                    ErrorCode::Unsupported,
                    "host callback is not implemented by this host",
                ));
            }
            if !payload.is_empty() {
                return Err(invalid());
            }
            let namespace = params
                .get("namespace")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?;
            let ns = namespaces.get(namespace).ok_or_else(invalid)?;
            let key = params
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?;
            if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
                return Err(invalid());
            }
            let scope = PluginStateScope {
                instance_id: id,
                revision,
                namespace: namespace.into(),
                schema_version: ns.declaration.schema_version,
                maximum_records: ns.declaration.maximum_records,
                maximum_bytes: ns.declaration.maximum_bytes,
            };
            match method.as_str() {
                "host.state.get" => {
                    let request: StateGetRequest =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    let record = store
                        .state_get(&scope, &request.key)
                        .await
                        .map_err(store_error)?;
                    encode(StateGetResult {
                        record: record.map(|r| StateRecord {
                            value: r.value,
                            version: r.version,
                            schema_version: r.schema_version,
                        }),
                    })
                }
                "host.state.put" => {
                    let request: StatePutRequest =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    if serde_json::to_vec(&request.value)
                        .map_err(|_| invalid())?
                        .len()
                        > ns.declaration.maximum_value_bytes as usize
                        || !ns.schema.is_valid(&request.value)
                    {
                        return Err(invalid());
                    }
                    let version = store
                        .state_put(
                            &scope,
                            &request.key,
                            request.expected_version,
                            Some(request.value),
                        )
                        .await
                        .map_err(store_error)?;
                    encode(StatePutResult { version })
                }
                "host.state.delete" => {
                    let request: StateDeleteRequest =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    store
                        .state_put(&scope, &request.key, Some(request.expected_version), None)
                        .await
                        .map_err(store_error)?;
                    encode(StateDeleteResult { deleted: true })
                }
                _ => encode(json!({})),
            }
        })
    }
}
