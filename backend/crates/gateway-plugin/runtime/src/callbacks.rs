//! 状态回调使用宿主保存的实例/revision，不接受插件指定另一个 owner。
use crate::{CallbackHandler, RpcReply};
use futures::future::BoxFuture;
use gateway_admin::{
    model::{AdminErrorKind, plugins::*},
    ports::plugins::PluginStore,
};
use gateway_plugin_sdk::{
    CallContext, ErrorCode, Manifest, PluginFault, Stage, StateNamespace, call::host::*,
};
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
    models: Option<Arc<crate::models::Models>>,
    quota: Option<Arc<dyn gateway_admin::ports::plugins::PluginQuotaService>>,
    resources: Option<Arc<dyn gateway_admin::ports::plugins::PluginResourceService>>,
    pub(crate) calls: Arc<crate::resources::ManagementCalls>,
    store: Arc<dyn PluginStore>,
    id: String,
    revision: u64,
    active: Arc<AtomicBool>,
    namespaces: Arc<BTreeMap<String, Namespace>>,
    data: Option<Arc<dyn gateway_admin::ports::plugins::PluginDataService>>,
}
impl Callbacks {
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
    fn begin(&self, context: &CallContext) {
        if context.stage == Stage::Management
            && let Some(models) = &self.models
        {
            // LivePlugin registers the authenticated actor before the RPC begins.
            let _ = models.bind(
                context,
                None,
                self.calls.actor(&context.resource_scope_id),
                gateway_core::lifecycle::CancellationToken::new(),
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
        let models = self.models.clone();
        Box::pin(async move {
            if !active.load(Ordering::Acquire)
                || context.instance_id != id
                || context.generation != revision
                || (context.stage != Stage::Management && context.stage != Stage::Authentication)
            {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "plugin generation is not active",
                ));
            }
            if context.stage == Stage::Authentication
                && !matches!(
                    method.as_str(),
                    "host.state.get" | "host.state.put" | "host.state.delete"
                )
            {
                return Err(PluginFault::new(
                    ErrorCode::PermissionDenied,
                    "authentication callbacks cannot access management capabilities",
                ));
            }
            if crate::models::is_method(&method) {
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
