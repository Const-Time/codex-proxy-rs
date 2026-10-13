//! Common upstream operations and explicitly versioned local settings services.
//! The original domain service owns validation, persistence and publication.
use super::{Origin, invoke, local};
use crate::model::{pricing, settings as domain};
use crate::{
    model::{
        AdminError, MutationContext, Revision,
        settings::{AdminApiKey, AdminApiKeyMutation, RegeneratedAdminApiKey},
    },
    use_case::settings::SettingsService,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub struct Service {
    inner: Arc<dyn SettingsService>,
}
impl Service {
    /// The caller is supplied by an authenticated host boundary, never by RPC input.
    pub fn dispatcher(
        &self,
        mut origin: Origin,
        actor: MutationContext,
    ) -> Arc<dyn gateway_core::engine::middleware::service::Dispatcher> {
        origin.dispatcher = None;
        Arc::new(Dispatcher {
            inner: self.inner.clone(),
            origin,
            actor,
        })
    }
    pub(crate) fn new(inner: Arc<dyn SettingsService>) -> Self {
        Self { inner }
    }

    pub async fn load(
        &self,
        origin: Option<Origin>,
    ) -> Result<domain::RuntimeSettings, AdminError> {
        let inner = self.inner.clone();
        let result: local::RuntimeSettings =
            invoke(origin, "local.settings.v1.load", (), move |()| {
                Box::pin(async move { inner.load().await.map(Into::into) })
            })
            .await?;
        result.try_into()
    }
    pub async fn replace(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
        command: domain::ReplaceRuntimeSettings,
    ) -> Result<domain::RuntimeSettings, AdminError> {
        let inner = self.inner.clone();
        mutation(
            origin,
            "local.settings.v1.replace",
            (context, local::ReplaceRuntimeSettings::from(command)),
            |input| &input.0,
            TryInto::try_into,
            move |(context, command)| {
                Box::pin(async move {
                    inner
                        .replace(&context, command.try_into()?)
                        .await
                        .map(local::RuntimeSettings::from)
                })
            },
        )
        .await
    }
    pub async fn pricing(
        &self,
        origin: Option<Origin>,
    ) -> Result<pricing::PricingCatalog, AdminError> {
        let inner = self.inner.clone();
        invoke(origin, "local.settings.v1.pricing", (), move |()| {
            Box::pin(async move { inner.pricing().await })
        })
        .await
    }
    pub async fn preview_pricing_sync(
        &self,
        origin: Option<Origin>,
    ) -> Result<pricing::PricingSyncPreview, AdminError> {
        let inner = self.inner.clone();
        invoke(
            origin,
            "local.settings.v1.preview_pricing_sync",
            (),
            move |()| Box::pin(async move { inner.preview_pricing_sync().await }),
        )
        .await
    }
    pub async fn sync_pricing(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
        command: pricing::PricingSyncPreview,
    ) -> Result<(), AdminError> {
        let inner = self.inner.clone();
        mutation(
            origin,
            "local.settings.v1.sync_pricing",
            (context, command),
            |input| &input.0,
            Ok,
            move |(context, command)| {
                Box::pin(async move { inner.sync_pricing(&context, command).await })
            },
        )
        .await
    }
    pub async fn update_pricing(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
        command: pricing::UpdatePricing,
    ) -> Result<(), AdminError> {
        let inner = self.inner.clone();
        mutation(
            origin,
            "local.settings.v1.update_pricing",
            (context, command),
            |input| &input.0,
            Ok,
            move |(context, command)| {
                Box::pin(async move { inner.update_pricing(&context, command).await })
            },
        )
        .await
    }
    pub async fn update_pricing_sync_policy(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
        command: pricing::PricingSyncPolicy,
    ) -> Result<(), AdminError> {
        let inner = self.inner.clone();
        mutation(
            origin,
            "local.settings.v1.update_pricing_sync_policy",
            (context, command),
            |input| &input.0,
            Ok,
            move |(context, command)| {
                Box::pin(async move { inner.update_pricing_sync_policy(&context, command).await })
            },
        )
        .await
    }
    pub async fn admin_api_key_exists(&self, origin: Option<Origin>) -> Result<bool, AdminError> {
        let inner = self.inner.clone();
        invoke(origin, "settings.admin_api_key_exists", (), move |()| {
            Box::pin(async move { inner.admin_api_key_exists().await })
        })
        .await
    }
    pub async fn regenerate_admin_api_key(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
    ) -> Result<RegeneratedAdminApiKey, AdminError> {
        let inner = self.inner.clone();
        let result: Regenerated = mutation(
            origin,
            "settings.regenerate_admin_api_key",
            context,
            |context| context,
            Ok,
            move |context| {
                Box::pin(async move {
                    let result = inner.regenerate_admin_api_key(&context).await?;
                    Ok(Regenerated {
                        mutation: result.mutation.into(),
                        key: result.key.expose_for_response().into(),
                    })
                })
            },
        )
        .await?;
        Ok(RegeneratedAdminApiKey {
            mutation: result.mutation.into(),
            key: AdminApiKey::new(result.key),
        })
    }
    pub async fn delete_admin_api_key(
        &self,
        origin: Option<Origin>,
        context: MutationContext,
    ) -> Result<AdminApiKeyMutation, AdminError> {
        let inner = self.inner.clone();
        let result: Mutation = mutation(
            origin,
            "settings.delete_admin_api_key",
            context,
            |context| context,
            Ok,
            move |context| {
                Box::pin(async move { inner.delete_admin_api_key(&context).await.map(Into::into) })
            },
        )
        .await?;
        Ok(result.into())
    }
}

#[derive(Clone)]
struct Dispatcher {
    inner: Arc<dyn SettingsService>,
    origin: Origin,
    actor: MutationContext,
}
impl gateway_core::engine::middleware::service::Dispatcher for Dispatcher {
    fn scoped(
        &self,
        context: &gateway_core::engine::middleware::service::Context,
    ) -> Arc<dyn gateway_core::engine::middleware::service::Dispatcher> {
        Arc::new(Self {
            inner: self.inner.clone(),
            actor: self.actor.clone(),
            origin: Origin {
                registry: self.origin.registry.clone(),
                request_id: context.request_id.clone(),
                call_id: context.call_id.clone(),
                cancellation: gateway_core::lifecycle::CancellationToken::linked([
                    self.origin.cancellation.clone(),
                    context.cancellation.clone(),
                ]),
                deadline_at: self
                    .origin
                    .deadline_at
                    .at()
                    .map_or(context.deadline_at, |at| context.deadline_at.cap(at)),
                call_scope: context.call_scope.clone(),
                dispatcher: None,
            },
        })
    }
    fn call(
        &self,
        instance: String,
        operation: String,
        input: serde_json::Value,
    ) -> BoxFuture<
        'static,
        Result<serde_json::Value, gateway_core::engine::middleware::service::Error>,
    > {
        let host = self.clone();
        Box::pin(async move {
            use gateway_core::engine::middleware::service::Error;
            if host.origin.cancellation.is_cancelled() || host.origin.deadline_at.is_elapsed() {
                return Err(Error::unavailable("服务调用已结束"));
            }
            let command = Command::decode(&operation, input)?;
            let mut origin = host.origin.clone();
            origin.call_scope = origin.call_scope.fork_http(instance).map_err(|error| {
                use gateway_core::error::GatewayErrorKind;
                Error {
                    kind: match error.kind() {
                        GatewayErrorKind::RateLimited => "rate_limited",
                        GatewayErrorKind::Unsupported => "unsupported",
                        GatewayErrorKind::InvalidRequest => "invalid",
                        _ => "unavailable",
                    }
                    .into(),
                    message: error.safe_message().into(),
                    details: None,
                }
            })?;
            origin.dispatcher = Some(Arc::new(host.clone()));
            let service = Service::new(host.inner);
            let result = match command {
                Command::Exists => service
                    .admin_api_key_exists(Some(origin))
                    .await
                    .and_then(encode),
                Command::Regenerate => service
                    .regenerate_admin_api_key(Some(origin), host.actor)
                    .await
                    .and_then(|result| {
                        encode(Regenerated {
                            mutation: result.mutation.into(),
                            key: result.key.expose_for_response().into(),
                        })
                    }),
                Command::Delete => service
                    .delete_admin_api_key(Some(origin), host.actor)
                    .await
                    .and_then(|result| encode(Mutation::from(result))),
                Command::Load => service
                    .load(Some(origin))
                    .await
                    .and_then(|result| encode(local::RuntimeSettings::from(result))),
                Command::Replace(command) => service
                    .replace(Some(origin), host.actor, command)
                    .await
                    .and_then(|result| encode(local::RuntimeSettings::from(result))),
                Command::Pricing => service.pricing(Some(origin)).await.and_then(encode),
                Command::Preview => service
                    .preview_pricing_sync(Some(origin))
                    .await
                    .and_then(encode),
                Command::Sync(command) => service
                    .sync_pricing(Some(origin), host.actor, command)
                    .await
                    .and_then(encode),
                Command::Update(command) => service
                    .update_pricing(Some(origin), host.actor, command)
                    .await
                    .and_then(encode),
                Command::Policy(command) => service
                    .update_pricing_sync_policy(Some(origin), host.actor, command)
                    .await
                    .and_then(encode),
            };
            result.map_err(super::encode_error)
        })
    }
}

// Decode once before taking a graph permit. Wire actor fields are syntax only;
// every mutation below receives the host-captured identity.
enum Command {
    Exists,
    Regenerate,
    Delete,
    Load,
    Replace(domain::ReplaceRuntimeSettings),
    Pricing,
    Preview,
    Sync(pricing::PricingSyncPreview),
    Update(pricing::UpdatePricing),
    Policy(pricing::PricingSyncPolicy),
}
impl Command {
    fn decode(
        operation: &str,
        input: serde_json::Value,
    ) -> Result<Self, gateway_core::engine::middleware::service::Error> {
        use gateway_core::engine::middleware::service::Error;
        fn decode<T: DeserializeOwned>(input: serde_json::Value) -> Result<T, Error> {
            serde_json::from_value(input).map_err(|_| Error::invalid("服务输入类型无效"))
        }
        Ok(match operation {
            "settings.admin_api_key_exists" => {
                decode::<()>(input)?;
                Self::Exists
            }
            "settings.regenerate_admin_api_key" => {
                decode::<MutationContext>(input)?;
                Self::Regenerate
            }
            "settings.delete_admin_api_key" => {
                decode::<MutationContext>(input)?;
                Self::Delete
            }
            "local.settings.v1.load" => {
                decode::<()>(input)?;
                Self::Load
            }
            "local.settings.v1.replace" => {
                let (_, command) =
                    decode::<(MutationContext, local::ReplaceRuntimeSettings)>(input)?;
                Self::Replace(command.try_into().map_err(super::encode_error)?)
            }
            "local.settings.v1.pricing" => {
                decode::<()>(input)?;
                Self::Pricing
            }
            "local.settings.v1.preview_pricing_sync" => {
                decode::<()>(input)?;
                Self::Preview
            }
            "local.settings.v1.sync_pricing" => {
                Self::Sync(decode::<(MutationContext, pricing::PricingSyncPreview)>(input)?.1)
            }
            "local.settings.v1.update_pricing" => {
                Self::Update(decode::<(MutationContext, pricing::UpdatePricing)>(input)?.1)
            }
            "local.settings.v1.update_pricing_sync_policy" => {
                Self::Policy(decode::<(MutationContext, pricing::PricingSyncPolicy)>(input)?.1)
            }
            _ => {
                return Err(Error {
                    kind: "unsupported".into(),
                    message: "服务操作尚未接入".into(),
                    details: None,
                });
            }
        })
    }
}
fn encode<T: Serialize>(value: T) -> Result<serde_json::Value, AdminError> {
    serde_json::to_value(value).map_err(|_| AdminError::invalid("服务结果编码失败"))
}

// Secret encoding is confined to this explicit operation; the domain key does
// not gain general Serialize or Debug access.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Regenerated {
    mutation: Mutation,
    key: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Mutation {
    config_revision: std::num::NonZeroU64,
    exists: bool,
}
impl From<AdminApiKeyMutation> for Mutation {
    fn from(value: AdminApiKeyMutation) -> Self {
        Self {
            config_revision: std::num::NonZeroU64::new(value.config_revision.get())
                .expect("positive domain revision"),
            exists: value.exists,
        }
    }
}
impl From<Mutation> for AdminApiKeyMutation {
    fn from(value: Mutation) -> Self {
        Self {
            config_revision: Revision::new(value.config_revision.get())
                .expect("positive wire revision"),
            exists: value.exists,
        }
    }
}

async fn mutation<I, O, R>(
    origin: Option<Origin>,
    operation: &'static str,
    input: I,
    actor: fn(&I) -> &MutationContext,
    finish: fn(O) -> Result<R, AdminError>,
    terminal: impl FnOnce(I) -> BoxFuture<'static, Result<O, AdminError>> + Send + 'static,
) -> Result<R, AdminError>
where
    I: Serialize + DeserializeOwned + Send + 'static,
    O: Serialize + DeserializeOwned + Send + 'static,
{
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let failure = Arc::new(Mutex::new(None));
    let entered = started.clone();
    let completed = finished.clone();
    let failed = failure.clone();
    let trusted = actor(&input).clone();
    let scope = origin.as_ref().map(|origin| origin.call_scope.clone());
    let result = invoke(origin, operation, input, move |input| {
        Box::pin(async move {
            if actor(&input) != &trusted {
                return Err(AdminError::invalid("插件不能改写服务审计身份或请求关联"));
            }
            // Once entered, keep the original transaction/publication alive even if
            // HTTP or middleware cancellation drops the waiter. Never replay it.
            entered.store(true, Ordering::SeqCst);
            if let Some(scope) = &scope {
                scope.observe_http_dispatch();
            }
            tokio::spawn(async move {
                // Retain the same graph permit until the native write/publication
                // finishes, even if the caller drops its response in the meantime.
                let result = terminal(input).await;
                if let Err(error) = &result {
                    *failed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
                }
                drop(scope);
                completed.store(true, Ordering::SeqCst);
                result
            })
            .await
            .map_err(|_| AdminError::upstream_result_unknown("服务写入结果未确认"))?
        })
    })
    .await
    .and_then(finish);
    // The original terminal's failure cannot be turned into a successful write.
    if started.load(Ordering::SeqCst) && !finished.load(Ordering::SeqCst) {
        return Err(AdminError::upstream_result_unknown(
            "服务写入已开始，结果未确认，请查询当前状态",
        ));
    }
    if let Some(error) = failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
    {
        return Err(error);
    }
    match result {
        Err(_) if started.load(Ordering::SeqCst) => Err(AdminError::upstream_result_unknown(
            "服务写入已开始，结果未确认，请查询当前状态",
        )),
        result => result,
    }
}
