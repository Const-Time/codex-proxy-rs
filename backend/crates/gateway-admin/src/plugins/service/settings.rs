//! Settings service operations whose upstream and local contracts agree.
//! The original domain service owns validation, persistence and publication.
use super::{Origin, invoke};
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
    pub(crate) fn new(inner: Arc<dyn SettingsService>) -> Self {
        Self { inner }
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
            move |context| {
                Box::pin(async move { inner.delete_admin_api_key(&context).await.map(Into::into) })
            },
        )
        .await?;
        Ok(result.into())
    }
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

async fn mutation<O>(
    origin: Option<Origin>,
    operation: &'static str,
    context: MutationContext,
    terminal: impl FnOnce(MutationContext) -> BoxFuture<'static, Result<O, AdminError>> + Send + 'static,
) -> Result<O, AdminError>
where
    O: Serialize + DeserializeOwned + Send + 'static,
{
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let failure = Arc::new(Mutex::new(None));
    let entered = started.clone();
    let completed = finished.clone();
    let failed = failure.clone();
    let trusted = context.clone();
    let scope = origin.as_ref().map(|origin| origin.call_scope.clone());
    let result = invoke(origin, operation, context, move |input| {
        Box::pin(async move {
            if input != trusted {
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
                let result = terminal(trusted).await;
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
    .await;
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
