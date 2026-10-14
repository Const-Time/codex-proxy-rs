//! Login callbacks consume the original OAuth coordination record before plugin execution.
use super::*;
use crate::{model::MutationActor, ports::store::AuthStore};
use gateway_core::{
    account::OpaqueProviderData,
    provider_ports::{
        NewOAuthPendingFlow, OAuthPendingBinding, OAuthPendingClaimOutcome,
        OAuthPendingConsumeOutcome, OAuthPendingFlowPort, OAuthPendingPutOutcome,
    },
    routing::ProviderKind,
};
use std::{num::NonZeroU64, time::Duration};

pub(super) struct Callbacks {
    pub pending: Arc<dyn OAuthPendingFlowPort>,
    pub auth: Arc<dyn AuthStore>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: String,
    revision: u64,
    incarnation: String,
    path: String,
    expires_at_ms: i64,
    owner: String,
    actor: MutationActor,
}
fn invalid() -> AdminError {
    AdminError::invalid("插件登录票据无效、已过期或已消费，请重新发起登录")
}
fn unavailable() -> AdminError {
    AdminError::unavailable("插件登录状态暂不可用，请重新发起登录")
}
fn namespace(id: &str) -> Result<ProviderKind, AdminError> {
    ProviderKind::new(format!("plugin-management-{id}")).map_err(|_| invalid())
}
fn binding(value: &str) -> Result<OAuthPendingBinding, AdminError> {
    OAuthPendingBinding::try_new(value).map_err(|_| invalid())
}
impl PluginManager {
    async fn callback_process(
        &self,
        id: &str,
        revision: u64,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        let process = self.process(id, revision)?;
        if self.stopping.is_cancelled()
            || process.access().is_revoked()
            || !self
                .store
                .list()
                .await?
                .iter()
                .any(|record| record.id == id && record.enabled && record.revision == revision)
        {
            return Err(invalid());
        }
        Ok(process)
    }
    async fn callback_actor(
        &self,
        id: &str,
        revision: u64,
        actor: &MutationContext,
    ) -> Result<(), AdminError> {
        if !matches!(
            actor.actor,
            MutationActor::AdminSession { .. } | MutationActor::AdminApiKey
        ) {
            return Err(AdminError::new(
                crate::model::AdminErrorKind::Forbidden,
                "插件登录必须由管理员发起",
            ));
        }
        let scope = PluginMutationScope {
            instance_id: id.into(),
            revision,
            actor: MutationContext {
                actor: MutationActor::Plugin {
                    instance_id: id.into(),
                    revision: NonZeroU64::new(revision).ok_or_else(invalid)?,
                },
                request_id: actor.request_id.clone(),
            },
        };
        crate::use_case::plugin_account_authority::authorize(
            self.callbacks
                .as_ref()
                .ok_or_else(unavailable)?
                .auth
                .as_ref(),
            &scope,
            actor,
        )
        .await
    }
    pub(super) async fn start_callback(
        &self,
        id: &str,
        revision: u64,
        path: &str,
        ttl_seconds: u32,
        actor: &MutationContext,
    ) -> Result<PluginCallbackTicket, AdminError> {
        if !(1..=600).contains(&ttl_seconds) {
            return Err(invalid());
        }
        self.callback_actor(id, revision, actor).await?;
        let process = self.callback_process(id, revision).await?;
        let descriptor = process.management_callback(path).ok_or_else(invalid)?;
        let flow = uuid::Uuid::new_v4().simple().to_string();
        // Independent random bindings disclose no administrator identity.
        let owner = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let expires_at_ms = chrono::Utc::now().timestamp_millis() + i64::from(ttl_seconds) * 1000;
        let value = serde_json::to_value(Pending {
            id: id.into(),
            revision,
            incarnation: descriptor.incarnation,
            path: path.into(),
            expires_at_ms,
            owner: owner.clone(),
            actor: actor.actor.clone(),
        })
        .map_err(|_| unavailable())?;
        let pending = NewOAuthPendingFlow::try_new(
            namespace(id)?,
            binding(&flow)?,
            binding(&owner)?,
            Duration::from_secs(u64::from(ttl_seconds)),
            OpaqueProviderData::new(serde_json::from_value(value).map_err(|_| unavailable())?),
        )
        .map_err(|_| unavailable())?;
        if self
            .callbacks
            .as_ref()
            .ok_or_else(unavailable)?
            .pending
            .put_if_absent(pending)
            .await
            .map_err(|_| unavailable())?
            != OAuthPendingPutOutcome::Stored
        {
            return Err(unavailable());
        }
        Ok(PluginCallbackTicket {
            state: format!("{flow}.{owner}"),
            expires_at_ms,
        })
    }
    pub(super) async fn complete_callback(
        &self,
        id: &str,
        revision: u64,
        path: &str,
        state: &str,
        request: PluginCallbackRequest,
    ) -> Result<PluginCallbackResponse, AdminError> {
        if request.query.len() > 8192
            || request.query.bytes().any(|b| b.is_ascii_control())
            || request.request_id.is_empty()
        {
            return Err(invalid());
        }
        let (flow, owner) = state
            .split_once('.')
            .filter(|(f, o)| {
                f.len() == 32
                    && o.len() == 64
                    && f.bytes().chain(o.bytes()).all(|b| b.is_ascii_hexdigit())
            })
            .ok_or_else(invalid)?;
        let namespace = namespace(id)?;
        let flow = binding(flow)?;
        let owner_binding = binding(owner)?;
        let claim = binding(&uuid::Uuid::new_v4().simple().to_string())?;
        let pending = &self.callbacks.as_ref().ok_or_else(unavailable)?.pending;
        let payload = match pending
            .claim_if_owner(
                &namespace,
                &flow,
                &owner_binding,
                &claim,
                Duration::from_secs(30),
            )
            .await
            .map_err(|_| unavailable())?
        {
            OAuthPendingClaimOutcome::Claimed(value) => value,
            _ => return Err(invalid()),
        };
        // Never replay after cancellation, crash or an unknown plugin result.
        if pending
            .consume_claim(&namespace, &flow, &owner_binding, &claim)
            .await
            .map_err(|_| unavailable())?
            != OAuthPendingConsumeOutcome::Consumed
        {
            return Err(invalid());
        }
        let stored: Pending = serde_json::from_value(
            serde_json::to_value(payload.expose_to_provider()).map_err(|_| invalid())?,
        )
        .map_err(|_| invalid())?;
        if stored.id != id
            || stored.revision != revision
            || stored.path != path
            || stored.owner != owner
            || stored.expires_at_ms <= chrono::Utc::now().timestamp_millis()
        {
            return Err(invalid());
        }
        let actor = MutationContext {
            actor: stored.actor,
            request_id: request.request_id,
        };
        self.callback_actor(id, revision, &actor).await?;
        let process = self.callback_process(id, revision).await?;
        let descriptor = process.management_callback(path).ok_or_else(invalid)?;
        if descriptor.incarnation != stored.incarnation {
            return Err(invalid());
        }
        let invocation = (request.invocation)(&actor);
        process
            .callback(PluginCallbackCall {
                incarnation: stored.incarnation,
                headers: request.headers,
                request: PluginHttpRequest {
                    invocation,
                    actor,
                    method: "GET".into(),
                    path: path.into(),
                    query: request.query,
                    content_type: None,
                    body: vec![],
                },
            })
            .await
    }
}
