//! SDK material remains unverified until the native Provider/Admin port prepares it.
use gateway_admin::{
    model::{
        AdminError, AdminErrorKind, MutationActor, MutationContext, Revision, plugins::*,
        provider_credentials::*,
    },
    ports::plugins::PluginCredentialService,
};
use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    engine::{middleware::service::Caller, nested::ExtensionCallScope},
    lifecycle::CancellationToken,
    routing::ProviderKind,
};
use gateway_plugin_sdk::call::host::{AuthSaveRequest, CredentialFacts};
use std::sync::Arc;

fn convert(request: AuthSaveRequest) -> Result<PluginCredentialSave, AdminError> {
    let facts = |facts: CredentialFacts| -> Result<UnverifiedCredentialFacts, AdminError> {
        let timestamp = |value: Option<i64>| {
            value
                .map(|value| {
                    chrono::DateTime::from_timestamp_millis(value)
                        .ok_or_else(|| AdminError::invalid("凭据时间无效"))
                })
                .transpose()
        };
        let facts = UnverifiedCredentialFacts {
            name: facts.name,
            authentication_kind: facts.authentication_kind,
            material: ProviderDocument::new(OpaqueProviderData::new(facts.material)),
            email: facts.email,
            upstream_user_id: facts.upstream_user_id,
            upstream_account_id: facts.upstream_account_id,
            plan_type: facts.plan_type,
            has_refresh_token: facts.has_refresh_token,
            access_token_expires_at: timestamp(facts.access_token_expires_at_ms)?,
            next_refresh_at: timestamp(facts.next_refresh_at_ms)?,
        };
        facts.validate()?;
        Ok(facts)
    };
    Ok(match request {
        AuthSaveRequest::Create {
            provider_id,
            facts: input,
        } => PluginCredentialSave::Create {
            provider: ProviderKind::new(provider_id)
                .map_err(|_| AdminError::invalid("Provider 无效"))?,
            facts: facts(input)?,
        },
        AuthSaveRequest::Replace {
            account_id,
            credential_revision,
            facts: input,
        } => PluginCredentialSave::Replace {
            account_id: ProviderAccountId::new(account_id)
                .map_err(|_| AdminError::invalid("账号 ID 无效"))?,
            credential_revision: Revision::new(credential_revision)
                .map_err(|_| AdminError::invalid("凭据版本无效"))?,
            facts: facts(input)?,
        },
    })
}

pub(super) async fn save_all(
    service: Option<&Arc<dyn PluginCredentialService>>,
    accounts: Vec<AuthSaveRequest>,
    caller: Caller,
    request_id: &str,
    graph: ExtensionCallScope,
    cancellation: CancellationToken,
    deadline: tokio::time::Instant,
) -> Result<usize, PluginCommandError> {
    let incomplete = |saved_accounts, reason| PluginCommandError::Incomplete {
        saved_accounts,
        reason,
    };
    let service = service.ok_or_else(|| incomplete(0, "账号提交入口尚未接入"))?;
    let scope = PluginMutationScope {
        instance_id: caller.instance_id().to_owned(),
        revision: caller.revision().get(),
        actor: MutationContext {
            actor: MutationActor::Plugin {
                instance_id: caller.instance_id().to_owned(),
                revision: caller.revision(),
            },
            request_id: request_id.to_owned(),
        },
    };
    let mut saved = 0;
    for account in accounts {
        if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            return Err(incomplete(saved, "账号提交已取消或超过命令总期限"));
        }
        let command = convert(account).map_err(|_| incomplete(saved, "账号凭据事实无效"))?;
        let cleanup = graph
            .fork_http(caller.instance_id().to_owned())
            .map_err(|_| incomplete(saved, "账号提交超过原调用图限制"))?;
        let result = tokio::select! { biased;
            () = cancellation.cancelled() => return Err(incomplete(saved, "账号提交已取消，进行中结果未确认")),
            result = tokio::time::timeout_at(deadline, service.save(scope.clone(), command, cleanup, cancellation.clone())) =>
                result.map_err(|_| incomplete(saved, "超过命令总期限，进行中结果未确认"))?,
        };
        result.map_err(|error| {
            incomplete(
                saved,
                match error.kind() {
                    AdminErrorKind::Invalid => "Provider 拒绝账号凭据",
                    AdminErrorKind::Conflict => "账号版本或插件授权已变化",
                    AdminErrorKind::NotFound => "账号或 Provider 不存在",
                    _ => "账号准备或提交结果未确认",
                },
            )
        })?;
        saved += 1;
    }
    Ok(saved)
}
