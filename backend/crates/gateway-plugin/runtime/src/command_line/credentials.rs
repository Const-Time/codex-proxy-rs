//! Command account results use the same native credential conversion and port as RPC callbacks.
use crate::credentials::convert;
use gateway_admin::{
    model::{AdminErrorKind, MutationActor, MutationContext, plugins::*},
    ports::plugins::PluginCredentialService,
};
use gateway_core::engine::middleware::service::Caller;
use gateway_plugin_sdk::call::host::AuthSaveRequest;
use std::sync::Arc;

pub(super) async fn save_all(
    service: Option<&Arc<dyn PluginCredentialService>>,
    accounts: Vec<AuthSaveRequest>,
    caller: Caller,
    invocation: Arc<crate::resources::InvocationCaller>,
    deadline: tokio::time::Instant,
) -> Result<usize, PluginCommandError> {
    let initiator = invocation.actor().clone();
    let graph = &invocation.graph;
    let cancellation = &invocation.cancellation;
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
            request_id: initiator.request_id.clone(),
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
        invocation
            .track(&cleanup)
            .map_err(|_| incomplete(saved, "账号提交调用已失效"))?;
        let result = tokio::select! { biased;
            () = cancellation.cancelled() => return Err(incomplete(saved, "账号提交已取消，进行中结果未确认")),
            result = tokio::time::timeout_at(deadline, service.save(scope.clone(), initiator.clone(), command, cleanup, cancellation.clone())) =>
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
