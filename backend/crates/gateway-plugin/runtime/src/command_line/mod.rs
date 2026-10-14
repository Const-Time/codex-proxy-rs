//! SDK command registration and invocation stay with the actual prepared process.
mod credentials;
mod duration;
mod parameters;

use crate::{RpcSession, models::Models};
use gateway_admin::model::{AdminError, plugins::*};
use gateway_core::{engine::middleware::service as core, lifecycle::CancellationToken};
use gateway_plugin_sdk::{Capability, Stage, call::management::*};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub(crate) async fn register(session: &RpcSession) -> Result<Vec<CommandDescriptor>, AdminError> {
    match session.capability_version(Capability::CommandLine) {
        None => return Ok(vec![]),
        Some(1)
            if session.capability_supports_stage(Capability::CommandLine, Stage::CommandLine) => {}
        Some(_) => return Err(AdminError::invalid("命令行能力版本或阶段不支持")),
    }
    let reply = session
        .call(
            "command_line.register",
            session.context(Stage::Registration, Duration::from_secs(5)),
            json!({}),
            vec![],
        )
        .await
        .map_err(|_| AdminError::invalid("插件命令注册失败"))?;
    if reply.result != json!({}) || reply.payload.len() > 64 * 1024 {
        return Err(AdminError::invalid("插件命令注册信封无效或过大"));
    }
    let registration: CommandRegistration = serde_json::from_slice(&reply.payload)
        .map_err(|_| AdminError::invalid("插件命令注册数据无效"))?;
    if registration.commands.is_empty() || registration.commands.len() > 32 {
        return Err(AdminError::invalid("每个插件须注册 1 至 32 个命令"));
    }
    let mut names = BTreeSet::new();
    for descriptor in &registration.commands {
        parameters::validate(descriptor)?;
        if !names.insert(&descriptor.name) {
            return Err(AdminError::invalid("插件命令名重复"));
        }
    }
    Ok(registration.commands)
}

fn find<'a>(
    commands: &'a [CommandDescriptor],
    name: &str,
) -> Result<&'a CommandDescriptor, AdminError> {
    commands
        .iter()
        .find(|command| command.name == name)
        .ok_or_else(|| AdminError::not_found("插件命令不存在"))
}
pub(crate) fn help(
    commands: &[CommandDescriptor],
    instance: &str,
    name: Option<&str>,
) -> Result<Option<String>, AdminError> {
    if commands.is_empty() {
        return Ok(None);
    }
    if let Some(name) = name {
        let command = find(commands, name)?;
        return Ok(Some(format!(
            "用法: codex-proxy-rs plugin {instance} {name} [参数]\n{}",
            parameters::help(command)
        )));
    }
    let mut output = format!("用法: codex-proxy-rs plugin {instance} <命令> [参数]\n\n");
    for command in commands {
        output.push_str(&format!("  {}  {}\n", command.name, command.description));
    }
    Ok(Some(output))
}

struct ModelScope {
    models: Option<Arc<Models>>,
    context: gateway_plugin_sdk::CallContext,
}
impl Drop for ModelScope {
    fn drop(&mut self) {
        if let Some(models) = &self.models {
            models.finish(&self.context);
        }
    }
}

pub(crate) struct Executor<'a> {
    pub(crate) credentials:
        Option<&'a Arc<dyn gateway_admin::ports::plugins::PluginCredentialService>>,
    pub(crate) session: &'a RpcSession,
    pub(crate) commands: &'a [CommandDescriptor],
    pub(crate) services: &'a Arc<crate::services::Registry>,
    pub(crate) outbound: &'a Arc<crate::outbound::Registry>,
    pub(crate) models: Option<Arc<Models>>,
    pub(crate) calls: &'a Arc<crate::resources::InvocationCalls>,
}
impl Executor<'_> {
    pub(crate) async fn invoke(
        self,
        request: PluginCommandRequest,
        factory: Option<Arc<dyn core::Factory>>,
    ) -> Result<PluginCommandOutput, PluginCommandError> {
        let invocation =
            parameters::parse(find(self.commands, &request.name)?, &request.arguments)?;
        let payload = serde_json::to_vec(&invocation)
            .map_err(|_| AdminError::invalid("插件命令参数无法编码"))?;
        if request.actor.request_id.is_empty()
            || request.cancellation.is_cancelled()
            || !request.call_scope.is_root()
        {
            return Err(AdminError::conflict("插件命令调用已取消或关联无效").into());
        }
        let may_save_accounts = matches!(
            &request.actor.actor,
            gateway_admin::model::MutationActor::System
        );
        let deadline = tokio::time::Instant::now() + self.session.maximum_call_timeout();
        let mut context = self
            .session
            .context(Stage::CommandLine, self.session.maximum_call_timeout());
        context.request_id = Some(request.actor.request_id.clone());
        let caller = self.services.caller();
        let cancellation = CancellationToken::linked([request.cancellation, caller.cancellation()]);
        let graph = request.call_scope;
        // A CLI command is a host root. It has no model parent or HTTP principal;
        // all host callbacks share this RPC's actual graph and frozen publication.
        let service_context = core::Context {
            operation: "command_line.execute",
            request_id: request.actor.request_id.clone(),
            call_id: context.resource_scope_id.clone(),
            parent_call_id: None,
            cancellation: cancellation.clone(),
            deadline_at: Default::default(),
            call_scope: graph.clone(),
            dispatcher: None,
        };
        let _services = self
            .services
            .bind(&context, factory, service_context)
            .map_err(|_| AdminError::unavailable("命令服务作用域不可用"))?;
        self.outbound
            .bind(&context, cancellation.clone(), Some(graph.clone()))
            .map_err(|_| AdminError::unavailable("命令网络作用域不可用"))?;
        let _network = self.outbound.guard(&context);
        let _caller = self
            .calls
            .enter(
                context.clone(),
                request.actor.clone(),
                None,
                cancellation.clone(),
                Some(graph.clone()),
            )
            .map_err(|_| AdminError::unavailable("命令调用容量已耗尽"))?;
        let invocation = self
            .calls
            .caller(&context.resource_scope_id)
            .ok_or_else(|| AdminError::unavailable("命令调用关联已失效"))?;
        if let Some(models) = &self.models {
            models
                .bind(
                    &context,
                    None,
                    Some(request.actor.clone()),
                    cancellation.clone(),
                    None,
                    Some(graph.clone()),
                )
                .map_err(|_| AdminError::unavailable("命令模型作用域不可用"))?;
        }
        let _models = ModelScope {
            models: self.models,
            context: context.clone(),
        };
        let incomplete = |reason| PluginCommandError::Incomplete {
            saved_accounts: 0,
            reason,
        };
        let reply = tokio::select! { biased;
            () = cancellation.cancelled() => return Err(incomplete("调用已取消")),
            result = tokio::time::timeout_at(deadline, self.session.call("command_line.execute", context, json!({}), payload)) =>
                result.map_err(|_| incomplete("超过命令总期限"))?.map_err(|_| incomplete("插件响应未确认"))?,
        };
        if reply.result != json!({}) {
            self.session.invalid_response(Stage::CommandLine);
            return Err(incomplete("结果信封无效"));
        }
        let result: CommandResult = serde_json::from_slice(&reply.payload).map_err(|_| {
            self.session.invalid_response(Stage::CommandLine);
            incomplete("结果数据无效")
        })?;
        if result.stdout.len() > 64 * 1024
            || result.stderr.len() > 64 * 1024
            || result.accounts.len() > 32
        {
            self.session.invalid_response(Stage::CommandLine);
            return Err(incomplete("结果大小或账号范围无效"));
        }
        let saved_accounts = if result.exit_code == 0 && !result.accounts.is_empty() {
            if !may_save_accounts {
                return Err(incomplete("当前命令操作者无账号提交权限"));
            }
            credentials::save_all(
                self.credentials,
                result.accounts,
                caller,
                invocation,
                deadline,
            )
            .await?
        } else {
            0
        };
        if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            return Err(PluginCommandError::Incomplete {
                saved_accounts,
                reason: "调用已取消或超过命令总期限",
            });
        }
        Ok(PluginCommandOutput {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
            saved_accounts,
        })
    }
}
