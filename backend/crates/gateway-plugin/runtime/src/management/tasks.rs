//! Bounded, host-owned continuations of an explicitly authenticated management call.
//! The plugin never supplies an actor; each step gets a fresh callback scope.
use crate::{RpcSession, resources::InvocationCalls};
use gateway_admin::model::{AdminError, AdminErrorKind, MutationActor, plugins::*};
use gateway_plugin_sdk::{
    Stage,
    call::{management::*, middleware::MiddlewareHeader},
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const NEXT: &str = "x-cpr-background-next";
const SUPPORT: &str = "x-cpr-background-version";
const ACTIVE: &str = "x-cpr-background-active";
const MAX_STEPS: usize = 162;

fn background_authorized(request: &PluginHttpRequest) -> bool {
    matches!(request.actor.actor, MutationActor::AdminSession { .. })
        && request.invocation.as_ref().is_none_or(|invocation| {
            // HTTP placement also binds direct browser requests. Only actual
            // descendants borrow authority/lifetime that cannot be detached.
            let context = &invocation.context;
            context.parent_call_id.is_none()
                && context.model_parent.is_none()
                && context.call_scope.is_root()
                && context.validate().is_ok()
        })
}

pub(crate) struct Tasks {
    active: Mutex<Option<(String, MutationActor)>>,
    capacity: Arc<Semaphore>,
}
impl Tasks {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(None),
            capacity: Arc::new(Semaphore::new(1)),
        })
    }
    pub(crate) async fn drain(&self) {
        let _permit = self.capacity.acquire().await;
    }
}
struct Lease {
    tasks: Arc<Tasks>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Lease {
    fn drop(&mut self) {
        *self
            .tasks
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Next {
    id: String,
    path: String,
    body: String,
}

#[derive(Clone)]
pub(crate) struct Executor {
    pub(crate) session: Arc<RpcSession>,
    pub(crate) calls: Arc<InvocationCalls>,
    pub(crate) access: PluginAccess,
    pub(crate) registration: ManagementRegistration,
    pub(crate) tasks: Arc<Tasks>,
}
impl Executor {
    pub(crate) async fn handle(
        &self,
        request: PluginHttpRequest,
    ) -> Result<PluginHttpResponse, AdminError> {
        let eligible = request.method == "POST" && background_authorized(&request);
        let actor = request.actor.clone();
        let (response, next) = self.invoke(request).await?;
        if let Some(next) = next {
            if !eligible {
                return Err(AdminError::new(
                    AdminErrorKind::Forbidden,
                    "后台检测需要管理员会话直接发起",
                ));
            }
            let permit = self
                .tasks
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| AdminError::conflict("已有后台检测正在执行，请先暂停或结束"))?;
            *self
                .tasks
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((next.id.clone(), actor.actor.clone()));
            let lease = Lease {
                tasks: self.tasks.clone(),
                _permit: permit,
            };
            let executor = self.clone();
            tokio::spawn(async move {
                let _lease = lease;
                // Never retry an errored or uncertain invocation. The plugin owns durable
                // pending/results and manual recovery after host restart or revocation.
                for _ in 0..MAX_STEPS {
                    if executor.access.is_revoked() {
                        break;
                    }
                    let request = PluginHttpRequest {
                        // A newly accepted administrator-owned root, not a clone
                        // of the completed HTTP scope (including its cancellation,
                        // graph and scoped dispatchers). Model callbacks recheck
                        // personal Key authority using the retained host actor.
                        invocation: None,
                        actor: actor.clone(),
                        method: "POST".into(),
                        path: next.path.clone(),
                        query: String::new(),
                        content_type: Some("application/json".into()),
                        body: next.body.as_bytes().to_vec(),
                    };
                    match executor.invoke(request).await {
                        Ok((response, Some(following)))
                            if (200..300).contains(&response.status) && following == next => {}
                        _ => break,
                    }
                }
            });
        }
        Ok(response)
    }

    async fn invoke(
        &self,
        request: PluginHttpRequest,
    ) -> Result<(PluginHttpResponse, Option<Next>), AdminError> {
        if self.access.is_revoked() {
            return Err(AdminError::conflict("插件已失效"));
        }
        let route = self
            .registration
            .routes
            .iter()
            .find(|r| r.method == request.method && r.path == request.path)
            .ok_or_else(|| AdminError::not_found("插件路由未注册"))?;
        if request.body.len() > 1024 * 1024 || request.query.len() > 8192 {
            return Err(AdminError::invalid("插件请求过大"));
        }
        if request
            .content_type
            .as_ref()
            .is_some_and(|t| !route.request_content_types.contains(t))
            || (!request.body.is_empty() && request.content_type.is_none())
        {
            return Err(AdminError::invalid("插件路由未声明此请求类型"));
        }
        let eligible = background_authorized(&request);
        let mut headers = vec![];
        if eligible {
            headers.push(MiddlewareHeader {
                name: SUPPORT.into(),
                value: b"1".to_vec(),
            });
            if let Some((id, owner)) = &*self
                .tasks
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                && owner == &request.actor.actor
            {
                headers.push(MiddlewareHeader {
                    name: ACTIVE.into(),
                    value: id.as_bytes().to_vec(),
                });
            }
        }
        let params = serde_json::to_value(ManagementRequest {
            method: request.method,
            path: request.path,
            query: request.query,
            content_type: request.content_type,
            headers,
        })
        .map_err(|_| AdminError::invalid("插件请求无效"))?;
        let mut context = self.session.context(
            Stage::Management,
            self.session.budget(gateway_plugin_sdk::Stage::Management),
        );
        context.request_id = Some(request.actor.request_id.clone());
        context.request = request
            .invocation
            .as_ref()
            .and_then(|v| crate::request_context::wire(&v.context.call_scope));
        let cancellation = self.access.cancellation();
        let _caller = self
            .calls
            .enter(
                context.clone(),
                request.actor,
                request.invocation,
                cancellation.clone(),
                None,
            )
            .map_err(|_| AdminError::unavailable("管理调用容量已耗尽"))?;
        let reply = tokio::select! { biased;
            () = cancellation.cancelled() => return Err(AdminError::conflict("插件后台调用已取消")),
            result = self.session.call("management.handle", context, params, request.body) =>
                result.map_err(|_| AdminError::bad_gateway("插件调用失败，结果可能已生效，请查询后再重试"))?,
        };
        let response: ManagementResponse = self
            .session
            .decode_response(Stage::Management, reply.result)
            .map_err(|_| AdminError::bad_gateway("插件响应无效"))?;
        if !(200..=599).contains(&response.status)
            || !route
                .response_content_types
                .contains(&response.content_type)
            || reply.payload.len() > 1024 * 1024
            || response.headers.len() > 1
        {
            self.session.invalid_response(Stage::Management);
            return Err(AdminError::bad_gateway(
                "插件响应状态、类型、头部或大小不受支持",
            ));
        }
        let next = if let Some(header) = response.headers.first() {
            if header.name != NEXT
                || header.value.len() > 16 * 1024
                || !(200..300).contains(&response.status)
                || !eligible
            {
                return Err(AdminError::bad_gateway("插件后台续调声明无效"));
            }
            let next: Next = serde_json::from_slice(&header.value)
                .map_err(|_| AdminError::bad_gateway("插件后台续调格式无效"))?;
            if next.id.is_empty()
                || next.id.len() > 100
                || !next
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || next.body.len() > 8192
                || !self.registration.routes.iter().any(|r| {
                    r.method == "POST"
                        && r.path == next.path
                        && r.request_content_types
                            .iter()
                            .any(|t| t == "application/json")
                })
            {
                return Err(AdminError::bad_gateway("插件后台续调目标无效"));
            }
            Some(next)
        } else {
            None
        };
        Ok((
            PluginHttpResponse {
                status: response.status,
                content_type: response.content_type,
                body: reply.payload,
            },
            next,
        ))
    }
}
