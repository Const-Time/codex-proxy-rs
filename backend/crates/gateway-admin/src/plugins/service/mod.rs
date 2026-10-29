//! The service terminal retains domain validation and the authenticated caller.
mod authority;
mod factory;
mod local;
mod local_v1;
pub use authority::PluginServiceAuthority;
pub mod settings;
use crate::ports::plugins::PluginProcess;
use futures::future::BoxFuture;
use gateway_core::{
    engine::middleware::service as core,
    lifecycle::CancellationToken,
    middleware::{Middleware, compose},
};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// The entry boundary supplies its existing publication and call graph.
#[derive(Clone)]
pub struct Origin {
    pub registry: super::middleware::PluginMiddlewareRegistry,
    pub request_id: String,
    pub call_id: String,
    pub cancellation: CancellationToken,
    pub deadline_at: gateway_core::lifecycle::Deadline,
    pub call_scope: gateway_core::engine::nested::ExtensionCallScope,
    pub dispatcher: Option<Arc<dyn core::Dispatcher>>,
}

async fn invoke<I, O>(
    origin: Option<Origin>,
    operation: &'static str,
    input: I,
    terminal: impl FnOnce(I) -> BoxFuture<'static, Result<O, crate::model::AdminError>> + Send + 'static,
) -> Result<O, crate::model::AdminError>
where
    I: Serialize + DeserializeOwned + Send + 'static,
    O: Serialize + DeserializeOwned + Send + 'static,
{
    let Some(origin) = origin else {
        return terminal(input).await;
    };
    let mut context = core::Context {
        operation,
        request_id: origin.request_id,
        call_id: uuid::Uuid::now_v7().to_string(),
        parent_call_id: Some(origin.call_id),
        cancellation: origin.cancellation,
        deadline_at: origin.deadline_at,
        call_scope: origin.call_scope,
        dispatcher: None,
    };
    context.dispatcher = origin
        .dispatcher
        .map(|dispatcher| dispatcher.scoped(&context));
    origin
        .registry
        .freeze_service(&context)?
        .call(context, input, move |input| {
            Box::pin(async move { terminal(input).await.map_err(encode_error) })
        })
        .await
        .map_err(decode_error)
}

fn encode_error(error: crate::model::AdminError) -> core::Error {
    use crate::model::AdminErrorKind as K;
    let kind = match error.kind() {
        K::Invalid => "invalid",
        K::Unauthorized => "unauthorized",
        K::Forbidden => "forbidden",
        K::NotFound => "not_found",
        K::Conflict => "conflict",
        K::RateLimited => "rate_limited",
        K::BadGateway => "bad_gateway",
        K::UpstreamResultUnknown => "upstream_result_unknown",
        K::Unavailable => "unavailable",
        K::Internal => "internal",
    };
    core::Error {
        kind: kind.into(),
        message: error.message().into(),
        details: None,
    }
}
fn decode_error(error: core::Error) -> crate::model::AdminError {
    use crate::model::{AdminError, AdminErrorKind as K};
    if error.details.as_ref().is_some_and(|details| {
        details
            .get("send_state")
            .and_then(serde_json::Value::as_str)
            == Some("ambiguous")
    }) {
        return AdminError::upstream_result_unknown(error.message);
    }
    let kind = match error.kind.as_str() {
        "invalid" => K::Invalid,
        "unauthorized" => K::Unauthorized,
        "forbidden" => K::Forbidden,
        "not_found" => K::NotFound,
        "conflict" => K::Conflict,
        "rate_limited" => K::RateLimited,
        "bad_gateway" => K::BadGateway,
        "upstream_result_unknown" => K::UpstreamResultUnknown,
        "internal" => K::Internal,
        _ => K::Unavailable,
    };
    AdminError::new(kind, error.message)
}

#[derive(Clone)]
pub struct PluginServiceMiddlewarePlan {
    processes: Arc<[Arc<dyn PluginProcess>]>,
    cancellation: CancellationToken,
}
impl PluginServiceMiddlewarePlan {
    pub(super) fn new(
        processes: Vec<Arc<dyn PluginProcess>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            processes: processes.into(),
            cancellation,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.processes.is_empty()
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    /// Encoding is local to the plugin boundary. A rewritten input is decoded
    /// into the terminal's actual type before any native service method runs.
    pub async fn call<I, O>(
        &self,
        context: core::Context,
        input: I,
        terminal: impl FnOnce(I) -> BoxFuture<'static, Result<O, core::Error>> + Send + 'static,
    ) -> Result<O, core::Error>
    where
        I: Serialize + DeserializeOwned + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.is_empty() {
            return complete(
                CancellationToken::linked([context.cancellation, self.cancellation.clone()]),
                context.deadline_at,
                async move { terminal(input).await },
            )
            .await;
        }
        let input =
            serde_json::to_value(input).map_err(|_| core::Error::invalid("服务输入编码失败"))?;
        let next = compose(vec![], move |input| {
            Box::pin(async move {
                let input = serde_json::from_value(input)
                    .map_err(|_| core::Error::invalid("服务输入类型无效"))?;
                serde_json::to_value(terminal(input).await?)
                    .map_err(|_| core::Error::invalid("服务结果编码失败"))
            })
        });
        let result = self.run(context, input, next).await?;
        serde_json::from_value(result).map_err(|_| core::Error::invalid("服务结果类型无效"))
    }
    pub async fn run(
        &self,
        mut context: core::Context,
        input: core::Value,
        terminal: core::Next,
    ) -> Result<core::Value, core::Error> {
        context.cancellation =
            CancellationToken::linked([context.cancellation, self.cancellation.clone()]);
        let cancellation = context.cancellation.clone();
        let deadline = context.deadline_at;
        let layers: Vec<Middleware<core::Value, core::Value, core::Error>> = self
            .processes
            .iter()
            .map(|process| {
                let process = process.clone();
                let context = context.clone();
                Box::new(move |input, next| {
                    Box::pin(async move { process.service_middleware(context, input, next).await })
                        as BoxFuture<'static, _>
                }) as Middleware<_, _, _>
            })
            .collect();
        let next = compose(layers, move |input| terminal.run(input));
        complete(cancellation, deadline, async move { next.run(input).await }).await
    }
}
async fn complete<T>(
    cancellation: CancellationToken,
    deadline: gateway_core::lifecycle::Deadline,
    call: impl std::future::Future<Output = Result<T, core::Error>>,
) -> Result<T, core::Error> {
    tokio::select! { biased;
        ()=cancellation.cancelled()=>Err(core::Error::unavailable("服务调用已取消")),
        ()=deadline.wait()=>Err(core::Error::unavailable("服务调用已到期")),
        result=call=>{
            if cancellation.is_cancelled() { return Err(core::Error::unavailable("服务调用已取消")); }
            if deadline.is_elapsed() { return Err(core::Error::unavailable("服务调用已到期")); }
            result
        },
    }
}
