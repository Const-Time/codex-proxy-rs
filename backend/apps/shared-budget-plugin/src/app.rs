use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{
        management::*,
        shared_budgets::{Target, UpdateGroupLimits},
    },
    client::{AuthorError, ComposedPlugin, PluginBuilder, TypedCall, TypedReply},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const JSON: &str = "application/json";

/// Assemble the installable plugin using only public SDK operations.
pub fn plugin() -> Result<ComposedPlugin, AuthorError> {
    let routes = [
        ("POST", "api/budget"),
        ("POST", "api/limits"),
        ("GET", "api/reset"),
        ("POST", "api/reset/prepare"),
        ("POST", "api/reset/execute"),
        ("POST", "api/reset/archive"),
    ]
    .into_iter()
    .map(|(method, path)| ManagementRoute {
        method: method.into(),
        path: path.into(),
        request_content_types: if method == "GET" {
            vec![]
        } else {
            vec![JSON.into()]
        },
        response_content_types: vec![JSON.into()],
    })
    .collect();
    PluginBuilder::from_json(crate::MANIFEST)?
        .management(
            ManagementRegistration {
                routes,
                resources: ["web/index.html", "web/app.js", "web/app.css"]
                    .into_iter()
                    .map(|path| ManagementResource {
                        path: path.into(),
                        public: false,
                    })
                    .collect(),
                pages: vec![ManagementPage {
                    id: "budgets".into(),
                    title: "共享额度管理".into(),
                    description: Some("查询共享用量、调整分组限额和显式重置".into()),
                    entry: "web/index.html".into(),
                    icon: None,
                }],
                callbacks: vec![],
            },
            handle,
        )?
        .build()
}

async fn handle(
    call: TypedCall<ManagementRequest>,
) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    match route(call).await {
        Ok(value) => reply(200, &value),
        Err(error) => {
            let status = match error.code {
                ErrorCode::InvalidInput => 400,
                ErrorCode::PermissionDenied => 403,
                ErrorCode::Conflict => 409,
                ErrorCode::Unsupported => 501,
                _ => 502,
            };
            // Keep error classification, never forward callback details or credentials.
            let message = match error.code {
                ErrorCode::InvalidInput => "输入无效，请检查用户、分组和额度。",
                ErrorCode::PermissionDenied => "当前操作者没有权限，或权限已发生变化。",
                ErrorCode::Conflict => "数据已变化或存在未确认的重置，请重新读取。",
                ErrorCode::Unsupported => "宿主未提供此本地共享额度接口。",
                _ => "操作结果未确认。限额修改请重新查询；重置请保留原编号重试。",
            };
            reply(
                status,
                &json!({"error":{"code":error.code,"message":message}}),
            )
        }
    }
}

async fn route(call: TypedCall<ManagementRequest>) -> Result<Value, PluginFault> {
    if !call.request.query.is_empty() || call.payload.len() > 8192 {
        return Err(invalid());
    }
    match (call.request.method.as_str(), call.request.path.as_str()) {
        ("GET", "api/reset") if call.payload.is_empty() => {
            encode(crate::reset::load(&call.host).await?)
        }
        ("POST", "api/budget") => encode(call.host.shared_budget(decode::<Target>(&call)?).await?),
        ("POST", "api/limits") => encode(
            call.host
                .update_shared_group_limits(decode::<UpdateGroupLimits>(&call)?)
                .await?,
        ),
        ("POST", "api/reset/prepare") => {
            encode(crate::reset::prepare(&call.host, decode(&call)?).await?)
        }
        ("POST", "api/reset/execute") => {
            encode(crate::reset::execute(&call.host, decode(&call)?).await?)
        }
        ("POST", "api/reset/archive") => {
            encode(crate::reset::archive(&call.host, decode(&call)?).await?)
        }
        _ => Err(invalid()),
    }
}
fn decode<T: DeserializeOwned>(call: &TypedCall<ManagementRequest>) -> Result<T, PluginFault> {
    if call
        .request
        .content_type
        .as_deref()
        .and_then(|v| v.split(';').next())
        .map(str::trim)
        != Some(JSON)
    {
        return Err(invalid());
    }
    serde_json::from_slice(&call.payload).map_err(|_| invalid())
}
fn encode(value: impl Serialize) -> Result<Value, PluginFault> {
    serde_json::to_value(value)
        .map_err(|_| PluginFault::new(ErrorCode::Fault, "Response encoding failed"))
}
fn reply(status: u16, value: &Value) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    let payload = serde_json::to_vec(value).map_err(|_| invalid())?;
    Ok(TypedReply::new(ManagementResponse {
        status,
        content_type: JSON.into(),
        headers: vec![],
    })
    .with_payload(payload))
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "Invalid management input")
}
