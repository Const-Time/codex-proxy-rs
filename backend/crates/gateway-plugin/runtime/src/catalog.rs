//! Decode a frozen catalog declaration; Core validates conflicts and target facts.
use crate::RpcSession;
use gateway_admin::model::AdminError;
use gateway_core::routing::{ContributedModelAlias, ProviderKind, PublicModelId, UpstreamModelId};
use gateway_plugin_sdk::{Capability, Stage, call::catalog::ModelCatalogRegistration};
use std::{collections::BTreeSet, time::Duration};
pub(crate) async fn register(
    session: &RpcSession,
    owner: &str,
) -> Result<Vec<ContributedModelAlias>, AdminError> {
    match session.capability_version(Capability::ModelCatalog) {
        None => return Ok(vec![]),
        Some(1) => {}
        Some(_) => return Err(AdminError::invalid("模型目录能力版本不支持")),
    }
    let reply = session
        .call(
            "model_catalog.register",
            session.context(Stage::Registration, Duration::from_secs(5)),
            serde_json::json!({}),
            vec![],
        )
        .await
        .map_err(|_| AdminError::invalid("插件模型目录注册失败"))?;
    if reply.result != serde_json::json!({}) || reply.payload.len() > 64 * 1024 {
        return Err(AdminError::invalid("插件模型目录信封无效或过大"));
    }
    let registration: ModelCatalogRegistration = serde_json::from_slice(&reply.payload)
        .map_err(|_| AdminError::invalid("插件模型目录声明无效"))?;
    if registration.models.len() > 256 {
        return Err(AdminError::invalid("单个插件最多贡献 256 个模型别名"));
    }
    let mut ids = BTreeSet::new();
    registration
        .models
        .into_iter()
        .map(|model| {
            if !matches!(model.provider.as_str(), "openai" | "xai") || model.id == model.model {
                return Err(AdminError::invalid(
                    "模型别名须直接指向内置 Provider 的其他模型",
                ));
            }
            let id = PublicModelId::new(model.id)
                .map_err(|_| AdminError::invalid("模型别名 ID 无效"))?;
            if !ids.insert(id.clone()) {
                return Err(AdminError::invalid("插件模型别名重复"));
            }
            Ok(ContributedModelAlias {
                owner: owner.into(),
                id,
                provider: ProviderKind::new(model.provider)
                    .map_err(|_| AdminError::invalid("模型别名 Provider 无效"))?,
                target: UpstreamModelId::new(model.model)
                    .map_err(|_| AdminError::invalid("模型别名目标无效"))?,
            })
        })
        .collect()
}
