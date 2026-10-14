//! SDK fact reads stay passive; explicit refresh follows native account authority and drain.
use crate::{RpcReply, resources::InvocationCaller};
use gateway_admin::{
    model::{
        AdminError, AdminErrorKind,
        plugins::{PluginAccountQuery, PluginMutationScope},
        provider_credentials::ProviderQuota,
    },
    ports::plugins::PluginDataService,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::data};
use serde_json::{Value, json};
use std::sync::Arc;
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid plugin facts request")
}
fn error(e: AdminError) -> PluginFault {
    PluginFault::new(
        match e.kind() {
            AdminErrorKind::NotFound => ErrorCode::Rejected,
            AdminErrorKind::Invalid => ErrorCode::InvalidInput,
            AdminErrorKind::Unauthorized | AdminErrorKind::Forbidden => ErrorCode::PermissionDenied,
            AdminErrorKind::Conflict => ErrorCode::Conflict,
            AdminErrorKind::RateLimited => ErrorCode::Capacity,
            AdminErrorKind::UpstreamResultUnknown => ErrorCode::Uncertain,
            AdminErrorKind::BadGateway => ErrorCode::Upstream,
            _ => ErrorCode::Fault,
        },
        "plugin facts unavailable",
    )
}
pub(crate) async fn call(
    service: &dyn PluginDataService,
    method: &str,
    params: Value,
    payload: &[u8],
) -> Result<RpcReply, PluginFault> {
    if params != json!({}) || payload.len() > 8192 {
        return Err(invalid());
    }
    let payload = match method {
        data::KEYS_GET => {
            let query: data::ClientKeyFactsQuery =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let value = service.key(&query.client_key_id).await.map_err(error)?;
            serde_json::to_vec(&data::ClientKeyFacts {
                schema_version: 1,
                client_key_id: value.id,
                enabled: value.enabled,
                group_ids: value.group_ids,
            })
        }
        data::ACCOUNTS_LIST => {
            let query: data::AccountFactsQuery =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let value = service
                .accounts(PluginAccountQuery {
                    provider: query.provider_id,
                    cursor: query.cursor,
                    limit: query.limit,
                })
                .await
                .map_err(error)?;
            serde_json::to_vec(&data::AccountFactsPage {
                schema_version: 1,
                accounts: value
                    .accounts
                    .into_iter()
                    .map(|v| data::AccountFacts {
                        account_id: v.id,
                        provider_id: v.provider,
                        name: v.name,
                        email: v.email,
                        group_ids: v.group_ids,
                        enabled: v.enabled,
                        updated_at_ms: v.updated_at.timestamp_millis(),
                    })
                    .collect(),
                next_cursor: value.next_cursor,
            })
        }
        data::QUOTA_GET => {
            let query: data::QuotaFactsQuery =
                serde_json::from_slice(payload).map_err(|_| invalid())?;
            let value = service.quota(&query.account_id).await.map_err(error)?;
            serde_json::to_vec(&quota_facts(query.account_id, value))
        }
        _ => {
            return Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unsupported plugin facts method",
            ));
        }
    }
    .map_err(|_| invalid())?;
    if payload.len() > 1024 * 1024 {
        return Err(PluginFault::new(
            ErrorCode::Capacity,
            "plugin facts response too large",
        ));
    }
    Ok(RpcReply {
        result: json!({}),
        payload,
    })
}

fn quota_facts(account_id: String, value: ProviderQuota) -> data::QuotaFacts {
    data::QuotaFacts {
        schema_version: 1,
        account_id,
        observed_at_ms: value.observed_at.map(|t| t.timestamp_millis()),
        windows: value
            .windows
            .into_iter()
            .map(|v| data::QuotaWindowFacts {
                key: v.key,
                window_seconds: v.window_seconds,
                used_percent: v.used_percent,
                reset_at_ms: v.reset_at.map(|t| t.timestamp_millis()),
            })
            .collect(),
    }
}

pub(crate) async fn refresh(
    service: Arc<dyn PluginDataService>,
    scope: PluginMutationScope,
    caller: Arc<InvocationCaller>,
    params: Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if params != json!({}) || payload.len() > 8192 {
        return Err(invalid());
    }
    let query: data::QuotaFactsQuery = serde_json::from_slice(&payload).map_err(|_| invalid())?;
    let account = gateway_core::account::ProviderAccountId::new(query.account_id.clone())
        .map_err(|_| invalid())?;
    let cleanup = caller
        .graph
        .fork_http(scope.instance_id.clone())
        .map_err(|_| PluginFault::new(ErrorCode::Capacity, "quota refresh call graph exhausted"))?;
    caller.track(&cleanup)?;
    let operation = service.refresh_quota(
        scope,
        caller.actor().clone(),
        account,
        cleanup,
        caller.cancellation.clone(),
    );
    let quota = tokio::select! { biased;
        ()=caller.cancellation.cancelled()=>return Err(PluginFault::new(ErrorCode::Uncertain, "quota refresh wait cancelled; query current observations before retrying")),
        ()=caller.deadline.wait()=>return Err(PluginFault::new(ErrorCode::Uncertain, "quota refresh deadline reached; query current observations before retrying")),
        result=operation=>result.map_err(error)?,
    };
    crate::credentials::encode(quota_facts(query.account_id, quota))
}
