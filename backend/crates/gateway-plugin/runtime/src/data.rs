//! SDK fact projections. Management only; no credentials, forecasts, refresh or ledger mutations.
use crate::RpcReply;
use gateway_admin::{
    model::{AdminError, AdminErrorKind, plugins::PluginAccountQuery},
    ports::plugins::PluginDataService,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::data};
use serde_json::{Value, json};
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid plugin facts request")
}
fn error(e: AdminError) -> PluginFault {
    PluginFault::new(
        match e.kind() {
            AdminErrorKind::NotFound => ErrorCode::Rejected,
            AdminErrorKind::Invalid => ErrorCode::InvalidInput,
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
            serde_json::to_vec(&data::QuotaFacts {
                schema_version: 1,
                account_id: query.account_id,
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
            })
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
