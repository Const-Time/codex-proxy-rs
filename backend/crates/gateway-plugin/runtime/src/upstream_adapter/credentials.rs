//! Credential reads are confined to the Provider's actual selected lease and epoch.
use crate::{
    RpcReply,
    credentials::{encode, error, export, runtime},
};
use gateway_admin::{
    model::{MutationActor, MutationContext, PageSize, plugins::*},
    ports::plugins::PluginCredentialService,
};
use gateway_core::{
    account::ProviderAccountId, engine::upstream_adapter::UpstreamAccountConnection,
    routing::ProviderKind,
};
use gateway_plugin_sdk::{CallContext, ErrorCode, PluginFault, call::host::*};
use serde_json::{Value, json};
use std::{num::NonZeroU64, sync::Arc};

fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid bound credential request")
}
fn denied() -> PluginFault {
    PluginFault::new(
        ErrorCode::PermissionDenied,
        "account is outside the selected Provider lease",
    )
}
fn check(
    record: &gateway_admin::model::accounts::AccountRecord,
    selected: &dyn UpstreamAccountConnection,
) -> Result<(), PluginFault> {
    if record.id != selected.account_id().as_str() || !record.enabled {
        return Err(denied());
    }
    if record.credential_revision.get() != selected.credential_revision().get()
        || record.authentication_kind != selected.authentication_kind()
    {
        return Err(PluginFault::new(
            ErrorCode::Conflict,
            "selected credential epoch has changed",
        ));
    }
    Ok(())
}

pub(super) async fn read(
    service: Arc<dyn PluginCredentialService>,
    context: CallContext,
    selected: Arc<dyn UpstreamAccountConnection>,
    method: &str,
    params: Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if method == "host.auth.save" {
        return Err(PluginFault::new(
            ErrorCode::Unsupported,
            "bound credential replacement is not enabled",
        ));
    }
    if params != json!({}) || payload.len() > 512 * 1024 {
        return Err(invalid());
    }
    // The caller supplies the exact context retrieved from the live managed-call map.
    if context.account_id.as_deref() != Some(selected.account_id().as_str())
        || context.credential_revision != Some(selected.credential_revision().get())
    {
        return Err(denied());
    }
    let actor = MutationContext {
        actor: MutationActor::Plugin {
            instance_id: context.instance_id.clone(),
            revision: NonZeroU64::new(context.generation).ok_or_else(invalid)?,
        },
        request_id: context.request_id.ok_or_else(denied)?,
    };
    let scope = PluginMutationScope {
        instance_id: context.instance_id,
        revision: context.generation,
        actor: actor.clone(),
    };
    let account = selected.account_id().clone();
    let (provider, cursor) = if method == "host.auth.list" {
        let request: AuthListRequest = serde_json::from_slice(&payload).map_err(|_| invalid())?;
        PageSize::new(request.limit).map_err(|_| invalid())?;
        (
            request
                .provider_id
                .map(ProviderKind::new)
                .transpose()
                .map_err(|_| invalid())?,
            request
                .cursor
                .map(ProviderAccountId::new)
                .transpose()
                .map_err(|_| invalid())?,
        )
    } else {
        let request: AuthGetRequest = serde_json::from_slice(&payload).map_err(|_| invalid())?;
        let requested = ProviderAccountId::new(request.account_id).map_err(|_| invalid())?;
        if requested != account {
            return Err(denied());
        }
        (None, None)
    };
    if method == "host.auth.get" {
        let result = service
            .credential(scope, actor, account)
            .await
            .map_err(|e| error(e, false))?;
        check(&result.account, selected.as_ref())?;
        return encode(export(result));
    }
    let result = service
        .accounts(
            scope,
            actor,
            PluginCredentialQuery {
                provider: None,
                cursor: None,
                account_id: Some(account.clone()),
                limit: PageSize::new(1).map_err(|_| invalid())?,
            },
        )
        .await
        .map_err(|e| error(e, false))?;
    let record = result
        .accounts
        .first()
        .ok_or_else(|| PluginFault::new(ErrorCode::Rejected, "selected account not found"))?;
    check(record, selected.as_ref())?;
    if method == "host.auth.get_runtime" {
        return encode(runtime(record));
    }
    if method != "host.auth.list" {
        return Err(PluginFault::new(
            ErrorCode::Unsupported,
            "unsupported bound credential method",
        ));
    }
    let matches = provider
        .as_ref()
        .is_none_or(|provider| provider == &record.provider_kind)
        && cursor.as_ref().is_none_or(|cursor| cursor < &account);
    encode(AuthListResult {
        accounts: if matches {
            vec![runtime(record)]
        } else {
            vec![]
        },
        next_cursor: None,
    })
}
