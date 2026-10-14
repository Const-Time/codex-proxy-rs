//! Credential RPC uses native Provider/Admin preparation and one host-owned call scope.
use crate::{RpcReply, resources::InvocationCaller};
use gateway_admin::{
    model::{AdminError, AdminErrorKind, PageSize, Revision, plugins::*, provider_credentials::*},
    ports::plugins::PluginCredentialService,
};
use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    routing::ProviderKind,
};
use gateway_plugin_sdk::{ErrorCode, PluginFault, call::host::*};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) fn convert(request: AuthSaveRequest) -> Result<PluginCredentialSave, AdminError> {
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

pub(crate) fn is_method(method: &str) -> bool {
    matches!(
        method,
        "host.auth.list" | "host.auth.get_runtime" | "host.auth.get" | "host.auth.save"
    )
}
fn invalid() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "invalid native account request")
}
pub(crate) fn error(error: AdminError, saving: bool) -> PluginFault {
    PluginFault::new(
        match error.kind() {
            AdminErrorKind::Invalid => ErrorCode::InvalidInput,
            AdminErrorKind::Forbidden | AdminErrorKind::Unauthorized => ErrorCode::PermissionDenied,
            AdminErrorKind::NotFound => ErrorCode::Rejected,
            AdminErrorKind::Conflict => ErrorCode::Conflict,
            _ if saving => ErrorCode::Uncertain,
            _ => ErrorCode::Fault,
        },
        if saving {
            "native account preparation or commit failed; verify current account before retrying"
        } else {
            "native account credential read failed"
        },
    )
}
pub(crate) fn runtime(
    account: &gateway_admin::model::accounts::AccountRecord,
) -> AuthRuntimeAccount {
    AuthRuntimeAccount {
        account_id: account.id.clone(),
        provider_id: account.provider_kind.as_str().into(),
        credential_revision: account.credential_revision.get(),
        name: account.name.clone(),
        email: account.email.clone(),
        upstream_user_id: account.upstream_user_id.clone(),
        upstream_account_id: account.upstream_account_id.clone(),
        plan_type: account.plan_type.clone(),
        authentication_kind: account.authentication_kind.clone(),
        enabled: account.enabled,
        credential_state: account.credential_state.as_str().into(),
        has_refresh_token: account.has_refresh_token,
        access_token_expires_at_ms: account
            .access_token_expires_at
            .map(|t| t.timestamp_millis()),
        next_refresh_at_ms: account.next_refresh_at.map(|t| t.timestamp_millis()),
    }
}
pub(crate) fn export(result: ProviderExportCredentialInput) -> AuthCredential {
    let account = result.account;
    AuthCredential {
        account_id: account.id,
        provider_id: account.provider_kind.as_str().into(),
        credential_revision: account.credential_revision.get(),
        facts: CredentialFacts {
            name: account.name,
            authentication_kind: account.authentication_kind,
            material: result.provider_material.into_provider_data().into_inner(),
            email: account.email,
            upstream_user_id: account.upstream_user_id,
            upstream_account_id: account.upstream_account_id,
            plan_type: account.plan_type,
            has_refresh_token: account.has_refresh_token,
            access_token_expires_at_ms: account
                .access_token_expires_at
                .map(|t| t.timestamp_millis()),
            next_refresh_at_ms: account.next_refresh_at.map(|t| t.timestamp_millis()),
        },
    }
}
pub(crate) fn encode(value: impl serde::Serialize) -> Result<RpcReply, PluginFault> {
    let payload = serde_json::to_vec(&value).map_err(|_| invalid())?;
    if payload.len() > 1024 * 1024 {
        return Err(PluginFault::new(
            ErrorCode::Capacity,
            "native account response too large",
        ));
    }
    Ok(RpcReply {
        result: json!({}),
        payload,
    })
}
pub(crate) async fn call(
    service: Arc<dyn PluginCredentialService>,
    scope: PluginMutationScope,
    caller: Arc<InvocationCaller>,
    method: &str,
    params: Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if params != json!({}) || payload.len() > 512 * 1024 {
        return Err(invalid());
    }
    let cleanup = caller
        .graph
        .fork_http(scope.instance_id.clone())
        .map_err(|_| {
            PluginFault::new(ErrorCode::Capacity, "native account call graph exhausted")
        })?;
    let initiator = caller.actor().clone();
    let operation = async {
        match method {
            "host.auth.list" | "host.auth.get_runtime" => {
                let _cleanup = cleanup;
                let query = if method == "host.auth.list" {
                    let request: AuthListRequest =
                        serde_json::from_slice(&payload).map_err(|_| invalid())?;
                    PluginCredentialQuery {
                        provider: request
                            .provider_id
                            .map(ProviderKind::new)
                            .transpose()
                            .map_err(|_| invalid())?,
                        cursor: request
                            .cursor
                            .map(ProviderAccountId::new)
                            .transpose()
                            .map_err(|_| invalid())?,
                        account_id: None,
                        limit: PageSize::new(request.limit).map_err(|_| invalid())?,
                    }
                } else {
                    let request: AuthGetRequest =
                        serde_json::from_slice(&payload).map_err(|_| invalid())?;
                    PluginCredentialQuery {
                        provider: None,
                        cursor: None,
                        account_id: Some(
                            ProviderAccountId::new(request.account_id).map_err(|_| invalid())?,
                        ),
                        limit: PageSize::new(1).map_err(|_| invalid())?,
                    }
                };
                let result = service
                    .accounts(scope, initiator, query)
                    .await
                    .map_err(|e| error(e, false))?;
                if method == "host.auth.get_runtime" {
                    encode(runtime(result.accounts.first().ok_or_else(|| {
                        PluginFault::new(ErrorCode::Rejected, "account not found")
                    })?))
                } else {
                    encode(AuthListResult {
                        accounts: result.accounts.iter().map(runtime).collect(),
                        next_cursor: result.next_cursor.map(|id| id.as_str().to_owned()),
                    })
                }
            }
            "host.auth.get" => {
                let _cleanup = cleanup;
                let request: AuthGetRequest =
                    serde_json::from_slice(&payload).map_err(|_| invalid())?;
                let result = service
                    .credential(
                        scope,
                        initiator,
                        ProviderAccountId::new(request.account_id).map_err(|_| invalid())?,
                    )
                    .await
                    .map_err(|e| error(e, false))?;
                encode(export(result))
            }
            "host.auth.save" => {
                let request: AuthSaveRequest =
                    serde_json::from_slice(&payload).map_err(|_| invalid())?;
                let command = convert(request).map_err(|e| error(e, true))?;
                caller.track(&cleanup)?;
                let result = service
                    .save(
                        scope,
                        initiator,
                        command,
                        cleanup,
                        caller.cancellation.clone(),
                    )
                    .await
                    .map_err(|e| error(e, true))?;
                let revision = result.credential_revision.ok_or_else(||PluginFault::new(ErrorCode::Uncertain, "account committed but current revision is unconfirmed; read account before retrying"))?;
                encode(AuthSaveResult {
                    account_id: result.account_id.as_str().into(),
                    credential_revision: revision.get(),
                })
            }
            _ => Err(PluginFault::new(
                ErrorCode::Unsupported,
                "unsupported native account method",
            )),
        }
    };
    tokio::select! {biased;
        ()=caller.cancellation.cancelled()=>Err(PluginFault::new(ErrorCode::Uncertain,"native account wait cancelled; in-flight commit may still finish")),
        ()=caller.deadline.wait()=>Err(PluginFault::new(ErrorCode::Uncertain,"native account invocation deadline reached; in-flight commit may still finish")),
        result=operation=>result,
    }
}
