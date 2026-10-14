//! Native commit and adoption keep the original child permit through cancellation.
use super::network::ManagedCall;
use crate::{
    RpcReply,
    credentials::{convert, encode, error},
    resources::InvocationCaller,
};
use gateway_admin::{
    model::{MutationActor, plugins::*},
    ports::plugins::PluginCredentialService,
};
use gateway_core::account::CredentialRevision;
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::host::{AuthSaveRequest, AuthSaveResult},
};
use serde_json::{Value, json};
use std::sync::Arc;

struct PendingChange {
    call: Arc<ManagedCall>,
    confirmed: bool,
}
impl Drop for PendingChange {
    fn drop(&mut self) {
        if !self.confirmed {
            self.call.cancellation.cancel();
        }
    }
}
fn fault(code: ErrorCode, message: &'static str) -> PluginFault {
    PluginFault::new(code, message)
}

pub(super) async fn save(
    service: Arc<dyn PluginCredentialService>,
    caller: Arc<InvocationCaller>,
    call: Arc<ManagedCall>,
    params: Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if params != json!({}) || payload.len() > 512 * 1024 {
        return Err(fault(
            ErrorCode::InvalidInput,
            "invalid bound credential replacement",
        ));
    }
    let request: AuthSaveRequest = serde_json::from_slice(&payload).map_err(|_| {
        fault(
            ErrorCode::InvalidInput,
            "invalid bound credential replacement",
        )
    })?;
    let AuthSaveRequest::Replace {
        account_id,
        credential_revision,
        ..
    } = &request
    else {
        return Err(fault(
            ErrorCode::PermissionDenied,
            "upstream replacement cannot create an account",
        ));
    };
    if account_id != call.account.account_id().as_str() {
        return Err(fault(
            ErrorCode::PermissionDenied,
            "account is outside the selected Provider lease",
        ));
    }
    let expected = CredentialRevision::new(*credential_revision)
        .map_err(|_| fault(ErrorCode::InvalidInput, "invalid credential revision"))?;
    if call.account.credential_updates().is_none() {
        return Err(fault(
            ErrorCode::Unsupported,
            "selected Provider cannot adopt native credentials",
        ));
    }
    let command = convert(request).map_err(|e| error(e, true))?;
    let MutationActor::Plugin {
        instance_id,
        revision,
    } = &caller.actor().actor
    else {
        return Err(fault(
            ErrorCode::PermissionDenied,
            "native upstream actor is unavailable",
        ));
    };
    let scope = PluginMutationScope {
        instance_id: instance_id.clone(),
        revision: revision.get(),
        actor: caller.actor().clone(),
    };
    let cleanup = caller
        .graph
        .fork_http(scope.instance_id.clone())
        .map_err(|_| fault(ErrorCode::Capacity, "native account call graph exhausted"))?;
    caller.track(&cleanup)?;
    let task_caller = caller.clone();
    let task = tokio::spawn(async move {
        // This clone is the same native child permit, retained through adoption.
        let _cleanup = cleanup.clone();
        let _epoch = tokio::select! { biased;
            () = task_caller.cancellation.cancelled() => return Err(fault(ErrorCode::Cancelled, "native account invocation ended")),
            () = task_caller.deadline.wait() => return Err(fault(ErrorCode::Cancelled, "native account invocation deadline reached")),
            guard = call.credential_gate.write() => guard,
        };
        if call.account.credential_revision() != expected {
            return Err(fault(
                ErrorCode::Conflict,
                "selected credential epoch has changed",
            ));
        }
        let mut change = PendingChange {
            call: call.clone(),
            confirmed: false,
        };
        call.reset_credential_transport().await;
        let result = service
            .save(
                scope,
                task_caller.actor().clone(),
                command,
                cleanup,
                task_caller.cancellation.clone(),
            )
            .await
            .map_err(|e| error(e, true))?;
        let committed = result.credential_revision.ok_or_else(|| {
            fault(
                ErrorCode::Uncertain,
                "account committed but revision is unconfirmed",
            )
        })?;
        if &result.account_id != call.account.account_id() || committed.get() <= expected.get() {
            return Err(fault(
                ErrorCode::Uncertain,
                "native credential receipt does not match the selected account",
            ));
        }
        let committed = CredentialRevision::new(committed.get())
            .map_err(|_| fault(ErrorCode::Uncertain, "native credential receipt is invalid"))?;
        call.account
            .credential_updates()
            .ok_or_else(|| {
                fault(
                    ErrorCode::Unsupported,
                    "native adoption owner is unavailable",
                )
            })?
            .adopt(expected, committed)
            .await
            .map_err(|_| {
                fault(
                    ErrorCode::Uncertain,
                    "committed credentials could not be adopted; invocation ended",
                )
            })?;
        call.adopt_transport_epoch();
        change.confirmed = true;
        encode(AuthSaveResult {
            account_id: result.account_id.as_str().into(),
            credential_revision: committed.get(),
        })
    });
    tokio::select! { biased;
        () = caller.cancellation.cancelled() => Err(fault(ErrorCode::Uncertain, "native account wait cancelled; commit and adoption may still finish")),
        () = caller.deadline.wait() => Err(fault(ErrorCode::Uncertain, "native account wait deadline reached; commit and adoption may still finish")),
        result = task => result.map_err(|_| fault(ErrorCode::Uncertain, "native account task ended; verify current account before retrying"))?,
    }
}
