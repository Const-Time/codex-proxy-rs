//! Durable UI draft only. The host owns reset idempotency, permissions and accounting.
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{host::*, shared_budgets::*},
    client::HostClient,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Draft {
    command: Reset,
    receipt: Option<ResetResult>,
    archived: bool,
}
#[derive(Serialize)]
pub(crate) struct View {
    version: Option<u64>,
    draft: Option<Draft>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Prepare {
    target: Target,
    expected_version: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Execute {
    event_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Archive {
    event_id: String,
    acknowledge_unknown: bool,
}

pub(crate) async fn load(host: &HostClient) -> Result<View, PluginFault> {
    let result: StateGetResult = metadata(
        host,
        "host.state.get",
        &StateGetRequest {
            namespace: "reset-draft".into(),
            key: "current".into(),
        },
    )
    .await?;
    match result.record {
        None => Ok(View {
            version: None,
            draft: None,
        }),
        Some(record) if record.schema_version == 1 => Ok(View {
            version: Some(record.version),
            draft: Some(serde_json::from_value(record.value).map_err(|_| conflict())?),
        }),
        Some(_) => Err(conflict()),
    }
}
async fn save(host: &HostClient, version: Option<u64>, draft: Draft) -> Result<View, PluginFault> {
    let value = serde_json::to_value(&draft).map_err(|_| conflict())?;
    let result: StatePutResult = metadata(
        host,
        "host.state.put",
        &StatePutRequest {
            namespace: "reset-draft".into(),
            key: "current".into(),
            value,
            expected_version: version,
        },
    )
    .await?;
    Ok(View {
        version: Some(result.version),
        draft: Some(draft),
    })
}
pub(crate) async fn prepare(host: &HostClient, input: Prepare) -> Result<View, PluginFault> {
    let current = load(host).await?;
    if current.version != input.expected_version
        || current
            .draft
            .as_ref()
            .is_some_and(|d| d.receipt.is_none() && !d.archived)
    {
        return Err(conflict());
    }
    host.shared_budget(input.target.clone()).await?;
    let draft = Draft {
        command: Reset {
            event_id: uuid::Uuid::new_v4().to_string(),
            targets: vec![input.target],
        },
        receipt: None,
        archived: false,
    };
    save(host, current.version, draft).await
}
pub(crate) async fn execute(host: &HostClient, input: Execute) -> Result<View, PluginFault> {
    let current = load(host).await?;
    let mut draft = current.draft.ok_or_else(conflict)?;
    if draft.command.event_id != input.event_id || draft.archived {
        return Err(conflict());
    }
    if draft.receipt.is_none() {
        // A lost callback or process restart leaves this exact command available.
        // Concurrent callers use the same native idempotency key and targets.
        draft.receipt = Some(host.reset_shared_budgets(draft.command.clone()).await?);
        save(host, current.version, draft).await
    } else {
        Ok(View {
            version: current.version,
            draft: Some(draft),
        })
    }
}
pub(crate) async fn archive(host: &HostClient, input: Archive) -> Result<View, PluginFault> {
    let current = load(host).await?;
    let mut draft = current.draft.ok_or_else(conflict)?;
    if !input.acknowledge_unknown || draft.command.event_id != input.event_id {
        return Err(conflict());
    }
    // This only archives the UI draft; it does not cancel or roll back a host reset.
    draft.archived = true;
    save(host, current.version, draft).await
}
async fn metadata<I: Serialize, O: DeserializeOwned>(
    host: &HostClient,
    method: &str,
    input: &I,
) -> Result<O, PluginFault> {
    let response = host
        .call(
            method,
            serde_json::to_value(input).map_err(|_| conflict())?,
            vec![],
        )
        .await
        .map_err(|e| e.into_plugin_fault())?;
    if !response.payload.is_empty() {
        return Err(conflict());
    }
    serde_json::from_value(response.result).map_err(|_| conflict())
}
fn conflict() -> PluginFault {
    PluginFault::new(ErrorCode::Conflict, "Reset draft changed or is incomplete")
}
