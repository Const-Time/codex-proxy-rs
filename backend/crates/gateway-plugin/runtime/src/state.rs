//! Candidate RPCs prepare values; the original Store commits data and revision together.
use crate::RpcSession;
use gateway_admin::{
    model::{AdminError, plugins::*},
    ports::plugins::{PluginProcess, PluginStore},
};
use gateway_plugin_sdk::{Manifest, Stage, call::host::*};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

fn invalid() -> AdminError {
    AdminError::invalid("插件状态迁移结果或目标声明无效")
}

pub(crate) async fn prepare(
    record: &PluginRecord,
    manifest: &Manifest,
    store: &dyn PluginStore,
    process: Option<&Arc<dyn PluginProcess>>,
) -> Result<Option<PluginStateCommit>, AdminError> {
    if !record.enabled {
        return Ok(None);
    }
    let expected = store
        .state_snapshot(&record.id, record.revision - 1)
        .await?;
    let mut namespaces: Vec<PluginStateSchema> = manifest
        .state
        .iter()
        .map(|ns| {
            serde_json::to_value(ns)
                .and_then(serde_json::from_value)
                .map_err(|_| invalid())
        })
        .collect::<Result<_, _>>()?;
    namespaces.sort_by(|a, b| a.namespace.cmp(&b.namespace));
    if expected
        .records
        .iter()
        .any(|row| !namespaces.iter().any(|ns| ns.namespace == row.namespace))
    {
        return Err(AdminError::conflict("仍有记录的插件状态命名空间不能移除"));
    }
    let source: BTreeMap<_, _> = expected
        .namespaces
        .iter()
        .map(|ns| (ns.namespace.as_str(), ns))
        .collect();
    if expected.records.iter().any(|row| {
        source
            .get(row.namespace.as_str())
            .is_none_or(|ns| ns.schema_version != row.schema_version)
    }) {
        return Err(AdminError::conflict("插件已存状态的 schema 与归属不一致"));
    }
    let mut records = Vec::new();
    for target in &namespaces {
        let old = source.get(target.namespace.as_str()).copied();
        let migrating = old.is_some_and(|old| old.schema_version != target.schema_version);
        if old.is_some_and(|old| {
            old.schema_version == target.schema_version && old.schema != target.schema
        }) || (migrating && !target.migrates_from.contains(&old.unwrap().schema_version))
        {
            return Err(AdminError::invalid(
                "插件状态 schema 变化必须声明版本及迁移来源",
            ));
        }
        let rows: Vec<_> = expected
            .records
            .iter()
            .filter(|row| row.namespace == target.namespace)
            .cloned()
            .collect();
        if migrating && !rows.is_empty() {
            let process = process.ok_or_else(invalid)?;
            let mut cursor = 0;
            while cursor < rows.len() {
                let start = cursor;
                let mut bytes = 0;
                while cursor < rows.len() && cursor - start < 64 {
                    let size = serde_json::to_vec(&rows[cursor].value)
                        .map_err(|_| invalid())?
                        .len()
                        + rows[cursor].key.len()
                        + 256;
                    if cursor > start && bytes + size > 512 * 1024 {
                        break;
                    }
                    bytes += size;
                    cursor += 1;
                }
                let batch = &rows[start..cursor];
                let changes = process
                    .migrate_state(PluginStateMigrationBatch {
                        namespace: target.namespace.clone(),
                        from_schema_version: old.unwrap().schema_version,
                        to_schema_version: target.schema_version,
                        records: batch.to_vec(),
                    })
                    .await?;
                if changes.len() != batch.len()
                    || changes
                        .iter()
                        .zip(batch)
                        .any(|(change, row)| change.key != row.key)
                {
                    return Err(invalid());
                }
                for (change, row) in changes.into_iter().zip(batch) {
                    let value = match change.action {
                        PluginStateMigrationAction::Keep => row.value.clone(),
                        PluginStateMigrationAction::Replace(value) => value,
                        PluginStateMigrationAction::Delete => continue,
                    };
                    records.push(PluginStateEntry {
                        value,
                        schema_version: target.schema_version,
                        ..row.clone()
                    });
                }
            }
        } else {
            records.extend(rows);
        }
        let validator = crate::engine::schema(&target.schema)?;
        let mut count = 0_u32;
        let mut bytes = 0_u64;
        for row in records
            .iter()
            .filter(|row| row.namespace == target.namespace)
        {
            let size = serde_json::to_vec(&row.value).map_err(|_| invalid())?.len() as u64;
            if size > u64::from(target.maximum_value_bytes) || !validator.is_valid(&row.value) {
                return Err(invalid());
            }
            count += 1;
            bytes += size;
        }
        if count > target.maximum_records || bytes > target.maximum_bytes {
            return Err(invalid());
        }
    }
    // Unchanged data contracts use the original revision transaction. Native
    // writes during ordinary configuration preparation must not become conflicts.
    if namespaces.len() == source.len()
        && namespaces.iter().all(|ns| {
            source
                .get(ns.namespace.as_str())
                .is_some_and(|old| *old == ns)
        })
    {
        return Ok(None);
    }
    records.sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
    Ok(Some(PluginStateCommit {
        expected,
        target: PluginStateSnapshot {
            namespaces,
            records,
        },
    }))
}

pub(crate) async fn invoke(
    session: &RpcSession,
    batch: PluginStateMigrationBatch,
) -> Result<Vec<PluginStateMigrationChange>, AdminError> {
    let request = StateMigrationRequest {
        namespace: batch.namespace,
        from_schema_version: batch.from_schema_version,
        to_schema_version: batch.to_schema_version,
        records: batch
            .records
            .into_iter()
            .map(|row| StateMigrationRecord {
                key: row.key,
                value: row.value,
                version: row.version,
            })
            .collect(),
    };
    let reply = session
        .call(
            "plugin.state.migrate",
            session.context(Stage::Configuration, Duration::from_secs(30)),
            json!({}),
            serde_json::to_vec(&request).map_err(|_| invalid())?,
        )
        .await
        .map_err(|_| AdminError::invalid("插件状态迁移调用失败"))?;
    if reply.result != json!({}) {
        return Err(invalid());
    }
    let result: StateMigrationResult =
        serde_json::from_slice(&reply.payload).map_err(|_| invalid())?;
    Ok(result
        .changes
        .into_iter()
        .map(|change| match change {
            StateMigrationChange::Keep { key } => PluginStateMigrationChange {
                key,
                action: PluginStateMigrationAction::Keep,
            },
            StateMigrationChange::Replace { key, value } => PluginStateMigrationChange {
                key,
                action: PluginStateMigrationAction::Replace(value),
            },
            StateMigrationChange::Delete { key } => PluginStateMigrationChange {
                key,
                action: PluginStateMigrationAction::Delete,
            },
        })
        .collect())
}
