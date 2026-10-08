//! Immutable request overrides: each branch owns its derived values and source history.
use super::RuntimeSnapshot;
use crate::account::FastMode;
use crate::policy::{ClientApiKeyId, ClientPolicy, RateLimits};
use crate::settings::{InvalidSettings, SettingsValues};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingOverride<T = Value> {
    pub instance_id: String,
    pub order: u64,
    pub value: T,
}

/// Full immutable execution values; null timeout means no total execution deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSettings {
    pub runtime: SettingsValues,
    pub fast_mode: FastMode,
    pub client_limits: RateLimits,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionOverrides {
    client_key_id: String,
    input: ExecutionSettings,
    fast_mode: Option<SettingOverride<FastMode>>,
    client_limits: Option<SettingOverride<RateLimits>>,
    timeout_ms: Option<SettingOverride<Option<u64>>>,
}

#[derive(Clone, Debug)]
pub struct RequestSettings {
    execution: Option<Arc<ExecutionOverrides>>,
    baseline: Arc<RuntimeSnapshot>,
    effective: Arc<RuntimeSnapshot>,
    overrides: Arc<BTreeMap<String, SettingOverride>>,
    order: u64,
}

impl RequestSettings {
    #[must_use]
    pub fn new(snapshot: Arc<RuntimeSnapshot>) -> Self {
        Self {
            execution: None,
            baseline: snapshot.clone(),
            effective: snapshot,
            overrides: Arc::default(),
            order: 0,
        }
    }
    pub fn values(&self) -> &SettingsValues {
        self.effective.settings()
    }
    pub fn snapshot(&self) -> Arc<RuntimeSnapshot> {
        self.effective.clone()
    }
    pub(crate) fn has_baseline(&self, snapshot: &Arc<RuntimeSnapshot>) -> bool {
        Arc::ptr_eq(&self.baseline, snapshot)
    }
    pub fn inspect(&self) -> Value {
        serde_json::json!({
            "config_revision": self.baseline.revision().get(),
            "host": self.baseline.settings(),
            "overrides": self.overrides,
            "execution": self.execution,
        })
    }
    /// Replace complete values, retaining provenance only for fields changed by this layer.
    pub fn replace(
        &self,
        values: SettingsValues,
        instance_id: &str,
    ) -> Result<Self, InvalidSettings> {
        if self.values() == &values {
            return Ok(self.clone());
        }
        if instance_id.is_empty() {
            return Err(InvalidSettings);
        }
        let effective = self.effective.resolve_settings(values.clone())?;
        let current = serde_json::to_value(self.values()).map_err(|_| InvalidSettings)?;
        let next = serde_json::to_value(values).map_err(|_| InvalidSettings)?;
        let order = self.order.checked_add(1).ok_or(InvalidSettings)?;
        let mut overrides = self.overrides.as_ref().clone();
        for (name, value) in next.as_object().ok_or(InvalidSettings)? {
            if current.get(name) != Some(value) {
                overrides.insert(
                    name.clone(),
                    SettingOverride {
                        instance_id: instance_id.into(),
                        order,
                        value: value.clone(),
                    },
                );
            }
        }
        Ok(Self {
            execution: self.execution.clone(),
            baseline: self.baseline.clone(),
            effective,
            overrides: Arc::new(overrides),
            order,
        })
    }
    /// A new invocation may use a fresh host snapshot; only explicit fields survive the rebase.
    pub fn rebase(&self, snapshot: Arc<RuntimeSnapshot>) -> Result<Self, InvalidSettings> {
        if self.has_baseline(&snapshot) {
            return Ok(self.clone());
        }
        let effective = if self.overrides.is_empty() {
            snapshot.clone()
        } else {
            let mut values =
                serde_json::to_value(snapshot.settings()).map_err(|_| InvalidSettings)?;
            let object = values.as_object_mut().ok_or(InvalidSettings)?;
            for (name, change) in self.overrides.iter() {
                object.insert(name.clone(), change.value.clone());
            }
            snapshot
                .resolve_settings(serde_json::from_value(values).map_err(|_| InvalidSettings)?)?
        };
        Ok(Self {
            execution: self.execution.clone(),
            baseline: snapshot,
            effective,
            overrides: self.overrides.clone(),
            order: self.order,
        })
    }

    /// Establish a Key scope without inheriting another Key's execution defaults or overrides.
    #[must_use]
    pub fn with_execution(mut self, policy: &ClientPolicy, timeout_ms: Option<u64>) -> Self {
        let previous = self
            .execution
            .as_ref()
            .filter(|scope| scope.client_key_id == policy.key_id().as_str());
        self.execution = Some(Arc::new(ExecutionOverrides {
            client_key_id: policy.key_id().as_str().into(),
            input: ExecutionSettings {
                runtime: self.values().clone(),
                fast_mode: FastMode::Default,
                client_limits: policy.limits(),
                timeout_ms,
            },
            fast_mode: previous.and_then(|scope| scope.fast_mode.clone()),
            client_limits: previous.and_then(|scope| scope.client_limits.clone()),
            timeout_ms: previous.and_then(|scope| scope.timeout_ms.clone()),
        }));
        self
    }
    pub(crate) fn is_for(&self, key: &ClientApiKeyId) -> bool {
        self.execution
            .as_ref()
            .is_some_and(|scope| scope.client_key_id == key.as_str())
    }
    pub fn execution_values(&self) -> Option<ExecutionSettings> {
        let scope = self.execution.as_ref()?;
        Some(ExecutionSettings {
            runtime: self.values().clone(),
            fast_mode: scope
                .fast_mode
                .as_ref()
                .map_or(scope.input.fast_mode, |change| change.value),
            client_limits: scope
                .client_limits
                .as_ref()
                .map_or(scope.input.client_limits, |change| change.value),
            timeout_ms: scope
                .timeout_ms
                .as_ref()
                .map_or(scope.input.timeout_ms, |change| change.value),
        })
    }
    pub fn replace_execution(
        &self,
        values: &ExecutionSettings,
        instance_id: &str,
    ) -> Result<Self, InvalidSettings> {
        let previous = self.execution_values().ok_or(InvalidSettings)?;
        let mut updated = self.replace(values.runtime.clone(), instance_id)?;
        if values.fast_mode == previous.fast_mode
            && values.client_limits == previous.client_limits
            && values.timeout_ms == previous.timeout_ms
        {
            return Ok(updated);
        }
        if instance_id.is_empty() {
            return Err(InvalidSettings);
        }
        let order = if updated.order == self.order {
            self.order.checked_add(1).ok_or(InvalidSettings)?
        } else {
            updated.order
        };
        let mut scope = self.execution.as_deref().cloned().ok_or(InvalidSettings)?;
        if values.fast_mode != previous.fast_mode {
            scope.fast_mode = Some(SettingOverride {
                instance_id: instance_id.into(),
                order,
                value: values.fast_mode,
            });
        }
        if values.client_limits != previous.client_limits {
            scope.client_limits = Some(SettingOverride {
                instance_id: instance_id.into(),
                order,
                value: values.client_limits,
            });
        }
        if values.timeout_ms != previous.timeout_ms {
            scope.timeout_ms = Some(SettingOverride {
                instance_id: instance_id.into(),
                order,
                value: values.timeout_ms,
            });
        }
        updated.execution = Some(Arc::new(scope));
        updated.order = order;
        Ok(updated)
    }
}
