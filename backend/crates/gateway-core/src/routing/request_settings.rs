//! Immutable request overrides: each branch owns its derived values and source history.
use super::RuntimeSnapshot;
use crate::settings::{InvalidSettings, SettingsValues};
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingOverride {
    pub instance_id: String,
    pub order: u64,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct RequestSettings {
    baseline: Arc<RuntimeSnapshot>,
    effective: Arc<RuntimeSnapshot>,
    overrides: Arc<BTreeMap<String, SettingOverride>>,
    order: u64,
}

impl RequestSettings {
    #[must_use]
    pub fn new(snapshot: Arc<RuntimeSnapshot>) -> Self {
        Self {
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
            baseline: snapshot,
            effective,
            overrides: self.overrides.clone(),
            order: self.order,
        })
    }
}
