//! Runtime settings 与管理员 API Key 用例。

use std::sync::Arc;

use async_trait::async_trait;
use gateway_core::policy::CodexClientVersion;
use gateway_core::runtime::SnapshotControl;
use rand_core::{OsRng, RngCore as _};

use crate::{
    model::{
        AdminError, MutationContext,
        settings::{
            AdminApiKey, AdminApiKeyMutation, RegeneratedAdminApiKey, ReplaceRuntimeSettings,
            RuntimeSettings,
        },
    },
    ports::store::SettingsStore,
};

use super::{map_store_error, publish_committed};

/// API 消费的 Runtime settings 管理服务。
#[async_trait]
pub trait SettingsService: Send + Sync {
    async fn refresh_pricing(&self) -> Result<(), AdminError>;
    async fn update_pricing_sync_policy(
        &self,
        context: &MutationContext,
        policy: crate::model::pricing::PricingSyncPolicy,
    ) -> Result<(), AdminError>;
    async fn preview_pricing_sync(
        &self,
    ) -> Result<crate::model::pricing::PricingSyncPreview, AdminError>;
    async fn sync_pricing(
        &self,
        context: &MutationContext,
        preview: crate::model::pricing::PricingSyncPreview,
    ) -> Result<(), AdminError>;
    async fn pricing(&self) -> Result<crate::model::pricing::PricingCatalog, AdminError>;
    async fn update_pricing(
        &self,
        context: &MutationContext,
        command: crate::model::pricing::UpdatePricing,
    ) -> Result<(), AdminError>;
    async fn load(&self) -> Result<RuntimeSettings, AdminError>;
    async fn replace(
        &self,
        context: &MutationContext,
        command: ReplaceRuntimeSettings,
    ) -> Result<RuntimeSettings, AdminError>;
    async fn admin_api_key_exists(&self) -> Result<bool, AdminError>;
    async fn regenerate_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> Result<RegeneratedAdminApiKey, AdminError>;
    async fn delete_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> Result<AdminApiKeyMutation, AdminError>;
}

pub(crate) struct DefaultSettingsService {
    providers: crate::ports::provider::ProviderAdminRegistry,
    pricing_source: Arc<dyn crate::ports::pricing::PricingSource>,
    store: Arc<dyn SettingsStore>,
    snapshot: Arc<dyn SnapshotControl>,
}

impl DefaultSettingsService {
    #[must_use]
    pub(crate) fn new(
        store: Arc<dyn SettingsStore>,
        snapshot: Arc<dyn SnapshotControl>,
        providers: crate::ports::provider::ProviderAdminRegistry,
        pricing_source: Arc<dyn crate::ports::pricing::PricingSource>,
    ) -> Self {
        Self {
            providers,
            pricing_source,
            store,
            snapshot,
        }
    }
}

#[async_trait]
impl SettingsService for DefaultSettingsService {
    async fn refresh_pricing(&self) -> Result<(), AdminError> {
        use crate::model::pricing::SyncPricing;
        let stored = self
            .store
            .load_pricing()
            .await
            .map_err(|error| map_store_error(error, "pricing sync policy"))?;
        if !stored.sync_policy.enabled {
            return Ok(());
        }
        let mut preview = self.pricing_source.fetch().await?;
        if !stored.sync_policy.update_existing {
            // 自动新增不能用社区价目替换已有的内置价目。
            let defaults = self.providers.pricing_catalog();
            for (provider, models) in &mut preview.prices {
                models.retain(|model, _| {
                    !defaults
                        .get(provider)
                        .is_some_and(|p| p.contains_key(model))
                });
            }
            preview.prices.retain(|_, models| !models.is_empty());
        }
        let context = MutationContext {
            actor: crate::model::MutationActor::System,
            request_id: format!("pricing-sync-{}", uuid::Uuid::now_v7()),
        };
        let revision = self
            .store
            .sync_pricing(
                SyncPricing {
                    preview,
                    automatic: Some(stored.sync_policy),
                },
                &context,
            )
            .await
            .map_err(|error| map_store_error(error, "automatic pricing sync"))?;
        if let Some(revision) = revision {
            publish_committed(self.snapshot.as_ref(), revision).await?;
        }
        Ok(())
    }

    async fn update_pricing_sync_policy(
        &self,
        context: &MutationContext,
        policy: crate::model::pricing::PricingSyncPolicy,
    ) -> Result<(), AdminError> {
        let revision = self
            .store
            .update_pricing_sync_policy(policy, context)
            .await
            .map_err(|error| map_store_error(error, "pricing sync policy"))?;
        publish_committed(self.snapshot.as_ref(), revision).await
    }

    async fn preview_pricing_sync(
        &self,
    ) -> Result<crate::model::pricing::PricingSyncPreview, AdminError> {
        self.pricing_source.fetch().await
    }

    async fn sync_pricing(
        &self,
        context: &MutationContext,
        preview: crate::model::pricing::PricingSyncPreview,
    ) -> Result<(), AdminError> {
        let current = self.pricing_source.fetch().await?;
        if current != preview {
            return Err(AdminError::invalid("远程价目已变化，请重新预览后确认"));
        }
        let revision = self
            .store
            .sync_pricing(
                crate::model::pricing::SyncPricing {
                    preview: current,
                    automatic: None,
                },
                context,
            )
            .await
            .map_err(|error| map_store_error(error, "model pricing sync"))?;
        if let Some(revision) = revision {
            publish_committed(self.snapshot.as_ref(), revision).await?;
        }
        Ok(())
    }

    async fn pricing(&self) -> Result<crate::model::pricing::PricingCatalog, AdminError> {
        let stored = self
            .store
            .load_pricing()
            .await
            .map_err(|error| map_store_error(error, "model pricing"))?;
        Ok(crate::model::pricing::PricingCatalog {
            defaults: self.providers.pricing_catalog(),
            overrides: stored.overrides,
            synced: stored.synced,
            synced_at: stored.synced_at,
            sync_policy: stored.sync_policy,
            source_hash: stored.source_hash,
        })
    }

    async fn update_pricing(
        &self,
        context: &MutationContext,
        command: crate::model::pricing::UpdatePricing,
    ) -> Result<(), AdminError> {
        use crate::model::pricing::PricingChange;
        let catalog = self.pricing().await?;
        if !catalog.defaults.contains_key(&command.provider)
            || command.models.is_empty()
            || command.models.len() > 500
            || command.models.iter().any(|model| {
                model.is_empty()
                    || model.len() > 128
                    || model.trim() != model
                    || model.chars().any(char::is_whitespace)
                    || model.chars().any(char::is_control)
            })
        {
            return Err(AdminError::invalid("Provider、模型 ID 或批量数量不合法"));
        }
        let defaults = gateway_core::metering::merge_pricing(catalog.defaults, &catalog.synced);
        let defaults = defaults.get(&command.provider);
        match &command.change {
            PricingChange::Replace(pricing) => {
                pricing.validate().map_err(AdminError::invalid)?;
                if command.provider == "xai"
                    && pricing
                        .bands
                        .keys()
                        .any(|band| matches!(band.as_str(), "image" | "flex" | "long_flex"))
                {
                    return Err(AdminError::invalid("xAI 不支持此价格档位"));
                }
                if !pricing.bands.contains_key("standard")
                    && command
                        .models
                        .iter()
                        .any(|model| !defaults.is_some_and(|p| p.contains_key(model)))
                {
                    return Err(AdminError::invalid("未知模型必须提供标准档价格"));
                }
            }
            PricingChange::Multiplier(bps, scope)
                if *bps > 1_000_000 || scope.validate().is_err() =>
            {
                return Err(AdminError::invalid(
                    "倍率须为 0 至 100 倍，且至少选择一个费用项",
                ));
            }
            PricingChange::Multiplier(_, _)
                if command.models.iter().any(|model| {
                    !defaults.is_some_and(|p| p.contains_key(model))
                        && !catalog
                            .overrides
                            .get(&command.provider)
                            .and_then(|p| p.get(model))
                            .is_some_and(|p| p.bands.contains_key("standard"))
                }) =>
            {
                return Err(AdminError::invalid("请先为未知模型配置标准档价格"));
            }
            _ => {}
        }
        let revision = self
            .store
            .update_pricing(command, context)
            .await
            .map_err(|error| map_store_error(error, "model pricing"))?;
        publish_committed(self.snapshot.as_ref(), revision).await
    }

    async fn load(&self) -> Result<RuntimeSettings, AdminError> {
        self.store
            .load_runtime_settings()
            .await
            .map_err(|error| map_store_error(error, "runtime settings"))
    }

    async fn replace(
        &self,
        context: &MutationContext,
        command: ReplaceRuntimeSettings,
    ) -> Result<RuntimeSettings, AdminError> {
        validate_settings(&command)?;
        let settings = self
            .store
            .replace_runtime_settings(command, context)
            .await
            .map_err(|error| map_store_error(error, "runtime settings"))?;
        publish_committed(self.snapshot.as_ref(), settings.config_revision).await?;
        Ok(settings)
    }

    async fn admin_api_key_exists(&self) -> Result<bool, AdminError> {
        self.store
            .admin_api_key_exists()
            .await
            .map_err(|error| map_store_error(error, "administrator API key"))
    }

    async fn regenerate_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> Result<RegeneratedAdminApiKey, AdminError> {
        let mut bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut bytes);
        let key = AdminApiKey::new(format!("admin-{}", hex::encode(bytes)));
        let mutation = self
            .store
            .replace_admin_api_key(key.clone(), context)
            .await
            .map_err(|error| map_store_error(error, "administrator API key"))?;
        publish_committed(self.snapshot.as_ref(), mutation.config_revision).await?;
        Ok(RegeneratedAdminApiKey { mutation, key })
    }

    async fn delete_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> Result<AdminApiKeyMutation, AdminError> {
        let mutation = self
            .store
            .delete_admin_api_key(context)
            .await
            .map_err(|error| map_store_error(error, "administrator API key"))?;
        publish_committed(self.snapshot.as_ref(), mutation.config_revision).await?;
        Ok(mutation)
    }
}

fn validate_settings(command: &ReplaceRuntimeSettings) -> Result<(), AdminError> {
    let valid = command
        .reset_credit_lead_minutes
        .is_none_or(|v| (1..=1440).contains(&v))
        && command
            .reset_credit_max_weekly_remaining_percent
            .flatten()
            .is_none_or(|v| v <= 100)
        && command.model_mappings.len() <= 512
        && i64::try_from(command.refresh_margin_seconds).is_ok()
        && command.refresh_margin_seconds > 0
        && command.refresh_concurrency > 0
        && command.max_concurrent_per_account > 0
        && command.usage_retention_days >= 31
        && command.ops_event_retention_days > 0
        && command.audit_retention_days > 0
        && valid_client_version(command.min_codex_desktop_version.as_deref())
        && valid_client_version(command.min_codex_cli_version.as_deref())
        && i64::try_from(command.request_interval_ms).is_ok();
    if valid {
        Ok(())
    } else {
        Err(AdminError::invalid("运行时设置不满足约束"))
    }
}

fn valid_client_version(value: Option<&str>) -> bool {
    value.is_none_or(|value| CodexClientVersion::parse(value).is_ok())
}
