//! Commands borrow the original active instance and revision; there is no CLI generation map.
use super::*;

impl PluginManager {
    async fn command_process(
        &self,
        id: &str,
        revision: u64,
    ) -> Result<Arc<dyn PluginProcess>, AdminError> {
        let process = self
            .active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .filter(|(active_revision, _)| *active_revision == revision)
            .map(|(_, process)| process.clone())
            .ok_or_else(|| AdminError::conflict("插件命令版本已失效"))?;
        let current = self.store.list().await?;
        if !current
            .iter()
            .any(|record| record.id == id && record.enabled && record.revision == revision)
            || process.status() != "ready"
            || process.access().is_revoked()
        {
            return Err(AdminError::conflict("插件命令配置或授权已变化"));
        }
        Ok(process)
    }
    pub async fn command_help(
        &self,
        id: &str,
        revision: u64,
        name: Option<&str>,
    ) -> Result<String, AdminError> {
        self.command_process(id, revision)
            .await?
            .command_help(name)?
            .ok_or_else(|| AdminError::not_found("插件实例没有可用命令"))
    }
    pub async fn execute_command(
        &self,
        id: &str,
        revision: u64,
        request: PluginCommandRequest,
    ) -> Result<PluginCommandOutput, PluginCommandError> {
        let process = self.command_process(id, revision).await?;
        let registry = self
            .middleware
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
            .with_services(self.services.clone());
        process.command(request, registry.service_factory()).await
    }
}
