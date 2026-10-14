//! Native account callbacks share one host-bound actor policy.
use super::map_store_error;
use crate::{
    model::{AdminError, MutationActor, MutationContext, plugins::PluginMutationScope},
    ports::store::AuthStore,
};

pub(crate) async fn authorize(
    auth: &dyn AuthStore,
    scope: &PluginMutationScope,
    initiator: &MutationContext,
) -> Result<(), AdminError> {
    let valid = matches!(&scope.actor.actor, MutationActor::Plugin { instance_id, revision } if instance_id == &scope.instance_id && revision.get() == scope.revision);
    if !valid || initiator.request_id.is_empty() || initiator.request_id != scope.actor.request_id {
        return Err(AdminError::conflict("插件凭据身份关联无效"));
    }
    let permitted = match &initiator.actor {
        MutationActor::AdminSession { admin_user_id } => auth
            .load_user(admin_user_id)
            .await
            .map_err(|e| map_store_error(e, "plugin credential administrator"))?
            .is_some_and(|user| user.enabled && user.role == crate::model::users::UserRole::Admin),
        MutationActor::System | MutationActor::AdminApiKey => true,
        MutationActor::Plugin {
            instance_id,
            revision,
        } => instance_id == &scope.instance_id && revision.get() == scope.revision,
    };
    if !permitted {
        return Err(AdminError::new(
            crate::model::AdminErrorKind::Forbidden,
            "当前操作者无账号凭据权限",
        ));
    }
    Ok(())
}
