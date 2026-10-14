//! Existing Provider affinity is a read-only hint bounded by the parent's authority.
use super::{COORDINATION_TIMEOUT, DefaultExecutionService};
use crate::{
    account::ProviderAccountId,
    engine::nested::NestedExecutionContext,
    error::{GatewayError, GatewayErrorKind},
    identity::ProviderKind,
    provider_ports::{ProviderSessionAffinityKey, ProviderStoreErrorKind},
};
use futures::{FutureExt as _, pin_mut, select_biased};
use futures_timer::Delay;

impl DefaultExecutionService {
    pub(super) async fn lookup_affinity(
        &self,
        parent: &NestedExecutionContext,
        provider: &ProviderKind,
        key: &ProviderSessionAffinityKey,
    ) -> Result<Option<ProviderAccountId>, GatewayError> {
        parent.validate()?;
        let scope = parent.client().policy().account_scope();
        if !scope.provider_kinds().contains(provider) {
            return Err(GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "affinity provider is outside the frozen authorization scope",
            ));
        }
        let affinity = self.session_affinity.as_ref().ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::Unsupported,
                "provider session affinity is unavailable",
            )
        })?;
        let access = parent.client().bound_access().ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::Unauthorized,
                "parent model authorization is unavailable",
            )
        })?;
        let access_token = access.cancellation();
        let cancellation_token = parent.cancellation();
        let revoked = access_token.cancelled().fuse();
        let cancelled = cancellation_token.cancelled().fuse();
        let deadline = parent.deadline().wait().fuse();
        let timeout = Delay::new(COORDINATION_TIMEOUT).fuse();
        let load = affinity.load(provider, key).fuse();
        pin_mut!(revoked, cancelled, deadline, timeout, load);
        let result = select_biased! {
            () = revoked => return Err(GatewayError::new(GatewayErrorKind::Unauthorized, "parent model authorization was revoked")),
            () = cancelled => return Err(GatewayError::new(GatewayErrorKind::Cancelled, "parent model execution was cancelled")),
            () = deadline => return Err(GatewayError::new(GatewayErrorKind::Timeout, "parent model deadline elapsed")),
            () = timeout => return Err(GatewayError::new(GatewayErrorKind::ProviderInfrastructureUnavailable, "provider session affinity lookup timed out")),
            result = load => result,
        };
        parent.validate()?;
        let account = result.map_err(|error| match error.kind() {
            ProviderStoreErrorKind::Unavailable => GatewayError::new(
                GatewayErrorKind::ProviderInfrastructureUnavailable,
                "provider session affinity is temporarily unavailable",
            ),
            ProviderStoreErrorKind::InvalidData | ProviderStoreErrorKind::Conflict => {
                GatewayError::new(
                    GatewayErrorKind::Internal,
                    "provider session affinity is invalid",
                )
            }
        })?;
        Ok(account.filter(|account| {
            scope.allows(account)
                && scope
                    .directory()
                    .account(account)
                    .is_some_and(|facts| facts.provider_kind() == provider)
        }))
    }
}
