//! 账号身份、凭据校验与带租约的授权轮换

use super::oauth::{GoogleOAuth, Material, VerifiedGrant, error, valid_token};
use crate::transport::unsent;
use chrono::Utc;
use gateway_admin::{
    model::{
        Revision,
        provider_credentials::{
            CredentialCommitGuard, PreparedCredentialCreate, PreparedCredentialRotation,
            PreparedCredentialRotationFacts, ProviderDocument,
        },
    },
    ports::provider::{ProviderAdminError, ProviderAdminErrorKind as Kind},
};
use gateway_core::{
    account::{
        AccountErrorReason, AccountStateChange, CredentialState, LoadedCredential,
        OpaqueProviderData, OutboundProxy, ProviderAccount, ProviderAccountId,
        ProviderAccountStore,
    },
    error::{ProviderError, ProviderErrorKind, StoreErrorKind},
    provider_ports::{
        ProviderCredentialStatePort, ProviderLeaseAcquisition, ProviderLeaseGuard,
        ProviderLeasePort, ProviderLeaseRequest, ProviderRefreshCapacityRequest,
        ProviderRefreshLeaseRequest, ProviderRuntimePolicyPort,
    },
    routing::ProviderKind,
};
use serde_json::Value;
use std::{sync::Arc, time::SystemTime};
use uuid::Uuid;

pub(crate) struct CredentialService {
    pub(super) kind: ProviderKind,
    pub(super) accounts: Arc<dyn ProviderAccountStore>,
    pub(super) leases: Arc<dyn ProviderLeasePort>,
    pub(super) runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
    pub(super) credential_state: Arc<dyn ProviderCredentialStatePort>,
    pub(super) oauth: Arc<GoogleOAuth>,
}
impl CredentialService {
    pub(crate) fn new(
        kind: ProviderKind,
        ports: &gateway_core::provider_ports::ProviderStorePorts,
        oauth: Arc<GoogleOAuth>,
    ) -> Self {
        Self {
            kind,
            accounts: ports.accounts(),
            leases: ports.leases(),
            runtime_policy: ports.runtime_policy(),
            credential_state: ports.credential_state(),
            oauth,
        }
    }
    pub(crate) fn material(
        &self,
        current: &LoadedCredential,
        check_expiry: bool,
    ) -> Result<Material, ProviderError> {
        let material: Material = serde_json::from_value(serde_json::Value::Object(
            current.credential.expose_to_provider().clone(),
        ))
        .map_err(|_| unsent(ProviderErrorKind::ProviderInfrastructureUnavailable))?;
        if material.registration != self.oauth.registration()
            || current.account.upstream_user_id() != Some(material.subject.as_str())
            || !valid_token(&material.access_token)
            || check_expiry
                && current
                    .account
                    .access_token_expires_at()
                    .is_some_and(|t| t <= SystemTime::now())
        {
            return Err(unsent(ProviderErrorKind::Unauthorized));
        }
        Ok(material)
    }
}
pub(super) struct RefreshGuards {
    _account: Box<dyn ProviderLeaseGuard>,
    _capacity: Box<dyn ProviderLeaseGuard>,
}

impl CredentialCommitGuard for RefreshGuards {
    fn finish(self: Box<Self>) {}
}

fn document(material: Material) -> Result<ProviderDocument, ProviderAdminError> {
    match serde_json::to_value(material) {
        Ok(Value::Object(map)) => Ok(ProviderDocument::new(OpaqueProviderData::new(map))),
        _ => Err(error(Kind::Internal)),
    }
}

impl CredentialService {
    pub(crate) async fn current(
        &self,
        id: &ProviderAccountId,
    ) -> Result<LoadedCredential, ProviderAdminError> {
        let loaded = self
            .accounts
            .load_current_credential(id)
            .await
            .map_err(|e| {
                error(match e.kind() {
                    StoreErrorKind::InvalidData | StoreErrorKind::InvalidState => Kind::NotFound,
                    StoreErrorKind::Unavailable => Kind::Unavailable,
                    StoreErrorKind::Conflict => Kind::Conflict,
                    _ => Kind::Internal,
                })
            })?;
        if loaded.account.provider() != &self.kind
            || loaded.account.authentication_kind() != "oauth"
        {
            return Err(error(Kind::NotFound));
        }
        Ok(loaded)
    }

    pub(super) async fn lock_rotation(
        &self,
        account: &ProviderAccount,
    ) -> Result<RefreshGuards, ProviderAdminError> {
        let policy = self
            .runtime_policy
            .load_refresh_policy()
            .await
            .map_err(|_| error(Kind::Unavailable))?;
        let leases = self.leases.clone();
        let acquire = |request| {
            let leases = leases.clone();
            async move {
                match leases
                    .try_acquire(request)
                    .await
                    .map_err(|_| error(Kind::Unavailable))?
                {
                    ProviderLeaseAcquisition::Acquired(guard) => Ok(guard),
                    ProviderLeaseAcquisition::Busy { .. } => Err(error(Kind::Conflict)),
                }
            }
        };
        let capacity = acquire(ProviderLeaseRequest::RefreshCapacity(
            ProviderRefreshCapacityRequest::new(policy.concurrency()),
        ))
        .await?;
        let guard = acquire(ProviderLeaseRequest::Refresh(
            ProviderRefreshLeaseRequest::new(account.id().clone(), account.revision()),
        ))
        .await?;
        // 租约可在初次读取之后才可用，发送会轮换 RT 的请求之前再次核实 revision
        if self
            .accounts
            .get_account(account.id())
            .await
            .map_err(|_| error(Kind::Unavailable))?
            .is_none_or(|a| {
                a.revision() != account.revision()
                    || a.outbound_proxy().map(OutboundProxy::expose_url)
                        != account.outbound_proxy().map(OutboundProxy::expose_url)
            })
        {
            return Err(error(Kind::Conflict));
        }
        Ok(RefreshGuards {
            _account: guard,
            _capacity: capacity,
        })
    }

    pub(super) fn prepare_create(
        &self,
        name: String,
        grant: VerifiedGrant,
        outbound_proxy: Option<OutboundProxy>,
    ) -> Result<PreparedCredentialCreate, ProviderAdminError> {
        let account_id = ProviderAccountId::new(format!("acct_{}", Uuid::now_v7().simple()))
            .map_err(|_| error(Kind::Internal))?;
        let name = if name.trim().is_empty() {
            "Antigravity account".to_owned()
        } else {
            name
        };
        Ok(PreparedCredentialCreate {
            account_id,
            provider_kind: self.kind.clone(),
            name,
            email: grant.email,
            upstream_user_id: Some(grant.material.subject.clone()),
            upstream_account_id: None,
            plan_type: None,
            authentication_kind: "oauth".into(),
            provider_material: document(grant.material)?,
            has_refresh_token: true,
            access_token_expires_at: Some(grant.expires_at),
            next_refresh_at: None,
            enabled: true,
            credential_state: CredentialState::Ready,
            credential_observed_at: Utc::now(),
            model_access: None,
            outbound_proxy,
        })
    }

    pub(crate) async fn refresh_current(
        &self,
        current: &LoadedCredential,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        self.oauth.ensure_configured()?;
        let guards = self.lock_rotation(&current.account).await?;
        let previous: Material = serde_json::from_value(Value::Object(
            current.credential.expose_to_provider().clone(),
        ))
        .map_err(|_| error(Kind::Internal))?;
        if current.account.upstream_user_id() != Some(previous.subject.as_str()) {
            return Err(error(Kind::Conflict));
        }
        let grant = match self
            .oauth
            .refresh(&previous, current.account.outbound_proxy())
            .await
        {
            Err(failure) if failure.kind() == Kind::CredentialRefreshRequired => {
                self.accounts
                    .apply_state_change(AccountStateChange {
                        account_id: current.account.id().clone(),
                        expected_revision: current.account.revision(),
                        credential_state: CredentialState::Invalid,
                        observed_at: std::time::SystemTime::now(),
                        error_reason: Some(AccountErrorReason::CredentialInvalid),
                        message: Some("凭据需要重新授权".into()),
                    })
                    .await
                    .map_err(|_| error(Kind::Unavailable))?;
                return Err(failure);
            }
            Err(failure) => {
                self.defer_refresh(current).await?;
                return Err(failure);
            }
            Ok(grant) => grant,
        };
        self.rotation(&current.account, grant, guards)
    }

    pub(super) fn rotation(
        &self,
        account: &ProviderAccount,
        grant: VerifiedGrant,
        guards: RefreshGuards,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        if account.upstream_user_id() != Some(grant.material.subject.as_str()) {
            return Err(
                error(Kind::Conflict).with_public_message("授权的 Google 账号与原账号不一致")
            );
        }
        let facts = PreparedCredentialRotationFacts {
            account_id: account.id().clone(),
            provider_kind: self.kind.clone(),
            expected_credential_revision: Revision::new(account.revision().get())
                .map_err(|_| error(Kind::Internal))?,
            replacement_identity: None,
            name: account.name().to_owned(),
            email: grant.email,
            plan_type: None,
            preserve_profile: true,
            preserve_access_token_expiry: false,
            provider_material: document(grant.material)?,
            has_refresh_token: true,
            access_token_expires_at: Some(grant.expires_at),
            next_refresh_at: None,
        };
        Ok(PreparedCredentialRotation::new(facts, Box::new(guards)))
    }
}
