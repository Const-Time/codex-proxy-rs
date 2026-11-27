//! Antigravity Google OAuth accounts and native Cloud Code generation.

mod admin;
mod billing;
mod chat;
mod credential;
mod profile;
mod provider;
mod transport;
mod workers;

use std::sync::Arc;

use gateway_admin::ports::provider::ProviderAdmin;
use gateway_core::{
    engine::provider::Provider, provider_ports::ProviderStorePorts, routing::ProviderKind,
    task::WorkerContribution,
};

use credential::{
    AuthorizationService, CredentialService, catalog::CatalogService, quota::QuotaService,
};

pub use billing::request_is_priceable;
pub use chat::ChatProjection;
pub use credential::{
    oauth::{
        GoogleOAuth, Material as OAuthMaterial, OAuthRegistration,
        VerifiedGrant as VerifiedOAuthGrant,
    },
    quota::QuotaDocument,
};
#[doc(hidden)]
pub use profile::{ClientProfile, ProfileService, ReleaseService, ReleaseTarget, Selection};
pub use provider::{
    SessionAffinity, apply_upstream_session_id, locked_account, session_key, upstream_session_id,
};
#[doc(hidden)]
pub use transport::Catalog as CloudCodeCatalog;
pub use transport::{
    CloudCodeClient, CloudCodeResponseContext, GenerateMode, decode_cloud_code_response,
    decode_count_response,
};

const NAME: &str = "antigravity";

#[derive(Debug, thiserror::Error)]
#[error("could not initialize the Antigravity provider")]
pub struct ConfigError;

pub struct ProviderBundle {
    core: Arc<dyn Provider>,
    admin: Arc<dyn ProviderAdmin>,
    workers: Vec<WorkerContribution>,
}

impl ProviderBundle {
    #[must_use]
    pub fn core_provider(&self) -> Arc<dyn Provider> {
        Arc::clone(&self.core)
    }

    #[must_use]
    pub fn admin_provider(&self) -> Arc<dyn ProviderAdmin> {
        Arc::clone(&self.admin)
    }

    pub fn take_worker_contributions(&mut self) -> Vec<WorkerContribution> {
        std::mem::take(&mut self.workers)
    }
}

pub fn initialize(ports: ProviderStorePorts) -> Result<ProviderBundle, ConfigError> {
    let kind = ProviderKind::new(NAME).map_err(|_| ConfigError)?;
    let release = Arc::new(profile::ReleaseService::new()?);
    let profiles = Arc::new(profile::ProfileService::new(
        kind.clone(),
        ports.runtime_policy(),
        release.clone(),
    ));
    let oauth = Arc::new(GoogleOAuth::from_build_configuration()?.with_profile(profiles.clone()));
    let origin = oauth.resource_origin().clone();
    let credentials = Arc::new(CredentialService::new(kind.clone(), &ports, oauth.clone()));
    let catalog = Arc::new(
        CatalogService::new(
            kind.clone(),
            ports.accounts(),
            credentials.clone(),
            oauth.registration(),
            origin.clone(),
        )
        .with_profile(profiles.clone()),
    );
    let quota = Arc::new(
        QuotaService::new(
            kind.clone(),
            ports.accounts(),
            credentials.clone(),
            catalog.clone(),
            origin,
        )
        .with_profile(profiles.clone()),
    );
    let core = Arc::new(
        provider::AntigravityProvider::new(
            kind.clone(),
            &ports,
            credentials.clone(),
            catalog.clone(),
            quota.clone(),
        )
        .with_profile(profiles.clone()),
    );
    let admin = Arc::new(admin::AntigravityAdmin {
        kind: kind.clone(),
        authorization: AuthorizationService {
            kind,
            credentials: credentials.clone(),
            oauth,
            store: ports.oauth_pending(),
        },
        credentials: credentials.clone(),
        catalog: catalog.clone(),
        quota: quota.clone(),
        profiles,
    });
    let mut workers = vec![workers::release_contribution(release).map_err(|_| ConfigError)?];
    workers.extend(workers::contributions(credentials, catalog, quota).map_err(|_| ConfigError)?);
    Ok(ProviderBundle {
        core,
        admin,
        workers,
    })
}
