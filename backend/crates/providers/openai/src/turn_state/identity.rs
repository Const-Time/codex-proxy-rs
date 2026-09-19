//! State binds to authentication, not the CAS revision shared with cookie writes.
use super::*;
use crate::credential::CodexRuntimeAuthentication;
use secrecy::ExposeSecret as _;

impl StateManager {
    pub(crate) fn identity_binding(
        &self,
        account: &ProviderAccount,
        authentication: &CodexRuntimeAuthentication,
        installation: &str,
    ) -> [u8; 32] {
        let secret = authentication.oauth().expect("OAuth provider");
        let mut input = Vec::new();
        for part in [
            account.id().as_str(),
            account.upstream_user_id().unwrap_or_default(),
            account.upstream_account_id().unwrap_or_default(),
            secret.access_token.expose_secret(),
            installation,
        ] {
            input.extend_from_slice(&(part.len() as u64).to_be_bytes());
            input.extend_from_slice(part.as_bytes());
        }
        crate::transport::session::hmac_sha256(
            &self.fingerprint_key,
            &[b"state-auth-binding/v1", &input],
        )
    }

    pub(super) async fn current_identity(
        &self,
        account: &ProviderAccount,
    ) -> Option<(ProviderAccount, [u8; 32])> {
        let loaded = self
            .repository
            .store()
            .load_current_credential(account.id())
            .await
            .ok()?;
        let credential = self.repository.decode_runtime_credential(&loaded).ok()?;
        let binding = self.identity_binding(
            &loaded.account,
            &credential.authentication,
            &credential.installation_id,
        );
        Some((loaded.account, binding))
    }
}

impl Current {
    pub(super) fn belongs_to(&self, account: &ProviderAccount, binding: &[u8; 32]) -> bool {
        // Never migrate a legacy ticket across an unknown credential change.
        self.binding.as_ref().map_or_else(
            || self.account_revision == account.revision().get(),
            |stored| stored == binding,
        )
    }
}
