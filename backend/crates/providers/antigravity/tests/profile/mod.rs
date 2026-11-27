mod release;
mod selection;

use std::sync::Arc;

use futures::future::BoxFuture;
use gateway_core::{
    account::{AccountSelectionPolicy, OpaqueProviderData},
    provider_ports::{ProviderRefreshPolicy, ProviderRuntimePolicyPort, ProviderStoreError},
    routing::ProviderKind,
};
use provider_antigravity::{ProfileService, ReleaseService};
use serde_json::{Value, json};

struct SavedProfile(OpaqueProviderData);

impl ProviderRuntimePolicyPort for SavedProfile {
    fn load_client_profile<'a>(
        &'a self,
        _: &'a ProviderKind,
    ) -> BoxFuture<'a, Result<Option<OpaqueProviderData>, ProviderStoreError>> {
        Box::pin(async { Ok(Some(self.0.clone())) })
    }

    fn load_account_selection_policy(
        &self,
    ) -> BoxFuture<'_, Result<AccountSelectionPolicy, ProviderStoreError>> {
        unreachable!("profile resolution only reads the client configuration")
    }

    fn load_refresh_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>> {
        unreachable!("profile resolution only reads the client configuration")
    }
}

fn configuration(client: &str, platform: &str, version: Option<&str>) -> OpaqueProviderData {
    let Value::Object(configuration) = json!({
        "client": client,
        "platform": platform,
        "arch": null,
        "versionMode": if version.is_some() { "fixed" } else { "latest" },
        "clientVersion": version,
    }) else {
        unreachable!();
    };
    OpaqueProviderData::new(configuration)
}

fn services(saved: OpaqueProviderData) -> (ProfileService, Arc<ReleaseService>) {
    let releases = Arc::new(ReleaseService::new().unwrap());
    let profiles = ProfileService::new(
        ProviderKind::new("antigravity").unwrap(),
        Arc::new(SavedProfile(saved)),
        releases.clone(),
    );
    (profiles, releases)
}

#[tokio::test]
async fn saved_profile_preview_and_outgoing_identity_share_resolution() {
    let saved = configuration("cli", "linux", Some("1.5.17"));
    let (profiles, _) = services(saved.clone());
    let current = profiles.current().await.unwrap();
    let preview = profiles.view(Some(&saved)).await.unwrap();
    assert_eq!(&preview.configuration, saved.expose_to_provider());
    assert_eq!(preview.user_agent, current.user_agent());
    let dashboard = profiles.dashboard().await.unwrap();
    assert_eq!(dashboard.user_agent, current.user_agent());
    assert_eq!(
        dashboard
            .attributes
            .iter()
            .find(|attribute| attribute.label == "客户端标识")
            .map(|attribute| attribute.value.as_str()),
        Some("antigravity/cli/1.5.17"),
    );
    assert_eq!(
        current.user_agent(),
        "antigravity/cli/1.5.17 (aidev_client; os_type=linux; arch=amd64; cl=1029496841; auth_method=consumer)"
    );
    let alternative = configuration("desktop", "macos", None);
    assert_ne!(
        profiles.view(Some(&alternative)).await.unwrap().user_agent,
        current.user_agent()
    );
    assert_eq!(
        profiles.current().await.unwrap().user_agent(),
        current.user_agent()
    );
}
