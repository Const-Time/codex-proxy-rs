use provider_antigravity::Selection;
use serde_json::json;

use super::{configuration, services};

const CLI_URL: &str = "https://storage.googleapis.com/antigravity-public/antigravity-cli/1.5.17-123/linux-x64/cli_linux_x64.tar.gz";
const DESKTOP_DOWNLOADS: &str =
    "https://storage.googleapis.com/antigravity-public/antigravity-hub/";

fn manifest(url: &str, version: &str, checksum: &str) -> Vec<u8> {
    json!({"version": version, "url": url, "sha512": checksum})
        .to_string()
        .into_bytes()
}

#[tokio::test]
async fn cli_metadata_requires_the_official_host_path_target_and_matching_version() {
    let configuration = configuration("cli", "linux", None);
    let target = Selection::parse(&configuration).unwrap().target();
    let (profiles, releases) = services(configuration);
    let checksum = "a".repeat(128);
    releases
        .apply_metadata(target, Some(&manifest(CLI_URL, "1.5.17", &checksum)))
        .unwrap();
    let release = profiles.view(None).await.unwrap().release.unwrap();
    assert_eq!(release.download_url, CLI_URL);
    assert_eq!(release.version, "1.5.17");
    assert_eq!(
        profiles
            .dashboard()
            .await
            .unwrap()
            .release
            .unwrap()
            .latest_build
            .as_deref(),
        Some("123")
    );

    for url in [
        CLI_URL.replace(
            "storage.googleapis.com",
            "storage.googleapis.com.example.com",
        ),
        CLI_URL.replace("storage.googleapis.com", "storage.googleapis.com:443"),
        CLI_URL.replace("linux-x64", "linux-arm"),
        CLI_URL.replace("-123/", "-123/../"),
        format!("{CLI_URL}?tracking=true"),
    ] {
        assert!(
            releases
                .apply_metadata(target, Some(&manifest(&url, "1.5.17", &checksum)))
                .is_err()
        );
    }
    assert!(
        releases
            .apply_metadata(target, Some(&manifest(CLI_URL, "1.5.18", &checksum)))
            .is_err()
    );
    assert!(
        releases
            .apply_metadata(target, Some(&manifest(CLI_URL, "1.5.17", "invalid")))
            .is_err()
    );
}

#[tokio::test]
async fn desktop_metadata_selects_one_exact_target_and_updates_the_latest_identity() {
    let configuration = configuration("desktop", "macos", None);
    let target = Selection::parse(&configuration).unwrap().target();
    let (profiles, releases) = services(configuration);
    let link = format!("{DESKTOP_DOWNLOADS}2.35.0-123/darwin-arm/Antigravity.dmg");
    let page = format!(
        r#"<a href="{DESKTOP_DOWNLOADS}2.35.0-123/darwin-x64/Antigravity.dmg"></a><a href='{link}'></a>"#
    );
    releases
        .apply_metadata(target, Some(page.as_bytes()))
        .unwrap();
    let view = profiles.view(None).await.unwrap();
    assert_eq!(view.release.unwrap().download_url, link);
    assert_eq!(
        view.user_agent,
        "antigravity/hub/2.35.0 (aidev_client; os_type=darwin; arch=arm64)"
    );
    assert_eq!(
        view.user_agent,
        profiles.current().await.unwrap().user_agent()
    );
    let conflicting =
        format!(r#"{page}<a href="{DESKTOP_DOWNLOADS}2.36.0-124/darwin-arm/Antigravity.dmg"></a>"#);
    assert!(
        releases
            .apply_metadata(target, Some(conflicting.as_bytes()))
            .is_err()
    );
}

#[tokio::test]
async fn failed_check_retains_the_accepted_release_and_its_timestamp() {
    let configuration = configuration("cli", "linux", None);
    let target = Selection::parse(&configuration).unwrap().target();
    let (profiles, releases) = services(configuration);
    let manifest = manifest(CLI_URL, "1.5.17", &"a".repeat(128));
    releases.apply_metadata(target, Some(&manifest)).unwrap();
    let original = profiles.view(None).await.unwrap().release.unwrap();
    assert!(releases.apply_metadata(target, None).is_err());
    let retained = profiles.view(None).await.unwrap().release.unwrap();
    assert_eq!(retained.download_url, original.download_url);
    assert_eq!(retained.checked_at, original.checked_at);
}
