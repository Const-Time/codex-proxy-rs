use gateway_admin::{
    model::{AdminErrorKind, plugins::PluginAccess},
    plugins::service::PluginServiceAuthority,
};
#[test]
fn service_authority_rejects_disabled_invalid_and_revoked_generations() {
    for (id, revision, enabled) in [
        ("", 1, true),
        ("plugin@other", 1, true),
        ("plugin_1", 0, true),
        ("plugin_1", 1, false),
        ("plugin 1", 1, true),
    ] {
        let mut record = super::record();
        record.id = id.into();
        record.revision = revision;
        record.enabled = enabled;
        assert!(PluginServiceAuthority::new(&record, PluginAccess::default()).is_err());
    }
    let access = PluginAccess::default();
    access.revoke();
    assert_eq!(
        PluginServiceAuthority::new(&super::record(), access)
            .err()
            .unwrap()
            .kind(),
        AdminErrorKind::Forbidden
    );
    assert!(PluginServiceAuthority::new(&super::record(), PluginAccess::default()).is_ok());
}
