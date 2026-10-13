mod authority;
mod settings;

use gateway_admin::model::plugins::PluginRecord;
fn record() -> PluginRecord {
    PluginRecord {
        id: "plugin_fixture".into(),
        revision: 7,
        enabled: true,
        name: "fixture".into(),
        artifact_sha256: "a".repeat(64),
        configuration: serde_json::json!({}),
        secrets: Default::default(),
        frontend_authentication: None,
        middleware: None,
        model_router: None,
        account_scheduler: None,
        observer: None,
        upstream_adapter: None,
        retry_policy: None,
    }
}
