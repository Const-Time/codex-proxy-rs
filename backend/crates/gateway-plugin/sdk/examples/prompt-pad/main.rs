//! Minimal useful plugin: a private page with selected-Key inference and streaming cancellation.
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::management::{ManagementPage, ManagementRegistration, ManagementResource},
    client::{PluginBuilder, PluginSession, SessionConfig},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin = PluginBuilder::from_json(include_bytes!("plugin.json"))?
        .management(
            ManagementRegistration {
                routes: Vec::new(),
                resources: vec![ManagementResource {
                    path: "web/index.html".into(),
                    public: false,
                }],
                pages: vec![ManagementPage {
                    id: "prompt".into(),
                    title: "推理便笺".into(),
                    description: None,
                    entry: "web/index.html".into(),
                    icon: None,
                }],
                callbacks: Vec::new(),
            },
            |_| async {
                Err(PluginFault::new(
                    ErrorCode::Unsupported,
                    "No management routes",
                ))
            },
        )?
        .build()?;
    PluginSession::accept(
        tokio::io::stdin(),
        tokio::io::stdout(),
        SessionConfig::default(),
    )
    .await?
    .run(plugin)
    .await?;
    Ok(())
}
