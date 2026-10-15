use cpr_shared_budget_plugin::{MANIFEST, PLUGIN_ID, app};
use gateway_plugin_sdk::{
    Manifest,
    client::{PluginSession, SessionConfig, SessionError},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = PluginSession::accept(
        tokio::io::stdin(),
        tokio::io::stdout(),
        SessionConfig::default(),
    )
    .await?;
    let manifest = Manifest::from_author_slice(MANIFEST)?;
    if session.handshake().plugin_id != PLUGIN_ID
        || session.handshake().contributes != manifest.contributes
        || !session
            .handshake()
            .configuration
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
    {
        return Err(SessionError::Handshake.into());
    }
    session.run(app::plugin()?).await?;
    Ok(())
}
