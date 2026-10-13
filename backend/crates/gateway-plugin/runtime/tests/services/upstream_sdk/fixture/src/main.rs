//! A separate binary built against the unmodified, pinned upstream SDK.
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::services::settings::{self as settings, MutationActor, MutationContext},
    client::{
        CallFuture, CallReply, MiddlewareInput, MiddlewareOutput, PluginCall, PluginHandler,
        PluginSession, ServiceCall, ServiceResponse, SessionConfig,
    },
};
use serde_json::{Value, json};

struct Worker {
    contributes: Value,
    mutate: bool,
    maintenance: bool,
    reconciled: std::sync::atomic::AtomicBool,
    observation: Option<std::path::PathBuf>,
    rewrite: Option<String>,
}
fn fault(error: gateway_plugin_sdk::call::services::ServiceError) -> PluginFault {
    PluginFault::new(ErrorCode::Fault, error.message)
}
async fn mutate(host: &gateway_plugin_sdk::client::HostClient) -> Result<(), PluginFault> {
    // Wire identity is deliberately false: the host must retain
    // its original plugin lease and persisted audit principal.
    let input = MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: "forged-user".into(),
        },
        request_id: "forged-request".into(),
    };
    assert!(
        !host
            .service::<settings::ApiKeyExists>(())
            .await
            .map_err(fault)?
    );
    let generated = host
        .service::<settings::RegenerateApiKey>(input.clone())
        .await
        .map_err(fault)?;
    assert!(generated.mutation.exists && generated.key.len() >= 32);
    assert!(
        host.service::<settings::ApiKeyExists>(())
            .await
            .map_err(fault)?
    );
    let deleted = host
        .service::<settings::DeleteApiKey>(input)
        .await
        .map_err(fault)?;
    assert!(!deleted.exists);
    Ok(())
}
impl PluginHandler for Worker {
    fn call(&self, call: PluginCall) -> CallFuture<'_> {
        Box::pin(async move {
            match call.method.as_str() {
                "plugin.reconcile" => {
                    assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Maintenance);
                    assert_eq!(call.params, json!({}));
                    assert!(call.payload.is_empty());
                    if self.maintenance
                        && !self
                            .reconciled
                            .swap(true, std::sync::atomic::Ordering::SeqCst)
                    {
                        mutate(&call.host).await?;
                    }
                    Ok(CallReply::unary(json!({}), vec![]))
                }
                "plugin.register" => {
                    if self.mutate {
                        mutate(&call.host).await?;
                    }
                    Ok(CallReply::unary(
                        json!({"contributes":self.contributes}),
                        vec![],
                    ))
                }
                "middleware.handle" => {
                    if call.params["operation"] == "settings.regenerate_admin_api_key"
                        && let Some(path) = &self.observation
                    {
                        let decoded =
                            serde_json::from_value::<MutationContext>(call.params["input"].clone());
                        // Record only the non-secret input identity and decoder outcome.
                        std::fs::write(
                            path,
                            serde_json::to_vec(&json!({
                                "actor": call.params["input"]["actor"],
                                "request_id": call.params["input"]["request_id"],
                                "decode_error": decoded.err().map(|error| error.to_string())
                            }))
                            .unwrap(),
                        )
                        .unwrap();
                    }
                    let call = ServiceCall::decode(call)?;
                    if call.is::<settings::RegenerateApiKey>() {
                        let mut call = call.into_typed::<settings::RegenerateApiKey>()?;
                        match self.rewrite.as_deref() {
                            Some("actor") => {
                                call.input.actor = MutationActor::AdminSession {
                                    admin_user_id: "middleware-forged-user".into(),
                                }
                            }
                            Some("request") => {
                                call.input.request_id = "middleware-forged-request".into()
                            }
                            _ => {}
                        }
                        let result = call.next.run(call.input).await;
                        ServiceResponse::from_result::<settings::RegenerateApiKey>(result)?.encode()
                    } else if call.is::<settings::DeleteApiKey>() {
                        let call = call.into_typed::<settings::DeleteApiKey>()?;
                        let result = call.next.run(call.input).await;
                        ServiceResponse::from_result::<settings::DeleteApiKey>(result)?.encode()
                    } else {
                        call.forward().await?.encode()
                    }
                }
                _ => Err(PluginFault::new(
                    ErrorCode::Unsupported,
                    "fixture method is not implemented",
                )),
            }
        })
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let session = PluginSession::accept(
        tokio::io::stdin(),
        tokio::io::stdout(),
        SessionConfig::default(),
    )
    .await
    .unwrap();
    let worker = Worker {
        contributes: serde_json::to_value(&session.handshake().contributes).unwrap(),
        mutate: session.handshake().configuration["mutate"]
            .as_bool()
            .unwrap_or(false),
        maintenance: session.handshake().configuration["maintenance"]
            .as_bool()
            .unwrap_or(false),
        reconciled: std::sync::atomic::AtomicBool::new(false),
        observation: session.handshake().configuration["observation"]
            .as_str()
            .map(Into::into),
        rewrite: session.handshake().configuration["rewrite"]
            .as_str()
            .map(str::to_owned),
    };
    session.run(worker).await.unwrap();
}
