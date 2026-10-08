use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    client::{CallFuture, CallReply, PluginCall, PluginHandler, PluginSession, SessionConfig},
};
use serde_json::json;
struct Worker {
    contributes: serde_json::Value,
    fail: bool,
}
impl PluginHandler for Worker {
    fn call(&self, call: PluginCall) -> CallFuture<'_> {
        Box::pin(async move {
            match call.method.as_str() {
            "plugin.register" if !self.fail=>Ok(CallReply::unary(json!({"contributes":self.contributes}),vec![])),
            "management.register"=>Ok(CallReply::unary(json!({}),serde_json::to_vec(&json!({
                "routes":[{"method":"POST","path":"echo","request_content_types":["application/json"],"response_content_types":["application/json"]}],
                "resources":[{"path":"index.html","public":false}],"pages":[{"id":"home","title":"Counter","entry":"index.html","icon":null}],"callbacks":[]
            })).unwrap())),
            "management.handle"=>{
                let input:serde_json::Value=serde_json::from_slice(&call.payload).unwrap();
                let result=if input["operation"]=="state" {
                    let existing=call.host.call("host.state.get",json!({"namespace":"counter","key":"value"}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                    let expected=existing.result["record"]["version"].as_u64();
                    let count=existing.result["record"]["value"].as_u64().unwrap_or(0)+1;
                    call.host.call("host.state.put",json!({"namespace":"counter","key":"value","value":count,"expected_version":expected}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                    json!({"count":count})
                } else if input["operation"]=="unsupported" {
                    match call.host.call("host.keys.reset_budget",json!({"client_key_id":"must-not-reset","period":"all"}),vec![]).await {
                        Ok(_) => json!({"unexpected":true}), Err(error)=>json!({"code":error.into_plugin_fault().code})
                    }
                } else if input["operation"]=="facts" {
                    use gateway_plugin_sdk::call::data::*;
                    let key=call.host.key_facts(ClientKeyFactsQuery{client_key_id:"key_test".into()}).await?;
                    let accounts=call.host.account_facts(AccountFactsQuery{provider_id:Some("openai".into()),cursor:None,limit:1}).await?;
                    let quota=call.host.quota_facts(QuotaFactsQuery{account_id:"acct_test".into()}).await?;
                    json!({"key":key,"accounts":accounts,"quota":quota})
                } else if input["operation"]=="facts-raw" {
                    match call.host.call(input["method"].as_str().unwrap(),input["params"].clone(),serde_json::to_vec(&input["payload"]).unwrap()).await {
                        Ok(reply)=>json!({"result":serde_json::from_slice::<serde_json::Value>(&reply.payload).unwrap()}),
                        Err(error)=>json!({"error":error.into_plugin_fault()})
                    }
                } else if input["operation"]=="crash" { std::process::exit(2); }
                else { input };
                Ok(CallReply::unary(json!({"status":200,"content_type":"application/json","headers":[]}),serde_json::to_vec(&result).unwrap()))
            }
            _=>Err(PluginFault::new(ErrorCode::Rejected,"fixture error with sensitive detail")),
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
    let handler = Worker {
        contributes: serde_json::to_value(&session.handshake().contributes).unwrap(),
        fail: session.handshake().configuration["fail"]
            .as_bool()
            .unwrap_or(false),
    };
    let _ = session.run(handler).await;
}
