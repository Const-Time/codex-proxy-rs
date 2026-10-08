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
            "middleware.handle" => worker_middleware::handle(call).await,
            "plugin.register" if !self.fail=>Ok(CallReply::unary(json!({"contributes":self.contributes}),vec![])),
            "frontend_auth.identifier"=>Ok(CallReply::unary(json!({"identifier":"fixture-auth"}),vec![])),
            "frontend_auth.authenticate"=>{
                let input:gateway_plugin_sdk::call::frontend_authentication::FrontendAuthenticationRequest=serde_json::from_slice(&call.payload).unwrap();
                assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::Authentication);
                let value=match input.authorization.as_str() {
                    "Bearer external-alice"=>json!({"outcome":"authenticated","principal":"alice"}),
                    "Bearer external-unknown"=>json!({"outcome":"authenticated","principal":"unknown"}),
                    "Bearer external-rejected"=>json!({"outcome":"rejected"}),
                    "Bearer external-forged"=>json!({"outcome":"authenticated","principal":"alice","client_key_id":"forged"}),
                    "Bearer external-fault"=>return Err(PluginFault::new(ErrorCode::Fault,"sensitive credential detail")),
                    "Bearer external-crash"=>std::process::exit(2),
                    "Bearer external-state"=>{
                        call.host.call("host.state.put",json!({"namespace":"counter","key":"auth","value":1,"expected_version":null}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                        assert!(call.host.call("host.data.keys.get",json!({}),b"{}".to_vec()).await.is_err());
                        json!({"outcome":"authenticated","principal":"alice"})
                    },
                    _=>json!({"outcome":"not_matched"}),
                };
                Ok(CallReply::unary(json!({}),serde_json::to_vec(&value).unwrap()))
            },
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
                } else if input["operation"]=="resources" {
                    use gateway_plugin_sdk::call::resources::*;
                    let group=call.host.ensure_group(GroupEnsureRequest{resource_key:"main".into(),name:"Group".into(),color:"#2563EBFF".into(),description:None}).await?;
                    let members=call.host.change_group_members(GroupMembersChange{resource_key:"main".into(),add:vec!["acct_001".into()],remove:vec![]}).await?;
                    let key=call.host.ensure_key(KeyEnsureRequest{resource_key:"key".into(),name:"Key".into(),group_resource_keys:vec!["main".into()],max_concurrency:1,requests_per_minute:2,daily_limit_usd:"0".into(),weekly_limit_usd:"0".into()}).await?;
                    json!({"group":group,"members":members,"key":key})
                } else if input["operation"]=="quota" {
                    use gateway_plugin_sdk::call::shared_budgets::*;
                    let target=Target{user_id:"alice".into(),group_id:"grp_00000000000000000000000000000077".into()};
                    let budget=call.host.shared_budget(target.clone()).await?;
                    let updated=call.host.update_shared_group_limits(UpdateGroupLimits{group_id:target.group_id.clone(),expected_revision:budget.revision,daily_limit_usd:Some("3".into()),weekly_limit_usd:None}).await?;
                    let reset=call.host.reset_shared_budgets(Reset{event_id:"90d3a7ec-5866-4f51-8b1a-4c00ad39da63".into(),targets:vec![target]}).await?;
                    json!({"budget":budget,"updated":updated,"reset":reset})
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

mod worker_middleware {
    //! A real SDK author handler, plus intentionally malformed wire variants.
    use gateway_plugin_sdk::{
        ErrorCode, PluginFault,
        call::middleware::*,
        client::{CallReply, MiddlewareOutput, PluginCall, RequestCall, ResponseStream},
    };
    use serde_json::json;

    pub async fn handle(call: PluginCall) -> Result<CallReply, PluginFault> {
        let input: serde_json::Value = serde_json::from_slice(&call.payload).unwrap();
        let mode = input["mode"].as_str().unwrap().to_owned();
        if mode == "reject" {
            return Err(PluginFault::new(ErrorCode::Rejected, "rejected by fixture"));
        }
        if mode == "short" {
            return Ok(CallReply::stream(
                json!({"response":null,"protocol":"openai","status":403,"header_mutations":[],"body":{"kind":"stream","framing":"raw_bytes"}}),
                vec![],
                ResponseStream::from_chunks(vec![
                    MiddlewareBodyFrame::new(b"denied".to_vec(), true).encode(),
                ]),
            ));
        }
        let host = call.host.clone();
        if mode.starts_with("raw-") {
            let params = json!({"body":"preserve"});
            let reply = host
                .call(NEXT_METHOD, params.clone(), vec![])
                .await
                .map_err(|e| e.into_plugin_fault())?;
            let next: MiddlewareNextResponse = serde_json::from_value(reply.result).unwrap();
            let body = next.body.unwrap();
            let mut output =
                json!({"response":next.response,"body":{"kind":"pass_through","body":body}});
            match mode.as_str() {
                "raw-double" => {
                    let result = host.call(NEXT_METHOD, params, vec![]).await;
                    assert!(result.is_err());
                }
                "raw-forged" => output["body"]["body"]["handle"] = json!("another-parent-handle"),
                "raw-empty" => output["body"] = json!({"kind":"empty"}),
                "raw-read-pass" | "raw-replay" | "raw-standalone" => {
                    let reply = host
                        .call(
                            BODY_READ_METHOD,
                            json!({"handle":body.handle,"maximum_bytes":65536}),
                            vec![],
                        )
                        .await
                        .map_err(|e| e.into_plugin_fault())?;
                    if mode != "raw-read-pass" {
                        let frame = if mode == "raw-standalone" {
                            MiddlewareBodyFrame::new(reply.payload, true)
                        } else {
                            // Source 1 is current; pretending source 2 must not release its envelope.
                            let frame = MiddlewareBodyFrame::new(reply.payload, true);
                            let mut encoded = frame.encode();
                            encoded[5..13].copy_from_slice(&2u64.to_be_bytes());
                            encoded[4] = MiddlewareBodyDisposition::Only as u8;
                            encoded[13] = 0;
                            return Ok(CallReply::stream(
                                json!({"response":next.response,"body":{"kind":"stream","framing":body.framing}}),
                                vec![],
                                ResponseStream::from_chunks(vec![encoded]),
                            ));
                        };
                        return Ok(CallReply::stream(
                            json!({"response":next.response,"body":{"kind":"stream","framing":body.framing}}),
                            vec![],
                            ResponseStream::from_chunks(vec![frame.encode()]),
                        ));
                    }
                }
                _ => unreachable!(),
            }
            return Ok(CallReply::stream(
                output,
                vec![],
                ResponseStream::from_chunks(vec![]),
            ));
        }
        let RequestCall {
            mut request, next, ..
        } = RequestCall::try_from(call)?;
        assert_eq!(request.head.client_key_id, "key_original");
        if mode == "rewrite" {
            request.replace_body(br#"{"model":"changed"}"#.to_vec());
            request.append_header("x-plugin", b"changed".to_vec());
            // These display facts never become host authority through next.
            request.head.client_key_id = "key_forged".into();
            request.head.request_id = "req_forged".into();
        }
        if mode == "settings" {
            request.head.settings = json!({"fast":true});
        }
        let mut response = next.run(request).await?;
        if mode == "wait" {
            std::future::pending::<()>().await;
        }
        if matches!(
            mode.as_str(),
            "inspect" | "inspect-transformed" | "map" | "expand" | "drop-first"
        ) {
            let mut first = true;
            response.body = response.body.with_facts()?.map_frames(move |mut frame| {
                if let Some(facts) = &mut frame.facts {
                    if mode == "inspect-transformed" {
                        assert!(facts.host.as_ref().unwrap().middleware_transformed);
                        assert!(
                            facts
                                .host
                                .as_ref()
                                .unwrap()
                                .middleware_origin_wire
                                .is_some()
                        );
                    }
                    // Plugin-side facts can be changed without changing the host receipt.
                    facts.facts.clear();
                    facts.host = None;
                }
                if mode == "map" {
                    frame.payload = br#"{"mapped":true}"#.to_vec();
                }
                if mode == "drop-first" && first {
                    first = false;
                    return Ok(vec![]);
                }
                if mode == "expand" {
                    return Ok(vec![
                        MiddlewareBodyFrame::new(b"data: {\"prefix\":true}\n\n".to_vec(), false),
                        frame,
                    ]);
                }
                Ok(vec![frame])
            })?;
        }
        response.encode()
    }
}
