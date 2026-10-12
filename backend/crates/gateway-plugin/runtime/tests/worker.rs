use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    client::{CallFuture, CallReply, PluginCall, PluginHandler, PluginSession, SessionConfig},
};
use serde_json::json;
mod worker_models {
    //! Exercise the real SDK host methods and event codec from a separate process.
    use gateway_plugin_sdk::{call::host::*, client::HostClient};
    use serde_json::{Value, json};
    pub async fn handle(host: &HostClient, input: &Value) -> Value {
        let result = handle_inner(host, input).await;
        match result {
            Ok(value) => value,
            Err(error) => json!({"error":error}),
        }
    }
    async fn handle_inner(
        host: &HostClient,
        input: &Value,
    ) -> Result<Value, gateway_plugin_sdk::PluginFault> {
        let mode = input["mode"].as_str().unwrap_or("unary");
        if mode == "raw" {
            let payload = input
                .get("body")
                .map(|b| serde_json::to_vec(b).unwrap())
                .unwrap_or_default();
            let reply = host
                .call(
                    input["method"].as_str().unwrap(),
                    input["params"].clone(),
                    payload,
                )
                .await
                .map_err(|e| e.into_plugin_fault())?;
            if input["method"] == "host.model.execute" {
                let batch = ModelEventBatch::decode(&reply.payload).unwrap();
                return Ok(json!({"result":reply.result,"batch":batch.events}));
            }
            return Ok(json!({"result":reply.result}));
        }
        let model = input["model"].as_str().unwrap_or("gpt-native");
        let request = ModelExecuteRequest {
            client_key_id: input["key"].as_str().map(str::to_owned),
            model: model.into(),
            protocol: "openai".into(),
            operation: ModelOperation::Generate,
            provider: input["provider"].as_str().map(str::to_owned),
            account_id: input["account"].as_str().map(str::to_owned),
            previous_response_id: None,
        };
        let body = serde_json::to_vec(
            &json!({"model":model,"input":"fixture","mode":input["provider_mode"],"failure":input["failure"],"nested_model":input["nested_model"],"after_next":input["after_next"]}),
        )
        .unwrap();
        if mode == "capacity" {
            let mut handles = vec![];
            for _ in 0..16 {
                let reply = host
                    .call(
                        "host.model.execute_stream",
                        serde_json::to_value(&request).unwrap(),
                        body.clone(),
                    )
                    .await
                    .map_err(|e| e.into_plugin_fault())?;
                let stream: ModelStreamResult = serde_json::from_value(reply.result).unwrap();
                handles.push(stream.stream);
            }
            let error = host
                .call(
                    "host.model.execute_stream",
                    serde_json::to_value(&request).unwrap(),
                    body,
                )
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(error.code, gateway_plugin_sdk::ErrorCode::Capacity);
            for stream in handles {
                host.call("host.model.stream_close", json!({"stream":stream}), vec![])
                    .await
                    .map_err(|e| e.into_plugin_fault())?;
            }
            return Ok(json!({"capacity":true}));
        }
        if mode == "unary" {
            let reply = host
                .call(
                    "host.model.execute",
                    serde_json::to_value(request).unwrap(),
                    body,
                )
                .await
                .map_err(|e| e.into_plugin_fault())?;
            let metadata: ModelExecuteResult = serde_json::from_value(reply.result).unwrap();
            let batch = ModelEventBatch::decode(&reply.payload).unwrap();
            assert_eq!(batch.events.len(), metadata.events as usize);
            return Ok(
                json!({"request":metadata.request_id,"events":metadata.events,"batch":batch.events}),
            );
        }
        let reply = host
            .call(
                "host.model.execute_stream",
                serde_json::to_value(request).unwrap(),
                body,
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
        let stream: ModelStreamResult = serde_json::from_value(reply.result).unwrap();
        if mode == "leak" {
            return Ok(json!({"stream":stream.stream}));
        }
        if mode == "crash-stream" {
            std::process::exit(2);
        }
        if mode == "close" {
            host.call(
                "host.model.stream_read",
                json!({"stream":stream.stream,"maximum_bytes":8388608}),
                vec![],
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            host.call(
                "host.model.stream_close",
                json!({"stream":stream.stream}),
                vec![],
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            return Ok(json!({"closed":true}));
        }
        if mode == "small" {
            let error = host
                .call(
                    "host.model.stream_read",
                    json!({"stream":stream.stream,"maximum_bytes":1}),
                    vec![],
                )
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(error.code, gateway_plugin_sdk::ErrorCode::Capacity);
        }
        let mut events = 0;
        let mut snapshots = vec![];
        loop {
            let reply = host
                .call(
                    "host.model.stream_read",
                    json!({"stream":stream.stream,"maximum_bytes":8388608}),
                    vec![],
                )
                .await
                .map_err(|e| e.into_plugin_fault())?;
            let metadata: ModelStreamReadResult = serde_json::from_value(reply.result).unwrap();
            if !reply.payload.is_empty() {
                let batch = ModelEventBatch::decode(&reply.payload).unwrap();
                assert_eq!(batch.events.len(), metadata.events as usize);
                snapshots.extend(batch.events);
            } else {
                assert_eq!(metadata.events, 0);
            }
            events += metadata.events;
            if metadata.end {
                break;
            }
        }
        Ok(json!({"request":stream.request_id,"events":events,"ended":true,"batch":snapshots}))
    }
}

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
                } else if input["operation"]=="models" {
                    worker_models::handle(&call.host, &input).await
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
        if mode == "native-model" {
            let result = super::worker_models::handle(&call.host, &input["native"]).await;
            return Ok(CallReply::stream(
                json!({"response":null,"protocol":"openai","status":200,"header_mutations":[],"body":{"kind":"stream","framing":"json_document"}}),
                vec![],
                ResponseStream::from_chunks(vec![
                    MiddlewareBodyFrame::new(serde_json::to_vec(&result).unwrap(), true).encode(),
                ]),
            ));
        }
        if mode == "reject" {
            return Err(PluginFault::new(ErrorCode::Rejected, "rejected by fixture"));
        }
        if mode == "native-short" {
            return Ok(CallReply::stream(
                json!({"response":null,"protocol":"openai","status":403,"header_mutations":[],"body":{"kind":"stream","framing":"raw_bytes"}}),
                vec![],
                ResponseStream::from_chunks(vec![
                    MiddlewareBodyFrame::new(
                        br#"{"error":{"message":"native blocked"}}"#.to_vec(),
                        true,
                    )
                    .encode(),
                ]),
            ));
        }
        if mode == "native-transform" {
            let host = call.host.clone();
            let stage = match call.context.stage {
                gateway_plugin_sdk::Stage::Request => "plugin_request",
                gateway_plugin_sdk::Stage::Attempt => "plugin_attempt",
                _ => panic!("model chain uses request/attempt"),
            };
            if stage == "plugin_request" && input["nested_model"] == true {
                let nested =
                    super::worker_models::handle(&call.host, &json!({"mode":"unary"})).await;
                assert!(nested.get("error").is_none(), "{nested}");
            }
            let RequestCall {
                mut request, next, ..
            } = RequestCall::try_from(call)?;
            assert_eq!(request.head.client_key_id, "key_alice");
            let mut body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            body[stage] = json!(true);
            request.replace_body(serde_json::to_vec(&body).unwrap());
            let mut response = next.run(request).await?;
            let nested = if stage == "plugin_attempt" && input["after_next"].is_object() {
                Some(super::worker_models::handle(&host, &input["after_next"]).await)
            } else {
                None
            };
            response.body = response.body.with_facts()?.map_frames(move |mut frame| {
                if let Some(facts) = &mut frame.facts {
                    facts.facts.clear();
                    facts.host = None;
                }
                if !frame.payload.is_empty() {
                    if frame.payload.starts_with(b"{") {
                        let mut value: serde_json::Value =
                            serde_json::from_slice(&frame.payload).unwrap();
                        value[stage] = json!(true);
                        if let Some(nested) = nested.as_ref() {
                            value["nested"] = nested.clone();
                        }
                        frame.payload = serde_json::to_vec(&value).unwrap();
                    } else {
                        let text = std::str::from_utf8(&frame.payload).unwrap();
                        if let Some(start) = text.find("data: ") {
                            let start = start + 6;
                            let end = start + text[start..].find('\n').unwrap();
                            let mut value: serde_json::Value =
                                serde_json::from_str(&text[start..end]).unwrap();
                            value[stage] = json!(true);
                            if let Some(nested) = nested.as_ref() {
                                value["nested"] = nested.clone();
                            }
                            frame.payload =
                                format!("{}{}{}", &text[..start], value, &text[end..]).into_bytes();
                        }
                    }
                }
                Ok(vec![frame])
            })?;
            return response.encode();
        }
        if mode == "ws-short" {
            return Ok(CallReply::stream(
                json!({"response":null,"protocol":"openai","status":403,"header_mutations":[],"body":{"kind":"stream","framing":"json_document"}}),
                vec![],
                ResponseStream::from_chunks(vec![
                    MiddlewareBodyFrame::new(br#"{"error":{"message":"blocked"}}"#.to_vec(), true)
                        .encode(),
                ]),
            ));
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
        let instance = call.context.instance_id.clone();
        let generation = call.context.generation;
        let RequestCall {
            mut request, next, ..
        } = RequestCall::try_from(call)?;
        assert_eq!(request.head.client_key_id, "key_original");
        if mode == "chain" {
            request.append_header("x-chain", format!("{instance}:{generation}").into_bytes());
        }
        if mode == "rewrite" {
            request.replace_body(br#"{"model":"changed"}"#.to_vec());
            request.append_header("x-plugin", b"changed".to_vec());
            // These display facts never become host authority through next.
            request.head.client_key_id = "key_forged".into();
            request.head.request_id = "req_forged".into();
        }
        if mode == "settings-first" {
            assert_eq!(request.head.settings["fast_mode"], "default");
            request.head.settings["fast_mode"] = json!("enabled");
            request.head.settings["runtime"]["request_interval_ms"] = json!(0);
            request.head.settings["timeout_ms"] = json!(0);
            // The host, not the editable head, owns provenance.
            request.head.settings_sources = json!({"overrides":{"forged":true}});
        }
        if mode == "settings-second" {
            assert_eq!(request.head.settings["fast_mode"], "enabled");
            assert_eq!(request.head.settings["timeout_ms"], 0);
            assert_eq!(
                request.head.settings_sources["execution"]["fast_mode"]["instance_id"],
                instance
            );
            assert_eq!(
                request.head.settings_sources["execution"]["fast_mode"]["order"],
                1
            );
            request.head.settings["fast_mode"] = json!("disabled");
        }
        if mode == "settings-attempt" {
            assert!(request.head.settings.is_null());
            assert_eq!(
                request.head.settings_sources["execution"]["fast_mode"]["value"],
                "enabled"
            );
        }
        if mode == "settings-unknown" {
            request.head.settings["unknown"] = json!(true);
        }
        if mode == "settings" {
            request.head.settings = json!({"fast":true});
        }
        if mode.starts_with("caps-") {
            use gateway_plugin_sdk::call::middleware::{CapabilityDeclaration, RequestFeature};
            let mut body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let declaration = match mode.as_str() {
                "caps-required" | "caps-preserve" => CapabilityDeclaration {
                    handled: vec![],
                    required: vec![RequestFeature::Reasoning],
                },
                "caps-missing" => CapabilityDeclaration {
                    handled: vec![RequestFeature::Vision],
                    required: vec![],
                },
                "caps-duplicate" => CapabilityDeclaration {
                    handled: vec![RequestFeature::Tools, RequestFeature::Tools],
                    required: vec![],
                },
                "caps-overlap" => CapabilityDeclaration {
                    handled: vec![RequestFeature::Tools],
                    required: vec![RequestFeature::Tools],
                },
                _ => CapabilityDeclaration {
                    handled: vec![RequestFeature::Tools],
                    required: vec![],
                },
            };
            if mode != "caps-retain" {
                body.as_object_mut().unwrap().remove("tools");
            }
            if mode != "caps-preserve" {
                request.replace_body(serde_json::to_vec(&body).unwrap());
            }
            request.declare_capabilities(declaration);
        }
        let mut response = next.run(request).await?;
        if mode == "chain" {
            response.append_header("x-chain", format!("{instance}:{generation}").into_bytes());
        }
        if mode == "wait" {
            std::future::pending::<()>().await;
        }
        if matches!(
            mode.as_str(),
            "inspect"
                | "inspect-transformed"
                | "map"
                | "expand"
                | "drop-first"
                | "ws-map"
                | "ws-expand"
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
                if mode == "ws-map" && !frame.payload.is_empty() {
                    let mut value: serde_json::Value =
                        serde_json::from_slice(&frame.payload).unwrap();
                    value["plugin"] = json!(true);
                    frame.payload = serde_json::to_vec(&value).unwrap();
                }
                if mode == "ws-expand" && !frame.payload.is_empty() {
                    return Ok(vec![
                        MiddlewareBodyFrame::new(
                            br#"{"type":"response.output_text.delta","delta":"prefix"}"#.to_vec(),
                            false,
                        ),
                        frame,
                    ]);
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
