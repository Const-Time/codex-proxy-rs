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
        if mode == "service-budget" {
            Box::pin(handle_inner(
                host,
                &json!({"mode":"unary","key":"key_alice"}),
            ))
            .await?;
            Box::pin(handle_inner(host, &json!({"mode":"http"}))).await?;
            for completed in 0..64 {
                if let Err(error) = host
                    .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
                    .await
                {
                    return Ok(json!({"completed":completed,"kind":error.kind}));
                }
            }
            return Ok(json!({"completed":64}));
        }
        if mode == "http" {
            if input["model_first"] == true {
                Box::pin(handle_inner(
                    host,
                    &json!({"mode":"unary", "key":"key_alice"}),
                ))
                .await?;
            }
            use gateway_plugin_sdk::call::middleware::{MiddlewareHeader, http::Version};
            use gateway_plugin_sdk::client::{HttpBody, HttpFrame, HttpRequest};
            let mut data = vec![];
            for _ in 0..input["count"].as_u64().unwrap_or(1) {
                let mut response = host
                    .dispatch_http(HttpRequest {
                        settings: Value::Null,
                        method: "POST".into(),
                        uri: input["uri"].as_str().unwrap_or("/internal/raw").into(),
                        version: Version::Http11,
                        headers: vec![MiddlewareHeader {
                            name: "x-explicit".into(),
                            value: b"plugin-value".to_vec(),
                        }],
                        timeout_ms: None,
                        body: HttpBody::from_bytes(vec![0, 255, 1, 0]),
                    })
                    .await?;
                while let Some(frame) = response.body.read().await? {
                    if let HttpFrame::Data(bytes) = frame {
                        data.extend(bytes);
                    }
                }
                response.body.close().await?;
            }
            return Ok(json!({"bytes": data}));
        }
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
            previous_response_id: input["previous"].as_str().map(str::to_owned),
        };
        let mut body = json!({"model":model,"input":"fixture","mode":input["provider_mode"],"failure":input["failure"],"nested_model":input["nested_model"],"after_next":input["after_next"],"retry_child":input["retry_child"],"route":input["route"]});
        if let Some(previous) = request.previous_response_id.as_ref() {
            body["previous_response_id"] = json!(previous);
        }
        let body = serde_json::to_vec(&body).unwrap();
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
    adapter_mode: String,
    catalog_mode: String,
    contributes: serde_json::Value,
    fail: bool,
    schedule: String,
    retry: String,
}
impl PluginHandler for Worker {
    fn call(&self, call: PluginCall) -> CallFuture<'_> {
        Box::pin(async move {
            match call.method.as_str() {
            "upstream_adapter.register" => {
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Registration);
                assert_eq!(call.params, json!({}));
                assert!(call.payload.is_empty());
                let mut declaration = json!({"id":"adapter","provider":"openai","base_url":"https://example.test/base/","paths":[{"path":"responses","purpose":"inference"},{"path":"audit","purpose":"auxiliary"}],"authentication_kinds":["oauth"],"transport":"http_sse","protocol":"openai","models":["public-model"]});
                match self.adapter_mode.as_str() {
                    "id" => declaration["id"] = json!("invalid id"),
                    "provider" => declaration["provider"] = json!("foreign"),
                    "base" => declaration["base_url"] = json!("https://user:private-fixture@example.test/base/"),
                    "path" => declaration["paths"][0]["path"] = json!("../responses"),
                    "path-purpose" => declaration["paths"][0]["purpose"] = json!("auxiliary"),
                    "auth" => declaration["authentication_kinds"] = json!([]),
                    "auth-id" => declaration["authentication_kinds"] = json!(["token invalid"]),
                    "protocol" => declaration["protocol"] = json!("custom"),
                    "transport" => declaration["transport"] = json!("tcp"),
                    "models" => declaration["models"] = json!(["repeat","repeat"]),
                    "unknown" => declaration["unexpected"] = json!(true),
                    _ => {}
                }
                let adapters = match self.adapter_mode.as_str() {
                    "empty" => vec![],
                    "too-many" => (0..17).map(|i| { let mut d=declaration.clone();d["id"]=json!(format!("adapter-{i}"));d }).collect(),
                    "duplicate" => vec![declaration.clone(),declaration],
                    _ => vec![declaration],
                };
                let payload = if self.adapter_mode == "oversized" { vec![b' ';65537] } else {serde_json::to_vec(&json!({"adapters":adapters})).unwrap()};
                Ok(CallReply::unary(if self.adapter_mode == "envelope" {json!({"extra":true})} else {json!({})}, payload))
            }
            "model_catalog.register"=>{
                use gateway_plugin_sdk::call::catalog::{ModelCatalogRegistration,ModelAlias};
                assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::Registration);
                for method in ["host.models.list","host.services.call","host.state.put"] {assert!(call.host.call(method,json!({}),vec![]).await.is_err());}
                let mut models=vec![ModelAlias{id:"plugin-model".into(),provider:"openai".into(),model:"gpt-native".into()}];
                match self.catalog_mode.as_str() {
                    "empty"=>models.clear(),"duplicate"=>models.push(models[0].clone()),"self"=>models[0].model=models[0].id.clone(),"provider"=>models[0].provider="foreign".into(),"bad-id"=>models[0].id="bad\nmodel".into(),"oversized"=>{models=(0..257).map(|i|ModelAlias{id:format!("alias-{i}"),provider:"openai".into(),model:"gpt-native".into()}).collect();},_=>{}
                }
                let mut payload=serde_json::to_vec(&ModelCatalogRegistration{models}).unwrap();
                if self.catalog_mode=="large-payload" {payload=vec![b' ';65537];}
                if self.catalog_mode=="unknown-field" {payload=br#"{"models":[],"extra":true}"#.to_vec();}
                if self.catalog_mode=="unknown-alias-field" {payload=br#"{"models":[{"id":"alias","provider":"openai","model":"gpt-native","capabilities":[] }]}"#.to_vec();}
                Ok(CallReply::unary(if self.catalog_mode=="envelope" {json!({"extra":true})}else{json!({})},payload))
            },
            "policy.retry_decision" => {
                use gateway_plugin_sdk::call::policy::{RetryDecisionRequest, RetryAction};
                let head: RetryDecisionRequest = serde_json::from_value(call.params.clone()).unwrap();
                assert!(call.payload.is_empty());
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Retry);
                assert_eq!(call.context.request_id.as_deref(), Some(head.request_id.as_str()));
                assert_eq!(call.context.attempt_id, Some(format!("{}:{}",head.request_id,head.attempt_index)));
                assert!(head.attempt_index > 0);
                assert!(head.allowed_actions.contains(&RetryAction::Stop));
                assert!(call.context.timeout_ms > 0 && call.context.timeout_ms <= 2000);
                // Retry may consult its private state, but may not recursively execute models
                // or access Key/credential/management facts at this policy stage.
                for method in ["host.models.list", "host.model.execute", "host.data.accounts.list", "host.keys.reset_budget"] {
                    let error = call.host.call(method,json!({}),vec![]).await.err().unwrap().into_plugin_fault();
                    assert_eq!(error.code, ErrorCode::PermissionDenied);
                }
                for (key,value) in [("retry_budget",call.context.timeout_ms),("retry_allowed",u64::from(head.allowed_actions.contains(&RetryAction::Retry)))] {
                    let existing=call.host.call("host.state.get",json!({"namespace":"counter","key":key}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                    call.host.call("host.state.put",json!({"namespace":"counter","key":key,"value":value,"expected_version":existing.result["record"]["version"].as_u64()}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                }
                let existing=call.host.call("host.state.get",json!({"namespace":"counter","key":"retry_calls"}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                call.host.call("host.state.put",json!({"namespace":"counter","key":"retry_calls","value":existing.result["record"]["value"].as_u64().unwrap_or(0)+1,"expected_version":existing.result["record"]["version"].as_u64()}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                let decision=match self.retry.as_str() {
                    "stop"=>json!({"decision":"stop"}),
                    "retry"=>json!({"decision":"retry"}),
                    "fault"=>return Err(PluginFault::new(ErrorCode::Fault,"private retry error detail")),
                    "payload"=>return Ok(CallReply::unary(json!({"decision":"delegate"}),b"invalid".to_vec())),
                    "malformed"=>json!({"decision":"delegate","unexpected":true}),
                    "unknown"=>json!({"decision":"force"}),
                    "wait"=>{futures::future::pending::<()>().await;unreachable!()},
                    "slow-delegate"=>{tokio::time::sleep(std::time::Duration::from_millis(750)).await;json!({"decision":"delegate"})},
                    _=>json!({"decision":"delegate"}),
                };
                Ok(CallReply::unary(decision,vec![]))
            },
            "policy.schedule_account" => {
                use gateway_plugin_sdk::call::policy::AccountScheduleRequest;
                let head:AccountScheduleRequest=serde_json::from_value(call.params.clone()).unwrap();
                assert!(call.payload.is_empty());
                assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::Scheduling);
                assert_eq!(call.context.request_id.as_deref(),Some(head.request_id.as_str()));
                assert_eq!(call.context.attempt_id,Some(format!("{}:{}",head.request_id,head.attempt_index)));
                assert!(head.attempt_index>0);
                assert_eq!(head.model.as_deref(),Some("gpt-native"));
                assert!(head.candidates.iter().all(|c| c.account_id!="acct_forbidden" && c.maximum_concurrency==1 && c.in_flight==0 && c.quota_remaining_rank==Some(7500) && c.failure_rate_basis_points==Some(300) && c.first_output_latency_ms==Some(250)));
                assert!(call.host.call("host.data.accounts.list",json!({}),vec![]).await.is_err());
                let existing=call.host.call("host.state.get",json!({"namespace":"counter","key":"schedules"}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                call.host.call("host.state.put",json!({"namespace":"counter","key":"schedules","value":existing.result["record"]["value"].as_u64().unwrap_or(0)+1,"expected_version":existing.result["record"]["version"].as_u64()}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                let decision=match self.schedule.as_str() {
                    "reject"=>json!({"decision":"reject"}),
                    "foreign"=>json!({"decision":"pick","account_id":"acct_forbidden"}),
                    "fault"=>return Err(PluginFault::new(ErrorCode::Fault,"private scheduling error detail")),
                    "payload"=>return Ok(CallReply::unary(json!({"decision":"delegate"}),b"invalid".to_vec())),
                    "malformed"=>json!({"decision":"delegate","unexpected":true}),
                    "wait"=>{
                        call.host.call("host.models.list",json!({"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                        futures::future::pending::<()>().await; unreachable!()
                    },
                    "child"=>{
                        let nested=worker_models::handle(&call.host,&json!({"mode":"unary"})).await;
                        assert!(nested.get("error").is_none(),"{nested}"); json!({"decision":"delegate"})
                    },
                    "pick-last"=>json!({"decision":"pick","account_id":head.candidates.last().unwrap().account_id}),
                    _=>json!({"decision":"delegate"}),
                };
                Ok(CallReply::unary(decision,vec![]))
            },
            "policy.route_model" => {
                use gateway_plugin_sdk::call::policy::ModelRouteRequest;
                let head: ModelRouteRequest = serde_json::from_value(call.params.clone()).unwrap();
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Routing);
                assert_eq!(call.context.request_id.as_deref(), Some(head.request_id.as_str()));
                assert_eq!(head.operation, "generate");
                assert_eq!(head.protocol, "openai");
                let body:serde_json::Value=serde_json::from_slice(&call.payload).unwrap();
                assert_eq!(head.model, body["model"]);
                assert!(call.host.call("host.data.accounts.list",json!({}),vec![]).await.is_err());
                let decision = match body["route"].as_str() {
                    Some("reject") => json!({"decision":"reject"}),
                    Some("empty") => json!({"decision":"route"}),
                    Some("fault") => return Err(PluginFault::new(ErrorCode::Fault,"private plugin error detail")),
                    Some("wait") => {
                        call.host.call("host.models.list", json!({"client_key_id":"key_alice","protocol":"openai","client_version":"fixture"}), vec![]).await.map_err(|e|e.into_plugin_fault())?;
                        futures::future::pending::<()>().await;
                        unreachable!()
                    },
                    Some("child" | "child-unhandled") => {
                        let nested = worker_models::handle(&call.host, &json!({"mode":"unary"})).await;
                        assert!(nested.get("error").is_none(), "{nested}");
                        if body["route"] == "child-unhandled" { json!({"decision":"unhandled"}) }
                        else { json!({"decision":"route","provider":"openai"}) }
                    },
                    Some("xai") => {
                        assert!(head.available_providers.contains(&"xai".into()) || head.available_providers == vec!["openai"]);
                        json!({"decision":"route","provider":"xai"})
                    },
                    Some("openai") => json!({"decision":"route","provider":"openai"}),
                    Some("malformed") => json!({"decision":"route","provider":"openai","unexpected":true}),
                    Some("malformed-unhandled") => json!({"decision":"unhandled","unexpected":true}),
                    Some("malformed-reject") => json!({"decision":"reject","unexpected":true}),
                    Some("payload") => return Ok(CallReply::unary(json!({"decision":"unhandled"}), b"invalid".to_vec())),
                    _ => body["route"].as_object().map_or(json!({"decision":"unhandled"}), |value| json!(value)),
                };
                Ok(CallReply::unary(decision,vec![]))
            },
            "middleware.handle" if call.context.stage == gateway_plugin_sdk::Stage::Http => worker_http::handle(call).await,
            "middleware.handle" if call.context.stage == gateway_plugin_sdk::Stage::WebSocket => worker_websocket(call).await,
            "middleware.handle" if call.context.stage == gateway_plugin_sdk::Stage::Service => worker_service(call).await,
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
                } else if input["operation"]=="services" {
                    if input["mode"]=="local" {
                        use gateway_plugin_sdk::call::services::local_settings_v1 as local;
                        match input["local_operation"].as_str().unwrap() {
                            "load"=>typed_service::<local::Load>(&call.host,input["input"].clone()).await,
                            "replace"=>typed_service::<local::Replace>(&call.host,input["input"].clone()).await,
                            "pricing"=>typed_service::<local::Pricing>(&call.host,input["input"].clone()).await,
                            "preview_pricing_sync"=>typed_service::<local::PreviewPricingSync>(&call.host,input["input"].clone()).await,
                            "sync_pricing"=>typed_service::<local::Sync>(&call.host,input["input"].clone()).await,
                            "update_pricing"=>typed_service::<local::Update>(&call.host,input["input"].clone()).await,
                            "update_pricing_sync_policy"=>typed_service::<local::UpdateSyncPolicy>(&call.host,input["input"].clone()).await,
                            _=>panic!("unknown fixture service"),
                        }
                    } else if input["mode"]=="raw" {
                        match call.host.call(gateway_plugin_sdk::call::services::CALL_METHOD,input["request"].clone(),if input["binary"]==true {vec![1]}else{vec![]}).await {
                            Ok(reply)=>json!({"result":reply.result}),Err(error)=>json!({"fault":error.into_plugin_fault()}),
                        }
                    } else {
                        match call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await {
                            Ok(value)=>json!({"exists":value}),Err(error)=>json!({"error":error}),
                        }
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
        adapter_mode: session.handshake().configuration["adapter_mode"]
            .as_str()
            .unwrap_or("valid")
            .into(),
        catalog_mode: session.handshake().configuration["catalog_mode"]
            .as_str()
            .unwrap_or("valid")
            .into(),
        contributes: serde_json::to_value(&session.handshake().contributes).unwrap(),
        retry: session.handshake().configuration["retry"]
            .as_str()
            .unwrap_or("delegate")
            .into(),
        schedule: session.handshake().configuration["schedule"]
            .as_str()
            .unwrap_or("delegate")
            .into(),
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
            if stage == "plugin_attempt" && input["retry_child"].is_string() {
                let nested =
                    super::worker_models::handle(&host, &json!({"failure":input["retry_child"]}))
                        .await;
                if input["retry_child"] == "success" {
                    assert!(nested.get("error").is_none(), "{nested}");
                } else {
                    assert!(nested.get("error").is_some(), "{nested}");
                }
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
mod worker_http {
    use gateway_plugin_sdk::call::middleware::{MiddlewareHeader, NEXT_METHOD, http as wire};
    use gateway_plugin_sdk::{
        ErrorCode, PluginFault,
        client::{
            CallReply, HttpBody, HttpCall, HttpFrame, HttpRequest, HttpResponse, PluginCall,
            ResponseStream,
        },
    };
    use serde_json::json;
    static PREVIOUS: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    pub async fn handle(call: PluginCall) -> Result<CallReply, PluginFault> {
        let original = call.params["request"].clone();
        let mut call = HttpCall::try_from(call)?;
        assert!(
            call.context.account_id.is_none()
                && call.context.attempt_id.is_none()
                && call.context.credential_revision.is_none()
        );
        assert_eq!(call.call_id, "host-call");
        let mode = call
            .request
            .headers
            .iter()
            .find(|h| h.name == "x-fixture-mode")
            .map(|h| String::from_utf8(h.value.clone()).unwrap())
            .unwrap_or_else(|| "pass".into());
        if mode == "deny" {
            return Err(PluginFault::new(ErrorCode::Rejected, "fixture denied"));
        }
        if mode == "dispatch" {
            let body = std::mem::replace(&mut call.request.body, HttpBody::empty());
            let request = HttpRequest {
                settings: call.request.settings.clone(),
                method: "POST".into(),
                uri: "/internal/raw?data=%2F".into(),
                version: wire::Version::Http2,
                headers: call.request.headers.clone(),
                timeout_ms: None,
                body,
            };
            return call.host.dispatch_http(request).await?.into_reply();
        }
        if mode == "dispatch-body-error" {
            let request = HttpRequest {
                settings: serde_json::Value::Null,
                method: "GET".into(),
                uri: "/internal/raw".into(),
                version: wire::Version::Http11,
                headers: vec![],
                timeout_ms: None,
                body: HttpBody::empty(),
            };
            let mut response = call.host.dispatch_http(request).await?;
            let error = response.body.read().await.err().unwrap();
            assert_eq!(error.code, ErrorCode::Fault);
            return HttpResponse::new(
                200,
                HttpBody::from_bytes(b"caught child body error".to_vec()),
            )
            .into_reply();
        }
        if mode == "dispatch-error" {
            let request = HttpRequest {
                settings: serde_json::Value::Null,
                method: "GET".into(),
                uri: "/internal/raw".into(),
                version: wire::Version::Http11,
                headers: vec![],
                timeout_ms: None,
                body: HttpBody::empty(),
            };
            let error = match call.host.dispatch_http(request).await {
                Ok(_) => panic!("must fail"),
                Err(error) => error,
            };
            assert_eq!(error.code, ErrorCode::Fault);
            assert_eq!(error.send_state, gateway_plugin_sdk::SendState::Ambiguous);
            return HttpResponse::new(200, HttpBody::from_bytes(b"caught child error".to_vec()))
                .into_reply();
        }
        if mode == "dispatch-invalid" || mode == "dispatch-missing" {
            let targets: &[&str] = if mode == "dispatch-invalid" {
                &[
                    "https://external.test/x",
                    "relative",
                    "http://127.0.0.1:9/x",
                ]
            } else {
                &["/internal/raw"]
            };
            for uri in targets {
                let request = HttpRequest {
                    settings: serde_json::Value::Null,
                    method: "GET".into(),
                    uri: (*uri).into(),
                    version: wire::Version::Http11,
                    headers: vec![],
                    timeout_ms: None,
                    body: HttpBody::empty(),
                };
                let error = match call.host.dispatch_http(request).await {
                    Ok(_) => panic!("must reject"),
                    Err(error) => error,
                };
                assert_eq!(
                    error.code,
                    if mode == "dispatch-invalid" {
                        ErrorCode::InvalidInput
                    } else {
                        ErrorCode::Unsupported
                    }
                );
            }
            return HttpResponse::new(200, HttpBody::from_bytes(b"rejected".to_vec())).into_reply();
        }
        if mode == "dispatch-total" {
            for number in 0..65 {
                let request = HttpRequest {
                    settings: serde_json::Value::Null,
                    method: "GET".into(),
                    uri: "/internal/raw".into(),
                    version: wire::Version::Http11,
                    headers: vec![],
                    timeout_ms: None,
                    body: HttpBody::empty(),
                };
                if number == 64 {
                    let error = match call.host.dispatch_http(request).await {
                        Ok(_) => panic!("must exhaust total budget"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code, ErrorCode::Capacity);
                } else {
                    let mut response = call.host.dispatch_http(request).await?;
                    while response.body.read().await?.is_some() {}
                    response.body.close().await?;
                }
            }
            return HttpResponse::new(200, HttpBody::from_bytes(b"bounded".to_vec())).into_reply();
        }
        if mode == "no-authority" {
            for method in [
                "host.model.execute",
                "host.models.list",
                "host.data.accounts.list",
                "host.groups.ensure",
            ] {
                let error = call
                    .host
                    .call(method, json!({}), vec![])
                    .await
                    .err()
                    .unwrap()
                    .into_plugin_fault();
                assert_eq!(error.code, ErrorCode::PermissionDenied);
            }
            return HttpResponse::new(200, HttpBody::from_bytes(b"denied".to_vec())).into_reply();
        }
        if mode == "leak" {
            *PREVIOUS.lock().unwrap() = original["body"]["handle"].as_str().map(str::to_owned);
            return HttpResponse::new(200, HttpBody::empty()).into_reply();
        }
        if mode == "stale" {
            let handle = PREVIOUS.lock().unwrap().clone().unwrap();
            let error = call
                .host
                .call(
                    wire::BODY_READ_METHOD,
                    json!({"handle":handle,"maximum_bytes":64}),
                    vec![],
                )
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(error.code, ErrorCode::Conflict);
            return HttpResponse::new(200, HttpBody::from_bytes(b"isolated".to_vec())).into_reply();
        }
        if mode == "capacity" {
            let mut handles = vec![];
            loop {
                match call
                    .host
                    .call(wire::BODY_CREATE_METHOD, json!({}), vec![])
                    .await
                {
                    Ok(reply) => {
                        let handle = reply.result["handle"].as_str().unwrap().to_owned();
                        call.host
                            .call(
                                wire::BODY_WRITE_METHOD,
                                json!({"handle":handle,"eof":true,"trailers":null}),
                                vec![],
                            )
                            .await
                            .map_err(|e| e.into_plugin_fault())?;
                        handles.push(handle);
                        assert!(handles.len() < 64);
                    }
                    Err(error) => {
                        assert_eq!(error.into_plugin_fault().code, ErrorCode::Capacity);
                        break;
                    }
                }
            }
            assert_eq!(handles.len(), 31);
            for handle in handles {
                call.host
                    .call(wire::BODY_CLOSE_METHOD, json!({"handle":handle}), vec![])
                    .await
                    .map_err(|e| e.into_plugin_fault())?;
            }
            return HttpResponse::new(200, HttpBody::from_bytes(b"bounded".to_vec())).into_reply();
        }
        if mode == "invalid-chunks" || mode == "forged-response" {
            return Ok(CallReply::stream(
                json!({"status":200,"version":"HTTP/1.1","headers":[],"body":{"kind":if mode=="invalid-chunks" {"stream"}else{"empty"}},"response":if mode=="forged-response" {json!("another-scope")}else{json!(null)},"session":false}),
                vec![],
                ResponseStream::from_chunks(if mode == "invalid-chunks" {
                    vec![vec![2, 1, 2]]
                } else {
                    vec![]
                }),
            ));
        }
        if matches!(
            mode.as_str(),
            "upgrade"
                | "changed-upgrade"
                | "discard-upgrade"
                | "upgrade-error"
                | "upgrade-send-error"
        ) {
            let mut response = call
                .upgrade(vec!["fixture".into()], |mut socket| async move {
                    if let Some(message) = socket.receive().await? {
                        socket.sender.send(message).await?;
                    }
                    Ok(())
                })
                .await?;
            if mode == "discard-upgrade" {
                drop(response);
                return HttpResponse::new(403, HttpBody::empty()).into_reply();
            }
            if mode == "changed-upgrade" {
                response.status = 403;
            }
            return response.into_reply();
        }
        if mode == "bad-next" {
            let mut invalid = original.clone();
            invalid["method"] = json!("bad\rmethod");
            let error = call
                .host
                .call(NEXT_METHOD, invalid, vec![])
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(error.code, ErrorCode::InvalidInput);
        }
        if mode == "settings-forged" {
            call.request.settings = json!({"fake_baseline":true});
        }
        if mode == "prefix" {
            let reply = call
                .host
                .call(
                    wire::BODY_READ_METHOD,
                    json!({"handle":original["body"]["handle"],"maximum_bytes":3}),
                    vec![],
                )
                .await
                .map_err(|error| error.into_plugin_fault())?;
            assert_eq!(reply.payload, b"abc");
        }
        if mode == "swallow-body-error" {
            let error = call.request.body.read().await.err().unwrap();
            assert_eq!(error.code, ErrorCode::Fault);
            assert_eq!(error.message, "HTTP body read failed");
            return HttpResponse::new(200, HttpBody::from_bytes(b"swallowed".to_vec()))
                .into_reply();
        }
        if mode == "swallow-next-error" {
            let error = call.next.run(call.request).await.err().unwrap();
            assert_eq!(error.code, ErrorCode::Rejected);
            assert_eq!(error.message, "HTTP middleware terminal failed");
            return HttpResponse::new(200, HttpBody::from_bytes(b"swallowed".to_vec()))
                .into_reply();
        }
        if mode == "read-wait" {
            let _ = call.request.body.read().await?;
        }
        call.request.uri = "/after?raw=%2F".into();
        call.request.headers.push(MiddlewareHeader {
            name: "x-chain".into(),
            value: b"plugin".to_vec(),
        });
        if matches!(mode.as_str(), "map" | "pipe-short") {
            call.request.body = call.request.body.map_frames(|frame| {
                Ok(Some(match frame {
                    HttpFrame::Data(bytes) => HttpFrame::Data(bytes.to_ascii_uppercase()),
                    other => other,
                }))
            });
        }
        let mut response = call.next.run(call.request).await?;
        if mode == "double" {
            let mut second = original;
            second["body"] = json!({"kind":"empty"});
            let error = call
                .host
                .call(NEXT_METHOD, second, vec![])
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(error.code, ErrorCode::Conflict);
        }
        response.headers.push(MiddlewareHeader {
            name: "x-return".into(),
            value: b"plugin".to_vec(),
        });
        if matches!(mode.as_str(), "map" | "stream-wait") {
            response.body = response.body.map_frames(|frame| {
                Ok(Some(match frame {
                    HttpFrame::Data(bytes) => HttpFrame::Data(bytes.to_ascii_uppercase()),
                    other => other,
                }))
            });
        }
        response.into_reply()
    }
}

async fn worker_service(
    call: gateway_plugin_sdk::client::PluginCall,
) -> Result<CallReply, PluginFault> {
    use gateway_plugin_sdk::call::{
        middleware::NEXT_METHOD,
        services::{self as wire, settings::ApiKeyExists},
    };
    use gateway_plugin_sdk::client::{
        MiddlewareInput, MiddlewareOutput, ServiceCall, ServiceResponse,
    };
    let host = call.host.clone();
    let instance = call.context.instance_id.clone();
    let mut input = call.params["input"].clone();
    let call = ServiceCall::decode(call)?;
    assert_eq!(call.call_id, "service-fixture-call");
    assert_eq!(
        call.parent_call_id.as_deref(),
        Some("service-fixture-parent")
    );
    let mode = call.request_id.clone();
    if mode == "host-service" {
        return ServiceResponse::from_result::<ApiKeyExists>(
            host.service::<ApiKeyExists>(()).await,
        )?
        .encode();
    }
    if mode == "reject" {
        let mut fault = PluginFault::new(ErrorCode::Rejected, "fixture service rejection");
        fault.send_state = gateway_plugin_sdk::SendState::Ambiguous;
        fault.details = Some(json!({"fixture":"original"}));
        return Err(fault);
    }
    if mode == "typed" {
        let call = call.into_typed::<ApiKeyExists>()?;
        let result = call.next.run(()).await;
        return ServiceResponse::from_result::<ApiKeyExists>(result)?.encode();
    }
    if mode == "bad-reply" {
        return Ok(CallReply::unary(json!({"unknown":true}), vec![]));
    }
    if mode == "binary-reply" {
        return Ok(CallReply::unary(json!({"Ok":false}), vec![1]));
    }
    if mode == "short" {
        return Ok(CallReply::unary(json!({"Ok":false}), vec![]));
    }
    if mode == "bad-output" {
        return Ok(CallReply::unary(json!({"Ok":"wrong-type"}), vec![]));
    }
    if mode == "forward" {
        return call.forward().await?.encode();
    }
    if mode == "bad-input" {
        input = json!({"value":"wrong-type","steps":[]});
    }
    if mode == "rewrite" {
        input["value"] = json!(input["value"].as_u64().unwrap() + 1);
        input["steps"]
            .as_array_mut()
            .unwrap()
            .push(json!(format!("{instance}:in")));
    }
    if mode == "double" {
        assert!(
            host.call(NEXT_METHOD, input.clone(), vec![1])
                .await
                .is_err()
        );
    }
    if mode == "no-authority" {
        assert!(
            host.call("host.keys.list", json!({}), vec![])
                .await
                .is_err()
        );
        assert!(
            host.call("host.http.dispatch", json!({}), vec![])
                .await
                .is_err()
        );
    }
    let reply = host
        .call(NEXT_METHOD, input, vec![])
        .await
        .map_err(|e| e.into_plugin_fault())?;
    if mode == "double" {
        assert!(host.call(NEXT_METHOD, json!(null), vec![]).await.is_err());
    }
    if mode == "wait" {
        return futures::future::pending().await;
    }
    let mut result: wire::Response = serde_json::from_value(reply.result).unwrap();
    if mode == "rewrite" {
        let output = result.as_mut().unwrap();
        output["value"] = json!(output["value"].as_u64().unwrap() + 100);
        output["steps"]
            .as_array_mut()
            .unwrap()
            .push(json!(format!("{instance}:out")));
    }
    Ok(CallReply::unary(
        serde_json::to_value(result).unwrap(),
        vec![],
    ))
}

async fn worker_websocket(
    call: gateway_plugin_sdk::client::PluginCall,
) -> Result<gateway_plugin_sdk::client::CallReply, PluginFault> {
    use gateway_plugin_sdk::client::{
        CallReply, MiddlewareInput, MiddlewareOutput, WebSocketCall, WebSocketKind,
        WebSocketMessage,
    };
    let mut call = WebSocketCall::decode(call)?;
    let mode = call
        .headers
        .iter()
        .find(|h| h.name == "x-fixture-mode")
        .map(|h| String::from_utf8(h.value.clone()).unwrap())
        .unwrap_or_default();
    if mode == "wait" {
        return futures::future::pending().await;
    }
    if mode == "forged" {
        return Ok(CallReply::unary(
            json!({"kind":{"kind":"binary"},"payload":{"kind":"handle","handle":"not-this-call"}}),
            vec![],
        ));
    }
    if mode == "drop" {
        return Option::<WebSocketMessage>::None.encode();
    }
    if mode == "send" || mode == "send-fail" {
        let sent = call
            .sender
            .send(WebSocketMessage::new(WebSocketKind::Binary, vec![0, 255]))
            .await;
        if mode == "send-fail" {
            assert!(sent.is_err());
            return Option::<WebSocketMessage>::None.encode();
        }
        sent?;
    }
    if mode == "double" {
        let request = json!({"kind":{"kind":"binary"},"payload":{"kind":"bytes"}});
        call.host
            .call(
                gateway_plugin_sdk::call::middleware::NEXT_METHOD,
                request.clone(),
                vec![1],
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
        assert!(
            call.host
                .call(
                    gateway_plugin_sdk::call::middleware::NEXT_METHOD,
                    request,
                    vec![2]
                )
                .await
                .is_err()
        );
        return Option::<WebSocketMessage>::None.encode();
    }
    if mode == "partial" {
        assert!(!call.message.payload.read().await?.unwrap().is_empty());
    }
    if mode == "rewrite" {
        let mut bytes = call.message.payload.collect().await?;
        bytes.push(42);
        call.message = WebSocketMessage::new(WebSocketKind::Binary, bytes);
    }
    let result = call.next.run(call.message).await;
    if mode == "both-fail" {
        assert!(result.is_err());
        assert!(
            call.sender
                .send(WebSocketMessage::new(WebSocketKind::Binary, vec![0]))
                .await
                .is_err()
        );
        return Option::<WebSocketMessage>::None.encode();
    }
    if mode == "next-fail" {
        assert!(result.is_err());
        return Option::<WebSocketMessage>::None.encode();
    }
    result?.encode()
}

async fn typed_service<O: gateway_plugin_sdk::call::services::Operation>(
    host: &gateway_plugin_sdk::client::HostClient,
    input: serde_json::Value,
) -> serde_json::Value {
    let input = serde_json::from_value::<O::Input>(input).unwrap();
    match host.service::<O>(input).await {
        Ok(value) => json!({"result":value}),
        Err(error) => json!({"error":error}),
    }
}
