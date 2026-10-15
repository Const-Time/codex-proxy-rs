use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    client::{CallFuture, CallReply, PluginCall, PluginHandler, PluginSession, SessionConfig},
};
use serde_json::json;
mod worker_credentials {
    //! SDK subprocess probes for the actual Provider-bound credential callbacks.
    use gateway_plugin_sdk::{ErrorCode, PluginFault, call::host::*, client::HostClient};
    use serde::de::DeserializeOwned;
    use serde_json::{Value, json};

    pub(super) async fn public_login(
        host: &HostClient,
        url: &str,
        query: &str,
    ) -> Result<Value, PluginFault> {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let codes = url::form_urlencoded::parse(query.as_bytes())
            .filter(|(key, _)| key == "code")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        if codes.len() != 1 || codes[0].is_empty() {
            return Err(PluginFault::new(
                ErrorCode::InvalidInput,
                "one authorization code required",
            ));
        }
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "authorization_code")
            .append_pair("client_id", "public-callback-fixture")
            .append_pair("code", &codes[0])
            .finish()
            .into_bytes();
        let token = host
            .http(
                HttpRequest {
                    method: "POST".into(),
                    url: url.into(),
                    headers: vec![(
                        "content-type".into(),
                        "application/x-www-form-urlencoded".into(),
                    )],
                },
                body,
            )
            .await?;
        if token.status != 200 {
            return Err(PluginFault::new(
                ErrorCode::Fault,
                "authorization exchange rejected",
            ));
        }
        let bytes = token.body.collect(1024 * 1024).await?;
        let token: Value = serde_json::from_slice(&bytes)
            .map_err(|_| PluginFault::new(ErrorCode::Fault, "invalid token response"))?;
        let access = token["access_token"]
            .as_str()
            .ok_or_else(|| PluginFault::new(ErrorCode::Fault, "missing access token"))?;
        let claims: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(access.split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap();
        let auth = &claims["https://api.openai.com/auth"];
        let input = json!({"action":"create", "provider_id":"openai", "facts":{
            "name":"Public callback OAuth", "authentication_kind":"oauth", "has_refresh_token":true,
            "upstream_user_id":auth["chatgpt_user_id"], "upstream_account_id":auth["chatgpt_account_id"],
            "access_token_expires_at_ms":claims["exp"].as_i64().unwrap()*1000, "next_refresh_at_ms":null,
            "material":{"schema_version":1,"installation_id":uuid::Uuid::new_v4().to_string(),
                "access_token":access, "refresh_token":token["refresh_token"],"id_token":null,"cookies":[]}
        }});
        let saved: Value = read(host, "host.auth.save", input).await?;
        let api_key_exists = host
            .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
            .await
            .map_err(|e| PluginFault::new(ErrorCode::Fault, e.message))?;
        Ok(json!({"saved":saved,"api_key_exists":api_key_exists}))
    }

    async fn request(
        host: &HostClient,
        method: &str,
        params: Value,
        payload: Vec<u8>,
    ) -> Result<
        Result<gateway_plugin_sdk::client::HostReply, gateway_plugin_sdk::client::SessionError>,
        PluginFault,
    > {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            host.call(method, params, payload),
        )
        .await
        .map_err(|_| {
            PluginFault::new(
                ErrorCode::Fault,
                format!("credential probe timed out: {method}"),
            )
        })
    }
    async fn read<T: DeserializeOwned>(
        host: &HostClient,
        method: &str,
        input: Value,
    ) -> Result<T, PluginFault> {
        let reply = request(host, method, json!({}), serde_json::to_vec(&input).unwrap())
            .await?
            .map_err(|e| e.into_plugin_fault())?;
        if reply.result != json!({}) {
            return Err(PluginFault::new(
                ErrorCode::Fault,
                "credential probe envelope changed",
            ));
        }
        serde_json::from_slice(&reply.payload).map_err(|_| {
            PluginFault::new(
                ErrorCode::Fault,
                "credential probe response could not be decoded",
            )
        })
    }
    async fn denied(
        host: &HostClient,
        method: &str,
        params: Value,
        payload: Vec<u8>,
        expected: ErrorCode,
    ) -> Result<(), PluginFault> {
        let result = request(host, method, params, payload).await?;
        let actual = result
            .err()
            .ok_or_else(|| {
                PluginFault::new(
                    ErrorCode::Fault,
                    format!(
                        "credential probe unexpectedly accepted {method}; expected {expected:?}"
                    ),
                )
            })?
            .into_plugin_fault()
            .code;
        if actual != expected {
            return Err(PluginFault::new(
                ErrorCode::Fault,
                format!("credential probe {method}: expected {expected:?}, received {actual:?}"),
            ));
        }
        Ok(())
    }

    pub async fn run(host: &HostClient, mode: &str, input: &Value) -> Result<(), PluginFault> {
        let selected = input["fixture_account"].as_str().unwrap();
        if mode.starts_with("callback-credentials-save") {
            return replace(host, mode, input).await;
        }
        if mode != "callback-credentials" {
            host.call(
                "host.upstream.http.do",
                json!({"method":"POST","path":"audit","headers":[],"query":[]}),
                b"credential-barrier".to_vec(),
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            for (method, request) in [
                ("host.auth.list", json!({"limit":200})),
                ("host.auth.get_runtime", json!({"account_id":selected})),
                ("host.auth.get", json!({"account_id":selected})),
            ] {
                denied(
                    host,
                    method,
                    json!({}),
                    serde_json::to_vec(&request).unwrap(),
                    ErrorCode::Conflict,
                )
                .await?;
            }
            return Ok(());
        }
        let provider = input["fixture_provider"].as_str().unwrap();
        let other = if provider == "openai" {
            "xai"
        } else {
            "openai"
        };
        for (filter, cursor, expected) in [
            (None, None, 1),
            (Some(provider), None, 1),
            (Some(other), None, 0),
            (None, Some(selected), 0),
        ] {
            let list: AuthListResult = read(
                host,
                "host.auth.list",
                json!({"provider_id":filter,"cursor":cursor,"limit":200}),
            )
            .await?;
            assert_eq!(list.accounts.len(), expected);
            assert!(list.next_cursor.is_none());
            for account in &list.accounts {
                assert_eq!(account.account_id, selected);
                assert_eq!(account.provider_id, provider);
                assert!(
                    !serde_json::to_string(account)
                        .unwrap()
                        .contains("fixture-provider-secret")
                );
            }
        }
        let runtime: AuthRuntimeAccount = read(
            host,
            "host.auth.get_runtime",
            json!({"account_id":selected}),
        )
        .await?;
        assert_eq!(runtime.account_id, selected);
        assert_eq!(
            runtime.credential_revision,
            input["fixture_credential_revision"].as_u64().unwrap()
        );
        let credential: AuthCredential =
            read(host, "host.auth.get", json!({"account_id":selected})).await?;
        assert_eq!(credential.account_id, selected);
        assert_eq!(credential.credential_revision, runtime.credential_revision);
        // Explicit sensitive export is the sole place the controlled test material is returned.
        assert!(
            serde_json::to_string(&credential.facts.material)
                .unwrap()
                .contains("fixture-provider-secret")
        );
        for method in ["host.auth.get_runtime", "host.auth.get"] {
            denied(
                host,
                method,
                json!({}),
                serde_json::to_vec(&json!({"account_id":input["fixture_forbidden"]})).unwrap(),
                ErrorCode::PermissionDenied,
            )
            .await?;
        }
        for (method, params, request) in [
            (
                "host.auth.get",
                json!({"actor":"system"}),
                json!({"account_id":selected}),
            ),
            (
                "host.auth.get",
                json!({}),
                json!({"account_id":selected,"actor":"system"}),
            ),
            ("host.auth.get_runtime", json!({}), json!({"account_id":""})),
            ("host.auth.list", json!({}), json!({"limit":0})),
            ("host.auth.list", json!({}), json!({"limit":201})),
            ("host.auth.list", json!({}), json!({"limit":1,"cursor":""})),
            (
                "host.auth.list",
                json!({}),
                json!({"limit":1,"provider_id":""}),
            ),
        ] {
            denied(
                host,
                method,
                params,
                serde_json::to_vec(&request).unwrap(),
                ErrorCode::InvalidInput,
            )
            .await?;
        }
        denied(
            host,
            "host.auth.get",
            json!({}),
            vec![b' '; 512 * 1024 + 1],
            ErrorCode::InvalidInput,
        )
        .await?;
        for save in [
            AuthSaveRequest::Create {
                provider_id: provider.into(),
                facts: credential.facts.clone(),
            },
            AuthSaveRequest::Replace {
                account_id: input["fixture_forbidden"].as_str().unwrap().into(),
                credential_revision: runtime.credential_revision,
                facts: credential.facts,
            },
        ] {
            denied(
                host,
                "host.auth.save",
                json!({}),
                serde_json::to_vec(&save).unwrap(),
                ErrorCode::PermissionDenied,
            )
            .await?;
        }
        host.call(
            "host.upstream.http.do",
            json!({"method":"POST","path":"responses","headers":[],"query":[]}),
            b"fixture-bound-auth".to_vec(),
        )
        .await
        .map_err(|e| e.into_plugin_fault())?;
        Ok(())
    }

    async fn replace(host: &HostClient, mode: &str, input: &Value) -> Result<(), PluginFault> {
        let selected = input["fixture_account"].as_str().unwrap();
        let exported: AuthCredential =
            read(host, "host.auth.get", json!({"account_id":selected})).await?;
        let mut facts = exported.facts;
        if mode.ends_with("invalid") {
            facts.material.insert("schema_version".into(), json!(999));
        } else {
            facts
                .material
                .insert("access_token".into(), input["fixture_save_token"].clone());
            if let Some(expiry) = input["fixture_save_expiry_ms"].as_i64() {
                facts.access_token_expires_at_ms = Some(expiry);
            }
        }
        let request = AuthSaveRequest::Replace {
            account_id: selected.into(),
            credential_revision: exported.credential_revision,
            facts,
        };
        let save = async {
            if mode.ends_with("gate") {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            read::<AuthSaveResult>(
                host,
                "host.auth.save",
                serde_json::to_value(&request).unwrap(),
            )
            .await
        };
        let saved = if mode.ends_with("gate") {
            let before = host.call(
                "host.upstream.http.do",
                json!({"method":"POST","path":"audit","headers":[],"query":[]}),
                b"credential-barrier".to_vec(),
            );
            let (before, saved) = tokio::join!(before, save);
            before.map_err(|e| e.into_plugin_fault())?;
            saved?
        } else {
            save.await?
        };
        assert_eq!(saved.account_id, selected);
        assert_eq!(saved.credential_revision, exported.credential_revision + 1);
        let current: AuthRuntimeAccount = read(
            host,
            "host.auth.get_runtime",
            json!({"account_id":selected}),
        )
        .await?;
        assert_eq!(current.credential_revision, saved.credential_revision);
        let list: AuthListResult = read(host, "host.auth.list", json!({"limit":200})).await?;
        assert_eq!(
            list.accounts[0].credential_revision,
            saved.credential_revision
        );
        denied(
            host,
            "host.auth.save",
            json!({}),
            serde_json::to_vec(&request).unwrap(),
            ErrorCode::Conflict,
        )
        .await?;
        if !mode.ends_with("websocket") {
            host.call(
                "host.upstream.http.do",
                json!({"method":"POST","path":"responses","headers":[],"query":[]}),
                b"fixture-saved-auth".to_vec(),
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
        }
        Ok(())
    }
    pub(super) async fn on_websocket(host: &HostClient, input: &Value) -> Result<(), PluginFault> {
        replace(host, "callback-credentials-save-websocket", input).await
    }
}

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
        if mode == "quota" {
            let reply = host
                .call(
                    input["method"].as_str().unwrap(),
                    input["params"].clone(),
                    serde_json::to_vec(&input["request"]).unwrap(),
                )
                .await
                .map_err(|error| error.into_plugin_fault())?;
            assert_eq!(reply.result, json!({}));
            let value: gateway_plugin_sdk::call::data::QuotaFacts =
                serde_json::from_slice(&reply.payload).unwrap();
            return Ok(serde_json::to_value(value).unwrap());
        }
        if mode == "credentials" {
            let method = input["method"].as_str().unwrap();
            let reply = host
                .call(
                    method,
                    input.get("params").cloned().unwrap_or_else(|| json!({})),
                    serde_json::to_vec(&input["request"]).unwrap(),
                )
                .await
                .map_err(|error| error.into_plugin_fault())?;
            assert_eq!(reply.result, json!({}));
            let value = match method {
                "host.auth.list" => serde_json::to_value(
                    serde_json::from_slice::<AuthListResult>(&reply.payload).unwrap(),
                )
                .unwrap(),
                "host.auth.get_runtime" => serde_json::to_value(
                    serde_json::from_slice::<AuthRuntimeAccount>(&reply.payload).unwrap(),
                )
                .unwrap(),
                "host.auth.get" => serde_json::to_value(
                    serde_json::from_slice::<AuthCredential>(&reply.payload).unwrap(),
                )
                .unwrap(),
                "host.auth.save" => serde_json::to_value(
                    serde_json::from_slice::<AuthSaveResult>(&reply.payload).unwrap(),
                )
                .unwrap(),
                _ => panic!("unknown fixture credential method"),
            };
            return Ok(value);
        }
        if mode == "service" {
            return Ok(
                json!({"exists":host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|e| gateway_plugin_sdk::PluginFault::new(gateway_plugin_sdk::ErrorCode::Fault,e.message))?}),
            );
        }
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
        let mut body = json!({"model":model,"input":"fixture","mode":input["provider_mode"],"failure":input["failure"],"nested_model":input["nested_model"],"after_next":input["after_next"],"retry_child":input["retry_child"],"route":input["route"],"service_probe":input["service_probe"]});
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

mod worker_outbound {
    use gateway_plugin_sdk::{ErrorCode, PluginFault, call::host::HttpRequest, client::HostClient};
    use serde_json::{Value, json};
    pub(super) async fn run(host: &HostClient, input: &Value) -> Result<Value, PluginFault> {
        let mode = input["mode"].as_str().unwrap_or("unary");
        let url = input["url"]
            .as_str()
            .unwrap_or("http://fixture.invalid/external");
        let mut headers = vec![("x-explicit".into(), "plugin-value".into())];
        if input["omit_auth"] != true {
            headers.push(("authorization".into(), "Bearer plugin-explicit".into()));
        }
        let request = HttpRequest {
            method: "POST".into(),
            url: url.into(),
            headers,
        };
        let body = input["body"]
            .as_str()
            .map_or_else(|| vec![0, 255, 1, 0], |s| s.as_bytes().to_vec());
        if mode == "raw" {
            return match host
                .call(
                    input["method"].as_str().unwrap(),
                    input["params"].clone(),
                    vec![],
                )
                .await
            {
                Ok(reply) => Ok(json!({"result":reply.result,"bytes":reply.payload})),
                Err(error) => Ok(json!({"error":error.into_plugin_fault()})),
            };
        }
        if mode == "open" || mode == "capacity" || mode == "handles" || mode == "concurrent" {
            let mut handles = vec![];
            for _ in 0..if mode == "capacity" { 16 } else { 1 } {
                let reply = host
                    .call(
                        "host.http.do_stream",
                        serde_json::to_value(&request).unwrap(),
                        body.clone(),
                    )
                    .await
                    .map_err(|e| e.into_plugin_fault())?;
                handles.push(reply.result["stream"].as_str().unwrap().to_owned());
            }
            let first = handles[0].clone();
            if mode == "open" {
                return Ok(json!({"stream":first}));
            }
            if mode == "concurrent" {
                let (reader_host, first) = (host.clone(), first.clone());
                let read = tokio::spawn(async move {
                    reader_host
                        .call(
                            "host.http.stream_read",
                            json!({"stream":first,"maximum_bytes":3}),
                            vec![],
                        )
                        .await
                });
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let fault = host
                    .call(
                        "host.http.stream_read",
                        json!({"stream":handles[0],"maximum_bytes":3}),
                        vec![],
                    )
                    .await
                    .err()
                    .unwrap()
                    .into_plugin_fault();
                assert_eq!(fault.code, ErrorCode::Conflict);
                host.call(
                    "host.http.stream_close",
                    json!({"stream":handles[0]}),
                    vec![],
                )
                .await
                .map_err(|e| e.into_plugin_fault())?;
                let result = read.await.unwrap().err().unwrap().into_plugin_fault();
                assert_eq!(result.code, ErrorCode::PermissionDenied);
                return Ok(json!({"checked":true}));
            }
            if mode == "capacity" {
                let fault = host
                    .call(
                        "host.http.do_stream",
                        serde_json::to_value(&request).unwrap(),
                        body.clone(),
                    )
                    .await
                    .err()
                    .unwrap()
                    .into_plugin_fault();
                assert_eq!(fault.code, ErrorCode::Capacity);
                assert_eq!(fault.send_state, gateway_plugin_sdk::SendState::Sent);
            } else {
                for maximum in [0, 65537] {
                    let fault = host
                        .call(
                            "host.http.stream_read",
                            json!({"stream":first,"maximum_bytes":maximum}),
                            vec![],
                        )
                        .await
                        .err()
                        .unwrap()
                        .into_plugin_fault();
                    assert_eq!(fault.code, ErrorCode::InvalidInput);
                }
                let fault = host
                    .call(
                        "host.http.stream_read",
                        json!({"stream":"unowned","maximum_bytes":3}),
                        vec![],
                    )
                    .await
                    .err()
                    .unwrap()
                    .into_plugin_fault();
                assert_eq!(fault.code, ErrorCode::PermissionDenied);
            }
            host.call("host.http.stream_close", json!({"stream":first}), vec![])
                .await
                .map_err(|e| e.into_plugin_fault())?;
            let fault = host
                .call(
                    "host.http.stream_read",
                    json!({"stream":first,"maximum_bytes":3}),
                    vec![],
                )
                .await
                .err()
                .unwrap()
                .into_plugin_fault();
            assert_eq!(fault.code, ErrorCode::PermissionDenied);
            // Closing a handle frees capacity before the next dispatch.
            if mode == "capacity" {
                host.call(
                    "host.http.do_stream",
                    serde_json::to_value(&request).unwrap(),
                    body,
                )
                .await
                .map_err(|e| e.into_plugin_fault())?;
            }
            return Ok(json!({"checked":true}));
        }
        if mode == "unary" || mode == "large" {
            return match host
                .call("host.http.do", serde_json::to_value(request).unwrap(), body)
                .await
            {
                Ok(reply) => Ok(json!({"status":reply.result["status"],"bytes":reply.payload})),
                Err(error) => Ok(json!({"error":error.into_plugin_fault()})),
            };
        }
        let mut response = host.http(request, body).await?;
        let status = response.status;
        if mode == "resume" {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(10), response.body.read())
                    .await
                    .is_err()
            );
        }
        if mode == "close" {
            response.body.close().await?;
            response.body.close().await?;
            assert!(response.body.read().await?.is_none());
            return Ok(json!({"closed":true}));
        }
        let bytes = response.body.collect(9 * 1024 * 1024).await?;
        Ok(json!({"status":status,"bytes":bytes}))
    }
}

mod upstream_callbacks {
    use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
    use serde_json::json;

    pub(super) async fn run(
        host: &HostClient,
        mode: &str,
        input: &serde_json::Value,
    ) -> Result<(), PluginFault> {
        if mode.starts_with("callback-credentials") {
            return super::worker_credentials::run(host, mode, input).await;
        }
        if mode.starts_with("callback-external") {
            let result=super::worker_outbound::run(host,&json!({"url":input["external_url"],"body":"fixture-external","omit_auth":input["omit_auth"],"mode":if mode=="callback-external-unary" {"unary"} else {"stream"}})).await?;
            assert_eq!(result["status"], 200);
            assert_eq!(
                result["bytes"],
                serde_json::to_value(b"fixture-response".to_vec()).unwrap()
            );
            if mode.ends_with("fault") {
                return Err(PluginFault::new(
                    ErrorCode::Fault,
                    "external effect before inference",
                ));
            }
            return Ok(());
        }
        if mode == "callback-model-abandon" {
            let request = gateway_plugin_sdk::call::host::ModelExecuteRequest {
                client_key_id: None,
                model: "gpt-native".into(),
                protocol: "openai".into(),
                operation: gateway_plugin_sdk::call::host::ModelOperation::Generate,
                provider: None,
                account_id: None,
                previous_response_id: None,
            };
            host.call(
                "host.model.execute_stream",
                serde_json::to_value(request).unwrap(),
                serde_json::to_vec(&json!({"model":"gpt-native","input":"unread child"})).unwrap(),
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            return Ok(());
        }
        if mode == "callback-state" {
            let params = json!({"namespace":"counter","key":"upstream"});
            let reply = host
                .call("host.state.get", params.clone(), vec![])
                .await
                .map_err(|e| e.into_plugin_fault())?;
            assert!(reply.result["record"].is_null());
            let reply = host
            .call(
                "host.state.put",
                json!({"namespace":"counter","key":"upstream","expected_version":null,"value":42}),
                vec![],
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            let version = reply.result["version"].as_u64().unwrap();
            let conflict = host.call("host.state.put", json!({"namespace":"counter","key":"upstream","expected_version":null,"value":43}), vec![]).await.err().unwrap().into_plugin_fault();
            assert_eq!(conflict.code, ErrorCode::Conflict);
            let reply = host
                .call("host.state.get", params, vec![])
                .await
                .map_err(|e| e.into_plugin_fault())?;
            assert_eq!(reply.result["record"]["value"], 42);
            host.call(
                "host.state.delete",
                json!({"namespace":"counter","key":"upstream","expected_version":version}),
                vec![],
            )
            .await
            .map_err(|e| e.into_plugin_fault())?;
            for method in [
                gateway_plugin_sdk::call::data::ACCOUNTS_LIST,
                gateway_plugin_sdk::call::shared_budgets::RESET,
                gateway_plugin_sdk::call::resources::GROUP_ENSURE,
            ] {
                let fault = host
                    .call(method, json!({}), vec![])
                    .await
                    .err()
                    .unwrap()
                    .into_plugin_fault();
                assert_eq!(fault.code, ErrorCode::PermissionDenied);
            }
            let fault = host.call("host.state.put", json!({"namespace":"counter","key":"invalid","expected_version":null,"value":"wrong-schema"}), vec![]).await.err().unwrap().into_plugin_fault();
            assert_eq!(fault.code, ErrorCode::InvalidInput);
            return Ok(());
        }
        if mode.starts_with("callback-http") {
            let result = super::worker_models::handle(host, &json!({"mode":"http"})).await;
            assert_eq!(result["bytes"], json!([0, 255, 1, 0]), "{result}");
        } else {
            let mut input =
                json!({"mode":if mode == "callback-model-stream" {"stream"} else {"unary"}});
            if mode == "callback-model-cancel" {
                input["provider_mode"] = json!("stall");
            }
            if mode == "callback-model-deny" {
                input["account"] = json!("acct_allowed");
                let result = super::worker_models::handle(host, &input).await;
                assert!(
                    result.get("error").is_some(),
                    "a child must not acquire its parent's account lease: {result}"
                );
                input["account"] = serde_json::Value::Null;
                input["key"] = json!("key_disabled");
                let result = super::worker_models::handle(host, &input).await;
                assert_eq!(result["error"]["code"], "permission_denied", "{result}");
                return Ok(());
            }
            let result = super::worker_models::handle(host, &input).await;
            assert!(result.get("error").is_none(), "{result}");
            assert!(result["events"].as_u64().unwrap() > 0);
        }
        if mode.ends_with("-fault") {
            return Err(PluginFault::new(
                ErrorCode::Fault,
                "fixture fault after callback effects",
            ));
        }
        Ok(())
    }
}

struct Worker {
    callback_oauth_url: String,
    registration_signal: String,
    state_migration: String,
    state_migration_signal: String,
    public_callbacks: bool,
    public_resource: bool,
    command_registration: serde_json::Value,
    command_result: serde_json::Value,
    adapter_models: serde_json::Value,
    adapter_provider: String,
    adapter_mode: String,
    adapter_url: String,
    adapter_transport: String,
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
            "command_line.register" => {
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Registration);
                assert_eq!(call.params, json!({}));
                assert!(call.payload.is_empty());
                let registration = if self.command_registration.is_null() {
                    json!({"commands":[{"name":"probe","description":"Inspect command execution","parameters":[{"name":"input","description":"Callback input","value_type":"string","required":false,"sensitive":true}]}]})
                } else { self.command_registration.clone() };
                let payload = if self.adapter_mode=="command-register-size" {vec![b' ';65537]} else {serde_json::to_vec(&registration).unwrap()};
                Ok(CallReply::unary(if self.adapter_mode=="command-register-envelope" {json!({"extra":true})} else {json!({})}, payload))
            }
            "command_line.execute" => {
                use gateway_plugin_sdk::call::management::{CommandInvocation,CommandValue};
                assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::CommandLine);
                assert_eq!(call.params,json!({}));
                let invocation:CommandInvocation=serde_json::from_slice(&call.payload).unwrap();
                let record=call.host.call("host.state.get",json!({"namespace":"counter","key":"commands"}),vec![]).await.map_err(|e|e.into_plugin_fault())?.result["record"].clone();
                call.host.call("host.state.put",json!({"namespace":"counter","key":"commands","value":record["value"].as_u64().unwrap_or(0)+1,"expected_version":record["version"]}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                if self.adapter_mode=="command-fault" {return Err(PluginFault::new(ErrorCode::Rejected,"private command failure"));}
                if self.adapter_mode=="command-crash" {std::process::exit(2);}
                let output=if let Some(CommandValue::String(input))=invocation.arguments.get("input") {
                    let input:serde_json::Value=serde_json::from_str(input).unwrap();
                    if input["mode"]=="external" {
                        worker_outbound::run(&call.host,&json!({"url":self.adapter_url,"mode":"stream"})).await?
                    } else { worker_models::handle(&call.host,&input).await }
                } else {serde_json::to_value(invocation).unwrap()};
                let mut result=if self.command_result.is_null() {json!({"stdout":serde_json::to_string(&output).unwrap(),"stderr":"","exit_code":0,"accounts":[]})} else {self.command_result.clone()};
                if self.adapter_mode=="command-size" {result["stdout"]=json!("x".repeat(65537));}
                if self.adapter_mode=="command-unknown" {result["unexpected"]=json!(true);}
                Ok(CallReply::unary(if self.adapter_mode=="command-envelope" {json!({"extra":true})} else {json!({})}, if self.adapter_mode=="command-invalid" {vec![255]} else {serde_json::to_vec(&result).unwrap()}))
            }
            "upstream_adapter.execute" => upstream_execution(call, self.contributes["upstream_adapter"]["version"] == 1).await,
            "upstream_adapter.register" => {
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Registration);
                assert_eq!(call.params, json!({}));
                assert!(call.payload.is_empty());
                let mut declaration = json!({"id":"adapter","provider":self.adapter_provider,"base_url":self.adapter_url,"paths":[{"path":"responses","purpose":"inference"},{"path":"audit","purpose":"auxiliary"}],"authentication_kinds":["oauth"],"transport":self.adapter_transport,"protocol":"openai","models":["public-model"]});
                if !self.adapter_models.is_null() {
                    declaration["models"] = self.adapter_models.clone();
                }
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
                    "overlap" => {let mut second=declaration.clone();second["id"]=json!("second");vec![declaration,second]},
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
                if self.retry == "service-probe" {
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|e| PluginFault::new(ErrorCode::Fault,e.message))?);
                }
                if self.retry == "external" {
                    let result=worker_outbound::run(&call.host,&json!({"url":self.adapter_url,"mode":"stream"})).await?;
                    assert_eq!(result["bytes"],json!([0,255,1,0]));
                }
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
                if self.schedule == "service-probe" {
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|e| PluginFault::new(ErrorCode::Fault,e.message))?);
                }
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
                if body["service_probe"] == true {
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|e| PluginFault::new(ErrorCode::Fault,e.message))?);
                }
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
            "observer.observe" => {
                use gateway_plugin_sdk::{Stage, call::observation::Event};
                assert_eq!(call.context.stage, Stage::Observation);
                assert!(call.context.timeout_ms <= 2000);
                let event: Event = serde_json::from_value(call.params.clone()).unwrap();
                assert_eq!(call.context.request_id.as_deref(), Some(event.request_id()));
                let event_id = match &event {
                    Event::RequestCompleted(e) => { assert!(call.payload.is_empty()); &e.event_id },
                    Event::WebSocketResponse(e) => { assert_eq!(e.payload_included, !call.payload.is_empty()); &e.event_id },
                };
                let denied_model = call.host.call("host.keys.list",json!({}),vec![]).await.is_err();
                assert!(denied_model); // A completed request is not an administrative session.
                let record = call.host.call("host.state.get",json!({"namespace":"observations","key":event_id}),vec![]).await.map_err(|e|e.into_plugin_fault())?.result["record"].clone();
                call.host.call("host.state.put",json!({"namespace":"observations","key":event_id,"expected_version":record["version"],"value":{
                    "event":call.params,"payload":(call.payload.len() <= 4096).then(|| String::from_utf8_lossy(&call.payload)),"payload_len":call.payload.len(),"timeout_ms":call.context.timeout_ms,"count":record["value"]["count"].as_u64().unwrap_or(0)+1
                }}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                if self.adapter_mode == "observer-service" || self.adapter_mode == "observer-wait" || self.adapter_mode == "observer-denied" {
                    let result = call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await;
                    if self.adapter_mode == "observer-denied" { assert!(result.is_err()); }
                    else { assert!(result.map_err(|e|PluginFault::new(ErrorCode::Fault,e.message))?); }
                }
                match self.adapter_mode.as_str() {
                    "observer-null" => Ok(CallReply::unary(serde_json::Value::Null, vec![])),
                    "observer-envelope" => Ok(CallReply::unary(json!({"extra":true}),vec![])),
                    "observer-payload" => Ok(CallReply::unary(json!({}),vec![1])),
                    "observer-fault" => Err(PluginFault::new(ErrorCode::Rejected,"private observer error detail")),
                    _ => Ok(CallReply::unary(json!({}),vec![])),
                }
            },
            "plugin.state.migrate" => {
                use gateway_plugin_sdk::call::host::*;
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Configuration);
                assert_eq!(call.params, json!({}));
                assert!(call.host.call("host.state.get", json!({"namespace":"counter","key":"value"}), vec![]).await.is_err());
                let request: StateMigrationRequest = serde_json::from_slice(&call.payload).unwrap();
                if self.state_migration == "envelope" { return Ok(CallReply::unary(json!({"unexpected":true}), b"{}".to_vec())); }
                if self.state_migration == "malformed" { return Ok(CallReply::unary(json!({}), b"not-json".to_vec())); }
                if self.state_migration == "fail" { return Err(PluginFault::new(ErrorCode::Fault,"migration fixture failure")); }
                if self.state_migration == "wait" {
                    tokio::fs::write(&self.state_migration_signal, std::process::id().to_string()).await.unwrap();
                    while !tokio::fs::try_exists(format!("{}.release", self.state_migration_signal)).await.unwrap() {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
                let mut changes: Vec<_> = request.records.into_iter().map(|row| {
                    if self.state_migration == "delete_all" { StateMigrationChange::Delete { key: row.key } }
                    else if self.state_migration == "unwrap" { StateMigrationChange::Replace { key: row.key, value: row.value["value"].clone() } }
                    else if self.state_migration == "wrap" { StateMigrationChange::Replace { key: row.key, value: json!({"value":row.value}) } }
                    else if self.state_migration == "bad_keys" { StateMigrationChange::Keep { key: "foreign-key".into() } }
                    else if self.state_migration == "invalid_value" { StateMigrationChange::Replace { key: row.key, value: json!(false) } }
                    else if self.state_migration == "actions" && row.key == "delete" { StateMigrationChange::Delete { key: row.key } }
                    else if self.state_migration == "actions" && row.key != "keep" { StateMigrationChange::Replace { key: row.key, value: json!(row.value.as_i64().unwrap()*10) } }
                    else { StateMigrationChange::Keep { key: row.key } }
                }).collect();
                if self.state_migration == "missing" { changes.pop(); }
                if self.state_migration == "reorder" { changes.reverse(); }
                Ok(CallReply::unary(json!({}), serde_json::to_vec(&StateMigrationResult { changes }).unwrap()))
            }
            "plugin.reconcile" => {
                use gateway_plugin_sdk::Stage;
                assert_eq!(call.context.stage, Stage::Maintenance);
                assert_eq!(call.params, json!({}));
                assert!(call.payload.is_empty());
                assert_eq!(call.context.request_id.as_deref(), Some(call.context.resource_scope_id.as_str()));
                if self.adapter_mode == "maintenance-quota" {
                    let refreshed = worker_models::handle(&call.host, &self.command_result).await;
                    assert!(refreshed["observed_at_ms"].is_i64(), "{refreshed}");
                    let mut passive = self.command_result.clone();
                    passive["method"] = json!("host.data.quota.get");
                    let cached = worker_models::handle(&call.host, &passive).await;
                    assert_eq!(cached, refreshed);
                }
                if self.adapter_mode == "maintenance-credentials" {
                    use gateway_plugin_sdk::call::host::{AuthSaveResult,AuthGetRequest};
                    let reply=call.host.call("host.auth.save",json!({}),serde_json::to_vec(&self.command_result).unwrap()).await.map_err(|error|error.into_plugin_fault())?;
                    assert_eq!(reply.result,json!({}));
                    let saved:AuthSaveResult=serde_json::from_slice(&reply.payload).unwrap();
                    for method in ["host.auth.get_runtime","host.auth.get"] {
                        let reply=call.host.call(method,json!({}),serde_json::to_vec(&AuthGetRequest{account_id:saved.account_id.clone()}).unwrap()).await.map_err(|error|error.into_plugin_fault())?;
                        assert_eq!(reply.result,json!({}));
                        let value:serde_json::Value=serde_json::from_slice(&reply.payload).unwrap();
                        assert_eq!(value["credential_revision"],saved.credential_revision);
                    }
                    call.host.call("host.auth.list",json!({}),serde_json::to_vec(&json!({"limit":1})).unwrap()).await.map_err(|error|error.into_plugin_fault())?;
                }
                if self.adapter_mode == "maintenance-service" || self.adapter_mode == "maintenance-wait" {
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|e|PluginFault::new(ErrorCode::Fault,e.message))?);
                }
                if self.adapter_mode == "maintenance-envelope" {return Ok(CallReply::unary(json!({"extra":true}), vec![]));}
                if self.adapter_mode == "maintenance-payload" {return Ok(CallReply::unary(json!({}), vec![1]));}
                let record=call.host.call("host.state.get",json!({"namespace":"counter","key":"reconcile"}),vec![]).await.map_err(|e|e.into_plugin_fault())?.result["record"].clone();
                let count=record["value"].as_u64().unwrap_or(0)+1;
                call.host.call("host.state.put",json!({"namespace":"counter","key":"reconcile","value":count,"expected_version":record["version"]}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                if self.adapter_mode == "maintenance-fault" {return Err(PluginFault::new(ErrorCode::Rejected,"private maintenance failure detail"));}
                Ok(CallReply::unary(json!({}),vec![]))
            },
            "plugin.register" if !self.fail=>{
                if !self.registration_signal.is_empty() {
                    tokio::fs::write(&self.registration_signal, b"registration").await.unwrap();
                }
                if self.adapter_mode == "registration-log-fault" || self.adapter_mode == "registration-log-pending" {
                    let reply = call.host.call("host.log", json!({"event":"fixture.registration"}), vec![]).await.map_err(|error| error.into_plugin_fault())?;
                    assert_eq!(reply.result, json!({"recorded":true}));
                    if self.adapter_mode == "registration-log-fault" {
                        return Err(PluginFault::new(ErrorCode::Rejected, "fixture registration failed after accepted log"));
                    }
                    futures::future::pending::<()>().await;
                }
                if self.adapter_mode == "registration-service" {
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|error| PluginFault::new(ErrorCode::Fault, error.message))?);
                    denied_service_stage_privileges(&call.host).await?;
                    assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|error| PluginFault::new(ErrorCode::Fault, error.message))?);
                }
                if self.adapter_mode == "registration-external" {
                    let result=worker_outbound::run(&call.host,&json!({"url":self.adapter_url,"mode":"stream"})).await?;
                    assert_eq!(result["bytes"],json!([0,255,1,0]));
                    assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::Registration);
                    let fault=call.host.call("host.state.put",json!({"namespace":"counter","key":"register","value":1,"expected_version":null}),vec![]).await.err().unwrap().into_plugin_fault();
                    assert_eq!(fault.code,ErrorCode::PermissionDenied);
                }
                Ok(CallReply::unary(json!({"contributes":self.contributes}),vec![]))
            },
            "frontend_auth.identifier"=>Ok(CallReply::unary(json!({"identifier":"fixture-auth"}),vec![])),
            "frontend_auth.authenticate"=>{
                let input:gateway_plugin_sdk::call::frontend_authentication::FrontendAuthenticationRequest=serde_json::from_slice(&call.payload).unwrap();
                assert_eq!(call.context.stage,gateway_plugin_sdk::Stage::Authentication);
                let value=match input.authorization.as_str() {
                    "Bearer external-service"=>{
                        assert!(call.host.service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(()).await.map_err(|error| PluginFault::new(ErrorCode::Fault, error.message))?);
                        denied_service_stage_privileges(&call.host).await?;
                        json!({"outcome":"authenticated","principal":"alice"})
                    },
                    "Bearer external-alice"=>json!({"outcome":"authenticated","principal":"alice"}),
                    "Bearer external-unknown"=>json!({"outcome":"authenticated","principal":"unknown"}),
                    "Bearer external-rejected"=>json!({"outcome":"rejected"}),
                    "Bearer external-forged"=>json!({"outcome":"authenticated","principal":"alice","client_key_id":"forged"}),
                    "Bearer external-fault"=>return Err(PluginFault::new(ErrorCode::Fault,"sensitive credential detail")),
                    "Bearer external-crash"=>std::process::exit(2),
                    "Bearer external-http"=>{
                        let result=worker_outbound::run(&call.host,&json!({"url":self.adapter_url,"mode":"stream"})).await?;
                        assert_eq!(result["bytes"],json!([0,255,1,0]));
                        for method in [gateway_plugin_sdk::call::data::ACCOUNTS_LIST,"host.model.execute",gateway_plugin_sdk::call::resources::GROUP_ENSURE] {
                            let fault=call.host.call(method,json!({}),vec![]).await.err().unwrap().into_plugin_fault();
                            assert_eq!(fault.code,ErrorCode::PermissionDenied);
                        }
                        json!({"outcome":"authenticated","principal":"alice"})
                    },
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
                "resources":[{"path":"index.html","public":self.public_resource}],"pages":[{"id":"home","title":"Counter","entry":"index.html","icon":null}],"callbacks":if self.public_callbacks {json!([{"path":"oauth","response_content_types":["application/json"]}])} else {json!([])}
            })).unwrap())),
            "management.callback" => {
                assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::PublicManagement);
                assert!(call.payload.is_empty());
                assert_eq!(call.params["method"], "GET");
                assert_eq!(call.params["path"], "oauth");
                let record = call.host.call("host.state.get", json!({"namespace":"counter","key":"callbacks"}), vec![]).await.map_err(|e| e.into_plugin_fault())?;
                let count = record.result["record"]["value"].as_u64().unwrap_or(0) + 1;
                call.host.call("host.state.put", json!({"namespace":"counter","key":"callbacks","value":count,"expected_version":record.result["record"]["version"].as_u64()}), vec![]).await.map_err(|e| e.into_plugin_fault())?;
                if !self.callback_oauth_url.is_empty() {
                    let value = worker_credentials::public_login(&call.host, &self.callback_oauth_url, call.params["query"].as_str().unwrap()).await?;
                    return Ok(CallReply::unary(json!({"status":200,"content_type":"application/json"}), serde_json::to_vec(&value).unwrap()));
                }
                if call.params["query"].as_str().unwrap().contains("fail=1") {
                    return Err(PluginFault::new(ErrorCode::Fault, "fixture fault after callback write"));
                }
                if call.params["query"].as_str().unwrap().contains("wait=1") {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
                Ok(CallReply::unary(json!({"status":200,"content_type":"application/json","headers":[{"name":"x-plugin-callback","value":[111,107]}]}), serde_json::to_vec(&json!({"count":count,"query":call.params["query"],"headers":call.params["headers"]})).unwrap()))
            },
            "management.handle"=>{
                let input:serde_json::Value=serde_json::from_slice(&call.payload).unwrap();
                let result=if input["operation"]=="state_read" {
                    call.host.call("host.state.get",json!({"namespace":"counter","key":"value"}),vec![]).await.map_err(|e|e.into_plugin_fault())?.result
                } else if input["operation"]=="state" {
                    let existing=call.host.call("host.state.get",json!({"namespace":"counter","key":"value"}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                    let expected=existing.result["record"]["version"].as_u64();
                    let count=existing.result["record"]["value"].as_u64().unwrap_or(0)+1;
                    call.host.call("host.state.put",json!({"namespace":"counter","key":"value","value":count,"expected_version":expected}),vec![]).await.map_err(|e|e.into_plugin_fault())?;
                    json!({"count":count})
                } else if input["operation"]=="external" {
                    match worker_outbound::run(&call.host,&input).await {
                        Ok(value)=>value,Err(error)=>json!({"error":error})
                    }
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
                } else if input["operation"]=="host-log" {
                    match call.host.call("host.log",input["params"].clone(),if input["binary"]==true {vec![1]}else{vec![]}).await {
                        Ok(reply)=>reply.result,
                        Err(error)=>json!({"error":{"code":error.into_plugin_fault().code}}),
                    }
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
        registration_signal: session.handshake().configuration["registration_signal"]
            .as_str()
            .unwrap_or_default()
            .into(),
        state_migration: session.handshake().configuration["state_migration"]
            .as_str()
            .unwrap_or("")
            .into(),
        state_migration_signal: session.handshake().configuration["state_migration_signal"]
            .as_str()
            .unwrap_or("")
            .into(),
        callback_oauth_url: session.handshake().configuration["callback_oauth_url"]
            .as_str()
            .unwrap_or_default()
            .into(),
        public_callbacks: session.handshake().configuration["public_callbacks"]
            .as_bool()
            .unwrap_or(false),
        public_resource: session.handshake().configuration["public_resource"]
            .as_bool()
            .unwrap_or(false),
        command_registration: session.handshake().configuration["command_registration"].clone(),
        command_result: session.handshake().configuration["command_result"].clone(),
        adapter_models: session.handshake().configuration["adapter_models"].clone(),
        adapter_provider: session.handshake().configuration["adapter_provider"]
            .as_str()
            .unwrap_or("openai")
            .into(),
        adapter_url: session.handshake().configuration["adapter_url"]
            .as_str()
            .unwrap_or("https://example.test/base/")
            .into(),
        adapter_transport: session.handshake().configuration["adapter_transport"]
            .as_str()
            .unwrap_or("http_sse")
            .into(),
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

async fn middleware_private_state(
    host: &gateway_plugin_sdk::client::HostClient,
    key: &str,
) -> Result<(), PluginFault> {
    let params = json!({"namespace":"counter","key":key});
    let previous = host
        .call("host.state.get", params.clone(), vec![])
        .await
        .map_err(|error| error.into_plugin_fault())?
        .result["record"]
        .clone();
    let value = previous["value"].as_u64().unwrap_or(0) + 1;
    let put = json!({"namespace":"counter","key":key,"value":value,"expected_version":previous["version"]});
    let saved = host
        .call("host.state.put", put.clone(), vec![])
        .await
        .map_err(|error| error.into_plugin_fault())?;
    assert_eq!(
        host.call("host.state.put", put, vec![])
            .await
            .err()
            .unwrap()
            .into_plugin_fault()
            .code,
        ErrorCode::Conflict
    );
    let read = host
        .call("host.state.get", params.clone(), vec![])
        .await
        .map_err(|error| error.into_plugin_fault())?;
    assert_eq!(read.result["record"]["value"], value);
    assert_eq!(read.result["record"]["version"], saved.result["version"]);
    host.call(
        "host.state.delete",
        json!({"namespace":"counter","key":key,"expected_version":saved.result["version"]}),
        vec![],
    )
    .await
    .map_err(|error| error.into_plugin_fault())?;
    assert!(
        host.call("host.state.get", params, vec![])
            .await
            .map_err(|error| error.into_plugin_fault())?
            .result["record"]
            .is_null()
    );
    host.call(
        "host.state.put",
        json!({"namespace":"counter","key":key,"value":value,"expected_version":null}),
        vec![],
    )
    .await
    .map_err(|error| error.into_plugin_fault())?;
    for (method, params, payload) in [
        (
            "host.state.put",
            json!({"namespace":"counter","key":"invalid","value":"invalid","expected_version":null}),
            vec![],
        ),
        (
            "host.state.get",
            json!({"namespace":"undeclared","key":key}),
            vec![],
        ),
        (
            "host.state.get",
            json!({"namespace":"counter","key":key}),
            vec![1],
        ),
    ] {
        assert_eq!(
            host.call(method, params, payload)
                .await
                .err()
                .unwrap()
                .into_plugin_fault()
                .code,
            ErrorCode::InvalidInput
        );
    }
    for method in ["host.auth.list", "host.groups.ensure"] {
        assert_eq!(
            host.call(method, json!({}), vec![])
                .await
                .err()
                .unwrap()
                .into_plugin_fault()
                .code,
            ErrorCode::PermissionDenied
        );
    }
    Ok(())
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
        if mode == "private-state" {
            super::middleware_private_state(
                &call.host,
                match call.context.stage {
                    gateway_plugin_sdk::Stage::Request => "request",
                    gateway_plugin_sdk::Stage::Attempt => "attempt",
                    _ => unreachable!(),
                },
            )
            .await?;
        }
        if input["service_probe"] == true {
            assert!(
                call.host
                    .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
                    .await
                    .map_err(|e| PluginFault::new(ErrorCode::Fault, e.message))?
            );
        }
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
                | "inspect-session-owner"
                | "inspect-transformed"
                | "map"
                | "expand"
                | "drop-first"
                | "ws-map"
                | "ws-expand"
        ) {
            let mut first = true;
            response.body = response.body.with_facts()?.map_frames(move |mut frame| {
                if mode == "inspect-session-owner" {
                    let state = frame
                        .facts
                        .as_ref()
                        .unwrap()
                        .host
                        .as_ref()
                        .unwrap()
                        .session_update
                        .as_ref()
                        .unwrap();
                    assert_eq!(state.provider, "openai");
                    let owner = state.extension_owner.as_ref().unwrap();
                    assert_eq!(owner.instance_id, "owner-instance");
                    assert_eq!(owner.contribution_id, "owner-contribution");
                    assert_eq!(owner.adapter_id, "owner-adapter");
                    assert_eq!(owner.generation, 7);
                    assert_eq!(owner.incarnation, "owner-process");
                    assert!(owner.connection_local);
                }
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
        assert!(!call.call_id.is_empty());
        let mode = call
            .request
            .headers
            .iter()
            .find(|h| h.name == "x-fixture-mode")
            .map(|h| String::from_utf8(h.value.clone()).unwrap())
            .unwrap_or_else(|| "pass".into());
        if mode == "private-state" {
            super::middleware_private_state(&call.host, "http").await?;
        }
        if mode != "service-probe" {
            assert_eq!(call.call_id, "host-call");
        }
        if mode == "service-probe" {
            let exists = call
                .host
                .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
                .await
                .map_err(|error| PluginFault::new(ErrorCode::Fault, error.message))?;
            return HttpResponse::new(
                200,
                HttpBody::from_bytes(serde_json::to_vec(&json!({"exists":exists})).unwrap()),
            )
            .into_reply();
        }
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
    if mode == "private-state" {
        middleware_private_state(&host, "service").await?;
    }
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
    if mode == "private-state" {
        middleware_private_state(&call.host, "websocket").await?;
    }
    if mode == "service-probe" {
        let exists = call
            .host
            .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
            .await
            .map_err(|error| PluginFault::new(ErrorCode::Fault, error.message))?;
        call.message = WebSocketMessage::new(
            WebSocketKind::Binary,
            serde_json::to_vec(&json!({"exists":exists})).unwrap(),
        );
    }
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

async fn upstream_execution(call: PluginCall, legacy: bool) -> Result<CallReply, PluginFault> {
    use gateway_plugin_sdk::{
        call::{
            model::{CanonicalEvent, ExecutionEvent, FinishReason, Usage, WireEvent, WirePayload},
            upstream_adapter::*,
        },
        client::{ResponseStream, UpstreamWebSocketUpgrade},
    };
    fn event(facts: Vec<CanonicalEvent>, wire: Option<WireEvent>) -> ExecutionEvent {
        ExecutionEvent {
            facts,
            wire,
            host: None,
        }
    }
    let (metadata, body) = if legacy {
        // v1 carries disable_fast rather than the v2 Fast tri-state. Check its
        // actual wire before projecting into the shared fixture assertions.
        assert_eq!(&call.payload[..4], b"GPAQ");
        let metadata_len = u32::from_be_bytes(call.payload[4..8].try_into().unwrap()) as usize;
        let body_len = u32::from_be_bytes(call.payload[8..12].try_into().unwrap()) as usize;
        assert_eq!(call.payload.len(), 12 + metadata_len + body_len);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&call.payload[12..12 + metadata_len]).unwrap();
        assert!(metadata.get("fast_mode").is_none());
        let disabled = metadata
            .as_object_mut()
            .unwrap()
            .remove("disable_fast")
            .unwrap()
            .as_bool()
            .unwrap();
        metadata["fast_mode"] = json!(if disabled { "disabled" } else { "default" });
        (
            serde_json::from_value::<UpstreamAdapterRequest>(metadata).unwrap(),
            call.payload[12 + metadata_len..].to_vec(),
        )
    } else {
        UpstreamAdapterRequest::decode(&call.payload).unwrap()
    };
    let input: serde_json::Value = serde_json::from_slice(&body).unwrap();
    if let Some(expected) = input["expect_continuation"].as_bool() {
        assert_eq!(metadata.continuation.is_some(), expected);
    }
    assert_eq!(call.context.stage, gateway_plugin_sdk::Stage::Upstream);
    assert_eq!(
        metadata.account_id,
        input["fixture_account"].as_str().unwrap_or("acct_fixture")
    );
    assert_eq!(
        metadata.credential_revision,
        input["fixture_credential_revision"].as_u64().unwrap_or(3)
    );
    assert_eq!(
        metadata.upstream_model,
        input["fixture_model"].as_str().unwrap_or("native-model")
    );
    assert!(!String::from_utf8_lossy(&call.payload).contains("fixture-provider-secret"));
    let mode = input["behavior"].as_str().unwrap_or("http");
    if mode == "service-probe" {
        assert!(
            call.host
                .service::<gateway_plugin_sdk::call::services::settings::ApiKeyExists>(())
                .await
                .map_err(|e| PluginFault::new(ErrorCode::Fault, e.message))?
        );
    }
    if mode.starts_with("callback-") {
        upstream_callbacks::run(&call.host, mode, &input).await?;
    }
    let callback_error = |_| PluginFault::new(ErrorCode::Fault, "managed callback failed");
    if mode.starts_with("websocket") {
        let connection = match call
            .host
            .upstream_websocket(UpstreamWebSocketRequest {
                path: "responses".into(),
                headers: vec![],
                query: vec![],
            })
            .await?
        {
            UpstreamWebSocketUpgrade::Connected { connection, .. } => connection,
            UpstreamWebSocketUpgrade::Rejected { .. } => panic!("expected upgrade"),
        };
        connection
            .send(WebSocketMessageKind::Text, b"fixture-ping".to_vec())
            .await?;
        if mode == "websocket-read-cancel" {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), connection.read())
                    .await
                    .is_err()
            );
            connection
                .send(WebSocketMessageKind::Text, b"fixture-resume".to_vec())
                .await?;
        }
        assert_eq!(connection.read().await?.unwrap().1, b"fixture-pong");
        if mode == "websocket-credential-save" {
            worker_credentials::on_websocket(&call.host, &input).await?;
            assert!(
                connection
                    .send(WebSocketMessageKind::Text, b"stale-connection".to_vec())
                    .await
                    .is_err()
            );
            let fresh = match call
                .host
                .upstream_websocket(UpstreamWebSocketRequest {
                    path: "responses".into(),
                    headers: vec![],
                    query: vec![],
                })
                .await?
            {
                UpstreamWebSocketUpgrade::Connected { connection, .. } => connection,
                UpstreamWebSocketUpgrade::Rejected { .. } => {
                    return Err(PluginFault::new(
                        ErrorCode::Fault,
                        "fresh native handshake rejected",
                    ));
                }
            };
            fresh
                .send(WebSocketMessageKind::Text, b"fixture-ping".to_vec())
                .await?;
            assert_eq!(fresh.read().await?.unwrap().1, b"fixture-pong");
        }
    } else if mode != "synthetic" && mode != "invalid" && !mode.starts_with("callback-") {
        let method = if mode == "http-stream" {
            "host.upstream.http.do_stream"
        } else {
            "host.upstream.http.do"
        };
        let reply=call.host.call(method,json!({"method":"POST","path":if mode=="auxiliary" {"audit"}else{"responses"},"headers":[["x-fixture","adapter"]],"query":[]}),body).await.map_err(callback_error)?;
        assert_eq!(reply.result["status"], 200);
        if mode == "http-stream" {
            let stream = reply.result["stream"].as_str().unwrap();
            let mut total = Vec::new();
            loop {
                let reply = call
                    .host
                    .call(
                        "host.upstream.http.stream_read",
                        json!({"stream":stream,"maximum_bytes":3}),
                        vec![],
                    )
                    .await
                    .map_err(callback_error)?;
                assert!(reply.payload.len() <= 3);
                total.extend(reply.payload);
                if reply.result["eof"] == true {
                    break;
                }
            }
            assert_eq!(total, b"fixture-response");
        } else {
            assert_eq!(reply.payload, b"fixture-response");
        }
    }
    if mode == "wait" {
        std::future::pending::<()>().await;
    }
    if mode == "failure" || mode == "auxiliary" {
        let mut event = UpstreamAdapterEvent::new(event(vec![], None));
        event.failure = Some(UpstreamFailure {
            kind: UpstreamFailureKind::RateLimited,
            status: Some(429),
            retry_after_ms: Some(1200),
            message: "fixture rejection".into(),
            code: Some("fixture_limit".into()),
        });
        return Ok(CallReply::stream(
            json!({}),
            vec![],
            ResponseStream::from_chunks(vec![event.encode().unwrap()]),
        ));
    }
    let raw_body = input["raw_body"]
        .as_str()
        .or_else(|| (mode == "raw-body").then_some("chunks"));
    let api_wire = raw_body.is_none() && input["fixture_api"].as_bool().unwrap_or(false);
    let wire = |kind: &str| {
        Some(WireEvent {
            protocol: "openai".into(),
            payload: WirePayload::Json {
                event: Some(kind.into()),
                data: json!({"type":kind,"response":{"id":"adapter-response","object":"response",
                "model":metadata.upstream_model,"status":if kind=="response.completed" {"completed"} else {"in_progress"},
                "output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}),
                id: None,
                retry: None,
                raw_sse: None,
            },
        })
    };
    let started = UpstreamAdapterEvent::new(event(
        vec![CanonicalEvent::Started {
            id: "adapter-response".into(),
            model: Some(metadata.upstream_model.clone()),
        }],
        if api_wire {
            wire("response.created")
        } else if raw_body.is_some() {
            Some(WireEvent {
                protocol: "openai".into(),
                payload: WirePayload::RawBody { body: vec![b'{'] },
            })
        } else {
            None
        },
    ))
    .encode()
    .unwrap();
    let mut completed = UpstreamAdapterEvent::new(event(
        vec![
            CanonicalEvent::Usage {
                usage: Usage {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    ..Default::default()
                },
            },
            CanonicalEvent::Completed {
                id: if mode == "invalid" {
                    "wrong-id"
                } else {
                    "adapter-response"
                }
                .into(),
                model: Some(metadata.upstream_model.clone()),
                reason: FinishReason::Stop,
            },
        ],
        if api_wire {
            wire("response.completed")
        } else {
            Some(WireEvent {
                protocol: "openai".into(),
                payload: WirePayload::RawJson {
                    body: br#"{"id":"adapter-response","status":"completed"}"#.to_vec(),
                },
            })
        },
    ));
    completed.service_tier = Some("priority".into());
    if mode == "continuation" || mode.starts_with("websocket") {
        if let Some(previous) = metadata.continuation {
            assert_eq!(previous.state["round"], 1);
        }
        completed.continuation = Some(UpstreamContinuation {
            scope: if mode.starts_with("websocket") {
                ContinuationScope::ConnectionLocal
            } else {
                ContinuationScope::Persisted
            },
            upstream_response_id: "adapter-response".into(),
            state: json!({"round":1}).as_object().unwrap().clone(),
        });
    }
    let mut chunks = vec![started];
    if let Some(raw) = raw_body {
        completed.event.wire = None;
        let body = "{ \"future\": 9007199254740993, \"text\": \"雪☃\", \"status\": \"completed\", \"id\": \"adapter-response\" }\n".as_bytes();
        for part in body.chunks(1).skip(1) {
            chunks.push(
                UpstreamAdapterEvent::new(event(
                    vec![],
                    Some(WireEvent {
                        protocol: "openai".into(),
                        payload: WirePayload::RawBody {
                            body: part.to_vec(),
                        },
                    }),
                ))
                .encode()
                .unwrap(),
            );
        }
        if raw == "limit" {
            for _ in 0..513 {
                chunks.push(
                    UpstreamAdapterEvent::new(event(
                        vec![],
                        Some(WireEvent {
                            protocol: "openai".into(),
                            payload: WirePayload::RawBody {
                                body: vec![b' '; 16 * 1024],
                            },
                        }),
                    ))
                    .encode()
                    .unwrap(),
                );
            }
        }
        if raw == "mixed" {
            chunks.push(
                UpstreamAdapterEvent::new(event(vec![], wire("response.completed")))
                    .encode()
                    .unwrap(),
            );
        }
    }
    chunks.push(completed.encode().unwrap());
    if raw_body == Some("wait") {
        chunks.pop();
        let (sender, stream) = ResponseStream::channel(std::num::NonZeroUsize::new(1).unwrap());
        let cancellation = call.cancellation.clone();
        tokio::spawn(async move {
            for chunk in chunks {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return,
                    result = sender.send(chunk) => if result.is_err() { return; },
                }
            }
            cancellation.cancelled().await;
        });
        return Ok(CallReply::stream(json!({}), vec![], stream));
    }
    if mode == "tail" {
        chunks.push(vec![0]);
    }
    if mode == "tail-fault" || raw_body == Some("tail-fault") {
        let (sender, stream) = ResponseStream::channel(std::num::NonZeroUsize::new(1).unwrap());
        tokio::spawn(async move {
            for chunk in chunks {
                sender.send(chunk).await.unwrap();
            }
            let mut fault = PluginFault::new(ErrorCode::Fault, "private fixture tail fault");
            fault.send_state = gateway_plugin_sdk::SendState::NotSent;
            sender.fail(fault).await.unwrap();
        });
        return Ok(CallReply::stream(json!({}), vec![], stream));
    }
    Ok(CallReply::stream(
        json!({}),
        vec![],
        ResponseStream::from_chunks(chunks),
    ))
}

async fn denied_service_stage_privileges(
    host: &gateway_plugin_sdk::client::HostClient,
) -> Result<(), PluginFault> {
    for method in [
        gateway_plugin_sdk::call::data::ACCOUNTS_LIST,
        "host.model.execute",
        gateway_plugin_sdk::call::resources::GROUP_ENSURE,
    ] {
        let fault = host
            .call(method, json!({}), vec![])
            .await
            .err()
            .unwrap()
            .into_plugin_fault();
        assert_eq!(fault.code, ErrorCode::PermissionDenied);
    }
    Ok(())
}
