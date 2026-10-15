use gateway_plugin_sdk::{
    CallContext, ErrorCode, Frame, Handshake, Manifest, Message, PROTOCOL_VERSION, PluginFault,
    Stage,
    client::{read_frame, write_frame},
};
use serde_json::{Value, json};
use tokio::process::{Child, ChildStdin, ChildStdout};

pub struct Host {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    id: u64,
    pub state: Option<(Value, u64)>,
    pub resets: Vec<Value>,
    pub limits: Vec<Value>,
    pub uncertain: bool,
    pub fail_receipt: bool,
}
impl Host {
    pub async fn new(state: Option<(Value, u64)>) -> Self {
        let manifest = Manifest::from_author_slice(cpr_shared_budget_plugin::MANIFEST).unwrap();
        let mut child =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_cpr-shared-budget-plugin"))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        write_frame(
            &mut input,
            &Frame::control(Message::Hello {
                handshake: Handshake {
                    protocol_version: PROTOCOL_VERSION,
                    artifact_sha256: "a".repeat(64),
                    plugin_id: cpr_shared_budget_plugin::PLUGIN_ID.into(),
                    instance_id: "budget-test".into(),
                    generation: 1,
                    incarnation: "budget-process".into(),
                    configuration: json!({}),
                    contributes: manifest.contributes,
                },
            }),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut output).await.unwrap().message,
            Message::Ready { .. }
        ));
        Self {
            child,
            input,
            output,
            id: 0,
            state,
            resets: vec![],
            limits: vec![],
            uncertain: false,
            fail_receipt: false,
        }
    }
    pub async fn request(&mut self, path: &str, body: Option<Value>) -> (u16, Value) {
        self.id = if self.id == 0 { 1 } else { self.id + 2 };
        let payload = body
            .as_ref()
            .map(|body| serde_json::to_vec(body).unwrap())
            .unwrap_or_default();
        write_frame(&mut self.input,&Frame {message:Message::Call {
            id:self.id,method:"management.handle".into(),context:CallContext {
                call_id:self.id,instance_id:"budget-test".into(),generation:1,incarnation:"budget-process".into(),stage:Stage::Management,
                timeout_ms:5000,resource_stream:false,resource_scope_id:format!("scope-{}",self.id),request_id:None,attempt_id:None,account_id:None,credential_revision:None,
            },params:json!({"method":if body.is_some(){"POST"}else{"GET"},"path":path,"query":"","content_type":body.as_ref().map(|_|"application/json"),"headers":[]}),
        },payload}).await.unwrap();
        loop {
            let frame = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                read_frame(&mut self.output),
            )
            .await
            .unwrap()
            .unwrap();
            match frame.message {
                Message::Result { id, result } => {
                    assert_eq!(id, self.id);
                    return (
                        result["status"].as_u64().unwrap() as u16,
                        serde_json::from_slice(&frame.payload).unwrap(),
                    );
                }
                Message::Callback {
                    id,
                    parent_id,
                    method,
                    params,
                } => {
                    assert_eq!(parent_id, self.id);
                    let response = self.callback(&method, params, &frame.payload);
                    let frame = match response {
                        Ok((result, payload)) => Frame {
                            message: Message::Result { id, result },
                            payload,
                        },
                        Err(error) => Frame::control(Message::Error { id, error }),
                    };
                    write_frame(&mut self.input, &frame).await.unwrap();
                }
                _ => panic!("unexpected plugin protocol frame"),
            }
        }
    }
    fn callback(
        &mut self,
        method: &str,
        params: Value,
        payload: &[u8],
    ) -> Result<(Value, Vec<u8>), PluginFault> {
        let conflict = || PluginFault::new(ErrorCode::Conflict, "fixture conflict");
        let body = if payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(payload).unwrap()
        };
        match method {
            "host.state.get" => Ok((
                json!({"record":self.state.as_ref().map(|(value,version)|json!({"value":value,"version":version,"schema_version":1}))}),
                vec![],
            )),
            "host.state.put" => {
                if params["expected_version"].as_u64() != self.state.as_ref().map(|(_, v)| *v) {
                    return Err(conflict());
                }
                if self.fail_receipt && !params["value"]["receipt"].is_null() {
                    return Err(PluginFault::new(
                        ErrorCode::Uncertain,
                        "fixture lost state acknowledgement",
                    ));
                }
                let version = self.state.as_ref().map_or(1, |(_, v)| v + 1);
                self.state = Some((params["value"].clone(), version));
                Ok((json!({"version":version}), vec![]))
            }
            "host.cpr.shared_budgets.v1.get" => {
                assert_eq!(params, json!({}));
                Ok((json!({}),serde_json::to_vec(&json!({"revision":7,"target":body,"enabled":true,"group_daily_limit_usd":"2","group_weekly_limit_usd":"10","quota_multiplier":"2","daily_limit_usd":"4","weekly_limit_usd":"20","daily_used_usd":"1","weekly_used_usd":"3","daily_resets_at_ms":null,"weekly_resets_at_ms":null,"last_reset_at_ms":null})).unwrap()))
            }
            "host.cpr.shared_budgets.v1.update_group_limits" => {
                self.limits.push(body.clone());
                Ok((json!({}),serde_json::to_vec(&json!({"revision":8,"group_id":body["group_id"],"daily_limit_usd":body["daily_limit_usd"],"weekly_limit_usd":body["weekly_limit_usd"]})).unwrap()))
            }
            "host.cpr.shared_budgets.v1.reset" => {
                self.resets.push(body);
                if self.uncertain {
                    return Err(PluginFault::new(
                        ErrorCode::Uncertain,
                        "fixture private details must not leak",
                    ));
                }
                Ok((
                    json!({}),
                    serde_json::to_vec(&json!({"revision":9,"affected":1})).unwrap(),
                ))
            }
            _ => panic!("unexpected host callback {method}"),
        }
    }
    pub async fn close(mut self) -> Option<(Value, u64)> {
        write_frame(&mut self.input, &Frame::control(Message::Shutdown))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        self.state
    }
}
