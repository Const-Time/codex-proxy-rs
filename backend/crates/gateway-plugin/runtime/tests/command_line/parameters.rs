use super::*;

#[tokio::test]
async fn all_typed_arguments_and_defaults_reach_the_real_sdk_invocation() {
    let fixture = Fixture::new().await;
    let mut default = parameter("default", "string");
    default["default"] = json!({"type":"string","value":"fallback"});
    let registration = descriptor(json!([
        parameter("flag", "bool"),
        parameter("text", "string"),
        parameter("int", "int"),
        parameter("wide", "int64"),
        parameter("number", "float64"),
        parameter("time", "duration"),
        default
    ]));
    let process = fixture
        .prepare(json!({"command_registration":registration}))
        .await
        .unwrap();
    process.activate();
    let result = process
        .command(
            request(&[
                "--flag",
                "--text=--literal",
                "--int",
                "-2147483648",
                "--wide=9223372036854775807",
                "--number=1.25",
                "--time=1h2m3.004s",
            ]),
            None,
        )
        .await
        .unwrap();
    let output: Value = serde_json::from_str(&result.stdout).unwrap();
    assert_eq!(
        output,
        json!({"name":"probe","arguments":{"flag":{"type":"bool","value":true},"text":{"type":"string","value":"--literal"},"int":{"type":"int","value":-2147483648_i32},"wide":{"type":"int64","value":9223372036854775807_i64},"number":{"type":"float64","value":1.25},"time":{"type":"duration","value":3723004000000_i64},"default":{"type":"string","value":"fallback"}}})
    );
    assert_eq!(fixture.count().await, Some(1));
    stop(&process).await;
}

#[tokio::test]
async fn sensitive_defaults_are_hidden_and_help_does_not_execute() {
    let fixture = Fixture::new().await;
    let mut secret = parameter("secret", "string");
    secret["default"] = json!({"type":"string","value":"private-fixture-default"});
    secret["sensitive"] = json!(true);
    let process = fixture
        .prepare(json!({"command_registration":descriptor(json!([secret]))}))
        .await
        .unwrap();
    process.activate();
    let help = process.command_help(Some("probe")).unwrap().unwrap();
    assert!(help.contains("已隐藏"));
    assert!(!help.contains("private-fixture-default"));
    assert_eq!(fixture.count().await, None);
    stop(&process).await;
}

#[tokio::test]
async fn invalid_arguments_are_rejected_before_rpc_and_never_echo_input() {
    let fixture = Fixture::new().await;
    let mut required = parameter("text", "string");
    required["required"] = json!(true);
    let process=fixture.prepare(json!({"command_registration":descriptor(json!([required,parameter("int","int"),parameter("number","float64")]))})).await.unwrap();
    process.activate();
    for args in [
        vec![],
        vec!["private-fixture-input"],
        vec!["--unknown=private-fixture-input"],
        vec!["--text"],
        vec!["--text=a", "--text=private-fixture-input"],
        vec!["--text=a", "--int=2147483648"],
        vec!["--text=a", "--number=NaN"],
        vec!["--text=a", "--number=inf"],
        vec!["--help"],
    ] {
        let error = process.command(request(&args), None).await.err().unwrap();
        assert!(matches!(error, PluginCommandError::Invalid(_)));
        assert!(!error.to_string().contains("private-fixture-input"));
    }
    let oversized = "x".repeat(65537);
    let error = process
        .command(request(&["--text", &oversized]), None)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, PluginCommandError::Invalid(_)));
    let mut too_many = request(&[]);
    too_many.arguments = vec!["--text=a".into(); 129];
    assert!(matches!(
        process.command(too_many, None).await.err().unwrap(),
        PluginCommandError::Invalid(_)
    ));
    assert_eq!(fixture.count().await, None);
    stop(&process).await;
}

#[tokio::test]
async fn registration_rejects_parameter_conflicts_and_wrong_defaults() {
    let fixture = Fixture::new().await;
    let mut wrong = parameter("flag", "bool");
    wrong["default"] = json!({"type":"string","value":"wrong"});
    let mut required = parameter("flag", "bool");
    required["required"] = json!(true);
    required["default"] = json!({"type":"bool","value":true});
    for parameters in [
        json!([parameter("help", "string")]),
        json!([parameter("same", "string"), parameter("same", "bool")]),
        json!([wrong]),
        json!([required]),
        json!(
            (0..65)
                .map(|i| parameter(&format!("arg-{i}"), "string"))
                .collect::<Vec<_>>()
        ),
    ] {
        assert_eq!(
            fixture
                .prepare(json!({"command_registration":descriptor(parameters)}))
                .await
                .err()
                .unwrap()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
}
