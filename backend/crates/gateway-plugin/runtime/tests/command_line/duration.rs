use super::*;

#[tokio::test]
async fn duration_is_exact_signed_nanoseconds_with_checked_boundaries() {
    let fixture = Fixture::new().await;
    let process = fixture
        .prepare(json!({"command_registration":descriptor(json!([parameter("time","duration")]))}))
        .await
        .unwrap();
    process.activate();
    for (text, expected) in [
        ("0", 0),
        ("+0", 0),
        ("-0", 0),
        ("1µs", 1000),
        (".5s", 500000000),
        ("1m30s", 90000000000),
        ("-1h2m", -3720000000000),
        ("9223372036854775807ns", i64::MAX),
        ("-9223372036854775808ns", i64::MIN),
        ("0.000000001s", 1),
    ] {
        let result = process
            .command(request(&[&format!("--time={text}")]), None)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&result.stdout).unwrap()["arguments"]["time"]["value"],
            expected,
            "{text}"
        );
    }
    for text in [
        "",
        "1",
        "1d",
        "1ns2",
        "0.1ns",
        "1.0000000001s",
        "1..2s",
        "1s-2s",
        "9223372036854775808ns",
        "-9223372036854775809ns",
        "340282366920938463463374607431768211455h",
        "NaNs",
    ] {
        assert!(
            matches!(
                process
                    .command(request(&[&format!("--time={text}")]), None)
                    .await
                    .err()
                    .unwrap(),
                PluginCommandError::Invalid(_)
            ),
            "{text}"
        );
    }
    assert_eq!(fixture.count().await, Some(10));
    stop(&process).await;
}
