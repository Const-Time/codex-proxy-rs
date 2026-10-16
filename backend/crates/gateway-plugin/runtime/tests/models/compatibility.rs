use super::fixture::Fixture;
use gateway_plugin_sdk::call::{host::ModelEventBatch, model::ExecutionEvent};
use serde_json::{Value, json};
use std::{io::Write as _, process::Stdio};

#[tokio::test]
async fn scoped_cost_batches_decode_with_upstream_sdk_without_changing_native_charges() {
    // Compile the unmodified upstream dependency, not a second copy of our decoder.
    let decoder = tokio::task::spawn_blocking(crate::services::upstream_sdk::binary)
        .await
        .unwrap();
    let fixture = Fixture::new().await;
    for (index, mode) in ["unary", "stream", "small"].into_iter().enumerate() {
        let result = fixture
            .call(json!({"mode":mode,"key":"key_alice","provider_mode":"scoped-cost"}))
            .await;
        assert!(result.get("error").is_none(), "{result}");
        let events: Vec<ExecutionEvent> = serde_json::from_value(result["batch"].clone()).unwrap();
        let payload = ModelEventBatch { events }.encode().unwrap();
        let decoded = tokio::task::spawn_blocking(move || {
            let mut child = std::process::Command::new(decoder)
                .arg("--decode-model-batch")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&payload).unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice::<Vec<Value>>(&output.stdout).unwrap()
        })
        .await
        .unwrap();
        let cost = decoded
            .iter()
            .find_map(|event| event.pointer("/host/costs/0/breakdown"))
            .unwrap();
        assert_eq!(cost["custom_multiplier_bps"], 20_000);
        assert_eq!(cost["input_amount"]["amount"], "0.0000000006");
        assert_eq!(cost["output_amount"]["amount"], "0.0000000005");
        assert!(cost.get("custom_multiplier_scope").is_none());
        fixture.clean(index + 1).await;
    }
    {
        let charges = fixture.core.charges.lock().unwrap();
        assert_eq!(charges.len(), 3);
        // Input-only 2x: 3 * 2 + 5 = 11, not (3 + 5) * 2 = 16.
        assert!(
            charges
                .iter()
                .all(|charge| charge.amount_usd.scaled() == 11)
        );
    }
    fixture.manager.shutdown().await;
}
