use std::time::{Duration, SystemTime};

use gateway_core::account::AccountQuotaUsageLimits;
use provider_antigravity::QuotaDocument;
use serde_json::json;

#[test]
fn local_window_limit_applies_to_its_models_even_with_credits() {
    let doc: QuotaDocument = serde_json::from_value(json!({
        "tier": {
            "id": "paid",
            "availableCredits": [{"creditType":"GOOGLE_ONE_AI","creditAmount":"20"}],
        },
        "summary": {"groups":[{
            "displayName": "Gemini",
            "buckets":[{
                "bucketId":"gemini-5h","displayName":"Five hours",
                "window":"5h","remainingFraction":0.8,
            }],
        }]},
        "models":{
            "gemini-main":{"remainingFraction":0.0},
            "other-model":{"remainingFraction":0.5},
        },
        "bucket_models":{"gemini-5h":["gemini-main"]},
    }))
    .unwrap();
    let now = SystemTime::now();
    let cap = AccountQuotaUsageLimits::new(20, 100).unwrap();
    assert!(doc.model_signals("gemini-main", true, cap, now, now).0);
    assert!(!doc.model_signals("other-model", true, cap, now, now).0);
    let higher_cap = AccountQuotaUsageLimits::new(21, 100).unwrap();
    assert!(
        !doc.model_signals("gemini-main", true, higher_cap, now, now)
            .0
    );
    assert!(
        !doc.model_signals(
            "gemini-main",
            true,
            AccountQuotaUsageLimits::UNLIMITED,
            now,
            now,
        )
        .0
    );
    assert!(
        !doc.model_signals(
            "gemini-main",
            true,
            cap,
            now - Duration::from_secs(601),
            now,
        )
        .0
    );
}
