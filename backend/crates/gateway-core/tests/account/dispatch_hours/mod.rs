use super::*;
use gateway_core::account::AccountDispatchHours;
use serde_json::json;

fn instant(text: &str) -> SystemTime {
    chrono::DateTime::parse_from_rfc3339(text).unwrap().into()
}

#[test]
fn dispatch_hours_exclude_even_preferred_and_diagnostic_requests_at_end() {
    let hours: AccountDispatchHours = serde_json::from_value(json!({"enabled":true,"timeZone":"Asia/Shanghai","windows":[{"start":"09:00","end":"10:00"},{"start":"22:00","end":"02:00"}]})).unwrap();
    let mut candidate = candidate("acct_hours", 0, None);
    candidate.account = candidate.account.with_dispatch_hours(hours);
    let mut context = context(RotationStrategy::Sticky);
    context.preferred_account = Some(candidate.account.id().clone());
    for eligibility in [
        AccountEligibilityPolicy::Enforce,
        AccountEligibilityPolicy::BypassForDiagnostic,
    ] {
        context.eligibility = eligibility;
        for (time, allowed) in [
            ("2020-11-18T01:00:00Z", true),
            ("2020-11-18T02:00:00Z", false),
            ("2020-11-18T14:00:00Z", true),
            ("2020-11-18T17:59:59Z", true),
            ("2020-11-18T18:00:00Z", false),
        ] {
            context.now = instant(time);
            assert_eq!(
                AccountSelector
                    .select(std::slice::from_ref(&candidate), &context)
                    .is_some(),
                allowed
            );
        }
    }
}

#[test]
fn dispatch_hours_validate_input_and_follow_local_clock_across_dst() {
    let config = json!({"enabled":true,"timeZone":"America/New_York","windows":[{"start":"01:00","end":"02:00"}]});
    let hours: AccountDispatchHours = serde_json::from_value(config.clone()).unwrap();
    for time in ["2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"] {
        assert!(hours.allows_at(instant(time)));
    }
    assert!(!hours.allows_at(instant("2026-11-01T07:00:00Z")));
    for (field, value) in [
        ("timeZone", json!("Unknown/City")),
        ("windows", json!([])),
        ("windows", json!([{"start":"8:00","end":"10:00"}])),
        ("windows", json!([{"start":"10:00","end":"10:00"}])),
    ] {
        let mut invalid = config.clone();
        invalid[field] = value;
        assert!(serde_json::from_value::<AccountDispatchHours>(invalid).is_err());
    }
    assert!(AccountDispatchHours::default().allows_at(instant("2026-11-01T07:00:00Z")));
}
