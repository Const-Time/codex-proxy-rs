use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use gateway_admin::model::turn_state::{FernetShape, TurnStatePolicy};

fn token(blocks: usize, issued_at: i64) -> String {
    let mut bytes = vec![0; 57 + 16 * blocks];
    bytes[0] = 0x80;
    bytes[1..9].copy_from_slice(&(issued_at as u64).to_be_bytes());
    URL_SAFE.encode(bytes)
}

#[test]
fn state_shape_is_only_a_filter_and_expiry_does_not_extend_on_observation() {
    let now = 1_789_710_000;
    let policy = TurnStatePolicy::default();
    assert!(!policy.enabled);
    policy.validate().unwrap();
    for (blocks, length, eligible) in [
        (10, 292, true),
        (11, 312, false),
        (12, 332, true),
        (13, 356, false),
    ] {
        let shape = FernetShape::parse(&token(blocks, now - 100)).unwrap();
        assert_eq!(shape.header_length, length);
        assert_eq!(shape.candidate_expiry(&policy, now).is_some(), eligible);
        if eligible {
            assert_eq!(
                shape.candidate_expiry(&policy, now),
                shape.candidate_expiry(&policy, now + 10)
            );
        }
    }
    assert!(
        FernetShape::parse(&token(10, now - 3601))
            .unwrap()
            .candidate_expiry(&policy, now)
            .is_none()
    );
    assert!(
        FernetShape::parse(&token(10, now + 61))
            .unwrap()
            .candidate_expiry(&policy, now)
            .is_none()
    );
    assert!(FernetShape::parse("not-a-state").is_none());
    assert!(FernetShape::parse(&"a".repeat(4097)).is_none());
}

#[test]
fn state_policy_supports_explicit_shape_and_rejects_partial_auto_or_unbounded_budgets() {
    let mut policy = TurnStatePolicy {
        header_length: 332,
        cipher_blocks: 12,
        ..Default::default()
    };
    policy.validate().unwrap();
    policy.header_length = 0;
    assert!(policy.validate().is_err());
    policy.cipher_blocks = 0;
    policy.max_attempts = 11;
    assert!(policy.validate().is_err());
}
