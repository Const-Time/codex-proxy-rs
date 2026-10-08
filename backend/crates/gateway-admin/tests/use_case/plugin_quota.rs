use super::AdminHarness;
use gateway_admin::model::{
    AdminErrorKind, MutationActor, MutationContext, plugins::*, users::SubscriptionTarget,
};
const GROUP: &str = "grp_00000000000000000000000000000077";
fn scope() -> PluginMutationScope {
    PluginMutationScope {
        instance_id: "plugin".into(),
        revision: 1,
        actor: MutationContext {
            actor: MutationActor::AdminApiKey,
            request_id: "quota".into(),
        },
    }
}
#[tokio::test]
async fn shared_quota_rejects_ambiguous_scope_unbounded_targets_and_invalid_amounts_before_store() {
    let service = AdminHarness::new().build().await.plugin_quota();
    let target = || SubscriptionTarget {
        user_id: "user".into(),
        group_id: GROUP.into(),
    };
    for event in ["not-uuid", "00000000-0000-7000-8000-000000000000"] {
        assert_eq!(
            service
                .reset(
                    scope(),
                    PluginBudgetReset {
                        event_id: event.into(),
                        targets: vec![target()]
                    }
                )
                .await
                .unwrap_err()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
    for targets in [
        vec![],
        vec![target(); 501],
        vec![SubscriptionTarget {
            user_id: " ".into(),
            group_id: GROUP.into(),
        }],
    ] {
        assert_eq!(
            service
                .reset(
                    scope(),
                    PluginBudgetReset {
                        event_id: uuid::Uuid::new_v4().to_string(),
                        targets
                    }
                )
                .await
                .unwrap_err()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
    for amount in [
        None,
        Some("-1"),
        Some("NaN"),
        Some("10000000000"),
        Some("0.00000000001"),
    ] {
        assert_eq!(
            service
                .update_group_limits(
                    scope(),
                    PluginGroupBudgetUpdate {
                        group_id: GROUP.into(),
                        expected_revision: 1,
                        daily_limit_usd: amount.map(str::to_owned),
                        weekly_limit_usd: None
                    }
                )
                .await
                .unwrap_err()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
    let mut system = scope();
    system.actor.actor = MutationActor::System;
    assert_eq!(
        service.get(system, target()).await.unwrap_err().kind(),
        AdminErrorKind::Invalid
    );
}
