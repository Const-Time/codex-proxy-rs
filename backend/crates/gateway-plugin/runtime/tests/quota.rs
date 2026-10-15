use super::support::*;
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationActor, Revision, plugins::*, users::SubscriptionTarget},
    plugins::PluginManager,
    ports::plugins::{PluginQuotaService, PluginService},
};
use gateway_plugin_runtime::PluginEngine;
use gateway_plugin_sdk::call::shared_budgets as wire;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

async fn packaged_call(
    api: &crate::support::api::ApiServer,
    cookie: &str,
    installed: &PluginView,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let response = api.client.post(format!("http://{}/api/admin/plugins/invoke",api.address)).header("cookie",cookie)
        .json(&json!({"id":installed.id,"revision":installed.revision,"method":if body.is_some(){"POST"}else{"GET"},"path":path,"contentType":body.as_ref().map(|_|"application/json"),"body":body.map_or_else(String::new,|body|STANDARD.encode(serde_json::to_vec(&body).unwrap()))}))
        .send().await.unwrap();
    let status = response.status().as_u16();
    let envelope: Value = response.json().await.unwrap();
    if status != 200 {
        return (status, envelope);
    }
    let body = STANDARD
        .decode(envelope["data"]["body"].as_str().unwrap())
        .unwrap();
    (
        envelope["data"]["status"].as_u64().unwrap() as u16,
        serde_json::from_slice(&body).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires the packaged shared-budget plugin and isolated PostgreSQL/Redis"]
async fn packaged_shared_budget_plugin_uses_native_permissions_cas_reset_and_restored_state() {
    use crate::{services::upstream_sdk::environment::Environment, support::api::ApiServer};
    use gateway_admin::model::{account_groups::*, auth::LoginCommand, users::CreateUser};
    use gateway_core::{engine::budget::ClientBudgetLimits, policy::RateLimits};
    let archive = std::fs::read(
        std::env::var_os("CPR_SHARED_BUDGET_ARCHIVE").expect("packaged plugin required"),
    )
    .unwrap();
    let env = Environment::with_login_admin()
        .await
        .expect("isolated database required");
    let build = || {
        Arc::new(PluginManager::new(
            env.store.plugin_store(),
            Arc::new(
                PluginEngine::new(
                    env.cache.path().join("shared-budget-plugin"),
                    "3.12.0".parse().unwrap(),
                    env.store.plugin_store(),
                )
                .with_quota(env.admin.services().plugin_quota()),
            ),
        ))
    };
    let mut manager = build();
    let artifact = manager.upload(archive.into(), &actor()).await.unwrap();
    assert_eq!(artifact.plugin_id, "cpr.shared-budget-manager");
    assert!(artifact.unsupported_capabilities.is_empty());
    let command = |revision, enabled| {
        let mut c = update(&artifact.sha256, revision, enabled);
        c.secrets.clear();
        c
    };
    let mut installed = manager
        .update(None, command(0, true), &actor())
        .await
        .unwrap();
    let login = env
        .admin
        .services()
        .auth()
        .login(LoginCommand {
            username: Some("sdk-fixture@fixture.test".into()),
            password: "sdk-business-fixture-password".into(),
        })
        .await
        .unwrap();
    let admin_id = env
        .admin
        .services()
        .auth()
        .resolve_admin_user_id(Some(&login.session_id))
        .await
        .unwrap()
        .unwrap();
    let context = gateway_admin::model::MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: admin_id,
        },
        request_id: "native-budget-setup".into(),
    };
    let group = env
        .admin
        .services()
        .account_groups()
        .create(
            &context,
            CreateAccountGroup {
                budget: ClientBudgetLimits {
                    daily_usd: "2".parse().unwrap(),
                    weekly_usd: "10".parse().unwrap(),
                },
                model_multipliers: Default::default(),
                name: "Shared plugin group".into(),
                description: None,
                color: AccountGroupColor::parse("#2563EBFF").unwrap(),
            },
        )
        .await
        .unwrap();
    let mut users = vec![];
    for name in ["alice", "bob"] {
        let user = env
            .admin
            .services()
            .auth()
            .create_user(
                CreateUser {
                    username: format!("{name}@fixture.test"),
                    display_name: name.into(),
                    password: "fixture-user-password".into(),
                    limits: RateLimits::unlimited(),
                    group_ids: vec![group.id.to_string()],
                },
                &context,
            )
            .await
            .unwrap();
        sqlx::query("insert into user_group_budget_windows(user_id,account_group_id,daily_start,daily_end,weekly_start,weekly_end,daily_used_usd,weekly_used_usd) values($1,$2,now()-interval '1 hour',now()+interval '1 day',now()-interval '1 hour',now()+interval '7 days',1,3)").bind(&user.id).bind(group.id.as_str()).execute(&env.pool).await.unwrap();
        users.push(user);
    }
    let target = json!({"user_id":users[0].id,"group_id":group.id.as_str()});
    let cookie = format!("cpr_admin_session={}", login.session_id);
    let mut api = ApiServer::new(&env, manager.clone()).await;
    let (status, budget) = packaged_call(
        &api,
        &cookie,
        &installed,
        "api/budget",
        Some(target.clone()),
    )
    .await;
    assert_eq!(status, 200, "{budget}");
    let limits = json!({"group_id":group.id.as_str(),"expected_revision":budget["revision"],"daily_limit_usd":"4","weekly_limit_usd":"20"});
    assert_eq!(
        packaged_call(
            &api,
            &cookie,
            &installed,
            "api/limits",
            Some(limits.clone())
        )
        .await
        .0,
        200
    );
    assert_eq!(
        packaged_call(&api, &cookie, &installed, "api/limits", Some(limits))
            .await
            .0,
        409
    );
    let draft = packaged_call(
        &api,
        &cookie,
        &installed,
        "api/reset/prepare",
        Some(json!({"target":target,"expected_version":null})),
    )
    .await;
    assert_eq!(draft.0, 200, "{}", draft.1);
    let event = draft.1["draft"]["command"]["event_id"].clone();
    api.close().await;
    drop(api);
    manager.shutdown().await;
    drop(manager);
    manager = build();
    manager.restore().await.unwrap();
    api = ApiServer::new(&env, manager.clone()).await;
    assert_eq!(
        packaged_call(&api, &cookie, &installed, "api/reset", None).await,
        draft
    );
    let result = packaged_call(
        &api,
        &cookie,
        &installed,
        "api/reset/execute",
        Some(json!({"event_id":event})),
    )
    .await;
    assert_eq!(result.0, 200, "{}", result.1);
    assert_eq!(result.1["draft"]["receipt"]["affected"], 1);
    let alice:String=sqlx::query_scalar("select daily_used_usd::text from user_group_budget_windows where user_id=$1 and account_group_id=$2").bind(&users[0].id).bind(group.id.as_str()).fetch_one(&env.pool).await.unwrap();
    assert_eq!(
        alice.parse::<gateway_core::metering::Decimal>().unwrap(),
        gateway_core::metering::Decimal::ZERO
    );
    let bob:String=sqlx::query_scalar("select weekly_used_usd::text from user_group_budget_windows where user_id=$1 and account_group_id=$2").bind(&users[1].id).bind(group.id.as_str()).fetch_one(&env.pool).await.unwrap();
    assert_eq!(
        bob.parse::<gateway_core::metering::Decimal>().unwrap(),
        "3".parse().unwrap()
    );
    sqlx::query("update user_group_budget_windows set daily_used_usd=0.5 where user_id=$1")
        .bind(&users[0].id)
        .execute(&env.pool)
        .await
        .unwrap();
    assert_eq!(
        packaged_call(
            &api,
            &cookie,
            &installed,
            "api/reset/execute",
            Some(json!({"event_id":event}))
        )
        .await,
        result
    );
    let retained: String = sqlx::query_scalar(
        "select daily_used_usd::text from user_group_budget_windows where user_id=$1",
    )
    .bind(&users[0].id)
    .fetch_one(&env.pool)
    .await
    .unwrap();
    assert_eq!(
        retained.parse::<gateway_core::metering::Decimal>().unwrap(),
        "0.5".parse().unwrap()
    );
    let count: i64 =
        sqlx::query_scalar("select count(*) from subscription_reset_events where reason='plugin'")
            .fetch_one(&env.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let login = env
        .admin
        .services()
        .auth()
        .login(LoginCommand {
            username: Some(users[0].username.clone()),
            password: "fixture-user-password".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        packaged_call(
            &api,
            &format!("cpr_admin_session={}", login.session_id),
            &installed,
            "api/budget",
            Some(target.clone())
        )
        .await
        .0,
        403
    );
    installed = manager
        .update(
            Some(&installed.id),
            command(installed.revision, false),
            &context,
        )
        .await
        .unwrap();
    assert_ne!(
        packaged_call(&api, &cookie, &installed, "api/budget", Some(target))
            .await
            .0,
        200
    );
    api.close().await;
    drop(api);
    manager.shutdown().await;
    drop(manager);
    env.close().await;
}
#[derive(Default)]
struct Quota(Mutex<Vec<PluginMutationScope>>);
#[async_trait]
impl PluginQuotaService for Quota {
    async fn get(
        &self,
        scope: PluginMutationScope,
        target: SubscriptionTarget,
    ) -> Result<PluginSharedBudget, AdminError> {
        self.0.lock().unwrap().push(scope);
        Ok(PluginSharedBudget {
            revision: Revision::new(10).unwrap(),
            target,
            enabled: true,
            group_daily_limit_usd: "2".into(),
            group_weekly_limit_usd: "10".into(),
            quota_multiplier: "2".into(),
            daily_limit_usd: "4".into(),
            weekly_limit_usd: "20".into(),
            daily_used_usd: "1".into(),
            weekly_used_usd: "3".into(),
            daily_resets_at: None,
            weekly_resets_at: None,
            last_reset_at: None,
        })
    }
    async fn update_group_limits(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupBudgetUpdate,
    ) -> Result<PluginResourceMutation<PluginGroupBudget>, AdminError> {
        assert_eq!(command.expected_revision, 10);
        assert_eq!(command.daily_limit_usd.as_deref(), Some("3"));
        assert!(command.weekly_limit_usd.is_none());
        self.0.lock().unwrap().push(scope);
        Ok(PluginResourceMutation {
            revision: Revision::new(11).unwrap(),
            value: PluginGroupBudget {
                group_id: command.group_id,
                daily_limit_usd: "3".into(),
                weekly_limit_usd: "10".into(),
            },
        })
    }
    async fn reset(
        &self,
        scope: PluginMutationScope,
        command: PluginBudgetReset,
    ) -> Result<PluginResourceMutation<u64>, AdminError> {
        assert_eq!(command.targets.len(), 1);
        assert_eq!(command.targets[0].user_id, "alice");
        self.0.lock().unwrap().push(scope);
        Ok(PluginResourceMutation {
            revision: Revision::new(12).unwrap(),
            value: 1,
        })
    }
}
#[tokio::test]
async fn shared_quota_typed_sdk_calls_keep_host_identity_and_reject_key_semantics_and_spoofing() {
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let quota = Arc::new(Quota::default());
    let engine = Arc::new(
        PluginEngine::new(
            cache.path().into(),
            "3.11.2".parse().unwrap(),
            store.clone(),
        )
        .with_quota(quota.clone()),
    );
    let manager = PluginManager::new(store, engine);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let req = |id: &str| {
        let mut r = request("quota");
        r.actor.request_id = id.into();
        r.actor.actor = MutationActor::AdminSession {
            admin_user_id: id.into(),
        };
        r
    };
    let (a, b) = tokio::join!(
        manager.handle(&installed.id, 1, req("a")),
        manager.handle(&installed.id, 1, req("b"))
    );
    for r in [a.unwrap(), b.unwrap()] {
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["reset"]["affected"], 1);
        assert_eq!(v["budget"]["daily_limit_usd"], "4");
        assert_eq!(v["updated"]["revision"], 11);
    }
    {
        let calls = quota.0.lock().unwrap();
        assert_eq!(calls.len(), 6);
        for c in calls.iter() {
            assert_eq!(c.instance_id, installed.id);
            assert_eq!(c.revision, 1);
            assert_eq!(
                c.actor.actor,
                MutationActor::AdminSession {
                    admin_user_id: c.actor.request_id.clone()
                }
            );
        }
    }
    for (method, params, payload, code) in [
        (
            wire::GET,
            json!({}),
            json!({"user_id":"alice","group_id":"group","actor":"admin"}),
            "invalid_input",
        ),
        (
            wire::RESET,
            json!({}),
            json!({"client_key_id":"key","event_id":"id"}),
            "invalid_input",
        ),
        (
            wire::GET,
            json!({"user_id":"alice"}),
            json!({"user_id":"alice","group_id":"group"}),
            "invalid_input",
        ),
        (
            "host.keys.reset_budget",
            json!({}),
            json!({"client_key_id":"key","period":"all"}),
            "unsupported",
        ),
    ] {
        let mut r = request("facts-raw");
        r.body = serde_json::to_vec(
            &json!({"operation":"facts-raw","method":method,"params":params,"payload":payload}),
        )
        .unwrap();
        let v: Value =
            serde_json::from_slice(&manager.handle(&installed.id, 1, r).await.unwrap().body)
                .unwrap();
        assert_eq!(v["error"]["code"], code);
    }
    assert_eq!(quota.0.lock().unwrap().len(), 6);
    manager
        .update(
            Some(&installed.id),
            update(&artifact.sha256, 1, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(manager.handle(&installed.id, 1, req("a")).await.is_err());
    manager.shutdown().await;
}
