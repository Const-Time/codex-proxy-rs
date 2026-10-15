use crate::{
    command_line::credentials::{Native, create},
    support,
};
use gateway_admin::model::{MutationActor, plugins::PluginCommandRequest};
use gateway_core::{engine::nested::ExtensionCallScope, lifecycle::CancellationToken};
use serde_json::{Value, json};

fn input(method: &str, request: Value) -> Value {
    json!({"operation":"models","mode":"credentials","method":method,"request":request})
}
async fn management(fixture: &Native, actor: MutationActor, value: Value) -> Value {
    let mut request = support::request("models");
    request.actor.actor = actor;
    request.body = serde_json::to_vec(&value).unwrap();
    let response = fixture.process.handle(request).await.unwrap();
    serde_json::from_slice(&response.body).unwrap()
}
async fn admin(fixture: &Native, method: &str, request: Value) -> Value {
    management(fixture, MutationActor::AdminApiKey, input(method, request)).await
}
async fn command(fixture: &Native, root: &ExtensionCallScope, value: Value) -> Value {
    let result = fixture
        .process
        .command(
            PluginCommandRequest::system(
                "probe".into(),
                vec!["--input".into(), serde_json::to_string(&value).unwrap()],
                CancellationToken::new(),
                root.clone(),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.saved_accounts, 0);
    serde_json::from_str(&result.stdout).unwrap()
}

#[tokio::test]
async fn credentials_callbacks_use_native_accounts_binary_exports_and_exact_commit_receipts() {
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    let root = ExtensionCallScope::default();
    let before: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&fixture.env.pool)
        .await
        .unwrap();
    let first = command(&fixture, &root, input("host.auth.save", create("auth-one"))).await;
    assert_eq!(first["credential_revision"], 1, "{first}");
    let id = first["account_id"].as_str().unwrap();
    let second = admin(&fixture, "host.auth.save", create("auth-two")).await;
    assert_eq!(second["credential_revision"], 1, "{second}");
    let list = admin(&fixture, "host.auth.list", json!({"limit":200})).await;
    assert_eq!(list["accounts"].as_array().unwrap().len(), 2, "{list}");
    assert!(list["next_cursor"].is_null());
    assert!(!list.to_string().contains("controlled-fixture-refresh"));
    assert!(
        list["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|account| account.get("material").is_none())
    );
    let page = admin(
        &fixture,
        "host.auth.list",
        json!({"provider_id":"openai","limit":1}),
    )
    .await;
    let cursor = page["next_cursor"].as_str().unwrap();
    assert_eq!(page["accounts"][0]["account_id"], cursor);
    let next = admin(
        &fixture,
        "host.auth.list",
        json!({"provider_id":"openai","cursor":cursor,"limit":1}),
    )
    .await;
    assert_eq!(next["accounts"].as_array().unwrap().len(), 1);
    assert_ne!(next["accounts"][0]["account_id"], cursor);
    assert!(next["next_cursor"].is_null());
    assert!(
        admin(
            &fixture,
            "host.auth.list",
            json!({"provider_id":"xai","limit":200})
        )
        .await["accounts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let runtime = command(
        &fixture,
        &root,
        input("host.auth.get_runtime", json!({"account_id":id})),
    )
    .await;
    assert_eq!(runtime["credential_revision"], 1);
    assert!(runtime.get("facts").is_none());
    let exported = admin(&fixture, "host.auth.get", json!({"account_id":id})).await;
    assert_eq!(exported["credential_revision"], 1);
    assert!(
        exported["facts"]["material"]
            .to_string()
            .contains("controlled-fixture-refresh-auth-one")
    );
    // Reimport returns the upsert transaction's version, rather than assuming 1.
    let imported = admin(&fixture, "host.auth.save", create("auth-one")).await;
    assert_eq!(imported["account_id"], id);
    assert_eq!(imported["credential_revision"], 2);
    let mut replacement = create("auth-one");
    replacement["action"] = json!("replace");
    replacement.as_object_mut().unwrap().remove("provider_id");
    replacement["account_id"] = json!(id);
    replacement["credential_revision"] = json!(1);
    let stale = admin(&fixture, "host.auth.save", replacement.clone()).await;
    assert_eq!(stale["error"]["code"], "conflict", "{stale}");
    replacement["credential_revision"] = json!(2);
    let saved = admin(&fixture, "host.auth.save", replacement).await;
    assert_eq!(saved["credential_revision"], 3, "{saved}");
    assert_eq!(fixture.count().await, 2);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 4);
    let native_revision: i64 =
        sqlx::query_scalar("select credential_revision from provider_accounts where id=$1")
            .bind(id)
            .fetch_one(&fixture.env.pool)
            .await
            .unwrap();
    assert_eq!(native_revision, 3);
    let after: i64 = sqlx::query_scalar("select count(*) from user_group_budget_windows")
        .fetch_one(&fixture.env.pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    root.wait_until_idle().await;
    fixture.close().await;
}

#[tokio::test]
async fn credentials_management_checks_actual_native_admin_role_and_enabled_state() {
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    let id: String = sqlx::query_scalar("select id from users where username='sdk-fixture'")
        .fetch_one(&fixture.env.pool)
        .await
        .unwrap();
    let actor = MutationActor::AdminSession {
        admin_user_id: id.clone(),
    };
    let allowed = management(
        &fixture,
        actor.clone(),
        input("host.auth.save", create("session-admin")),
    )
    .await;
    assert_eq!(allowed["credential_revision"], 1, "{allowed}");
    let account_id = allowed["account_id"].as_str().unwrap();
    for (role, enabled) in [("user", true), ("admin", false)] {
        sqlx::query("update users set role=$2, enabled=$3 where id=$1")
            .bind(&id)
            .bind(role)
            .bind(enabled)
            .execute(&fixture.env.pool)
            .await
            .unwrap();
        for (method, request) in [
            ("host.auth.list", json!({"limit":1})),
            ("host.auth.get_runtime", json!({"account_id":account_id})),
            ("host.auth.get", json!({"account_id":account_id})),
            ("host.auth.save", create("denied-session")),
        ] {
            let denied = management(&fixture, actor.clone(), input(method, request)).await;
            assert_eq!(
                denied["error"]["code"], "permission_denied",
                "{method} {role} {enabled}: {denied}"
            );
        }
    }
    sqlx::query("update users set role='admin', enabled=true where id=$1")
        .bind(&id)
        .execute(&fixture.env.pool)
        .await
        .unwrap();
    let restored = management(
        &fixture,
        actor,
        input("host.auth.get", json!({"account_id":account_id})),
    )
    .await;
    assert_eq!(restored["account_id"], account_id);
    let missing = management(
        &fixture,
        MutationActor::AdminSession {
            admin_user_id: "missing-user".into(),
        },
        input("host.auth.list", json!({"limit":1})),
    )
    .await;
    assert_eq!(missing["error"]["code"], "permission_denied");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn credentials_management_rejects_forged_system_and_plugin_authority() {
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    for actor in [
        MutationActor::System,
        MutationActor::Plugin {
            instance_id: fixture.record.id.clone(),
            revision: std::num::NonZeroU64::new(fixture.record.revision).unwrap(),
        },
    ] {
        let denied = management(
            &fixture,
            actor,
            input("host.auth.save", create("forged-admin")),
        )
        .await;
        assert_eq!(denied["error"]["code"], "permission_denied", "{denied}");
    }
    let mut forged = input("host.auth.save", create("forged-input"));
    forged["params"] = json!({"stage":"command_line","actor":"system"});
    let denied = management(&fixture, MutationActor::AdminApiKey, forged).await;
    assert_eq!(denied["error"]["code"], "invalid_input", "{denied}");
    assert_eq!(fixture.count().await, 0);
    fixture.close().await;
}

#[tokio::test]
async fn credentials_callbacks_validate_limits_typed_ids_and_unknown_fields_without_commit() {
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    for (method, request, code) in [
        ("host.auth.list", json!({"limit":0}), "invalid_input"),
        ("host.auth.list", json!({"limit":201}), "invalid_input"),
        (
            "host.auth.list",
            json!({"limit":1,"owner":"another-user"}),
            "invalid_input",
        ),
        ("host.auth.get", json!({"account_id":""}), "invalid_input"),
        (
            "host.auth.get_runtime",
            json!({"account_id":""}),
            "invalid_input",
        ),
        (
            "host.auth.get_runtime",
            json!({"account_id":"acct_missing"}),
            "rejected",
        ),
        (
            "host.auth.get",
            json!({"account_id":"acct_missing"}),
            "rejected",
        ),
    ] {
        let output = admin(&fixture, method, request).await;
        assert_eq!(output["error"]["code"], code, "{method}: {output}");
    }
    let mut forged = create("unknown-field");
    forged["actor"] = json!("system");
    let output = admin(&fixture, "host.auth.save", forged).await;
    assert_eq!(output["error"]["code"], "invalid_input", "{output}");
    assert_eq!(fixture.count().await, 0);
    assert!(fixture.env.published.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn credentials_reads_and_saves_lock_actual_enabled_plugin_revision_before_account_access() {
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    let saved = admin(&fixture, "host.auth.save", create("fenced-auth")).await;
    let account_id = saved["account_id"].as_str().unwrap();
    let mut record = fixture.record.clone();
    for enabled in [false, true] {
        let expected = record.revision;
        record.revision += 1;
        record.enabled = enabled;
        fixture
            .env
            .store
            .plugin_store()
            .save(&record, expected, &support::actor(), None)
            .await
            .unwrap();
        assert_eq!(
            fixture.process.status(),
            "ready",
            "test must exercise the database fence while the old process still runs"
        );
        for (method, request) in [
            ("host.auth.list", json!({"limit":1})),
            ("host.auth.get_runtime", json!({"account_id":account_id})),
            ("host.auth.get", json!({"account_id":account_id})),
            ("host.auth.save", create("denied-fenced-auth")),
        ] {
            let output = admin(&fixture, method, request).await;
            assert_eq!(
                output["error"]["code"], "conflict",
                "{method} enabled={enabled}: {output}"
            );
        }
    }
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn credentials_maintenance_uses_host_issued_plugin_actor_and_original_native_store() {
    let Some(fixture) = Native::with_archive(
        json!({"adapter_mode":"maintenance-credentials","command_result":create("maintenance-auth")}),
        None,support::maintenance_archive("1.0.0")).await else {return;};
    assert!(fixture.process.supports_maintenance());
    for expected in [1, 2] {
        fixture
            .process
            .reconcile(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(fixture.count().await, 1);
        let revision: i64 = sqlx::query_scalar("select credential_revision from provider_accounts")
            .fetch_one(&fixture.env.pool)
            .await
            .unwrap();
        assert_eq!(revision, expected);
    }
    assert_eq!(fixture.env.published.lock().unwrap().len(), 2);
    fixture.close().await;
}

#[tokio::test]
async fn credentials_callback_cancel_and_deadline_retain_inflight_native_commit_on_original_graph()
{
    use gateway_admin::model::plugins::PluginCommandError;
    use std::time::Duration;
    for deadline in [false, true] {
        let Some(fixture) = Native::with_configuration(json!({}), None).await else {
            return;
        };
        let mut lock = fixture.env.pool.begin().await.unwrap();
        let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        sqlx::query("select id from runtime_settings where id=1 for update")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let root = ExtensionCallScope::default();
        let cancellation = CancellationToken::new();
        let mut invocation = Box::pin(fixture.process.command(
            PluginCommandRequest::system(
                "probe".into(),
                vec![
                        "--input".into(),
                        serde_json::to_string(&input(
                            "host.auth.save",
                            create("inflight-callback"),
                        ))
                        .unwrap(),
                    ],
                cancellation.clone(),
                root.clone(),
            ),
            None,
        ));
        tokio::select! {
            result=&mut invocation=>panic!("save must reach the native transaction before returning: {}",result.is_ok()),
            result=tokio::time::timeout(Duration::from_secs(5),async {
                loop {
                    let blocked:bool=sqlx::query_scalar("select exists(select 1 from pg_stat_activity where datname=current_database() and query like 'update runtime_settings%' and $1=any(pg_blocking_pids(pid)))")
                        .bind(lock_pid).fetch_one(&fixture.env.pool).await.unwrap();
                    if blocked {break;}
                    tokio::task::yield_now().await;
                }
            })=>result.unwrap(),
        }
        if deadline {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(121)).await;
        } else {
            cancellation.cancel();
        }
        let result = invocation.await;
        if deadline {
            tokio::time::resume();
        }
        assert!(matches!(
            result,
            Err(PluginCommandError::Incomplete {
                saved_accounts: 0,
                ..
            })
        ));
        let mut draining = Box::pin(root.wait_until_idle());
        assert!(
            futures::poll!(draining.as_mut()).is_pending(),
            "the native save must retain the actual root's permit"
        );
        assert_eq!(fixture.count().await, 0);
        lock.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), draining)
            .await
            .unwrap();
        assert_eq!(fixture.count().await, 1);
        assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
        fixture.close().await;
    }
}

async fn wait_for_blocked_commit(fixture: &Native, lock_pid: i32) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("select exists(select 1 from pg_stat_activity where datname=current_database() and query like 'update runtime_settings%' and $1=any(pg_blocking_pids(pid)))")
                .bind(lock_pid).fetch_one(&fixture.env.pool).await.unwrap();
            if blocked {return;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn credentials_management_drop_and_process_stop_drain_own_commit_without_holding_parent_permits()
 {
    use gateway_admin::model::plugins::PluginInvocationContext;
    use gateway_core::engine::middleware::http::Context;
    use std::time::Duration;
    let Some(fixture) = Native::with_configuration(json!({}), None).await else {
        return;
    };
    let root = ExtensionCallScope::default();
    let parent = root.fork_http("native-parent".into()).unwrap();
    let sibling = root.fork_http("native-sibling".into()).unwrap();
    let parent_cancellation = CancellationToken::new();
    let mut request = support::request("models");
    request.actor.actor = MutationActor::AdminApiKey;
    request.body = serde_json::to_vec(&input("host.auth.save", create("parent-drain"))).unwrap();
    request.invocation = Some(PluginInvocationContext {
        context: Context {
            request_id: request.actor.request_id.clone(),
            call_id: "native-http-parent".into(),
            parent_call_id: None,
            call_scope: parent.clone(),
            model_parent: None,
            cancellation: parent_cancellation.clone(),
            deadline_at: Default::default(),
        },
        services: None,
        http: None,
    });
    let mut lock = fixture.env.pool.begin().await.unwrap();
    let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("select id from runtime_settings where id=1 for update")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let mut invocation = Box::pin(fixture.process.handle(request));
    tokio::select! {
        _=&mut invocation=>panic!("native save must be blocked"),
        ()=wait_for_blocked_commit(&fixture,lock_pid)=>{},
    }
    drop(invocation);
    let mut stopping = Box::pin(fixture.process.stop());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    assert_eq!(fixture.count().await, 0);
    lock.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), stopping)
        .await
        .unwrap();
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
    assert!(
        !parent_cancellation.is_cancelled(),
        "plugin shutdown must not cancel the original parent"
    );
    let mut parent_drain = Box::pin(root.wait_until_idle());
    assert!(
        futures::poll!(parent_drain.as_mut()).is_pending(),
        "parent and sibling remain independently alive after plugin shutdown"
    );
    drop(parent);
    drop(sibling);
    tokio::time::timeout(Duration::from_secs(5), parent_drain)
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn credentials_maintenance_cancel_and_active_stop_wait_for_original_native_commit() {
    use std::time::Duration;
    for cancel in [false, true] {
        let Some(fixture)=Native::with_archive(json!({"adapter_mode":"maintenance-credentials","command_result":create("maintenance-drain")}),None,support::maintenance_archive("1.0.0")).await else {return;};
        let mut lock = fixture.env.pool.begin().await.unwrap();
        let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        sqlx::query("select id from runtime_settings where id=1 for update")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let mut invocation = Box::pin(fixture.process.reconcile(cancellation.clone()));
        tokio::select! {
            _=&mut invocation=>panic!("native maintenance save must be blocked"),
            ()=wait_for_blocked_commit(&fixture,lock_pid)=>{},
        }
        if cancel {
            cancellation.cancel();
            assert!(invocation.await.is_err());
        } else {
            fixture.process.revoke();
            assert!(invocation.await.is_err());
        }
        let mut stopping = Box::pin(fixture.process.stop());
        assert!(futures::poll!(stopping.as_mut()).is_pending());
        assert_eq!(fixture.count().await, 0);
        lock.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), stopping)
            .await
            .unwrap();
        assert_eq!(fixture.count().await, 1);
        assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
        fixture.close().await;
    }
}

#[tokio::test]
async fn credentials_manager_shutdown_waits_for_detached_management_account_commit() {
    use gateway_admin::{plugins::PluginManager, ports::plugins::PluginService};
    use gateway_plugin_runtime::PluginEngine;
    use std::{sync::Arc, time::Duration};
    let Some(fixture) =
        Native::with_archive(json!({}), None, support::maintenance_archive("1.0.0")).await
    else {
        return;
    };
    let manager = PluginManager::new(
        fixture.env.store.plugin_store(),
        Arc::new(
            PluginEngine::new(
                fixture.env.cache.path().join("manager-drain"),
                "3.11.2".parse().unwrap(),
                fixture.env.store.plugin_store(),
            )
            .with_credentials(fixture.env.admin.services().plugin_credentials()),
        ),
    );
    manager.restore().await.unwrap();
    let mut request = support::request("models");
    request.actor.actor = MutationActor::AdminApiKey;
    request.body = serde_json::to_vec(&input("host.auth.save", create("manager-drain"))).unwrap();
    let mut lock = fixture.env.pool.begin().await.unwrap();
    let lock_pid: i32 = sqlx::query_scalar("select pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("select id from runtime_settings where id=1 for update")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let mut invocation =
        Box::pin(manager.handle(&fixture.record.id, fixture.record.revision, request));
    tokio::select! {
        _=&mut invocation=>panic!("native managed save must be blocked"),
        ()=wait_for_blocked_commit(&fixture,lock_pid)=>{},
    }
    drop(invocation);
    let mut stopping = Box::pin(manager.shutdown());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    assert_eq!(fixture.count().await, 0);
    lock.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), stopping)
        .await
        .unwrap();
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.env.published.lock().unwrap().len(), 1);
    drop(manager);
    fixture.close().await;
}
