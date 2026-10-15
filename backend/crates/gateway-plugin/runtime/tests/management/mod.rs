//! Real Redis tickets and SDK subprocesses share the original administrator and process owner.
mod oauth;
use crate::{services::upstream_sdk::environment::Environment, support::*};
use gateway_admin::{
    model::{MutationActor, MutationContext, auth::LoginCommand, plugins::*},
    plugins::PluginManager,
    ports::plugins::PluginService,
};
use gateway_plugin_runtime::PluginEngine;
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn request(query: &str) -> PluginCallbackRequest {
    PluginCallbackRequest {
        request_id: "callback-fixture".into(),
        query: query.into(),
        headers: vec![gateway_core::engine::middleware::MiddlewareHeader::new(
            "x-fixture",
            "callback".into(),
        )],
        invocation: Arc::new(|_| None),
    }
}
fn manager(env: &Environment) -> PluginManager {
    let store = env.store.plugin_store();
    PluginManager::new(
        store.clone(),
        Arc::new(PluginEngine::new(
            env.cache.path().join("callback-plugins"),
            "3.11.2".parse().unwrap(),
            store,
        )),
    )
    .with_management_callbacks(
        env.store.provider_ports().oauth_pending(),
        env.store.admin_ports().auth(),
    )
}
async fn install(manager: &PluginManager, digest: &str) -> PluginView {
    let mut settings = update(digest, 0, true);
    settings.configuration = json!({"public_callbacks":true});
    manager.update(None, settings, &actor()).await.unwrap()
}
async fn administrator(env: &Environment) -> MutationContext {
    let services = env.admin.services();
    let auth = services.auth();
    let session = auth
        .login(LoginCommand {
            username: Some("sdk-fixture@fixture.test".into()),
            password: "sdk-business-fixture-password".into(),
        })
        .await
        .unwrap();
    let id = auth
        .resolve_admin_user_id(Some(&session.session_id))
        .await
        .unwrap()
        .unwrap();
    MutationContext {
        actor: MutationActor::AdminSession { admin_user_id: id },
        request_id: "callback-start".into(),
    }
}
#[tokio::test]
async fn management_callbacks_are_once_only_bound_to_path_owner_revision_and_actual_incarnation() {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let manager = manager(&env);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = install(&manager, &artifact.sha256).await;
    let second = install(&manager, &artifact.sha256).await;
    let admin = administrator(&env).await;
    for ttl in [0, 601] {
        assert!(
            manager
                .callback_ticket(&first.id, first.revision, "oauth", ttl, &admin)
                .await
                .is_err()
        );
    }
    assert!(
        manager
            .callback_ticket(&first.id, first.revision, "missing", 60, &admin)
            .await
            .is_err()
    );
    assert!(
        manager
            .callback_ticket(&first.id, first.revision, "oauth", 60, &actor())
            .await
            .is_err()
    );
    let ticket = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    let mut wrong_owner = ticket.state.clone();
    let tail = wrong_owner.pop().unwrap();
    wrong_owner.push(if tail == '0' { '1' } else { '0' });
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &wrong_owner,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .callback(
                &second.id,
                second.revision,
                "oauth",
                &ticket.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    let (one, two) = tokio::join!(
        manager.callback(
            &first.id,
            first.revision,
            "oauth",
            &ticket.state,
            request("code=dummy")
        ),
        manager.callback(
            &first.id,
            first.revision,
            "oauth",
            &ticket.state,
            request("code=dummy")
        )
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    let result = one.or(two).ok().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.response.body).unwrap()["count"],
        1
    );
    assert_eq!(result.headers[0].name(), "x-plugin-callback");
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &ticket.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    for (path, revision) in [("missing", first.revision), ("oauth", first.revision + 1)] {
        let ticket = manager
            .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
            .await
            .unwrap();
        assert!(
            manager
                .callback(
                    &first.id,
                    revision,
                    path,
                    &ticket.state,
                    request("code=dummy")
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .callback(
                    &first.id,
                    first.revision,
                    "oauth",
                    &ticket.state,
                    request("code=dummy")
                )
                .await
                .is_err()
        );
    }
    let expired = manager
        .callback_ticket(&first.id, first.revision, "oauth", 1, &admin)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &expired.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    let stale = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    manager.shutdown().await;
    let restarted = self::manager(&env);
    restarted.restore().await.unwrap();
    assert!(
        restarted
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &stale.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    let new = restarted
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    let result = restarted
        .callback(
            &first.id,
            first.revision,
            "oauth",
            &new.state,
            request("code=dummy"),
        )
        .await
        .ok()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.response.body).unwrap()["count"],
        2
    );
    restarted.shutdown().await;
    env.close().await;
}
#[tokio::test]
async fn management_callback_fault_disable_and_revoked_administrator_never_replay_effects() {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let manager = manager(&env);
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = install(&manager, &artifact.sha256).await;
    let admin = administrator(&env).await;
    let failed = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &failed.state,
                request("fail=1")
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &failed.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    let next = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    let result = manager
        .callback(
            &first.id,
            first.revision,
            "oauth",
            &next.state,
            request("code=dummy"),
        )
        .await
        .ok()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.response.body).unwrap()["count"],
        2
    );
    let revoked = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    let MutationActor::AdminSession { admin_user_id } = &admin.actor else {
        panic!()
    };
    sqlx::query("UPDATE users SET enabled = false WHERE id = $1")
        .bind(admin_user_id)
        .execute(&env.pool)
        .await
        .unwrap();
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &revoked.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE users SET enabled = true WHERE id = $1")
        .bind(admin_user_id)
        .execute(&env.pool)
        .await
        .unwrap();
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &revoked.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    let disabled = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    manager
        .update(
            Some(&first.id),
            update(&artifact.sha256, first.revision, false),
            &actor(),
        )
        .await
        .unwrap();
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &disabled.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    manager.shutdown().await;
    env.close().await;
}

#[tokio::test]
async fn management_callback_cancellation_keeps_committed_state_and_consumes_host_bound_actor_grant()
 {
    let Some(env) = Environment::with_login_admin().await else {
        return;
    };
    let manager = Arc::new(manager(&env));
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let first = install(&manager, &artifact.sha256).await;
    let admin = administrator(&env).await;
    let ticket = manager
        .callback_ticket(&first.id, first.revision, "oauth", 60, &admin)
        .await
        .unwrap();
    let actors = Arc::new(std::sync::Mutex::new(vec![]));
    let mut input = request("wait=1");
    let recorded = actors.clone();
    input.invocation = Arc::new(move |actor| {
        recorded.lock().unwrap().push(actor.clone());
        None
    });
    let running = {
        let manager = manager.clone();
        let id = first.id.clone();
        let ticket = ticket.state.clone();
        tokio::spawn(async move {
            manager
                .callback(&id, first.revision, "oauth", &ticket, input)
                .await
        })
    };
    let scope = PluginStateScope {
        instance_id: first.id.clone(),
        revision: first.revision,
        namespace: "counter".into(),
        schema_version: 1,
        maximum_records: 10,
        maximum_bytes: 1024,
    };
    let store = env.store.plugin_store();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .state_get(&scope, "callbacks")
                .await
                .unwrap()
                .is_some_and(|r| r.value == json!(1))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    running.abort();
    assert!(running.await.err().unwrap().is_cancelled());
    let captured = actors.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].actor, admin.actor);
    assert_eq!(captured[0].request_id, "callback-fixture");
    assert!(
        manager
            .callback(
                &first.id,
                first.revision,
                "oauth",
                &ticket.state,
                request("code=dummy")
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .state_get(&scope, "callbacks")
            .await
            .unwrap()
            .unwrap()
            .value,
        json!(1)
    );
    manager.shutdown().await;
    env.close().await;
}
