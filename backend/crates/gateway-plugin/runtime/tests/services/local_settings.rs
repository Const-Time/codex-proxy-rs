use super::upstream_sdk::{environment::Environment, manager};
use crate::support::{actor, api::ApiServer, archive, update};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_admin::{
    model::{AdminError, pricing::PricingSyncPreview},
    ports::{plugins::PluginService, pricing::PricingSource},
};
use serde_json::{Value, json};
use sqlx::Row as _;
use std::sync::{Arc, Mutex};

struct Source(Mutex<PricingSyncPreview>);
#[async_trait]
impl PricingSource for Source {
    async fn fetch(&self) -> Result<PricingSyncPreview, AdminError> {
        Ok(self.0.lock().unwrap().clone())
    }
}

async fn call(api: &ApiServer, cookie: &str, id: &str, operation: &str, input: Value) -> Value {
    let body = serde_json::to_vec(&json!({
        "operation":"services", "mode":"local", "local_operation":operation, "input":input
    }))
    .unwrap();
    let response = api.client.post(format!("http://{}/api/admin/plugins/invoke", api.address))
        .header("cookie", cookie).header("x-request-id", format!("local-service-{operation}"))
        .json(&json!({"id":id,"revision":1,"method":"POST","path":"echo","contentType":"application/json","body":STANDARD.encode(body)}))
        .send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["data"]["status"], 200, "{response}");
    serde_json::from_slice(
        &STANDARD
            .decode(response["data"]["body"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap()
}
async fn success(api: &ApiServer, cookie: &str, id: &str, operation: &str, input: Value) -> Value {
    let value = call(api, cookie, id, operation, input).await;
    assert!(value.get("error").is_none(), "{operation}: {value}");
    value.get("result").expect("typed SDK result").clone()
}
async fn revision(env: &Environment) -> i64 {
    sqlx::query_scalar("select config_revision from runtime_settings where id=1")
        .fetch_one(&env.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn typed_local_services_preserve_native_settings_pricing_audit_and_source_confirmation() {
    let price = json!({"multiplierBps":10000,"multiplierScope":"all","bands":{"standard":{
        "input":"2","output":"10","cacheRead":"0","cacheWrite":"1"
    }}});
    let source = Arc::new(Source(Mutex::new(
        serde_json::from_value(json!({
            "prices":{"openai":{"local-service-model":price}},
            "skipped":[], "sourceHash":"a".repeat(64)
        }))
        .unwrap(),
    )));
    let Some(env) = Environment::with_pricing_source(source.clone()).await else {
        return;
    };
    let manager = manager(&env).await;
    let artifact = manager
        .upload(archive("1.0.0", false), &actor())
        .await
        .unwrap();
    let installed = manager
        .update(None, update(&artifact.sha256, 0, true), &actor())
        .await
        .unwrap();
    let id = &installed.id;
    let mut api = ApiServer::new(&env, manager.clone()).await;
    let login = api.client.post(format!("http://{}/api/admin/auth/login", api.address))
        .json(&json!({"username":"sdk-fixture@fixture.test","password":"sdk-business-fixture-password"})).send().await.unwrap();
    assert_eq!(login.status(), reqwest::StatusCode::OK);
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let session = cookie.split_once('=').unwrap().1;
    let user = env
        .admin
        .services()
        .auth()
        .resolve_admin_user_id(Some(session))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("update runtime_settings set reset_credit_lead_minutes=25, reset_credit_max_weekly_remaining_percent=40 where id=1").execute(&env.pool).await.unwrap();
    let before = revision(&env).await;
    env.published.lock().unwrap().clear();
    let forged = json!({"actor":{"admin_session":{"admin_user_id":"forged-admin"}},"request_id":"forged-request"});

    let loaded = success(&api, &cookie, id, "load", Value::Null).await;
    assert_eq!(loaded["config_revision"], before);
    assert!(
        loaded.get("reset_credit_lead_minutes").is_none(),
        "v1 response stays frozen"
    );
    let mut replacement = loaded.clone();
    replacement
        .as_object_mut()
        .unwrap()
        .remove("config_revision");
    replacement.as_object_mut().unwrap().remove("updated_at");
    replacement["subscription_auto_reset_enabled"] = json!(false);
    replacement["model_mappings"] = json!({"local-service-alias":"local-service-model"});
    replacement["request_interval_ms"] = json!(37);
    let saved = success(&api, &cookie, id, "replace", json!([forged, replacement])).await;
    assert_eq!(saved["config_revision"], before + 1);
    assert_eq!(saved["subscription_auto_reset_enabled"], false);
    let settings = env
        .store
        .admin_ports()
        .settings()
        .load_runtime_settings()
        .await
        .unwrap();
    assert!(!settings.subscription_auto_reset_enabled);
    assert_eq!(settings.reset_credit_lead_minutes, 25);
    assert_eq!(settings.reset_credit_max_weekly_remaining_percent, Some(40));
    assert_eq!(settings.request_interval_ms, 37);
    assert_eq!(
        settings
            .model_mappings
            .get(&gateway_core::routing::PublicModelId::new("local-service-alias").unwrap())
            .unwrap()
            .as_str(),
        "local-service-model"
    );

    let initial = success(&api, &cookie, id, "pricing", Value::Null).await;
    assert!(initial["defaults"]["openai"].is_object());
    let preview = success(&api, &cookie, id, "preview_pricing_sync", Value::Null).await;
    assert_eq!(preview["sourceHash"], "a".repeat(64));
    success(&api, &cookie, id, "sync_pricing", json!([forged, preview])).await;
    let stored = env
        .store
        .admin_ports()
        .settings()
        .load_pricing()
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&stored.synced["openai"]["local-service-model"]).unwrap(),
        price
    );
    assert_eq!(stored.source_hash.as_deref(), Some("a".repeat(64).as_str()));
    assert!(stored.synced_at.is_some());

    success(
        &api,
        &cookie,
        id,
        "update_pricing",
        json!([forged,{
            "provider":"openai", "models":["local-service-model"],
            "change":{"Multiplier":[15000,{"components":["input","cacheRead"]}]}
        }]),
    )
    .await;
    let policy = json!({"enabled":false,"updateExisting":true});
    success(
        &api,
        &cookie,
        id,
        "update_pricing_sync_policy",
        json!([forged, policy]),
    )
    .await;
    let catalog = success(&api, &cookie, id, "pricing", Value::Null).await;
    assert_eq!(catalog["sync_policy"], policy);
    assert_eq!(catalog["source_hash"], "a".repeat(64));
    let stored = env
        .store
        .admin_ports()
        .settings()
        .load_pricing()
        .await
        .unwrap();
    assert!(!stored.sync_policy.enabled);
    assert!(stored.sync_policy.update_existing);
    assert_eq!(
        catalog["overrides"],
        serde_json::to_value(&stored.overrides).unwrap()
    );
    let overridden = &stored.overrides["openai"]["local-service-model"];
    assert_eq!(overridden.multiplier_bps, 15000);
    assert_eq!(
        serde_json::to_value(&overridden.multiplier_scope).unwrap(),
        json!({"components":["input","cacheRead"]})
    );
    let committed = revision(&env).await;
    assert_eq!(committed, before + 4);
    assert_eq!(env.published.lock().unwrap().len(), 4);

    // Confirmation must compare the current source, not trust the plugin's preview.
    source.0.lock().unwrap().source_hash = "b".repeat(64);
    let rejected = call(&api, &cookie, id, "sync_pricing", json!([forged, preview])).await;
    assert_eq!(rejected["error"]["kind"], "invalid", "{rejected}");
    let mut invalid = replacement;
    invalid["refresh_concurrency"] = json!(0);
    assert!(
        call(&api, &cookie, id, "replace", json!([forged, invalid]))
            .await
            .get("error")
            .is_some()
    );
    assert_eq!(revision(&env).await, committed);
    assert_eq!(env.published.lock().unwrap().len(), 4);
    let unchanged = success(&api, &cookie, id, "pricing", Value::Null).await;
    assert_eq!(unchanged, catalog);

    let audits = sqlx::query("select actor_kind,actor_admin_user_id,admin_request_id from admin_audit_events where actor_admin_user_id=$1 and config_revision>$2")
        .bind(&user).bind(before).fetch_all(&env.pool).await.unwrap();
    assert_eq!(audits.len(), 4);
    for row in audits {
        assert_eq!(row.get::<String, _>("actor_kind"), "admin_session");
        assert_eq!(
            row.get::<Option<String>, _>("actor_admin_user_id")
                .as_deref(),
            Some(user.as_str())
        );
        assert_ne!(row.get::<String, _>("admin_request_id"), "forged-request");
    }
    for query in [
        "select count(*) from model_requests",
        "select count(*) from user_group_charge_events",
        "select count(*) from user_group_budget_windows",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(&env.pool)
                .await
                .unwrap(),
            0
        );
    }
    let v2 = success(&api, &cookie, id, "load_v2", Value::Null).await;
    assert_eq!(v2["reset_credit_lead_minutes"], 25);
    assert_eq!(v2["reset_credit_max_weekly_remaining_percent"], 40);
    let mut update = v2;
    update.as_object_mut().unwrap().remove("config_revision");
    update.as_object_mut().unwrap().remove("updated_at");
    update["reset_credit_lead_minutes"] = json!(35);
    update["reset_credit_max_weekly_remaining_percent"] = Value::Null;
    let saved = success(&api, &cookie, id, "replace_v2", json!([forged, update])).await;
    assert_eq!(saved["reset_credit_lead_minutes"], 35);
    assert_eq!(
        saved["reset_credit_max_weekly_remaining_percent"],
        Value::Null
    );
    let stored = env
        .store
        .admin_ports()
        .settings()
        .load_runtime_settings()
        .await
        .unwrap();
    assert_eq!(stored.reset_credit_lead_minutes, 35);
    assert_eq!(stored.reset_credit_max_weekly_remaining_percent, None);
    assert!(!stored.subscription_auto_reset_enabled);
    api.close().await;
    drop(api);
    manager.shutdown().await;
    drop(manager);
    env.close().await;
}
