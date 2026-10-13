mod dispatcher;
pub(crate) mod fixture;
mod websocket;

async fn host_service_request(
    f: &Fixture,
    cookie: &str,
    operation: &str,
    input: serde_json::Value,
) -> axum::response::Response {
    use base64::Engine;
    let payload = base64::engine::general_purpose::STANDARD.encode(
        serde_json::to_vec(&serde_json::json!({"operation":operation,"input":input})).unwrap(),
    );
    recovery_request(f,"POST","/api/admin/plugins/invoke",cookie,serde_json::json!({"id":"fixture","revision":1,"method":"POST","path":"echo","contentType":"application/json","body":payload})).await
}
async fn host_service_reply(response: axum::response::Response) -> serde_json::Value {
    use base64::Engine;
    assert_eq!(response.status(), StatusCode::OK);
    let envelope: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(envelope["data"]["body"].as_str().unwrap())
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
#[tokio::test]
async fn host_services_from_management_use_actual_admin_actor_and_exclude_the_calling_plugin() {
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let input = serde_json::json!({"actor":{"admin_session":{"admin_user_id":"forged-user"}},"request_id":"forged-request"});
    for (cookie, status) in [
        ("", StatusCode::UNAUTHORIZED),
        (
            "cpr_admin_session=middleware-ordinary",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = host_service_request(
            &f,
            cookie,
            "settings.regenerate_admin_api_key",
            input.clone(),
        )
        .await;
        assert_eq!(response.status(), status);
        body(response).await;
    }
    assert!(f.plugin.service_ports.lock().unwrap().is_empty());
    let generated = host_service_reply(
        host_service_request(&f, &cookie, "settings.regenerate_admin_api_key", input).await,
    )
    .await;
    assert!(
        generated["Ok"]["key"]
            .as_str()
            .unwrap()
            .starts_with("admin-")
    );
    let exists = host_service_reply(
        host_service_request(
            &f,
            &cookie,
            "settings.admin_api_key_exists",
            serde_json::Value::Null,
        )
        .await,
    )
    .await;
    assert_eq!(exists, serde_json::json!({"Ok":true}));
    let deleted = host_service_reply(
        host_service_request(
            &f,
            &cookie,
            "settings.delete_admin_api_key",
            serde_json::json!({"actor":"system","request_id":"untrusted"}),
        )
        .await,
    )
    .await;
    assert_eq!(deleted["Ok"]["exists"], false);
    {
        let mutations = f.admin.settings.key_mutations.lock().unwrap();
        assert_eq!(mutations.len(), 2);
        for mutation in mutations.iter() {
            assert_eq!(
                mutation.actor,
                gateway_admin::model::MutationActor::AdminSession {
                    admin_user_id: "admin_1".into()
                }
            );
            assert_ne!(mutation.request_id, "forged-request");
            assert_ne!(mutation.request_id, "untrusted");
        }
    }
    assert!(
        f.plugin.service_seen.lock().unwrap().is_empty(),
        "calling instance must be excluded from its service child"
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn host_services_validate_catalog_input_and_cannot_reuse_a_completed_http_scope() {
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    for (operation, input, kind) in [
        ("settings.load", serde_json::Value::Null, "unsupported"),
        (
            "settings.admin_api_key_exists",
            serde_json::json!({}),
            "invalid",
        ),
        (
            "settings.delete_admin_api_key",
            serde_json::json!({"actor":"system","request_id":"x","extra":1}),
            "invalid",
        ),
    ] {
        let value =
            host_service_reply(host_service_request(&f, &cookie, operation, input).await).await;
        assert_eq!(value["Err"]["kind"], kind);
    }
    assert!(f.admin.settings.key_mutations.lock().unwrap().is_empty());
    let port = f.plugin.service_ports.lock().unwrap()[0].clone();
    let error = port
        .call(
            "fixture".into(),
            "settings.regenerate_admin_api_key".into(),
            serde_json::json!({"actor":"system","request_id":"x"}),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, "unavailable");
    assert!(f.admin.settings.key_mutations.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn host_services_and_http_children_consume_the_same_graph_budget() {
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let value = host_service_reply(
        host_service_request(&f, &cookie, "budget", serde_json::Value::Null).await,
    )
    .await;
    assert_eq!(value, serde_json::json!({"shared":true}));
    f.manager.shutdown().await;
}

fn local_settings_input() -> serde_json::Value {
    serde_json::json!({"subscription_auto_reset_enabled":false,"model_mappings":{"alias":"gpt-5.4"},"refresh_margin_seconds":1800,"refresh_concurrency":4,"max_concurrent_per_account":5,"request_interval_ms":25,"rotation_strategy":"round_robin","min_codex_desktop_version":null,"min_codex_cli_version":null,"usage_retention_days":32,"ops_event_retention_days":31,"audit_retention_days":91})
}
fn settings_http_input() -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (name, value) in local_settings_input().as_object().unwrap() {
        let mut words = name.split('_');
        let mut key = words.next().unwrap().to_owned();
        for word in words {
            let mut chars = word.chars();
            key.extend(chars.next().unwrap().to_uppercase());
            key.extend(chars);
        }
        object.insert(key, value.clone());
    }
    object.into()
}
async fn local_call(
    f: &Fixture,
    cookie: &str,
    operation: &str,
    input: serde_json::Value,
) -> serde_json::Value {
    host_service_reply(
        host_service_request(f, cookie, &format!("local.settings.v1.{operation}"), input).await,
    )
    .await
}
fn untrusted_actor() -> serde_json::Value {
    serde_json::json!({"actor":"system","request_id":"forged-request"})
}
#[tokio::test]
async fn local_services_use_native_settings_subscription_and_scoped_pricing_facts() {
    use serde_json::json;
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let before = local_call(&f, &cookie, "load", json!(null)).await;
    let updated = local_call(
        &f,
        &cookie,
        "replace",
        json!([untrusted_actor(), local_settings_input()]),
    )
    .await;
    assert_eq!(updated["Ok"]["subscription_auto_reset_enabled"], false);
    assert_eq!(
        updated["Ok"]["config_revision"].as_u64().unwrap(),
        before["Ok"]["config_revision"].as_u64().unwrap() + 1
    );
    assert_eq!(updated["Ok"]["model_mappings"]["alias"], "gpt-5.4");
    assert!(
        updated["Ok"]
            .get("openai_guardian_reserved_concurrency")
            .is_none()
    );
    let mut unchanged = local_settings_input();
    unchanged["subscription_auto_reset_enabled"] = json!(null);
    assert_eq!(
        local_call(
            &f,
            &cookie,
            "replace",
            json!([untrusted_actor(), unchanged])
        )
        .await["Ok"]["subscription_auto_reset_enabled"],
        false
    );
    let preview = local_call(&f, &cookie, "preview_pricing_sync", json!(null)).await["Ok"].clone();
    assert_eq!(preview["sourceHash"], "a".repeat(64));
    assert_eq!(
        local_call(
            &f,
            &cookie,
            "sync_pricing",
            json!([untrusted_actor(), preview])
        )
        .await,
        json!({"Ok":null})
    );
    assert_eq!(local_call(&f,&cookie,"update_pricing",json!([untrusted_actor(),{"provider":"openai","models":["gpt-5.4"],"change":{"Multiplier":[15000,{"components":["input","cacheRead"]}]}}])).await,json!({"Ok":null}));
    assert_eq!(
        local_call(
            &f,
            &cookie,
            "update_pricing_sync_policy",
            json!([untrusted_actor(),{"enabled":false,"updateExisting":true}])
        )
        .await,
        json!({"Ok":null})
    );
    let pricing = local_call(&f, &cookie, "pricing", json!(null)).await;
    assert_eq!(
        pricing["Ok"]["sync_policy"],
        json!({"enabled":false,"updateExisting":true})
    );
    assert_eq!(pricing["Ok"]["source_hash"], "a".repeat(64));
    assert!(pricing["Ok"]["synced_at"].is_string());
    assert_eq!(
        pricing["Ok"]["overrides"]["openai"]["gpt-5.4"]["multiplierBps"],
        15000
    );
    assert_eq!(
        pricing["Ok"]["overrides"]["openai"]["gpt-5.4"]["multiplierScope"],
        json!({"components":["input","cacheRead"]})
    );
    assert_eq!(
        f.admin
            .services
            .settings()
            .load()
            .await
            .unwrap()
            .request_interval_ms,
        25
    );
    {
        let mutations = f.admin.settings.service_mutations.lock().unwrap();
        assert_eq!(mutations.len(), 5);
        for actor in mutations.iter() {
            assert_eq!(
                actor.actor,
                gateway_admin::model::MutationActor::AdminSession {
                    admin_user_id: "admin_1".into()
                }
            );
            assert_ne!(actor.request_id, "forged-request");
        }
    }
    assert!(f.plugin.service_seen.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn local_services_reject_wrong_contract_and_preserve_preview_source_validation() {
    use serde_json::json;
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let before = f.admin.services.settings().load().await.unwrap();
    let mut unsupported = local_settings_input();
    unsupported["openai_guardian_reserved_concurrency"] = json!(1);
    let mut invalid = local_settings_input();
    invalid["refresh_margin_seconds"] = json!(u64::MAX);
    let mut models = local_settings_input();
    models["model_mappings"] = json!({"bad\nmodel":"ok"});
    for value in [unsupported, invalid, models] {
        assert_eq!(
            local_call(&f, &cookie, "replace", json!([untrusted_actor(), value])).await["Err"]["kind"],
            "invalid"
        );
    }
    let preview = local_call(&f, &cookie, "preview_pricing_sync", json!(null)).await["Ok"].clone();
    let mut wrong_hash = preview.clone();
    wrong_hash["sourceHash"] = json!("b".repeat(64));
    let mut wrong_price = preview;
    wrong_price["prices"]["openai"]["gpt-5.4"]["bands"]["standard"]["input"] = json!("0");
    for value in [wrong_hash, wrong_price] {
        assert_eq!(
            local_call(
                &f,
                &cookie,
                "sync_pricing",
                json!([untrusted_actor(), value])
            )
            .await["Err"]["kind"],
            "invalid"
        );
    }
    for change in [
        json!("Delete"),
        json!({"Multiplier":15000}),
        json!({"Multiplier":[15000,{"components":[]}]}),
    ] {
        assert_eq!(local_call(&f,&cookie,"update_pricing",json!([untrusted_actor(),{"provider":"openai","models":["gpt-5.4"],"change":change}])).await["Err"]["kind"],"invalid");
    }
    let old = host_service_reply(
        host_service_request(
            &f,
            &cookie,
            "settings.replace",
            json!([untrusted_actor(), local_settings_input()]),
        )
        .await,
    )
    .await;
    assert_eq!(old["Err"]["kind"], "unsupported");
    assert_eq!(f.admin.services.settings().load().await.unwrap(), before);
    assert!(
        f.admin
            .settings
            .service_mutations
            .lock()
            .unwrap()
            .is_empty()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn local_service_http_endpoints_share_the_same_middleware_operations() {
    use serde_json::json;
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    for (method, path, input) in [
        ("GET", "/api/admin/settings", json!(null)),
        ("POST", "/api/admin/settings/update", settings_http_input()),
        ("GET", "/api/admin/settings/pricing", json!(null)),
        (
            "POST",
            "/api/admin/settings/pricing/sync/preview",
            json!(null),
        ),
        (
            "POST",
            "/api/admin/settings/pricing/sync/policy",
            json!({"enabled":false,"updateExisting":false}),
        ),
        (
            "POST",
            "/api/admin/settings/pricing/update",
            json!({"provider":"openai","models":["gpt-5.4"],"change":{"action":"replace","pricing":{"multiplierBps":12000,"multiplierScope":{"components":["output"]},"bands":{"standard":{"input":"2.5","output":"15","cacheRead":"0.25","cacheWrite":"0"}}}}}),
        ),
    ] {
        let response = recovery_request(&f, method, path, &cookie, input).await;
        assert_eq!(response.status(), 200, "{path}");
        body(response).await;
    }
    let preview = f
        .admin
        .services
        .settings()
        .preview_pricing_sync()
        .await
        .unwrap();
    let response = recovery_request(
        &f,
        "POST",
        "/api/admin/settings/pricing/sync",
        &cookie,
        serde_json::to_value(preview).unwrap(),
    )
    .await;
    assert_eq!(response.status(), 200);
    body(response).await;
    let seen = f
        .plugin
        .service_seen
        .lock()
        .unwrap()
        .iter()
        .map(|(ctx, _)| ctx.operation)
        .collect::<Vec<_>>();
    assert_eq!(
        seen,
        vec![
            "local.settings.v1.load",
            "local.settings.v1.replace",
            "local.settings.v1.pricing",
            "local.settings.v1.preview_pricing_sync",
            "local.settings.v1.update_pricing_sync_policy",
            "local.settings.v1.update_pricing",
            "local.settings.v1.sync_pricing"
        ]
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn local_service_command_rewrites_keep_domain_validation_and_cannot_change_actor() {
    for mode in [Mode::Settings, Mode::ExpandInvalid, Mode::Rewrite] {
        let succeeds = matches!(mode, Mode::Settings);
        let (f, cookie) = service_fixture(mode).await;
        let before = f.admin.services.settings().load().await.unwrap();
        let response = recovery_request(
            &f,
            "POST",
            "/api/admin/settings/update",
            &cookie,
            settings_http_input(),
        )
        .await;
        assert_eq!(
            response.status(),
            if succeeds {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            }
        );
        body(response).await;
        let after = f.admin.services.settings().load().await.unwrap();
        if succeeds {
            assert_eq!(after.request_interval_ms, 137);
            assert_eq!(f.admin.settings.service_mutations.lock().unwrap().len(), 1);
        } else {
            assert_eq!(after, before);
            assert!(
                f.admin
                    .settings
                    .service_mutations
                    .lock()
                    .unwrap()
                    .is_empty()
            );
        }
        f.manager.shutdown().await;
    }
}
#[tokio::test]
async fn local_service_native_setting_write_survives_cancel_and_reports_uncertainty() {
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let before = f.admin.services.settings().load().await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *f.admin.settings.service_gate.lock().unwrap() = Some(gate.clone());
    let app = f.app.clone();
    let input = settings_http_input();
    let task = tokio::spawn(async move {
        app.oneshot(
            Request::post("/api/admin/settings/update")
                .header("cookie", cookie)
                .header("content-type", "application/json")
                .body(Body::from(input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while f
            .admin
            .settings
            .service_mutations
            .lock()
            .unwrap()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["code"], 50202);
    assert_eq!(f.admin.services.settings().load().await.unwrap(), before);
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while f
            .admin
            .services
            .settings()
            .load()
            .await
            .unwrap()
            .config_revision
            == before.config_revision
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        f.admin
            .services
            .settings()
            .load()
            .await
            .unwrap()
            .request_interval_ms,
        25
    );
    assert_eq!(f.admin.settings.service_mutations.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}

async fn service_fixture(mode: Mode) -> (Fixture, String) {
    let f = Fixture::with_mounts(
        mode,
        vec![gateway_admin::model::plugins::PluginMiddlewareMount::Service],
    )
    .await;
    f.admin.auth.insert_session("service-admin");
    (f, "cpr_admin_session=service-admin".into())
}
#[tokio::test]
async fn service_settings_api_preserves_auth_native_mutations_and_host_audit_identity() {
    let (f, cookie) = service_fixture(Mode::MessageIdentity).await;
    let path = "/api/admin/settings/admin-api-key";
    for (cookie, status) in [
        ("", StatusCode::UNAUTHORIZED),
        (
            "cpr_admin_session=middleware-ordinary",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = recovery_request(&f, "GET", path, cookie, serde_json::Value::Null).await;
        assert_eq!(response.status(), status);
        body(response).await;
    }
    assert!(f.plugin.service_seen.lock().unwrap().is_empty());
    for (method, suffix) in [
        ("GET", ""),
        ("POST", "/regenerate"),
        ("GET", ""),
        ("POST", "/delete"),
        ("GET", ""),
    ] {
        let response = recovery_request(
            &f,
            method,
            &format!("{path}{suffix}"),
            &cookie,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
        if suffix == "/regenerate" {
            assert!(value["data"]["key"].as_str().unwrap().starts_with("admin-"));
        }
    }
    assert!(
        !f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    {
        let calls = f.plugin.service_seen.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .map(|(ctx, _)| ctx.operation)
                .collect::<Vec<_>>(),
            vec![
                "settings.admin_api_key_exists",
                "settings.regenerate_admin_api_key",
                "settings.admin_api_key_exists",
                "settings.delete_admin_api_key",
                "settings.admin_api_key_exists"
            ]
        );
        let mutations = f.admin.settings.key_mutations.lock().unwrap();
        assert_eq!(mutations.len(), 2);
        for ((context, input), mutation) in calls
            .iter()
            .filter(|(_, input)| !input.is_null())
            .zip(mutations.iter())
        {
            assert!(context.parent_call_id.is_some());
            assert_ne!(context.parent_call_id.as_ref().unwrap(), &context.call_id);
            assert_eq!(input, &serde_json::to_value(mutation).unwrap());
            assert_eq!(
                mutation.actor,
                gateway_admin::model::MutationActor::AdminSession {
                    admin_user_id: "admin_1".into()
                }
            );
            assert_eq!(context.request_id, mutation.request_id);
        }
    }
    f.manager.shutdown().await;
}
#[tokio::test]
async fn service_settings_rejects_forged_audit_inputs_before_native_mutation() {
    let (f, cookie) = service_fixture(Mode::Rewrite).await;
    for suffix in ["regenerate", "delete"] {
        let response = recovery_request(
            &f,
            "POST",
            &format!("/api/admin/settings/admin-api-key/{suffix}"),
            &cookie,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        body(response).await;
    }
    assert!(f.admin.settings.key_mutations.lock().unwrap().is_empty());
    assert!(
        !f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn service_settings_short_circuit_and_post_write_failure_have_distinct_effects() {
    let (f, cookie) = service_fixture(Mode::ShortCircuit).await;
    f.admin.settings.set_api_key("fixture-original-admin-key");
    let response = recovery_request(
        &f,
        "GET",
        "/api/admin/settings/admin-api-key",
        &cookie,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["data"]["exists"], false);
    assert!(
        f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    f.manager.shutdown().await;

    let (f, cookie) = service_fixture(Mode::BadResponse).await;
    let response = recovery_request(
        &f,
        "POST",
        "/api/admin/settings/admin-api-key/regenerate",
        &cookie,
        serde_json::Value::Null,
    )
    .await;
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["code"], 50202);
    assert_eq!(f.admin.settings.key_mutations.lock().unwrap().len(), 1);
    assert!(
        f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn service_settings_revocation_after_native_write_keeps_committed_fact_and_recovery_reachable()
 {
    let (f, cookie) = service_fixture(Mode::WaitAfterNext).await;
    let app = f.app.clone();
    let task_cookie = cookie.clone();
    let task = tokio::spawn(async move {
        app.oneshot(
            Request::post("/api/admin/settings/admin-api-key/regenerate")
                .header("cookie", task_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !f
            .admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["code"], 50202);
    assert_eq!(f.admin.settings.key_mutations.lock().unwrap().len(), 1);
    assert!(
        f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    let recovery = recovery_request(
        &f,
        "GET",
        "/api/admin/plugins",
        &cookie,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(recovery.status(), StatusCode::OK);
    body(recovery).await;
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_settings_entered_transaction_survives_cancel_and_premature_plugin_success() {
    for premature in [false, true] {
        let (f, cookie) = service_fixture(if premature {
            Mode::Head
        } else {
            Mode::MessageIdentity
        })
        .await;
        let gate = std::sync::Arc::new(tokio::sync::Notify::new());
        *f.admin.settings.key_gate.lock().unwrap() = Some(gate.clone());
        let app = f.app.clone();
        let task = tokio::spawn(async move {
            app.oneshot(
                Request::post("/api/admin/settings/admin-api-key/regenerate")
                    .header("cookie", cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while f.admin.settings.key_mutations.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !f.admin
                .services
                .settings()
                .admin_api_key_exists()
                .await
                .unwrap()
        );
        if !premature {
            f.plugin.access.revoke();
        }
        let response = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
        assert_eq!(value["code"], 50202);
        gate.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !f
                .admin
                .services
                .settings()
                .admin_api_key_exists()
                .await
                .unwrap()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(f.admin.settings.key_mutations.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn service_settings_original_store_failure_cannot_be_masked_as_success_by_plugin() {
    use gateway_admin::ports::store::{AdminStoreError, AdminStoreErrorKind};
    let (f, cookie) = service_fixture(Mode::InvalidModel).await;
    *f.admin.settings.key_failure.lock().unwrap() = Some(AdminStoreError::new(
        AdminStoreErrorKind::Conflict,
        "settings",
        "fixture conflict",
    ));
    let response = recovery_request(
        &f,
        "POST",
        "/api/admin/settings/admin-api-key/regenerate",
        &cookie,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    body(response).await;
    assert_eq!(f.admin.settings.key_mutations.lock().unwrap().len(), 1);
    assert!(
        !f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    f.manager.shutdown().await;
}

#[tokio::test]
async fn service_settings_keeps_shared_graph_permit_until_cancelled_native_transaction_finishes() {
    use gateway_admin::{
        model::{AdminErrorKind, MutationActor, MutationContext},
        plugins::service::Origin,
        ports::plugins::PluginService,
    };
    let (f, _) = service_fixture(Mode::MessageIdentity).await;
    let root = gateway_core::engine::nested::ExtensionCallScope::default();
    let child = root.fork_http("host-service-parent".into()).unwrap();
    let cancellation = gateway_core::lifecycle::CancellationToken::default();
    let origin = Origin {
        dispatcher: None,
        registry: f.manager.middleware_registry().unwrap(),
        request_id: "service-audit".into(),
        call_id: "http-parent".into(),
        cancellation: cancellation.clone(),
        deadline_at: Default::default(),
        call_scope: child,
    };
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    *f.admin.settings.key_gate.lock().unwrap() = Some(gate.clone());
    let service = f.admin.services.service_settings();
    let task = tokio::spawn(async move {
        service
            .regenerate_admin_api_key(
                Some(origin),
                MutationContext {
                    actor: MutationActor::System,
                    request_id: "service-audit".into(),
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.admin.settings.key_mutations.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let siblings = (0..15)
        .map(|i| root.fork_http(format!("sibling-{i}")).unwrap())
        .collect::<Vec<_>>();
    assert!(root.fork_http("overflow".into()).is_err());
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.unwrap_err().kind(),
        AdminErrorKind::UpstreamResultUnknown
    );
    // Recorded contexts themselves retain the graph permit; release the test
    // observation so only the still-running native transaction owns it.
    f.plugin.service_seen.lock().unwrap().clear();
    assert!(root.fork_http("still-full".into()).is_err());
    gate.notify_one();
    let released = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(scope) = root.fork_http("released".into()) {
                break scope;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        f.admin
            .services
            .settings()
            .admin_api_key_exists()
            .await
            .unwrap()
    );
    assert_eq!(f.admin.settings.key_mutations.lock().unwrap().len(), 1);
    drop((siblings, released));
    f.manager.shutdown().await;
}

#[tokio::test]
async fn recovery_login_and_disable_survive_rejection_wait_and_fault_without_bypassing_admin_auth()
{
    use gateway_admin::ports::plugins::PluginService;
    for (mode, revoked, pending) in [
        (Mode::MessageIdentity, false, false),
        (Mode::ShortCircuit, false, false),
        (Mode::WaitBeforeNext, false, true),
        (Mode::MessageIdentity, true, false),
    ] {
        let f = new(mode).await;
        if revoked {
            f.plugin.access.revoke();
        }
        let waiting = if pending {
            let app = f.app.clone();
            let task =
                tokio::spawn(async move { app.oneshot(request("/v1/responses")).await.unwrap() });
            entered(&f).await;
            Some(task)
        } else {
            None
        };
        let login = recovery_request(
            &f,
            "POST",
            "/api/admin/auth/login",
            "",
            serde_json::json!({"username":"admin@example.com", "password":"strong-admin-password"}),
        )
        .await;
        assert_eq!(login.status(), StatusCode::OK);
        let cookie = login.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        body(login).await;
        let input = serde_json::json!({"id":"fixture","revision":1,"name":"fixture","artifactSha256":"a".repeat(64),"enabled":false,"configuration":{},"secrets":{},"middleware":{"order":0,"mounts":["http"],"scope":{"kind":"all"}}});
        for (cookie, expected) in [
            ("", StatusCode::UNAUTHORIZED),
            (
                "cpr_admin_session=middleware-ordinary",
                StatusCode::FORBIDDEN,
            ),
        ] {
            let denied = recovery_request(
                &f,
                "POST",
                "/api/admin/plugins/update",
                cookie,
                input.clone(),
            )
            .await;
            assert_eq!(denied.status(), expected);
            body(denied).await;
            assert_eq!(f.manager.list().await.unwrap()[0].revision, 1);
        }
        let response = recovery_request(
            &f,
            "POST",
            "/api/admin/plugins/update",
            &cookie,
            input.clone(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
        assert_eq!(value["data"]["enabled"], false);
        assert_eq!(value["data"]["revision"], 2);
        let stale = recovery_request(&f, "POST", "/api/admin/plugins/update", &cookie, input).await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        body(stale).await;
        if let Some(waiting) = waiting {
            let response = tokio::time::timeout(Duration::from_secs(3), waiting)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            body(response).await;
        }
        assert!(
            f.plugin
                .http_seen
                .lock()
                .unwrap()
                .iter()
                .all(|(path, _)| !path.starts_with("/api/admin/"))
        );
        let response = f.request(false).await;
        assert_eq!(response.status(), StatusCode::OK);
        body(response).await;
        f.settled().await;
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}

async fn recovery_request(
    f: &Fixture,
    method: &str,
    path: &str,
    cookie: &str,
    value: serde_json::Value,
) -> axum::response::Response {
    tokio::time::timeout(
        Duration::from_secs(3),
        f.app.clone().oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("cookie", cookie)
                .header("content-type", "application/json")
                .body(Body::from(value.to_string()))
                .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn recovery_assets_remain_available_but_similar_paths_and_plugin_execution_stay_gated() {
    let root = std::env::temp_dir().join(format!("cpr-plugin-recovery-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join("assets")).unwrap();
    std::fs::write(root.join("index.html"), "recovery-fixture-index").unwrap();
    std::fs::write(root.join("assets/app.js"), "recovery-fixture-script").unwrap();
    std::fs::write(root.join("favicon.svg"), "recovery-fixture-icon").unwrap();
    let f = Fixture::with_assets(Mode::MessageIdentity, root.clone()).await;
    f.plugin.access.revoke();
    for (path, expected) in [
        ("/", "recovery-fixture-index"),
        ("/login", "recovery-fixture-index"),
        ("/plugins?selected=fixture", "recovery-fixture-index"),
        ("/assets/app.js", "recovery-fixture-script"),
        ("/favicon.svg?v=1", "recovery-fixture-icon"),
    ] {
        let response = f
            .app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(body(response).await, expected.as_bytes());
    }
    for path in [
        "/v1/responses",
        "/plugins-other",
        "/assets-other",
        "/api/admin/plugins-extra",
        "/api/admin/plugins/resource",
        "/api/admin/plugins/invoke",
        "/api/admin/plugins/models/responses",
    ] {
        let response = f.app.clone().oneshot(request(path)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{path}");
        body(response).await;
    }
    for path in ["/api/admin/plugins", "/api/admin/plugins/artifacts"] {
        let response = recovery_request(&f, "GET", path, "", serde_json::json!({})).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        body(response).await;
    }
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

use crate::openai::middleware::fixture::{Fixture, Mode};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gateway_admin::model::plugins::PluginMiddlewareMount;
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::ServiceExt;

async fn new(mode: Mode) -> Fixture {
    Fixture::with_mounts(mode, vec![PluginMiddlewareMount::Http]).await
}
fn request(path: &str) -> Request<Body> {
    Request::post(path)
        .header("authorization", "Bearer sk_middleware")
        .header("content-type", "application/json")
        .header("x-request-id", "http-ingress-fixture")
        .body(Body::from(
            r#"{"model":"model-a","input":"original input","stream":false}"#,
        ))
        .unwrap()
}
async fn body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
async fn entered(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.plugin.http_seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn http_method_and_uri_are_rewritten_before_route_matching_with_original_authorization_and_charge()
 {
    let f = new(Mode::Rewrite).await;
    let mut input = request("/missing-route");
    *input.method_mut() = http::Method::PUT;
    let response = f.app.clone().oneshot(input).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-http-layer"], "fixture");
    assert!(
        String::from_utf8(body(response).await)
            .unwrap()
            .contains("resp_original")
    );
    f.settled().await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(
        f.effects.charges.lock().unwrap()[0].key_id.as_str(),
        "key_api_test"
    );
    assert_eq!(f.plugin.http_seen.lock().unwrap()[0].0, "/missing-route");
    assert_eq!(
        f.plugin.http_seen.lock().unwrap()[0].1.request_id,
        "http-ingress-fixture"
    );
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_header_rewrite_still_uses_normal_key_authentication_before_core_admission() {
    let f = new(Mode::HttpAuthError).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    body(response).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_rejection_short_circuits_before_auth_decode_admission_or_metering() {
    let f = new(Mode::ShortCircuit).await;
    let response = f
        .app
        .clone()
        .oneshot(
            Request::post("/v1/responses")
                .body(Body::from(vec![0, 255]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["error"]["type"], "middleware_error");
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_zero_timeout_prevents_terminal_admission_and_has_the_existing_408_status() {
    let f = new(Mode::ZeroTimeout).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    body(response).await;
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    assert!(f.effects.charges.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_runtime_overrides_keep_provenance_and_establish_only_the_authenticated_keys_execution_defaults()
 {
    let f = new(Mode::Settings).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    body(response).await;
    f.settled().await;
    {
        let settings = f.effects.settings.lock().unwrap();
        assert_eq!(settings.len(), 1);
        assert_eq!(
            serde_json::to_value(settings[0].values()).unwrap()["request_interval_ms"],
            137
        );
        assert_eq!(
            settings[0].inspect()["overrides"]["request_interval_ms"]["instance_id"],
            "fixture-http"
        );
        assert_eq!(
            settings[0].inspect()["execution"]["client_key_id"],
            "key_api_test"
        );
        assert_eq!(
            settings[0].execution_values().unwrap().fast_mode,
            gateway_core::account::FastMode::Default
        );
    }
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_placement_runs_for_health_routes_as_well_as_model_routes() {
    let f = new(Mode::MessageIdentity).await;
    let response = f
        .app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["x-http-layer"], "fixture");
    body(response).await;
    assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}

struct Source {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    frames: std::collections::VecDeque<Frame<bytes::Bytes>>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl HttpBody for Source {
    type Data = bytes::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}
fn source() -> (Body, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut trailers = http::HeaderMap::new();
    trailers.append("x-trailer", "one".parse().unwrap());
    trailers.append("x-trailer", "two".parse().unwrap());
    (
        Body::new(Source {
            polls: polls.clone(),
            drops: drops.clone(),
            frames: [
                Frame::data(bytes::Bytes::from_static(b"raw\0invalid\xff")),
                Frame::trailers(trailers),
            ]
            .into(),
        }),
        polls,
        drops,
    )
}
#[tokio::test]
async fn http_short_circuit_keeps_raw_body_lazy_and_preserves_trailers_without_any_core_execution()
{
    let f = new(Mode::ShortCircuitRaw).await;
    let (input, polls, drops) = source();
    let response = f
        .app
        .clone()
        .oneshot(Request::post("/v1/responses").body(input).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let mut output = response.into_body();
    assert_eq!(
        output.frame().await.unwrap().unwrap().into_data().unwrap(),
        b"raw\0invalid\xff".as_slice()
    );
    let trailers = output
        .frame()
        .await
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(
        trailers.get_all("x-trailer").iter().collect::<Vec<_>>(),
        vec!["one", "two"]
    );
    assert!(output.frame().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_revocation_during_before_next_wait_drops_unread_input_and_never_admits() {
    let f = new(Mode::WaitBeforeNext).await;
    let (input, polls, drops) = source();
    let mut run = Box::pin(
        f.app
            .clone()
            .oneshot(Request::post("/v1/responses").body(input).unwrap()),
    );
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=entered(&f)=>{}}
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 0);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_revocation_during_after_next_wait_keeps_the_original_single_settlement() {
    let f = new(Mode::WaitAfterNext).await;
    let mut run = Box::pin(f.app.clone().oneshot(request("/v1/responses")));
    tokio::select! {result=&mut run=>panic!("must wait: {}",result.is_ok()),()=async {while f.effects.charges.lock().unwrap().is_empty() {tokio::task::yield_now().await;}}=>{}}
    f.plugin.access.revoke();
    let response = tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    f.settled().await;
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
#[tokio::test]
async fn http_post_reply_revocation_blocks_buffered_delivery_after_core_settlement() {
    let f = new(Mode::MessageIdentity).await;
    let response = f
        .app
        .clone()
        .oneshot(request("/v1/responses"))
        .await
        .unwrap();
    f.settled().await;
    f.plugin.access.revoke();
    assert!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .is_err()
    );
    assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}

#[tokio::test]
async fn http_request_and_attempt_settings_share_the_frozen_publication_and_original_key_ledger() {
    for streaming in [false, true] {
        let f = Fixture::with_mounts(
            Mode::Settings,
            vec![
                PluginMiddlewareMount::Http,
                PluginMiddlewareMount::Request,
                PluginMiddlewareMount::Attempt,
            ],
        )
        .await;
        let response = f.request(streaming).await;
        assert_eq!(response.status(), StatusCode::OK);
        body(response).await;
        f.settled().await;
        assert_eq!(f.plugin.http_seen.lock().unwrap().len(), 1);
        assert_eq!(f.plugin.seen.lock().unwrap().len(), 2);
        {
            let settings = f.effects.settings.lock().unwrap();
            assert_eq!(settings.len(), 1);
            let sources = settings[0].inspect();
            assert_eq!(
                sources["overrides"]["request_interval_ms"]["instance_id"],
                "fixture-http"
            );
            assert_eq!(sources["execution"]["fast_mode"]["instance_id"], "fixture");
            assert_eq!(sources["execution"]["client_key_id"], "key_api_test");
            assert_eq!(
                settings[0].execution_values().unwrap().timeout_ms,
                Some(60_000)
            );
        }
        assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.effects.charges.lock().unwrap().len(), 1);
        f.manager.shutdown().await;
    }
}

#[tokio::test]
async fn http_revocation_interrupts_stalled_sse_and_releases_original_admission_with_one_finalization()
 {
    use futures::StreamExt;
    let f = new(Mode::MessageIdentity).await;
    *f.effects.completion.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let response = f.request(true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(f.effects.active.load(Ordering::SeqCst), 1);
    f.plugin.access.revoke();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(bytes) => {
                    assert!(!String::from_utf8_lossy(&bytes).contains("response.completed"))
                }
                Err(_) => break,
            }
        }
        while f.effects.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.effects.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.finalizations.lock().unwrap().len(), 1);
    {
        let charges = f.effects.charges.lock().unwrap();
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].amount_usd, gateway_core::metering::Decimal::ZERO);
    }
    f.manager.shutdown().await;
}

#[tokio::test]
async fn local_service_invalid_post_write_result_is_uncertain_and_does_not_undo_commit() {
    let (f, cookie) = service_fixture(Mode::ResponseRewrite).await;
    let response = recovery_request(
        &f,
        "POST",
        "/api/admin/settings/update",
        &cookie,
        settings_http_input(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let value: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(value["code"], 50202);
    let saved = f.admin.services.settings().load().await.unwrap();
    assert_eq!(saved.request_interval_ms, 25);
    assert_eq!(saved.rotation_strategy.as_str(), "round_robin");
    assert_eq!(f.admin.settings.service_mutations.lock().unwrap().len(), 1);
    f.manager.shutdown().await;
}
