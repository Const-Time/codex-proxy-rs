use super::credentials::Native;
use crate::support::xai::{Proxy, Trust, account};
use gateway_admin::model::plugins::*;
use gateway_core::{
    account::ProviderAccountId, engine::nested::ExtensionCallScope, lifecycle::CancellationToken,
};
use serde_json::json;
use sqlx::Row as _;

#[tokio::test]
#[ignore = "requires the isolated offline OAuth container from check-native-xai-create"]
async fn command_xai_creation_uses_official_oauth_before_native_commit() {
    assert_eq!(std::env::var("CPR_NATIVE_XAI_OFFLINE").as_deref(), Ok("1"));
    let root = match std::env::var_os("CPR_NATIVE_XAI_TRUST") {
        Some(root) => std::path::PathBuf::from(root),
        None => {
            Trust::new().run_ignored("command_line::native_create::command_xai_creation_uses_official_oauth_before_native_commit").await;
            return;
        }
    };
    for mode in ["save", "refresh", "subject", "issuer"] {
        let id = ProviderAccountId::new("acct_create-fixture").unwrap();
        let subject = format!("subject-{id}");
        let candidate = account(&id, "fixture-xai-candidate-token");
        let expiry = chrono::Utc::now()
            + chrono::Duration::seconds(if mode == "refresh" { 20 } else { 3600 });
        let result = json!({"action":"create", "provider_id":"xai", "facts":{
            "name":"Native created xAI", "authentication_kind":"oauth", "has_refresh_token":true,
            "upstream_user_id":subject, "access_token_expires_at_ms":expiry.timestamp_millis(), "next_refresh_at_ms":1,
            "material":candidate.credential.expose_to_provider()}});
        let server = Proxy::start_direct(&root, &subject, mode).await;
        let f = Native::with_configuration(json!({"command_result":{"stdout":"created\n", "stderr":"", "exit_code":0, "accounts":[result]}}), None).await.unwrap();
        assert_eq!(
            f.count().await,
            0,
            "creation must not seed a replacement account"
        );
        let before: i64 =
            sqlx::query_scalar("select config_revision from runtime_settings where id=1")
                .fetch_one(&f.env.pool)
                .await
                .unwrap();
        let graph = ExtensionCallScope::default();
        let result = f
            .process
            .command(
                PluginCommandRequest::system(
                    "probe".into(),
                    vec![],
                    CancellationToken::new(),
                    graph.clone(),
                ),
                None,
            )
            .await;
        graph.wait_until_idle().await;
        let success = matches!(mode, "save" | "refresh");
        if success {
            let output = result.unwrap();
            assert_eq!(output.saved_accounts, 1);
            assert_eq!(output.stdout, "created\n");
        } else {
            assert!(matches!(
                result.err().unwrap(),
                PluginCommandError::Incomplete {
                    saved_accounts: 0,
                    ..
                }
            ));
        }
        assert_eq!(f.count().await, i64::from(success));
        if success {
            let row = sqlx::query("select id,name,enabled,weight,outbound_proxy_url,upstream_user_id,plan_type,access_token_expires_at from provider_accounts").fetch_one(&f.env.pool).await.unwrap();
            let actual = ProviderAccountId::new(row.get::<String, _>("id")).unwrap();
            assert_ne!(actual, id, "native provider owns the generated account ID");
            assert_eq!(row.get::<String, _>("name"), "Native created xAI");
            assert!(row.get::<bool, _>("enabled"));
            assert_eq!(row.get::<Option<String>, _>("outbound_proxy_url"), None);
            assert_eq!(row.get::<String, _>("upstream_user_id"), subject);
            assert_eq!(row.get::<Option<String>, _>("plan_type"), None);
            assert!(
                row.get::<chrono::DateTime<chrono::Utc>, _>("access_token_expires_at")
                    > chrono::Utc::now() + chrono::Duration::minutes(50)
            );
            let stored = f
                .env
                .store
                .provider_ports()
                .accounts()
                .load_current_credential(&actual)
                .await
                .unwrap();
            assert_eq!(stored.account.revision().get(), 1);
            assert_eq!(
                stored.credential.expose_to_provider()["access_token"],
                if mode == "refresh" {
                    "fixture-xai-verified-token"
                } else {
                    "fixture-xai-candidate-token"
                }
            );
            assert_eq!(
                stored.credential.expose_to_provider()["refresh_token"],
                if mode == "refresh" {
                    "fixture-xai-verified-refresh"
                } else {
                    "fixture-refresh-token"
                }
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id=1")
                .fetch_one(&f.env.pool)
                .await
                .unwrap(),
            before + i64::from(success)
        );
        assert_eq!(f.env.published.lock().unwrap().len(), usize::from(success));
        let audits: Vec<(String, Vec<String>)> = sqlx::query_as(
            "select actor_ref,changed_fields from admin_audit_events where actor_kind='plugin'",
        )
        .fetch_all(&f.env.pool)
        .await
        .unwrap();
        assert_eq!(
            audits,
            if success {
                vec![(
                    "plugin:command-fixture@1".into(),
                    vec!["credentials".into()],
                )]
            } else {
                vec![]
            }
        );
        for query in [
            "select count(*) from model_requests",
            "select count(*) from user_group_charge_events",
            "select count(*) from user_group_budget_windows",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(query)
                    .fetch_one(&f.env.pool)
                    .await
                    .unwrap(),
                0
            );
        }
        let events = server.events();
        assert_eq!(
            events.iter().filter(|e| e["kind"] == "discovery").count(),
            1
        );
        let userinfo: Vec<_> = events.iter().filter(|e| e["kind"] == "userinfo").collect();
        assert_eq!(userinfo.len(), usize::from(mode != "issuer"));
        assert!(userinfo.iter().all(|e| e["authorization"] == true));
        let refresh: Vec<_> = events.iter().filter(|e| e["kind"] == "refresh").collect();
        assert_eq!(refresh.len(), usize::from(mode == "refresh"));
        assert!(refresh.iter().all(|e| e["validated"] == true));
        assert!(!events.iter().any(|e| matches!(
            e["kind"].as_str(),
            Some("forbidden" | "inference" | "connect")
        )));
        f.close().await;
        server.stop().await;
    }
}
