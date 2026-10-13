use chrono::Utc;
use gateway_store::postgres::{AdminAuditActorKind, AdminAuditEvent};

#[test]
fn audit_event_rejects_more_than_sixty_four_changed_fields() {
    let event = AdminAuditEvent {
        id: "audit-1".to_owned(),
        actor_kind: AdminAuditActorKind::System,
        actor_admin_user_id: None,
        actor_ref: "system".to_owned(),
        admin_request_id: None,
        action: "update".to_owned(),
        entity_kind: "settings".to_owned(),
        entity_ref: "1".to_owned(),
        config_revision: Some(2),
        changed_fields: (0..65).map(|index| format!("field-{index}")).collect(),
        created_at: Utc::now(),
    };
    assert!(event.validate().is_err());
}

fn plugin_event() -> AdminAuditEvent {
    AdminAuditEvent {
        id: uuid::Uuid::new_v4().to_string(),
        actor_kind: AdminAuditActorKind::Plugin,
        actor_admin_user_id: None,
        actor_ref: "plugin:plugin_fixture@7".into(),
        admin_request_id: Some("host-request".into()),
        action: "admin_api_key.replace".into(),
        entity_kind: "settings".into(),
        entity_ref: "runtime".into(),
        config_revision: None,
        changed_fields: vec!["admin_api_key".into()],
        created_at: Utc::now(),
    }
}
#[test]
fn plugin_audit_identity_rejects_wrong_principal_and_malformed_or_zero_revision() {
    assert!(plugin_event().validate().is_ok());
    for identity in [
        "plugin:@1",
        "plugin:other@0",
        "plugin:other@01",
        "plugin:other@+1",
        "plugin:other@18446744073709551616",
        "plugin:other@1\n",
        "admin:other",
        "plugin:other@fork@1",
    ] {
        let mut event = plugin_event();
        event.actor_ref = identity.into();
        assert!(event.validate().is_err(), "{identity}");
    }
    let mut event = plugin_event();
    event.actor_admin_user_id = Some("test-owner".into());
    assert!(event.validate().is_err());
    let mut event = plugin_event();
    event.actor_ref = format!("plugin:other@{}", u64::MAX);
    assert!(event.validate().is_ok());
    for (length, valid) in [(256, true), (257, false)] {
        let mut event = plugin_event();
        event.actor_ref = format!("plugin:{}@1", "a".repeat(length));
        assert_eq!(event.validate().is_ok(), valid);
    }
}
#[tokio::test]
async fn plugin_audit_upgrade_preserves_old_rows_and_enforces_identity_in_postgres() {
    let Some(db) = super::TestDatabase::create_at_version("plugin_audit_upgrade", 38).await else {
        return;
    };
    sqlx::query("insert into admin_audit_events(id, actor_kind, actor_ref, action, entity_kind, entity_ref, created_at) values ('legacy', 'system', 'system', 'fixture', 'settings', 'runtime', now())").execute(&db.pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(a) from admin_audit_events a where id='legacy'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    super::TEST_MIGRATOR.run(&db.pool).await.unwrap();
    let after: serde_json::Value =
        sqlx::query_scalar("select to_jsonb(a) from admin_audit_events a where id='legacy'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    for identity in [
        "plugin:@1",
        "plugin:other@0",
        "plugin:other@01",
        "plugin:other@+1",
        "plugin:other@18446744073709551616",
        "plugin:other@1\n",
        "admin:other",
        "plugin:other@fork@1",
    ] {
        let error = sqlx::query("insert into admin_audit_events(id, actor_kind, actor_ref, action, entity_kind, entity_ref, created_at) values ($1, 'plugin', $2, 'fixture', 'settings', 'runtime', now())").bind(uuid::Uuid::new_v4().to_string()).bind(identity).execute(&db.pool).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "{identity}"
        );
    }
    use gateway_store::postgres::{AdminSecurityAuditRepository, PgAdminSecurityAuditRepository};
    let repository = PgAdminSecurityAuditRepository::new(db.pool.clone());
    let event = plugin_event();
    let id = event.id.clone();
    repository.append_admin_audit_event(event).await.unwrap();
    let stored: (String, String, Option<String>, Option<String>) = sqlx::query_as("select actor_kind, actor_ref, actor_admin_user_id, admin_request_id from admin_audit_events where id=$1").bind(id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        stored,
        (
            "plugin".into(),
            "plugin:plugin_fixture@7".into(),
            None,
            Some("host-request".into())
        )
    );
    let mut maximum = plugin_event();
    maximum.actor_ref = format!("plugin:other@{}", u64::MAX);
    repository.append_admin_audit_event(maximum).await.unwrap();
    for (length, valid) in [(256, true), (257, false)] {
        let identity = format!("plugin:{}@1", "a".repeat(length));
        let result = sqlx::query("insert into admin_audit_events(id, actor_kind, actor_ref, action, entity_kind, entity_ref, created_at) values ($1, 'plugin', $2, 'fixture', 'settings', 'runtime', now())").bind(uuid::Uuid::new_v4().to_string()).bind(identity).execute(&db.pool).await;
        assert_eq!(result.is_ok(), valid);
    }
    db.close().await;
}
