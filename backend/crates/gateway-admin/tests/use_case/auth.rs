use std::{collections::BTreeMap, sync::Mutex};

use async_trait::async_trait;
use chrono::{TimeDelta, Utc};

use gateway_admin::{
    model::{
        auth::{AdminAuditEvent, AdminSession, LoginCommand},
        settings::AdminApiKey,
    },
    ports::store::{AdminStoreResult, AuthStore},
};

#[derive(Default)]
struct MemoryAuthStore {
    password_hash: Mutex<Option<String>>,
    sessions: Mutex<BTreeMap<String, AdminSession>>,
    audits: Mutex<Vec<AdminAuditEvent>>,
}

#[async_trait]
impl AuthStore for MemoryAuthStore {
    async fn set_user_password(
        &self,
        _: &str,
        hash: &str,
        _: bool,
        _: &gateway_admin::model::MutationContext,
    ) -> AdminStoreResult<()> {
        *self.password_hash.lock().unwrap() = Some(hash.to_owned());
        Ok(())
    }
    async fn find_user(
        &self,
        username: &str,
    ) -> AdminStoreResult<Option<gateway_admin::model::users::UserRecord>> {
        if username.eq_ignore_ascii_case("admin@example.com") {
            self.load_user("admin").await
        } else {
            Ok(None)
        }
    }
    async fn load_user(
        &self,
        id: &str,
    ) -> AdminStoreResult<Option<gateway_admin::model::users::UserRecord>> {
        Ok(
            (id == "admin").then(|| gateway_admin::model::users::UserRecord {
                display_name: String::new(),
                quota_multipliers: Default::default(),
                limits: gateway_core::policy::RateLimits::unlimited(),
                id: id.to_owned(),
                username: "admin@example.com".to_owned(),
                role: gateway_admin::model::users::UserRole::Admin,
                enabled: true,
                auth_version: 1,
                group_ids: vec![],
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }),
        )
    }
    async fn user_groups(
        &self,
        _: &str,
    ) -> AdminStoreResult<Vec<gateway_admin::model::users::UserGroup>> {
        Ok(vec![])
    }
    async fn list_users(&self) -> AdminStoreResult<Vec<gateway_admin::model::users::UserRecord>> {
        Ok(vec![])
    }
    async fn create_user(
        &self,
        _: &str,
        _: gateway_admin::model::users::UserIdentity<'_>,
        _: &str,
        _: &[String],
        _: gateway_core::policy::RateLimits,
        _: &gateway_admin::model::MutationContext,
    ) -> AdminStoreResult<gateway_admin::model::users::UserRecord> {
        panic!("unexpected user creation in this fixture")
    }
    async fn update_user(
        &self,
        _: gateway_admin::model::users::UpdateUser,
        _: &gateway_admin::model::MutationContext,
    ) -> AdminStoreResult<(
        gateway_admin::model::Revision,
        gateway_admin::model::users::UserRecord,
    )> {
        panic!("unexpected user mutation in this fixture")
    }
    async fn change_password(
        &self,
        _: &str,
        _: i64,
        _: &str,
        _: &gateway_admin::model::MutationContext,
    ) -> AdminStoreResult<()> {
        panic!("unexpected password mutation in this fixture")
    }

    async fn load_password_hash(&self, _: &str) -> AdminStoreResult<Option<String>> {
        Ok(self.password_hash.lock().expect("password hash").clone())
    }

    async fn create_password_hash_if_absent(
        &self,
        _: &str,
        password_hash: &str,
    ) -> AdminStoreResult<bool> {
        let mut stored = self.password_hash.lock().expect("password hash");
        if stored.is_some() {
            return Ok(false);
        }
        *stored = Some(password_hash.to_owned());
        Ok(true)
    }

    async fn load_admin_api_key(&self) -> AdminStoreResult<Option<AdminApiKey>> {
        Ok(None)
    }

    async fn load_session(&self, session_id: &str) -> AdminStoreResult<Option<AdminSession>> {
        Ok(self
            .sessions
            .lock()
            .expect("sessions")
            .get(session_id)
            .cloned())
    }

    async fn store_session(
        &self,
        session_id: &str,
        session: &AdminSession,
    ) -> AdminStoreResult<()> {
        self.sessions
            .lock()
            .expect("sessions")
            .insert(session_id.to_owned(), session.clone());
        Ok(())
    }

    async fn delete_session(&self, session_id: &str) -> AdminStoreResult<Option<AdminSession>> {
        Ok(self.sessions.lock().expect("sessions").remove(session_id))
    }

    async fn append_audit_event(&self, event: AdminAuditEvent) -> AdminStoreResult<()> {
        self.audits.lock().expect("audits").push(event);
        Ok(())
    }
}

#[tokio::test]
async fn administrator_password_reset_generates_distinct_valid_passwords_and_hashes_them() {
    use gateway_admin::model::{MutationActor, MutationContext};
    let store = std::sync::Arc::new(MemoryAuthStore::default());
    let services = super::AdminHarness::new().auth(store.clone()).build().await;
    let context = MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: "admin".into(),
        },
        request_id: "password-reset".into(),
    };
    let first = services
        .auth()
        .set_user_password("admin", None, &context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.len(), 32);
    assert!(
        first
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    );
    let hash = store.password_hash.lock().unwrap().clone().unwrap();
    assert!(hash.starts_with("$argon2"));
    assert!(!hash.contains(&first));
    assert!(
        services
            .auth()
            .login(LoginCommand {
                username: Some("admin@example.com".into()),
                password: first.clone()
            })
            .await
            .is_ok()
    );
    let second = services
        .auth()
        .set_user_password("admin", None, &context)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert!(
        services
            .auth()
            .login(LoginCommand {
                username: Some("admin@example.com".into()),
                password: first
            })
            .await
            .is_err()
    );
    assert!(
        services
            .auth()
            .set_user_password("admin", Some("short"), &context)
            .await
            .is_err()
    );
    assert!(
        services
            .auth()
            .set_user_password("admin", Some(&"x".repeat(1025)), &context)
            .await
            .is_err()
    );
    assert!(
        services
            .auth()
            .set_user_password("admin", Some("manually-set-password-123"), &context)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        services
            .auth()
            .login(LoginCommand {
                username: Some("admin@example.com".into()),
                password: "manually-set-password-123".into()
            })
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn successful_login_should_create_expiring_session_and_audit() {
    let store = std::sync::Arc::new(MemoryAuthStore::default());
    let services = super::AdminHarness::new().auth(store.clone()).build().await;

    let result = services
        .auth()
        .login(LoginCommand {
            username: Some(" Admin@Example.com ".to_owned()),
            password: "strong-test-password".to_owned(),
        })
        .await
        .expect("login");

    assert!(
        services
            .auth()
            .validate_session(Some(&result.session_id))
            .await
            .expect("validate")
    );
    assert_eq!(store.audits.lock().expect("audits").len(), 1);
}

#[tokio::test]
async fn login_requires_explicit_email_and_preserves_existing_email_passwords() {
    use gateway_admin::model::auth::LoginError;
    let store = std::sync::Arc::new(MemoryAuthStore::default());
    let services = super::AdminHarness::new().auth(store.clone()).build().await;
    for username in [
        None,
        Some("admin"),
        Some("bad@@example.com"),
        Some("admin@example.com\ninvalid"),
    ] {
        assert_eq!(
            services
                .auth()
                .login(LoginCommand {
                    username: username.map(str::to_owned),
                    password: "strong-test-password".into(),
                })
                .await
                .unwrap_err(),
            LoginError::InvalidCredentials
        );
    }
    assert!(store.sessions.lock().unwrap().is_empty());
    let session = services
        .auth()
        .login(LoginCommand {
            username: Some(" ADMIN@example.com ".into()),
            password: "strong-test-password".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        services
            .auth()
            .current_user(Some(&session.session_id))
            .await
            .unwrap()
            .unwrap()
            .id,
        "admin"
    );
}

#[tokio::test]
async fn user_creation_and_email_edits_reject_invalid_addresses_before_writing() {
    use gateway_admin::model::{
        AdminErrorKind, MutationActor, MutationContext,
        users::{CreateUser, UpdateUser},
    };
    let services = super::AdminHarness::new()
        .auth(std::sync::Arc::new(MemoryAuthStore::default()))
        .build()
        .await;
    let context = MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: "admin".into(),
        },
        request_id: "validate-email".into(),
    };
    for email in [
        "",
        "alice",
        "a@@example.com",
        "a b@example.com",
        "a@-example.com",
        "a@example..com",
        ".alice@example.com",
        "alice..smith@example.com",
        "alice@example.com\nother",
        &format!("{}@example.com", "x".repeat(65)),
    ] {
        assert_eq!(
            services
                .auth()
                .create_user(
                    CreateUser {
                        display_name: String::new(),
                        username: email.into(),
                        password: "strong-test-password".into(),
                        group_ids: vec![],
                        limits: gateway_core::policy::RateLimits::unlimited()
                    },
                    &context
                )
                .await
                .unwrap_err()
                .kind(),
            AdminErrorKind::Invalid
        );
        assert_eq!(
            services
                .auth()
                .update_user(
                    UpdateUser {
                        display_name: None,
                        username: Some(email.into()),
                        id: "admin".into(),
                        enabled: true,
                        group_ids: vec![],
                        quota_multipliers: None,
                        limits: gateway_core::policy::RateLimits::unlimited()
                    },
                    &context
                )
                .await
                .unwrap_err()
                .kind(),
            AdminErrorKind::Invalid
        );
    }
    for email in [
        "alice@example.com",
        "ALICE+team@Example.COM",
        "admin@cpr.local",
        "first.last@sub.example.com",
    ] {
        assert!(gateway_admin::model::users::is_login_email(email));
    }
}

#[tokio::test]
async fn repeated_default_initialization_should_not_replace_password() {
    let store = std::sync::Arc::new(MemoryAuthStore::default());
    super::AdminHarness::new()
        .auth(store.clone())
        .default_password("first-strong-password")
        .build()
        .await;
    let services = super::AdminHarness::new()
        .auth(store)
        .default_password("second-strong-password")
        .build()
        .await;

    assert!(
        services
            .auth()
            .login(LoginCommand {
                username: Some("admin@example.com".to_owned()),
                password: "first-strong-password".to_owned(),
            })
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn login_with_huge_session_ttl_should_clamp_expiry_instead_of_panicking() {
    let store = std::sync::Arc::new(MemoryAuthStore::default());
    let services = super::AdminHarness::new()
        .auth(store.clone())
        .session_ttl_minutes(i64::MAX as u64)
        .build()
        .await;

    let before = Utc::now();
    let result = services
        .auth()
        .login(LoginCommand {
            username: Some("admin@example.com".to_owned()),
            password: "strong-test-password".to_owned(),
        })
        .await
        .expect("login with huge session TTL");

    assert!(result.expires_at > before);
    assert!(result.expires_at <= before + TimeDelta::days(366) + TimeDelta::minutes(1));
    assert!(
        services
            .auth()
            .validate_session(Some(&result.session_id))
            .await
            .expect("validate")
    );
}
