//! 管理员认证状态机。

use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration, Utc};
use rand_core::{OsRng, RngCore as _};
use subtle::ConstantTimeEq as _;
use uuid::Uuid;

use crate::{
    model::{
        AdminError,
        auth::{
            AdminAuditEvent, AdminSession, AuditActorKind, LoginCommand, LoginError, LoginResult,
        },
    },
    ports::store::AuthStore,
};

use super::map_store_error;
use crate::model::{
    AdminErrorKind, MutationContext,
    users::{CreateUser, UpdateUser, UserRecord, UserRole},
};

fn validate_display_name(value: &str) -> Result<(), AdminError> {
    if value.chars().count() > 128 || value.chars().any(char::is_control) {
        return Err(AdminError::invalid(
            "用户名最多 128 个字符，不能包含控制字符",
        ));
    }
    Ok(())
}

fn validate_user_limits(limits: gateway_core::policy::RateLimits) -> Result<(), AdminError> {
    if limits.max_concurrency > 9_007_199_254_740_991
        || limits.requests_per_minute > 9_007_199_254_740_991
    {
        return Err(AdminError::invalid("并发和 RPM 必须为安全范围内的非负整数"));
    }
    Ok(())
}

/// API 鉴权与管理员登录消费的统一服务。
#[async_trait]
pub trait AuthService: Send + Sync {
    async fn record_operation(
        &self,
        _event: crate::model::operations::OperationLog,
    ) -> Result<(), AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    async fn operation_logs(
        &self,
        _query: crate::model::operations::OperationLogQuery,
    ) -> Result<crate::model::operations::OperationLogPage, AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    async fn usage_key_options(
        &self,
        _owner: Option<&str>,
    ) -> Result<Vec<crate::model::operations::UsageKeyOption>, AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    async fn subscriptions(&self)
    -> Result<Vec<crate::model::users::UserSubscription>, AdminError>;
    async fn reset_subscriptions(
        &self,
        event_id: uuid::Uuid,
        targets: &[crate::model::users::SubscriptionTarget],
        context: &MutationContext,
    ) -> Result<u64, AdminError>;
    async fn user_groups(
        &self,
        id: &str,
    ) -> Result<Vec<crate::model::users::UserGroup>, AdminError>;
    async fn current_user(
        &self,
        session_id: Option<&str>,
    ) -> Result<Option<UserRecord>, AdminError>;
    async fn list_users(&self) -> Result<Vec<UserRecord>, AdminError>;
    async fn create_user(
        &self,
        command: CreateUser,
        context: &MutationContext,
    ) -> Result<UserRecord, AdminError>;
    async fn update_user(
        &self,
        command: UpdateUser,
        context: &MutationContext,
    ) -> Result<UserRecord, AdminError>;
    async fn delete_user(&self, _id: &str, _context: &MutationContext) -> Result<(), AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    async fn set_user_enabled(
        &self,
        _id: &str,
        _enabled: bool,
        _context: &MutationContext,
    ) -> Result<(), AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    /// Returns plaintext only when a random password was requested. Never log the result.
    async fn set_user_password(
        &self,
        _id: &str,
        _password: Option<&str>,
        _context: &MutationContext,
    ) -> Result<Option<String>, AdminError> {
        Err(AdminError::new(AdminErrorKind::Unavailable, "服务不可用"))
    }
    async fn change_password(
        &self,
        user: &UserRecord,
        current_password: &str,
        new_password: &str,
        context: &MutationContext,
    ) -> Result<(), AdminError>;
    async fn ensure_default_admin(&self, password: &str) -> Result<bool, AdminError>;
    async fn resolve_admin_user_id(
        &self,
        session_id: Option<&str>,
    ) -> Result<Option<String>, AdminError>;
    async fn verify_admin_api_key(&self, key: &str) -> Result<bool, AdminError>;
    async fn login(&self, command: LoginCommand) -> Result<LoginResult, LoginError>;
    async fn validate_session(&self, session_id: Option<&str>) -> Result<bool, AdminError>;
    async fn logout(&self, session_id: &str) -> Result<(), AdminError>;
}

/// 会话有效期上限(366 天);超出的配置按上限截断,保证 TTL 构造与
/// `Utc::now() + session_ttl` 永不越界。
const MAX_SESSION_TTL_MINUTES: i64 = 366 * 24 * 60;

/// User authentication with a bootstrap administrator.
pub(crate) struct DefaultAuthService {
    default_admin_user_id: String,
    session_ttl: Duration,
    store: Arc<dyn AuthStore>,
    snapshot: Arc<dyn gateway_core::runtime::SnapshotControl>,
}

impl DefaultAuthService {
    #[must_use]
    pub(crate) fn new(
        default_admin_user_id: impl Into<String>,
        session_ttl_minutes: u64,
        store: Arc<dyn AuthStore>,
        snapshot: Arc<dyn gateway_core::runtime::SnapshotControl>,
    ) -> Self {
        let minutes = i64::try_from(session_ttl_minutes)
            .unwrap_or(MAX_SESSION_TTL_MINUTES)
            .clamp(1, MAX_SESSION_TTL_MINUTES);
        Self {
            default_admin_user_id: default_admin_user_id.into(),
            session_ttl: Duration::minutes(minutes),
            store,
            snapshot,
        }
    }

    fn auth_audit(&self, action: &str, occurred_at: chrono::DateTime<Utc>) -> AdminAuditEvent {
        AdminAuditEvent {
            id: format!("audit_{}", Uuid::now_v7().simple()),
            actor_kind: AuditActorKind::AdminSession,
            actor_admin_user_id: Some(self.default_admin_user_id.clone()),
            actor_ref: crate::model::auth::admin_session_actor_ref(&self.default_admin_user_id),
            request_id: None,
            action: action.to_owned(),
            entity_kind: "admin_session".to_owned(),
            entity_ref: self.default_admin_user_id.clone(),
            config_revision: None,
            changed_fields: Vec::new(),
            occurred_at,
        }
    }
}

#[async_trait]
impl AuthService for DefaultAuthService {
    async fn record_operation(
        &self,
        event: crate::model::operations::OperationLog,
    ) -> Result<(), AdminError> {
        self.store
            .record_operation(event)
            .await
            .map_err(|e| map_store_error(e, "operation log"))
    }
    async fn operation_logs(
        &self,
        query: crate::model::operations::OperationLogQuery,
    ) -> Result<crate::model::operations::OperationLogPage, AdminError> {
        self.store
            .operation_logs(query)
            .await
            .map_err(|e| map_store_error(e, "operation log"))
    }
    async fn usage_key_options(
        &self,
        owner: Option<&str>,
    ) -> Result<Vec<crate::model::operations::UsageKeyOption>, AdminError> {
        self.store
            .usage_key_options(owner)
            .await
            .map_err(|e| map_store_error(e, "operation log"))
    }
    async fn subscriptions(
        &self,
    ) -> Result<Vec<crate::model::users::UserSubscription>, AdminError> {
        self.store
            .subscriptions()
            .await
            .map_err(|e| map_store_error(e, "subscriptions"))
    }
    async fn reset_subscriptions(
        &self,
        event_id: uuid::Uuid,
        targets: &[crate::model::users::SubscriptionTarget],
        context: &MutationContext,
    ) -> Result<u64, AdminError> {
        if targets.is_empty()
            || targets.len() > 500
            || targets.iter().any(|t| {
                t.user_id.is_empty()
                    || t.group_id.is_empty()
                    || t.user_id.len() > 256
                    || t.group_id.len() > 256
            })
        {
            return Err(AdminError::invalid("请选择 1 至 500 个用户分组订阅"));
        }
        self.store
            .reset_subscriptions(&format!("manual:{event_id}"), targets, context)
            .await
            .map_err(|e| map_store_error(e, "subscriptions"))
    }
    async fn user_groups(
        &self,
        id: &str,
    ) -> Result<Vec<crate::model::users::UserGroup>, AdminError> {
        self.store
            .user_groups(id)
            .await
            .map_err(|e| map_store_error(e, "user groups"))
    }
    async fn current_user(
        &self,
        session_id: Option<&str>,
    ) -> Result<Option<UserRecord>, AdminError> {
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let Some(session) = self
            .store
            .load_session(session_id)
            .await
            .map_err(|e| map_store_error(e, "user session"))?
        else {
            return Ok(None);
        };
        if session.expires_at <= Utc::now() {
            return Ok(None);
        }
        Ok(self
            .store
            .load_user(&session.admin_user_id)
            .await
            .map_err(|e| map_store_error(e, "user"))?
            .filter(|user| user.enabled && user.auth_version == session.auth_version))
    }

    async fn list_users(&self) -> Result<Vec<UserRecord>, AdminError> {
        self.store
            .list_users()
            .await
            .map_err(|e| map_store_error(e, "user"))
    }

    async fn create_user(
        &self,
        command: CreateUser,
        context: &MutationContext,
    ) -> Result<UserRecord, AdminError> {
        validate_display_name(command.display_name.trim())?;
        validate_password(&command.password)?;
        validate_user_limits(command.limits)?;
        let username = command.username.trim();
        if !crate::model::users::is_login_email(username) {
            return Err(AdminError::invalid("请输入有效的邮箱地址作为登录账号"));
        }
        validate_grants(&command.group_ids)?;
        let hash = hash_admin_password(&command.password)?;
        self.store
            .create_user(
                &format!("user_{}", Uuid::now_v7().simple()),
                crate::model::users::UserIdentity::new(username, command.display_name.trim()),
                &hash,
                &command.group_ids,
                command.limits,
                context,
            )
            .await
            .map_err(|e| map_store_error(e, "user"))
    }

    async fn update_user(
        &self,
        mut command: UpdateUser,
        context: &MutationContext,
    ) -> Result<UserRecord, AdminError> {
        if let Some(name) = &mut command.display_name {
            *name = name.trim().to_owned();
            validate_display_name(name)?;
        }
        if let Some(username) = &mut command.username {
            *username = username.trim().to_owned();
            if !crate::model::users::is_login_email(username) {
                return Err(AdminError::invalid("请输入有效的邮箱地址作为登录账号"));
            }
        }
        validate_grants(&command.group_ids)?;
        if let Some(multipliers) = &command.quota_multipliers {
            for (group, value) in multipliers {
                let valid_number = value.parse::<gateway_core::metering::Decimal>().is_ok()
                    && value
                        .parse::<f64>()
                        .is_ok_and(|n| (0.01..=1000.0).contains(&n))
                    && value
                        .split('.')
                        .nth(1)
                        .is_none_or(|fraction| fraction.len() <= 2);
                if !command.group_ids.contains(group) || !valid_number {
                    return Err(AdminError::invalid(
                        "额度倍率须对应授权分组，为 0.01 至 1000 之间、最多两位小数的数值",
                    ));
                }
            }
        }
        validate_user_limits(command.limits)?;
        let (revision, user) = self
            .store
            .update_user(command, context)
            .await
            .map_err(|e| map_store_error(e, "user"))?;
        super::publish_committed(self.snapshot.as_ref(), revision).await?;
        Ok(user)
    }

    async fn delete_user(&self, id: &str, context: &MutationContext) -> Result<(), AdminError> {
        let revision = self
            .store
            .delete_user(id, context)
            .await
            .map_err(|e| map_store_error(e, "user"))?;
        super::publish_committed(self.snapshot.as_ref(), revision).await
    }

    async fn set_user_enabled(
        &self,
        id: &str,
        enabled: bool,
        context: &MutationContext,
    ) -> Result<(), AdminError> {
        let revision = self
            .store
            .set_user_enabled(id, enabled, context)
            .await
            .map_err(|e| map_store_error(e, "user"))?;
        super::publish_committed(self.snapshot.as_ref(), revision).await
    }

    async fn set_user_password(
        &self,
        id: &str,
        password: Option<&str>,
        context: &MutationContext,
    ) -> Result<Option<String>, AdminError> {
        let generated = if password.is_none() {
            let mut bytes = [0_u8; 24];
            OsRng
                .try_fill_bytes(&mut bytes)
                .map_err(|_| AdminError::internal("密码生成失败"))?;
            Some(URL_SAFE_NO_PAD.encode(bytes))
        } else {
            None
        };
        let value = password
            .or(generated.as_deref())
            .ok_or_else(|| AdminError::internal("密码生成失败"))?;
        validate_password(value)?;
        let hash = hash_admin_password(value)?;
        self.store
            .set_user_password(id, &hash, generated.is_some(), context)
            .await
            .map_err(|e| map_store_error(e, "user password"))?;
        Ok(generated)
    }

    async fn change_password(
        &self,
        user: &UserRecord,
        current_password: &str,
        new_password: &str,
        context: &MutationContext,
    ) -> Result<(), AdminError> {
        validate_password(new_password)?;
        let hash = self
            .store
            .load_password_hash(&user.id)
            .await
            .map_err(|e| map_store_error(e, "user"))?
            .ok_or_else(|| AdminError::not_found("用户不存在"))?;
        if !verify_admin_password(current_password, &hash)? {
            return Err(AdminError::invalid("当前密码不正确"));
        }
        self.store
            .change_password(
                &user.id,
                user.auth_version,
                &hash_admin_password(new_password)?,
                context,
            )
            .await
            .map_err(|e| map_store_error(e, "user"))
    }

    async fn ensure_default_admin(&self, password: &str) -> Result<bool, AdminError> {
        let hash = hash_admin_password(password)?;
        self.store
            .create_password_hash_if_absent(&self.default_admin_user_id, &hash)
            .await
            .map_err(|error| map_store_error(error, "administrator"))
    }

    async fn resolve_admin_user_id(
        &self,
        session_id: Option<&str>,
    ) -> Result<Option<String>, AdminError> {
        let user = self.current_user(session_id).await?;
        if user.as_ref().is_some_and(|u| u.role != UserRole::Admin) {
            return Err(AdminError::new(AdminErrorKind::Forbidden, "需要管理员权限"));
        }
        Ok(user.map(|u| u.id))
    }

    async fn verify_admin_api_key(&self, key: &str) -> Result<bool, AdminError> {
        if !valid_admin_api_key_shape(key) {
            return Ok(false);
        }
        let stored = self
            .store
            .load_admin_api_key()
            .await
            .map_err(|error| map_store_error(error, "administrator API key"))?;
        Ok(stored.as_ref().is_some_and(|stored| {
            let stored = stored.expose_for_auth();
            key.len() == stored.len() && bool::from(key.as_bytes().ct_eq(stored.as_bytes()))
        }))
    }

    async fn login(&self, command: LoginCommand) -> Result<LoginResult, LoginError> {
        let username = command
            .username
            .as_deref()
            .ok_or(LoginError::InvalidCredentials)?
            .trim();
        if !crate::model::users::is_login_email(username) {
            return Err(LoginError::InvalidCredentials);
        }
        let user = self
            .store
            .find_user(username)
            .await
            .map_err(|_| LoginError::Unavailable)?
            .filter(|user| user.enabled)
            .ok_or(LoginError::InvalidCredentials)?;
        let hash = self
            .store
            .load_password_hash(&user.id)
            .await
            .map_err(|_| LoginError::Unavailable)?
            .ok_or(LoginError::InvalidCredentials)?;
        if !verify_admin_password(&command.password, &hash).map_err(|_| LoginError::Unavailable)? {
            return Err(LoginError::InvalidCredentials);
        }

        let session_id = random_session_token();
        let expires_at = Utc::now() + self.session_ttl;
        self.store
            .store_session(
                &session_id,
                &AdminSession {
                    admin_user_id: user.id.clone(),
                    auth_version: user.auth_version,
                    expires_at,
                },
            )
            .await
            .map_err(|_| LoginError::Unavailable)?;
        let mut audit = self.auth_audit("user.login", Utc::now());
        audit.actor_admin_user_id = Some(user.id.clone());
        audit.actor_ref = crate::model::auth::admin_session_actor_ref(&user.id);
        audit.entity_ref = user.id;
        if self.store.append_audit_event(audit).await.is_err() {
            let _ = self.store.delete_session(&session_id).await;
            return Err(LoginError::Unavailable);
        }
        Ok(LoginResult {
            session_id,
            expires_at,
        })
    }

    async fn validate_session(&self, session_id: Option<&str>) -> Result<bool, AdminError> {
        Ok(self.current_user(session_id).await?.is_some())
    }

    async fn logout(&self, session_id: &str) -> Result<(), AdminError> {
        let session = self
            .store
            .delete_session(session_id)
            .await
            .map_err(|error| map_store_error(error, "administrator session"))?;
        if let Some(session) = session {
            let mut event = self.auth_audit("admin.logout", Utc::now());
            event.actor_admin_user_id = Some(session.admin_user_id.clone());
            event.actor_ref = crate::model::auth::admin_session_actor_ref(&session.admin_user_id);
            event.entity_ref = session.admin_user_id;
            self.store
                .append_audit_event(event)
                .await
                .map_err(|error| map_store_error(error, "administrator audit"))?;
        }
        Ok(())
    }
}

fn hash_admin_password(password: &str) -> Result<String, AdminError> {
    Argon2::default()
        .hash_password(password.as_bytes())
        .map(|hash| hash.to_string())
        .map_err(|_| AdminError::internal("管理员密码哈希失败"))
}

fn verify_admin_password(password: &str, encoded: &str) -> Result<bool, AdminError> {
    let hash = PasswordHash::new(encoded)
        .map_err(|_| AdminError::internal("已保存的管理员密码哈希不合法"))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &hash)
        .is_ok())
}

fn random_session_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("session_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn valid_admin_api_key_shape(value: &str) -> bool {
    value.len() == 70
        && value.starts_with("admin-")
        && value[6..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_password(password: &str) -> Result<(), AdminError> {
    if !(12..=1024).contains(&password.len()) {
        return Err(AdminError::invalid("密码须为 12 至 1024 字节"));
    }
    Ok(())
}

fn validate_grants(groups: &[String]) -> Result<(), AdminError> {
    if groups.len() > 200
        || groups.iter().any(|id| id.is_empty() || id.len() > 128)
        || groups
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != groups.len()
    {
        return Err(AdminError::invalid("授权分组不合法或重复"));
    }
    Ok(())
}
