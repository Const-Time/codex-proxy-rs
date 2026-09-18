use async_trait::async_trait;
use gateway_admin::{
    model::MutationContext,
    ports::{
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
        turn_state::TurnStateStore,
    },
};
use sqlx::{PgPool, Row as _};

pub struct PgTurnStateStore {
    pool: PgPool,
}

impl PgTurnStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn unavailable() -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Unavailable, "turn state", "存储不可用")
}

#[async_trait]
impl TurnStateStore for PgTurnStateStore {
    async fn load(&self) -> AdminStoreResult<(u64, Option<Vec<u8>>)> {
        let row = sqlx::query("select revision, sealed from turn_state_management where id=1")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable())?;
        let revision: i64 = row.try_get("revision").map_err(|_| unavailable())?;
        Ok((
            revision as u64,
            row.try_get("sealed").map_err(|_| unavailable())?,
        ))
    }

    async fn save(
        &self,
        revision: u64,
        sealed: Vec<u8>,
        context: Option<&MutationContext>,
    ) -> AdminStoreResult<u64> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        let next: Option<i64> = sqlx::query_scalar(
            "update turn_state_management set sealed=$1, revision=revision+1, updated_at=now()
             where id=1 and revision=$2 returning revision",
        )
        .bind(sealed)
        .bind(i64::try_from(revision).map_err(|_| unavailable())?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
        let next = next.ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::StaleRevision,
                "turn state",
                "请重新加载设置",
            )
        })?;
        if let Some(context) = context {
            let audit = crate::mutation_audit(
                context,
                "turn_state.update",
                "turn_state",
                "settings",
                vec!["turn_state".to_owned()],
            );
            let config_revision: i64 =
                sqlx::query_scalar("select config_revision from runtime_settings where id=1")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| unavailable())?;
            let config_revision =
                crate::Revision::new(config_revision as u64).map_err(|_| unavailable())?;
            super::append_admin_audit_event_in_transaction(&mut tx, audit, config_revision)
                .await
                .map_err(|_| unavailable())?;
        }
        tx.commit().await.map_err(|_| unavailable())?;
        Ok(next as u64)
    }
}
