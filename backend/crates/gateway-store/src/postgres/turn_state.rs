use async_trait::async_trait;
use gateway_admin::{
    model::{MutationContext, turn_state::*},
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
    async fn records(&self, query: &TurnStateRecordQuery) -> AdminStoreResult<TurnStateRecordPage> {
        super::turn_state_records::list(&self.pool, query).await
    }
    async fn begin_probe(&self, probe: &TurnStateProbeStart) -> AdminStoreResult<()> {
        let result = sqlx::query(
            "insert into turn_state_probe_records
             (id, phase, model, started_at, deadline_at, account_id, account_name, cycle_id, pool_id, route_name, facts)
             select $1,$2,$3,$4,$4 + ($5::bigint * interval '1 second'),a.id,a.name,$7,$8,$9,
             '{\"decision\":\"running\"}'::jsonb from provider_accounts a where a.id=$6",
        )
        .bind(&probe.id)
        .bind(&probe.phase)
        .bind(&probe.model)
        .bind(probe.started_at)
        .bind(i64::try_from(probe.timeout_seconds).map_err(|_| unavailable())?)
        .bind(&probe.account_id)
        .bind(&probe.cycle_id)
        .bind(&probe.pool_id)
        .bind(&probe.route_name)
        .execute(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        if result.rows_affected() != 1 {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn finish_probe(&self, id: &str, result: &TurnStateProbeResult) -> AdminStoreResult<()> {
        // Verification describes the candidate sent, not any replacement header.
        let state = result
            .sent_state
            .as_ref()
            .or(result.returned_state.as_ref());
        let facts = TurnStateProbeFacts {
            completed: result.succeeded,
            status: result.status,
            decision: if result.decision.is_empty() {
                "failed".to_owned()
            } else {
                result.decision.clone()
            },
            reason: result.reason.clone(),
            message: result.message.clone(),
            latency_ms: Some(result.latency_ms),
            input_tokens: result.input_tokens,
            output_tokens: result.output_tokens,
            cached_tokens: result.cached_tokens,
            reasoning_tokens: result.reasoning_tokens,
            total_tokens: result.total_tokens,
            state_length: state.map(|s| s.chars().count()),
            shape: state.and_then(|s| FernetShape::parse(s)),
            fingerprint: state.map(|s| {
                use sha2::{Digest, Sha256};
                hex::encode(Sha256::digest(s.as_bytes()))[..16].to_owned()
            }),
            expires_at: result.expires_at,
            egress: result.egress.clone(),
        };
        sqlx::query(
            "update turn_state_probe_records set finished_at=now(), facts=$2
             where id=$1 and finished_at is null",
        )
        .bind(id)
        .bind(serde_json::to_value(facts).map_err(|_| unavailable())?)
        .execute(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        Ok(())
    }

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
