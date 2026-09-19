use async_trait::async_trait;
use gateway_admin::{
    model::{
        MutationContext,
        turn_state::{TurnStateProbeResult, TurnStateProbeStart},
    },
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
    async fn begin_probe(&self, probe: &TurnStateProbeStart) -> AdminStoreResult<()> {
        let result = sqlx::query(
            "insert into model_requests (
                id, client_api_key_ref, config_revision, protocol, operation, endpoint,
                client_transport, requested_model_id, upstream_model_id, provider_kind,
                provider_account_id, provider_account_ref, provider_account_name_snapshot,
                provider_account_email_snapshot, provider_account_authentication_kind_snapshot,
                upstream_transport, attempt_count, upstream_send_state, request_kind,
                started_at, deadline_at, routing_scope
             ) select $1, 'system:turn-state', r.config_revision, 'openai', 'maintenance_probe', $2,
                'maintenance', $3, $3, a.provider_kind, a.id, a.id, a.name, a.email,
                a.authentication_kind, 'http_sse', 1, 'ambiguous', 'state_probe',
                $4, $4 + ($5::bigint * interval '1 second'), 'legacy_provider'
             from provider_accounts a cross join runtime_settings r where a.id=$6 and r.id=1",
        )
        .bind(&probe.id)
        .bind(format!("/internal/turn-state/{}", probe.phase))
        .bind(&probe.model)
        .bind(probe.started_at)
        .bind(i64::try_from(probe.timeout_seconds).map_err(|_| unavailable())?)
        .bind(&probe.account_id)
        .execute(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        if result.rows_affected() != 1 {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn finish_probe(&self, id: &str, result: &TurnStateProbeResult) -> AdminStoreResult<()> {
        let number = |v: Option<u64>| v.and_then(|v| i64::try_from(v).ok());
        let observation = serde_json::json!({
            "turnState": result.returned_state.as_ref().or(result.sent_state.as_ref()),
            "turnStateSource": if result.returned_state.is_some() { "response" } else { "request" },
            "turnStateSent": result.sent_state,
            "turnStateSentSource": result.sent_state.as_ref().map(|_| "maintenance"),
            "turnStateReturned": result.returned_state,
            "maintenanceProbe": true
        });
        sqlx::query(
            "update model_requests set outcome=$2, completed_at=now(),
                client_status_code=$3, upstream_status_code=$4, upstream_request_id=$5,
                input_tokens=$6, output_tokens=$7, cached_tokens=$8, reasoning_tokens=$9,
                total_tokens=$10, latency_ms=$11, error_message=$12,
                provider_observation_json=$13, downstream_committed_at=case when $14 then now() else null end
             where id=$1 and request_kind='state_probe' and outcome='running'"
        ).bind(id).bind(if result.succeeded { "succeeded" } else { "failed" })
        .bind(if result.succeeded { 200i32 } else { 502i32 })
        .bind(result.status.map(i32::from)).bind(&result.request_id)
        .bind(number(result.input_tokens)).bind(number(result.output_tokens))
        .bind(number(result.cached_tokens)).bind(number(result.reasoning_tokens))
        .bind(number(result.total_tokens)).bind(number(Some(result.latency_ms)))
        .bind(&result.message).bind(observation).bind(result.succeeded)
        .execute(&self.pool).await.map_err(|_| unavailable())?;
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
