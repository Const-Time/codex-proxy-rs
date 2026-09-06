-- Existing value checks also need explicit presence checks: CHECK accepts NULL.
alter table model_requests
  add constraint model_requests_fact_completeness_ck check (
    (provider_account_id is null or provider_account_ref is not null)
    and (cost_source = 'unavailable' or cost_currency is not null)
    and num_nonnulls(capacity_used_slots, capacity_total_slots) in (0, 2)
    and num_nonnulls(
      upstream_connection_id, upstream_connection_exit_reason,
      upstream_connection_age_ms, upstream_connection_idle_ms
    ) in (0, 4)
    and (
      recovered_at is null
      or (
        completed_at is not null
        and recovery_retry_delay_ms is not null
        and recovery_total_latency_ms is not null
      )
    )
  );

alter table ops_events
  add constraint ops_events_account_snapshot_present_ck check (
    provider_account_id is null or provider_account_ref is not null
  );

alter table backup_records
  add constraint backup_records_completion_order_ck check (
    completed_at is null or completed_at >= started_at
  );

update admin_audit_events
set actor_ref = 'admin:' || actor_admin_user_id
where actor_admin_user_id is not null
  and actor_kind = 'admin_session'
  and actor_ref is distinct from 'admin:' || actor_admin_user_id;

alter table admin_audit_events
  add constraint admin_audit_events_actor_identity_ck check (
    actor_admin_user_id is null
    or (actor_kind = 'admin_session' and actor_ref = 'admin:' || actor_admin_user_id)
  );
