-- Live references support FK maintenance; historical queries use *_ref indexes.
drop index model_requests_account_idx;
create index model_requests_account_idx on model_requests (provider_account_id)
  where provider_account_id is not null;

drop index model_requests_client_idx;
create index model_requests_client_idx on model_requests (client_api_key_id)
  where client_api_key_id is not null;

drop index ops_events_account_idx;
create index ops_events_account_idx on ops_events (provider_account_id)
  where provider_account_id is not null;

drop index admin_audit_events_actor_idx;
create index admin_audit_events_actor_idx on admin_audit_events (actor_admin_user_id)
  where actor_admin_user_id is not null;

-- Key usage is updated continuously; a small management list can sort on demand.
drop index client_api_keys_last_used_idx;

-- These leading keys no longer match the current runtime queries.
drop index provider_accounts_quota_access_idx;
drop index provider_accounts_refresh_due_idx;
drop index account_groups_list_idx;
drop index ops_events_component_idx;
drop index ops_events_failure_idx;
drop index admin_audit_events_actor_ref_idx;
drop index admin_audit_events_entity_idx;
