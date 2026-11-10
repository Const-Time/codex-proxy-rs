-- Historical request and error facts outlive account configuration. Deletion
-- must not rewrite those facts or wait for historical rows to become writable.
-- Keep IDs, identity snapshots, usage and billing exactly as recorded. Routing
-- reads provider_accounts and its memberships, never these historical IDs.
alter table model_requests drop constraint model_requests_account_fk;
alter table ops_events drop constraint ops_events_account_fk;
