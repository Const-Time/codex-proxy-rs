-- v3.5.0 emergency cleanup, only for unused groups with ZERO consumption.
-- Back up PostgreSQL, stop the application (keep PostgreSQL running), then run:
-- psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v group_id=grp_<actual_id> -f cleanup-unused-group-window.sql
-- This does not delete groups, requests, keys, or charge history. Restart the app
-- and delete the group from the UI afterwards. Nonzero/charged groups need the fix.
\set ON_ERROR_STOP on
begin;
select id, name from account_groups where id = :'group_id' for update;
select
    (select count(*) from client_api_key_groups where account_group_id = :'group_id') as key_bindings,
    (select count(*) from user_group_charge_events where account_group_id = :'group_id') as historical_charges,
    (select count(*) from user_group_budget_windows where account_group_id = :'group_id') as budget_windows;
delete from user_group_budget_windows w
where w.account_group_id = :'group_id'
  and w.daily_used_usd = 0 and w.weekly_used_usd = 0
  and not exists (select 1 from client_api_key_groups k where k.account_group_id = w.account_group_id)
  and not exists (select 1 from user_group_charge_events e where e.account_group_id = w.account_group_id)
  and not exists (select 1 from model_requests r where w.account_group_id = any(r.routing_group_refs) and r.completed_at is null)
returning w.user_id, w.account_group_id;
commit;
