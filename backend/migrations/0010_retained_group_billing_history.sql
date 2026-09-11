-- Billing facts retain the group identifier after its configuration is deleted.
-- Live keys still reference account_groups with ON DELETE RESTRICT. Historical
-- windows and charges must survive deletion, including late in-flight settlement.
alter table user_group_budget_windows
    drop constraint user_group_budget_windows_account_group_id_fkey;
alter table user_group_charge_events
    drop constraint user_group_charge_events_account_group_id_fkey;
