-- 同步策略与已校验来源摘要由 PostgreSQL 持久化；不修改历史账单。
alter table runtime_settings add column pricing_sync_policy_json jsonb not null
    default '{"enabled":true,"updateExisting":false}'
    check (jsonb_typeof(pricing_sync_policy_json) = 'object');
alter table runtime_settings add column pricing_source_hash text
    check (pricing_source_hash ~ '^[0-9a-f]{64}$');
