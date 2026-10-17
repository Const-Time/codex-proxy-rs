-- Preserve the group directory's original successful-request fee semantics,
-- independently of the usage-card ledger/recovered-request filters.
alter table usage_hourly_stats add column group_billing_usd numeric not null default 0;
-- Existing hours lack this projection. Atomically invalidate them; readers use
-- original facts until the same bounded worker has rebuilt each hour.
insert into usage_hourly_dirty(bucket_start)
select bucket_start from usage_hourly_covered on conflict do nothing;
