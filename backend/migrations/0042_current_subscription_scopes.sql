-- Current grants, not historical key bindings, own subscription visibility and resets.
-- Disabled users keep explicitly granted subscriptions visible for administration.
-- Preserve the existing implicit-admin fallback only until explicit grants are configured.
create view current_user_subscription_scopes as
select ug.user_id, ug.account_group_id
from user_account_groups ug
join users u on u.id = ug.user_id
where u.deleted_at is null
union
select k.owner_user_id, kg.account_group_id
from client_api_keys k
join client_api_key_groups kg on kg.client_api_key_id = k.id
join users u on u.id = k.owner_user_id
where u.deleted_at is null and u.role = 'admin' and not u.group_grants_configured;

-- Keep event idempotency and ledger reconciliation; change only eligible subscriptions.
create or replace function reset_user_subscriptions_legacy(event_id text, group_ids text[], source_account text,
    reason text, reset_time timestamptz, targets jsonb default null) returns bigint language plpgsql as $$
declare affected bigint;
begin
    insert into subscription_reset_events(id, account_id, reason, occurred_at, targets)
        values(event_id, source_account, reason, reset_time, targets) on conflict do nothing;
    if not found then
        if (select e.targets from subscription_reset_events e where e.id = event_id) is distinct from targets then
            raise check_violation using message = 'reset request selection changed';
        end if;
        return (select affected_count from subscription_reset_events where id = event_id);
    end if;
    -- Serialize with settlement before reading the ledger in the next statement.
    perform 1 from user_group_budget_windows w join account_groups g on g.id = w.account_group_id
        where exists(select 1 from current_user_subscription_scopes s
            where s.user_id = w.user_id and s.account_group_id = w.account_group_id)
        and (group_ids is null or w.account_group_id = any(group_ids))
        and (targets is null or exists(select 1 from jsonb_array_elements(targets) t
            where t->>'userId' = w.user_id and t->>'groupId' = w.account_group_id))
        order by w.user_id, w.account_group_id for update of w;
    insert into user_group_budget_windows(user_id, account_group_id, daily_start, daily_end,
        weekly_start, weekly_end, daily_used_usd, weekly_used_usd, last_reset_at, last_reset_reason)
    select s.user_id, s.account_group_id, greatest(reset_time, d.day), d.day + interval '1 day',
        reset_time, reset_time + interval '7 days',
        least(9999999999.9999999999, coalesce((select sum(amount_usd) from user_group_charge_events c
            where c.user_id = s.user_id and c.account_group_id = s.account_group_id
            and c.completed_at >= greatest(reset_time, d.day) and c.completed_at < d.day + interval '1 day'), 0)),
        least(9999999999.9999999999, coalesce((select sum(amount_usd) from user_group_charge_events c
            where c.user_id = s.user_id and c.account_group_id = s.account_group_id
            and c.completed_at >= reset_time and c.completed_at < reset_time + interval '7 days'), 0)),
        reset_time, reason
    from current_user_subscription_scopes s
    join account_groups g on g.id = s.account_group_id
    cross join (select date_trunc('day', now() at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai' as day) d
    where (group_ids is null or s.account_group_id = any(group_ids))
    and (targets is null or exists(select 1 from jsonb_array_elements(targets) t
        where t->>'userId' = s.user_id and t->>'groupId' = s.account_group_id))
    order by s.user_id, s.account_group_id
    on conflict(user_id, account_group_id) do update set daily_start = excluded.daily_start,
        daily_end = excluded.daily_end, weekly_start = excluded.weekly_start,
        weekly_end = excluded.weekly_end, daily_used_usd = excluded.daily_used_usd, weekly_used_usd = excluded.weekly_used_usd,
        last_reset_at = excluded.last_reset_at, last_reset_reason = excluded.last_reset_reason
    where user_group_budget_windows.last_reset_at is null
        or user_group_budget_windows.last_reset_at < excluded.last_reset_at;
    get diagnostics affected = row_count;
    update subscription_reset_events set affected_count = affected where id = event_id;
    return affected;
end
$$;
