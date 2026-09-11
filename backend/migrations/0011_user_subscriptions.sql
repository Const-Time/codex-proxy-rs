-- User quota factors are independent of model billing factors.
alter table user_account_groups add column quota_multiplier numeric(6,2) not null default 1
    check (quota_multiplier between 0.01 and 1000);
alter table user_group_budget_windows
    add column last_reset_at timestamptz,
    add column last_reset_reason text;

create function user_group_quota_limit(base numeric, uid text, gid text) returns numeric
language sql stable as $$
    select case when base = 0 then 0 else greatest(0.0000000001,
        round(base * coalesce((select quota_multiplier from user_account_groups
            where user_id = uid and account_group_id = gid), 1), 10)) end
$$;

create function validate_user_quota_limit() returns trigger language plpgsql as $$
begin
    if exists(select 1 from user_account_groups ug join account_groups g on g.id = ug.account_group_id
        where greatest(g.daily_limit_usd, g.weekly_limit_usd) * ug.quota_multiplier > 9999999999.9999999999) then
        raise check_violation using message = 'effective user quota exceeds supported amount';
    end if;
    return new;
end
$$;
create trigger user_quota_limit_check after insert or update on user_account_groups
    for each statement execute function validate_user_quota_limit();
create trigger group_quota_limit_check after update of daily_limit_usd, weekly_limit_usd on account_groups
    for each statement execute function validate_user_quota_limit();

create table subscription_reset_events (
    id text primary key,
    account_id text,
    reason text not null,
    affected_count bigint not null default 0,
    occurred_at timestamptz not null default now()
);
create index subscription_reset_account_time on subscription_reset_events(account_id, occurred_at desc);

-- Historical windows for deleted groups are deliberately excluded.
create function reset_user_subscriptions(event_id text, group_ids text[], source_account text,
    reason text, reset_time timestamptz) returns bigint language plpgsql as $$
declare affected bigint;
begin
    insert into subscription_reset_events(id, account_id, reason, occurred_at)
        values(event_id, source_account, reason, reset_time) on conflict do nothing;
    if not found then
        return (select affected_count from subscription_reset_events where id = event_id);
    end if;
    -- Serialize with settlement before reading the ledger in the next statement.
    perform 1 from user_group_budget_windows w join account_groups g on g.id = w.account_group_id
        where group_ids is null or w.account_group_id = any(group_ids)
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
    from (select user_id, account_group_id from user_account_groups
        union select k.owner_user_id, kg.account_group_id from client_api_keys k
        join client_api_key_groups kg on kg.client_api_key_id = k.id) s
    join account_groups g on g.id = s.account_group_id
    cross join (select date_trunc('day', now() at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai' as day) d
    where group_ids is null or s.account_group_id = any(group_ids)
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

-- Latest neutral upstream window facts; baseline survives service restarts.
create table subscription_quota_observations (
    account_id text not null references provider_accounts(id) on delete cascade,
    window_key text not null,
    observed_at timestamptz not null,
    reset_at timestamptz,
    used_percent double precision,
    primary key(account_id, window_key)
);
