-- A group, not an individual subscriber, owns the weekly boundary.
-- No historical counters or ledger entries are rewritten during migration.
-- No FK: admitted requests must still settle after their group is deleted.
create index user_group_charge_cycle_lookup
    on user_group_charge_events(user_id, account_group_id, completed_at) include (amount_usd);

create table subscription_group_cycles (
    account_group_id text primary key,
    mode text not null check (mode in ('upstream', 'independent')),
    source_account_id text,
    weekly_start timestamptz not null,
    weekly_end timestamptz not null,
    check (weekly_end > weekly_start)
);

create function ensure_subscription_group_cycle(gid text, at_time timestamptz)
returns subscription_group_cycles language plpgsql as $$
declare
    c subscription_group_cycles;
    enabled boolean;
    members bigint;
    source_id text;
    desired_mode text;
    upstream_end timestamptz;
    initial_end timestamptz;
    boundary timestamptz;
    affected bigint;
    changed_targets jsonb;
begin
    select subscription_auto_reset_enabled into enabled from runtime_settings where id = 1 for share;
    -- Same lock order for admissions, settlements, observations and manual resets.
    perform 1 from account_groups where id = gid for update;
    perform pg_advisory_xact_lock(hashtextextended('subscription-group:' || gid, 0));
    -- Count configured members, including disabled ones. Disabling an account is
    -- not an instruction to turn a multi-account group into a linked subscription.
    select count(*), min(provider_account_id) into members, source_id
        from account_group_accounts where account_group_id = gid;
    desired_mode := case when enabled and members = 1 then 'upstream' else 'independent' end;
    if desired_mode = 'upstream' then
        select reset_at into upstream_end from subscription_quota_observations
            where account_id = source_id and reset_at is not null
            order by observed_at desc, window_key limit 1;
    end if;
    select * into c from subscription_group_cycles where account_group_id = gid for update;
    if not found then
        -- Conservatively adopt the latest existing end; never clear counters just
        -- because an upgraded installation first establishes a shared boundary.
        select coalesce(max(weekly_end),
            (date_trunc('day', at_time at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai')
                + interval '168 hours') into initial_end
            from user_group_budget_windows where account_group_id = gid;
        initial_end := coalesce(upstream_end, initial_end);
        insert into subscription_group_cycles values
            (gid, desired_mode, case when desired_mode = 'upstream' then source_id end,
             initial_end - interval '168 hours', initial_end) returning * into c;
    elsif c.mode <> desired_mode
        or c.source_account_id is distinct from
            (case when desired_mode = 'upstream' then source_id end) then
        -- Membership/settings transitions preserve usage; no retroactive resets.
        initial_end := coalesce(upstream_end, greatest(c.weekly_end, at_time + interval '168 hours'));
        update subscription_group_cycles set mode = desired_mode,
            source_account_id = case when desired_mode = 'upstream' then source_id end,
            weekly_start = initial_end - interval '168 hours', weekly_end = initial_end
            where account_group_id = gid returning * into c;
    elsif c.mode = 'upstream' and upstream_end is not null and upstream_end <> c.weekly_end then
        -- Observation only establishes a boundary. Only the separately recorded
        -- upstream reset event may clear usage (not expiry or a first baseline).
        update subscription_group_cycles set weekly_start = upstream_end - interval '168 hours',
            weekly_end = upstream_end where account_group_id = gid returning * into c;
    end if;
    if c.mode = 'independent' and c.weekly_end <= at_time then
        boundary := c.weekly_end + floor(extract(epoch from (at_time - c.weekly_end)) / 604800)
            * interval '168 hours';
        update subscription_group_cycles set weekly_start = boundary,
            weekly_end = boundary + interval '168 hours'
            where account_group_id = gid returning * into c;
        with changed as (
            update user_group_budget_windows w set weekly_start = c.weekly_start,
                weekly_end = c.weekly_end,
                weekly_used_usd = least(9999999999.9999999999, coalesce((
                    select sum(e.amount_usd) from user_group_charge_events e
                    where e.user_id = w.user_id and e.account_group_id = gid
                    and e.completed_at >= c.weekly_start and e.completed_at < c.weekly_end), 0)),
                last_reset_at = boundary, last_reset_reason = 'group_period'
                where w.account_group_id = gid returning w.user_id
        ) select count(*), jsonb_agg(jsonb_build_object('userId', user_id, 'groupId', gid))
            into affected, changed_targets from changed;
        if affected > 0 then
            insert into subscription_reset_events(id, reason, occurred_at, affected_count, targets)
                values ('group-period:' || gid || ':' || extract(epoch from boundary)::text,
                    'group_period', boundary, affected, changed_targets) on conflict do nothing;
        end if;
    end if;
    -- Retain counters and individual manual-reset start markers on adoption.
    update user_group_budget_windows set weekly_end = c.weekly_end,
        weekly_start = least(weekly_start, c.weekly_end - interval '1 microsecond')
        where account_group_id = gid and weekly_end is distinct from c.weekly_end;
    return c;
end
$$;

create function advance_subscription_windows(uid text, gid text, at_time timestamptz)
returns void language plpgsql as $$
declare
    c subscription_group_cycles;
    day_start timestamptz;
    affected bigint;
begin
    c := ensure_subscription_group_cycle(gid, at_time);
    day_start := date_trunc('day', at_time at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai';
    insert into user_group_budget_windows(user_id, account_group_id, daily_start, daily_end,
        weekly_start, weekly_end)
        values(uid, gid, day_start, day_start + interval '24 hours', c.weekly_start, c.weekly_end)
        on conflict do nothing;
    update user_group_budget_windows w set daily_start = day_start,
        daily_end = day_start + interval '24 hours',
        daily_used_usd = least(9999999999.9999999999, coalesce((
            select sum(e.amount_usd) from user_group_charge_events e
            where e.user_id = uid and e.account_group_id = gid
                and e.completed_at >= day_start and e.completed_at < day_start + interval '24 hours'), 0)),
        last_reset_at = greatest(coalesce(w.last_reset_at, day_start), day_start),
        last_reset_reason = case when w.last_reset_at >= day_start then w.last_reset_reason else 'daily_period' end
        where w.user_id = uid and w.account_group_id = gid and w.daily_end <= at_time;
    get diagnostics affected = row_count;
    if affected > 0 then
        insert into subscription_reset_events(id, reason, occurred_at, affected_count, targets)
            values ('daily-period:' || uid || ':' || gid || ':' || extract(epoch from day_start)::text,
                'daily_period', day_start, affected,
                jsonb_build_array(jsonb_build_object('userId', uid, 'groupId', gid)))
            on conflict do nothing;
    end if;
end
$$;

-- Keep the proven ledger reconciliation/idempotency implementation, but serialize
-- it with group cycles and constrain upstream events to single-account groups.
alter function reset_user_subscriptions(text, text[], text, text, timestamptz, jsonb)
    rename to reset_user_subscriptions_legacy;
create function reset_user_subscriptions(event_id text, group_ids text[], source_account text,
    reason text, reset_time timestamptz, targets jsonb default null)
returns bigint language plpgsql as $$
declare
    selected_groups text[] := array[]::text[];
    gid text;
    c subscription_group_cycles;
    affected bigint;
    actual_end timestamptz;
begin
    if exists(select 1 from subscription_reset_events where id = event_id) then
        return reset_user_subscriptions_legacy(event_id, group_ids, source_account, reason, reset_time, targets);
    end if;
    perform 1 from runtime_settings where id = 1 for share;
    -- Lock all selected parents in sorted order before computing membership.
    for gid in select g.id from account_groups g
        where (group_ids is null or g.id = any(group_ids))
        and (targets is null or exists(select 1 from jsonb_array_elements(targets) t where t->>'groupId' = g.id))
        order by g.id for update
    loop
        c := ensure_subscription_group_cycle(gid, reset_time);
        if source_account is null or (c.mode = 'upstream' and c.source_account_id = source_account) then
            selected_groups := array_append(selected_groups, gid);
        end if;
    end loop;
    affected := reset_user_subscriptions_legacy(event_id, selected_groups, source_account, reason, reset_time, targets);
    foreach gid in array selected_groups loop
        select * into c from subscription_group_cycles where account_group_id = gid;
        if source_account is not null and reset_time >= c.weekly_start then
            actual_end := reset_time + interval '168 hours';
            if reason = 'upstream_window' then
                select coalesce(max(reset_at) filter(where reset_at > reset_time), actual_end)
                    into actual_end from subscription_quota_observations where account_id = source_account;
            end if;
            update subscription_group_cycles set weekly_start = reset_time, weekly_end = actual_end
                where account_group_id = gid returning * into c;
        end if;
        -- A selected-user manual reset clears that user's usage but never moves
        -- the shared group end or resets an unselected subscriber.
        update user_group_budget_windows set weekly_end = greatest(c.weekly_end, reset_time + interval '1 microsecond')
            where account_group_id = gid and last_reset_at = reset_time and last_reset_reason = reason;
    end loop;
    return affected;
end
$$;
