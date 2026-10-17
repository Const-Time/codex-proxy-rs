create or replace function advance_subscription_windows(uid text, gid text, at_time timestamptz)
returns void language plpgsql as $$
declare
    c subscription_group_cycles;
    day_start timestamptz;
    affected bigint;
    has_daily_limit boolean;
begin
    c := ensure_subscription_group_cycle(gid, at_time);
    select coalesce(daily_limit_usd > 0, false) into has_daily_limit from account_groups where id = gid;
    has_daily_limit := coalesce(has_daily_limit, false);
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
        last_reset_at = case when has_daily_limit then greatest(coalesce(w.last_reset_at, day_start), day_start) else w.last_reset_at end,
        last_reset_reason = case when not has_daily_limit or w.last_reset_at >= day_start then w.last_reset_reason else 'daily_period' end
        where w.user_id = uid and w.account_group_id = gid and w.daily_end <= at_time;
    get diagnostics affected = row_count;
    if affected > 0 and has_daily_limit then
        insert into subscription_reset_events(id, reason, occurred_at, affected_count, targets)
            values ('daily-period:' || uid || ':' || gid || ':' || extract(epoch from day_start)::text,
                'daily_period', day_start, affected,
                jsonb_build_array(jsonb_build_object('userId', uid, 'groupId', gid)))
            on conflict do nothing;
    end if;
end
$$;

-- Daily accounting still rolls over, even without a limit, so enabling a limit
-- later uses the existing day consumption. Only reset metadata is repaired;
-- original audit events, counters, boundaries and the ledger are retained.
with repaired as (
    select w.user_id, w.account_group_id, previous.occurred_at, previous.reason
    from user_group_budget_windows w
    join account_groups g on g.id = w.account_group_id
    left join lateral (
        select e.occurred_at, e.reason
        from subscription_reset_events e
        where e.reason <> 'daily_period' and e.affected_count > 0
          and e.occurred_at <= w.last_reset_at
          and e.targets @> jsonb_build_array(jsonb_build_object('userId', w.user_id, 'groupId', w.account_group_id))
        order by e.occurred_at desc, e.id desc limit 1
    ) previous on true
    where g.daily_limit_usd = 0 and w.last_reset_reason = 'daily_period'
)
update user_group_budget_windows w
set last_reset_at = r.occurred_at, last_reset_reason = r.reason
from repaired r where w.user_id = r.user_id and w.account_group_id = r.account_group_id;
