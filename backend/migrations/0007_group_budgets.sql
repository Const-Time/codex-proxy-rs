alter table account_groups
    add column daily_limit_usd numeric(20,10) not null default 0 check (daily_limit_usd >= 0),
    add column weekly_limit_usd numeric(20,10) not null default 0 check (weekly_limit_usd >= 0);

-- Give each legacy key an explicit migration group. This preserves different old
-- limits/windows and the union of its currently authorized accounts without
-- silently granting additional accounts. Administrators can consolidate later.
insert into account_groups(id, name, description, color, enabled, created_at, updated_at, daily_limit_usd, weekly_limit_usd)
select 'grp_' || md5('user-budget-migration:' || k.id),
    '迁移-' || md5(k.id), '旧密钥迁移：' || k.name, '#64748BFF', true, now(), now(),
    k.daily_limit_usd, k.weekly_limit_usd from client_api_keys k;
insert into account_group_accounts(account_group_id, provider_account_id, created_at)
select distinct 'grp_' || md5('user-budget-migration:' || k.id), a.id, now()
from client_api_keys k cross join provider_accounts a
where not exists(select 1 from client_api_key_groups kg where kg.client_api_key_id = k.id)
or exists(select 1 from client_api_key_groups kg
    join account_groups g on g.id = kg.account_group_id and g.enabled
    join account_group_accounts ga on ga.account_group_id = g.id
    where kg.client_api_key_id = k.id and ga.provider_account_id = a.id);
delete from client_api_key_groups;
insert into client_api_key_groups(client_api_key_id, account_group_id, created_at)
    select id, 'grp_' || md5('user-budget-migration:' || id), now() from client_api_keys;
create unique index client_api_key_single_group_idx on client_api_key_groups(client_api_key_id);
insert into user_account_groups(user_id, account_group_id)
    select owner_user_id, 'grp_' || md5('user-budget-migration:' || id) from client_api_keys
    on conflict do nothing;

create table user_group_budget_windows (
    user_id text not null references users(id) on delete restrict,
    account_group_id text not null references account_groups(id) on delete restrict,
    daily_start timestamptz not null,
    daily_end timestamptz not null,
    weekly_start timestamptz not null,
    weekly_end timestamptz not null,
    daily_used_usd numeric(20,10) not null default 0 check (daily_used_usd >= 0),
    weekly_used_usd numeric(20,10) not null default 0 check (weekly_used_usd >= 0),
    primary key(user_id, account_group_id)
);
insert into user_group_budget_windows
    select k.owner_user_id, kg.account_group_id, w.daily_start, w.daily_end,
        w.weekly_start, w.weekly_end, w.daily_used_usd, w.weekly_used_usd
    from client_key_budget_windows w join client_api_keys k on k.id = w.client_api_key_id
    join client_api_key_groups kg on kg.client_api_key_id = k.id;

-- Settlement records outlive key deletion and freeze ownership at admission.
create table user_group_charge_events (
    request_id text primary key,
    user_id text not null references users(id) on delete restrict,
    account_group_id text not null references account_groups(id) on delete restrict,
    client_api_key_ref text not null,
    amount_usd numeric(20,10) not null check (amount_usd >= 0),
    completed_at timestamptz not null
);
insert into user_group_charge_events
    select e.request_id, k.owner_user_id, kg.account_group_id, k.id, e.amount_usd, e.completed_at
    from client_key_charge_events e join client_api_keys k on k.id = e.client_api_key_id
    join client_api_key_groups kg on kg.client_api_key_id = k.id;
drop table client_key_charge_events;
drop table client_key_budget_windows;
alter table client_api_keys drop column daily_limit_usd, drop column weekly_limit_usd;
