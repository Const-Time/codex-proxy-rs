-- Proxy-owned policy and observations; accounts inherit by proxy binding.
alter table outbound_proxies
    add column location_policy jsonb not null default '{"mode":"auto"}'::jsonb,
    add column last_test_location jsonb;

alter table outbound_proxies add constraint outbound_proxy_location_mode
    check (location_policy->>'mode' in ('auto', 'manual', 'passthrough'));

create function outbound_proxy_request_location(proxy_id text) returns jsonb
language sql stable as $$
    select case
        when location_policy->>'mode' = 'manual' then location_policy->'location'
        when location_policy->>'mode' = 'auto' and last_test_success is true then last_test_location
        else null
    end
    from outbound_proxies where id = proxy_id
$$;
