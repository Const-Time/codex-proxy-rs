-- Capture policy provenance with the same account/proxy read snapshot.
create function outbound_proxy_request_context(proxy_id text) returns jsonb
language sql stable as $$
    select jsonb_build_object(
        'proxyId', id,
        'mode', location_policy->>'mode',
        'detectedIp', case when last_test_success is true then last_test_ip else null end,
        'detectedAt', case when last_test_success is true then last_test_at::text else null end,
        'location', outbound_proxy_request_location(id)
    )
    from outbound_proxies where id = proxy_id
$$;
