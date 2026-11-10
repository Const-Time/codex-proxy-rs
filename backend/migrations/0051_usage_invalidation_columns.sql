-- Live foreign-key detachments do not change historical statistics, which use
-- immutable *_ref/snapshot fields. Avoid touching the shared dirty-hour rows
-- when deleting an account or client key.
drop trigger model_requests_usage_hour_dirty on model_requests;
create trigger model_requests_usage_hour_dirty
after insert or delete or update of
    started_at, user_id, client_api_key_ref, routing_group_refs,
    provider_account_ref, operation, provider_kind, requested_model_id,
    upstream_model_id, outcome, client_status_code, upstream_status_code,
    client_transport, upstream_transport, recovered_at, downstream_committed_at,
    request_kind, image_generation_requested, input_tokens, output_tokens,
    cached_tokens, cache_write_tokens, reasoning_tokens, image_input_tokens,
    image_output_tokens, total_tokens, latency_ms, cost_currency, cost_amount,
    billed_cost_amount
on model_requests
for each row execute function invalidate_request_usage_hour();
