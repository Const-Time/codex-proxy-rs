-- Authenticated entry failures have no execution/attempt row. Keep their safe
-- request context separately; never invent a model_requests foreign key.
alter table ops_events add column entry_context jsonb
    check (entry_context is null or (component = 'request_entry' and jsonb_typeof(entry_context) = 'object'));

-- Recover only facts explicitly retained by the old entry recorder. No model,
-- limit subtype or IP can be reconstructed from these legacy records.
with legacy as materialized (
    select id, case when pg_input_is_valid(message, 'jsonb') then message::jsonb end as context
    from ops_events where component = 'request_entry' and operation = 'reject'
)
update ops_events e set entry_context = jsonb_build_object(
        'requestId', legacy.context->>'requestId', 'clientKeyId', legacy.context->>'clientKeyId',
        'userId', (select owner_user_id from client_api_keys where id = legacy.context->>'clientKeyId'),
        'clientStatusCode', case when e.failure_kind = 'rate_limited' then 429 end),
    message = legacy.context->>'message',
    upstream_send_state = case when legacy.context->>'message' in (
        'request exceeds user limits', 'request exceeds client API key limits', 'user group budget is exhausted'
    ) then 'not_sent' else e.upstream_send_state end
from legacy where e.id = legacy.id
    and jsonb_typeof(legacy.context->'requestId') = 'string'
    and jsonb_typeof(legacy.context->'clientKeyId') = 'string'
    and jsonb_typeof(legacy.context->'message') = 'string'
    and length(legacy.context->>'message') > 0;
create index ops_events_entry_user_created_idx
    on ops_events ((entry_context ->> 'userId'), created_at desc)
    where entry_context is not null;
