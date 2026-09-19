-- Maintenance traffic has its own lifecycle and never enters business usage/billing.
create table turn_state_probe_records (
    id text primary key,
    cycle_id text not null,
    account_id text not null,
    account_name text not null,
    model text not null,
    phase text not null check (phase in ('collect', 'verify')),
    pool_id text,
    route_name text not null,
    started_at timestamptz not null,
    deadline_at timestamptz not null,
    finished_at timestamptz,
    facts jsonb not null default '{}'::jsonb
);
create index turn_state_probe_records_time on turn_state_probe_records (started_at desc, id desc);
create index turn_state_probe_records_account_time on turn_state_probe_records (account_id, started_at desc);
create index turn_state_probe_records_cycle on turn_state_probe_records (cycle_id);

-- Preserve known historical facts, not guessed candidate decisions or egress.
-- Do not copy raw State or arbitrary upstream messages to the diagnostic store.
insert into turn_state_probe_records
    (id, cycle_id, account_id, account_name, model, phase, route_name,
     started_at, deadline_at, finished_at, facts)
select id, id, coalesce(provider_account_ref, ''), coalesce(provider_account_name_snapshot, provider_account_ref, '已删除账号'),
    requested_model_id,
    case when endpoint like '%/verify' then 'verify' else 'collect' end,
    '历史记录 · 出口未记录', started_at, deadline_at, completed_at,
    jsonb_build_object(
        'decision', 'legacy', 'completed', outcome = 'succeeded',
        'status', upstream_status_code, 'latencyMs', latency_ms,
        'inputTokens', input_tokens, 'outputTokens', output_tokens,
        'cachedTokens', cached_tokens, 'reasoningTokens', reasoning_tokens, 'totalTokens', total_tokens,
        'stateLength', length(case when endpoint like '%/verify'
            then provider_observation_json->>'turnStateSent'
            else provider_observation_json->>'turnStateReturned' end),
        'message', '历史探测迁移：候选处理结果及出口信息未记录')
from model_requests where request_kind = 'state_probe'
    and client_api_key_ref = 'system:turn-state' and operation = 'maintenance_probe';

delete from model_requests where request_kind = 'state_probe'
    and client_api_key_ref = 'system:turn-state' and operation = 'maintenance_probe';
