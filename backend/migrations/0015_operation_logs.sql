-- Security audit contains only allowlisted request metadata.
create table operation_logs (
    id text primary key,
    occurred_at timestamptz not null,
    payload jsonb not null
);
create index operation_logs_time_idx on operation_logs(occurred_at desc, id desc);
create index operation_logs_email_idx on operation_logs(lower(payload->>'email'), occurred_at desc);
