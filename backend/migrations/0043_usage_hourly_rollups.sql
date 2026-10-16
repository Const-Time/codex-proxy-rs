-- Rebuildable statistics only. Request records and the consumption ledger remain authoritative.
create table usage_hourly_covered (
  bucket_start timestamptz primary key
);
create table usage_hourly_dirty (
  bucket_start timestamptz primary key
);
create table usage_hourly_cursor (
  singleton boolean primary key default true check (singleton),
  next_hour timestamptz not null
);
insert into usage_hourly_cursor (next_hour)
select date_bin(interval '1 hour', coalesce(min(started_at), now()), timestamptz '1970-01-01 00:00:00+00')
from model_requests;

create table usage_hourly_stats (
  bucket_start timestamptz not null references usage_hourly_covered(bucket_start) on delete cascade,
  user_id text,
  client_api_key_ref text not null,
  routing_group_refs text[] not null,
  provider_account_ref text,
  operation text not null,
  provider_kind text,
  requested_model_id text,
  upstream_model_id text,
  outcome text not null,
  client_status_code integer,
  upstream_status_code integer,
  client_transport text not null,
  upstream_transport text,
  request_count bigint not null,
  success_count bigint not null,
  input_tokens bigint not null,
  output_tokens bigint not null,
  cached_tokens bigint not null,
  cache_write_tokens bigint not null,
  total_tokens bigint not null,
  latency_sum bigint not null,
  latency_count bigint not null,
  billing_usd numeric not null,
  usage_billing_usd numeric
);
create index usage_hourly_stats_bucket_idx on usage_hourly_stats(bucket_start);
create index usage_hourly_stats_owner_bucket_idx on usage_hourly_stats(user_id, bucket_start);

create function mark_usage_hour_dirty(at_time timestamptz) returns void language sql as $$
  insert into usage_hourly_dirty(bucket_start)
  select date_bin(interval '1 hour', at_time, timestamptz '1970-01-01 00:00:00+00')
  where at_time is not null
  on conflict do nothing;
$$;
create function invalidate_request_usage_hour() returns trigger language plpgsql as $$
begin
  if TG_OP <> 'INSERT' then perform mark_usage_hour_dirty(OLD.started_at); end if;
  if TG_OP <> 'DELETE' then perform mark_usage_hour_dirty(NEW.started_at); end if;
  return null;
end;
$$;
create trigger model_requests_usage_hour_dirty
after insert or update or delete on model_requests
for each row execute function invalidate_request_usage_hour();

-- Late journal settlement can change billed consumption without changing the request.
create function invalidate_charge_usage_hour() returns trigger language plpgsql as $$
begin
  if TG_OP <> 'INSERT' then
    perform mark_usage_hour_dirty(started_at) from model_requests where id = OLD.request_id;
  end if;
  if TG_OP <> 'DELETE' then
    perform mark_usage_hour_dirty(started_at) from model_requests where id = NEW.request_id;
  end if;
  return null;
end;
$$;
create trigger user_group_charge_events_usage_hour_dirty
after insert or update or delete on user_group_charge_events
for each row execute function invalidate_charge_usage_hour();
