-- Existing requests, including in-flight ones, keep the original all-fee rule.
alter table model_requests add column billing_multiplier_scope jsonb not null default '"all"';
alter table user_group_charge_events add column billing_multiplier_scope jsonb not null default '"all"';

create or replace function freeze_request_billing_multiplier() returns trigger language plpgsql as $$
declare rule jsonb;
begin
    if new.billing_multiplier is null then
        select g.model_multipliers -> new.requested_model_id into rule
          from account_groups g where g.id = new.routing_group_refs[1];
        if jsonb_typeof(rule) = 'object' then
            new.billing_multiplier := (rule ->> 'multiplier')::numeric;
            new.billing_multiplier_scope := coalesce(rule -> 'scope', '"all"'::jsonb);
        else
            new.billing_multiplier := (rule #>> '{}')::numeric;
            new.billing_multiplier_scope := '"all"';
        end if;
        new.billing_multiplier := coalesce(new.billing_multiplier, 1);
    end if;
    return new;
end;
$$;

-- Only frozen component costs are eligible. Unknown/unitemized fees retain their
-- raw price rather than guessing their split or applying a whole-bill multiplier.
create function scoped_billed_cost(raw numeric, rate numeric, scope jsonb, snapshot jsonb)
returns numeric language plpgsql immutable parallel safe as $$
declare selected numeric := 0; item text; part numeric;
begin
    if raw is null then return null; end if;
    if scope = '"all"'::jsonb then return round(raw * rate, 10); end if;
    if rate = 1 then return raw; end if;
    if snapshot is null or snapshot->>'version' <> '1'
        or (snapshot->>'total')::numeric is distinct from raw then return raw; end if;
    for item in select distinct jsonb_array_elements_text(scope->'components') loop
        part := (snapshot->>item)::numeric;
        if part is null or part < 0 then return raw; end if;
        if item in ('input', 'cacheRead') and jsonb_typeof(snapshot->'image') = 'object' then
            part := part + coalesce((snapshot->'image'->>item)::numeric, 0);
        end if;
        selected := selected + part;
    end loop;
    if selected > raw then return raw; end if;
    return round(raw - selected + selected * rate, 10);
end;
$$;

alter table model_requests alter column billed_cost_amount set expression as
    (scoped_billed_cost(cost_amount, billing_multiplier, billing_multiplier_scope, billing_snapshot_json));

create or replace function apply_user_group_charge_multiplier() returns trigger language plpgsql as $$
declare snapshot jsonb;
begin
    -- Explicit raw amount identifies a restored historical ledger entry.
    if new.raw_amount_usd is null then
        new.raw_amount_usd := new.amount_usd;
        select billing_multiplier, billing_multiplier_scope, billing_snapshot_json
          into new.billing_multiplier, new.billing_multiplier_scope, snapshot
          from model_requests where id = new.request_id;
        new.billing_multiplier := coalesce(new.billing_multiplier, 1);
        new.billing_multiplier_scope := coalesce(new.billing_multiplier_scope, '"all"'::jsonb);
        new.amount_usd := scoped_billed_cost(new.raw_amount_usd, new.billing_multiplier, new.billing_multiplier_scope, snapshot);
    end if;
    return new;
end;
$$;
