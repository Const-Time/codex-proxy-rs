alter table account_groups add column model_multipliers jsonb not null default '{}'::jsonb
    check (jsonb_typeof(model_multipliers) = 'object');

-- Existing requests retain their original billing; new requests freeze the rate
-- before upstream execution, using the public requested model and routing group.
alter table model_requests add column billing_multiplier numeric(20,10) not null default 1
    check (billing_multiplier >= 0 and billing_multiplier <= 1000);
alter table model_requests alter column billing_multiplier drop default;
alter table model_requests add column billed_cost_amount numeric generated always as
    (round(cost_amount * billing_multiplier, 10)) stored;

create function freeze_request_billing_multiplier() returns trigger language plpgsql as $$
begin
    if new.billing_multiplier is null then
        select coalesce((g.model_multipliers ->> new.requested_model_id)::numeric, 1)
          into new.billing_multiplier
          from account_groups g where g.id = new.routing_group_refs[1];
        new.billing_multiplier := coalesce(new.billing_multiplier, 1);
    end if;
    return new;
end;
$$;
create trigger freeze_request_billing_multiplier before insert on model_requests
    for each row execute function freeze_request_billing_multiplier();

alter table user_group_charge_events add column raw_amount_usd numeric(20,10);
alter table user_group_charge_events add column billing_multiplier numeric(20,10) not null default 1;
update user_group_charge_events set raw_amount_usd = amount_usd;
alter table user_group_charge_events alter column raw_amount_usd set not null;

create function apply_user_group_charge_multiplier() returns trigger language plpgsql as $$
begin
    -- Explicit raw amount identifies restored historical charge facts.
    if new.raw_amount_usd is null then
        new.raw_amount_usd := new.amount_usd;
        select billing_multiplier into new.billing_multiplier from model_requests where id = new.request_id;
        new.billing_multiplier := coalesce(new.billing_multiplier, 1);
        new.amount_usd := round(new.raw_amount_usd * new.billing_multiplier, 10);
    end if;
    return new;
end;
$$;
create trigger apply_user_group_charge_multiplier before insert on user_group_charge_events
    for each row execute function apply_user_group_charge_multiplier();
