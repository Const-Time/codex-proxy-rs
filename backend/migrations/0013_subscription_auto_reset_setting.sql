alter table runtime_settings add column subscription_auto_reset_enabled boolean not null default true;

-- Start a fresh observation baseline when enabling, including after a disabled interval
-- with no provider refreshes. Never replay a rollover that happened while disabled.
create function reset_subscription_observation_baseline() returns trigger language plpgsql as $$
begin
    if new.subscription_auto_reset_enabled and not old.subscription_auto_reset_enabled then
        delete from subscription_quota_observations;
    end if;
    return new;
end
$$;
create trigger runtime_settings_subscription_baseline
    after update of subscription_auto_reset_enabled on runtime_settings
    for each row execute function reset_subscription_observation_baseline();
