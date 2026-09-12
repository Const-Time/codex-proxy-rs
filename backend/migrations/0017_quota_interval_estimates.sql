-- One bounded sampling state per account/window; never used for admission or billing.
create table account_quota_estimate_samples (
    account_id text not null references provider_accounts(id) on delete cascade,
    window_key text not null,
    observed_at timestamptz not null,
    state jsonb not null,
    primary key (account_id, window_key)
);
