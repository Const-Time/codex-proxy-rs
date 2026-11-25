alter table provider_accounts
    add column quota_limit_window_minutes integer
    check (quota_limit_window_minutes between 1 and 65535);
