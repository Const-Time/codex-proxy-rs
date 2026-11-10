ALTER TABLE provider_accounts
    ADD COLUMN quota_short_term_percent smallint NOT NULL DEFAULT 100
        CHECK (quota_short_term_percent BETWEEN 1 AND 100),
    ADD COLUMN quota_weekly_percent smallint NOT NULL DEFAULT 100
        CHECK (quota_weekly_percent BETWEEN 1 AND 100);
