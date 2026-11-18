ALTER TABLE provider_accounts ADD COLUMN dispatch_hours jsonb NOT NULL
    DEFAULT '{"enabled":false,"timeZone":"Asia/Shanghai","windows":[]}'::jsonb;
