ALTER TABLE runtime_settings
    ADD COLUMN max_retry_after_seconds BIGINT NOT NULL DEFAULT 240
    CHECK (max_retry_after_seconds BETWEEN 0 AND 4294967295);
