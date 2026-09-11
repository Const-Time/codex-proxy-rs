-- Limits apply to a user's aggregate traffic across all owned API keys.
alter table users add column max_concurrency bigint not null default 0 check (max_concurrency between 0 and 9007199254740991);
alter table users add column requests_per_minute bigint not null default 0 check (requests_per_minute between 0 and 9007199254740991);
