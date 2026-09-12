-- Keep historical ownership while allowing a deleted email to be registered again.
alter table users add column deleted_at timestamptz;
alter table users add constraint users_deleted_disabled_ck check (deleted_at is null or not enabled);
drop index users_username_idx;
create unique index users_username_idx on users (lower(username)) where deleted_at is null;
