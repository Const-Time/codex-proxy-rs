-- Preserve the existing login identity and all stable user references.
-- users.username is the legacy login email; display_name is the optional public username.
alter table users add column display_name text not null default '';
alter table users add constraint users_display_name_ck check (length(display_name) <= 128 and display_name = btrim(display_name));
