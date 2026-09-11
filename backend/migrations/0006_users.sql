-- Existing administrator identities become users without changing their IDs.
alter table admin_users rename to users;
alter table users
    add column username text,
    add column role text not null default 'user' check (role in ('admin', 'user')),
    add column enabled boolean not null default true,
    add column auth_version bigint not null default 1 check (auth_version > 0);
update users set username = id, role = 'admin';
alter table users alter column username set not null;
create unique index users_username_idx on users (lower(username));
alter table users add constraint users_username_ck
    check (length(username) between 1 and 128 and username = btrim(username));

create table user_account_groups (
    user_id text not null references users(id) on delete restrict,
    account_group_id text not null references account_groups(id) on delete cascade,
    primary key (user_id, account_group_id)
);

alter table client_api_keys add column owner_user_id text references users(id) on delete restrict;
update client_api_keys set owner_user_id = (select id from users order by created_at, id limit 1);
alter table client_api_keys alter column owner_user_id set not null;
create index client_api_keys_owner_idx on client_api_keys (owner_user_id, created_at desc, id desc);

-- Historical ownership must survive key deletion. Unknown history stays admin-only.
alter table model_requests add column user_id text references users(id) on delete restrict;
update model_requests r set user_id = k.owner_user_id from client_api_keys k
    where k.id = r.client_api_key_ref;
create index model_requests_user_created_idx on model_requests (user_id, started_at desc, id desc);
