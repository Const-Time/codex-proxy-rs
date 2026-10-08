-- Actual keys, ownership and budgets remain in their existing tables.
create table plugin_managed_keys (
    instance_id text not null references plugin_instances(id) on delete cascade,
    resource_key text not null check (octet_length(resource_key) between 1 and 128),
    client_key_id text unique references client_api_keys(id) on delete set null,
    owner_user_id text not null references users(id) on delete restrict,
    created_at timestamptz not null default now(),
    primary key(instance_id, resource_key)
);
