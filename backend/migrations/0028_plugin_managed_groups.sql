-- Ownership and idempotency, not another account or budget authority.
create table plugin_managed_groups (
    instance_id text not null references plugin_instances(id) on delete cascade,
    resource_key text not null check (octet_length(resource_key) between 1 and 128),
    group_id text unique references account_groups(id) on delete set null,
    created_at timestamptz not null default now(),
    primary key(instance_id, resource_key)
);
-- A null group_id is a tombstone: repeating ensure must not recreate an administrator-deleted group.
