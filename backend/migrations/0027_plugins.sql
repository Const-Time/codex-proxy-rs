-- Plugin configuration is independent of user/group budgets. Existing migrations remain frozen.
create table plugin_artifacts (
    sha256 text primary key check (sha256 ~ '^[0-9a-f]{64}$'),
    info jsonb not null,
    archive bytea not null check (octet_length(archive) <= 33554432),
    created_at timestamptz not null default now()
);
create table plugin_instances (
    id text primary key,
    revision bigint not null check (revision > 0),
    name text not null,
    artifact_sha256 text not null references plugin_artifacts(sha256),
    enabled boolean not null,
    configuration jsonb not null check (jsonb_typeof(configuration) = 'object'),
    secrets jsonb not null check (jsonb_typeof(secrets) = 'object'),
    updated_at timestamptz not null default now()
);
create sequence plugin_state_version;
create table plugin_private_state (
    instance_id text not null references plugin_instances(id) on delete cascade,
    namespace text not null,
    key text not null,
    schema_version integer not null check (schema_version > 0),
    version bigint not null default nextval('plugin_state_version'),
    value jsonb not null,
    primary key (instance_id, namespace, key)
);
