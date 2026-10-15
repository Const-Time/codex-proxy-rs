-- Data schema metadata remains unchanged while a different artifact is disabled.
-- Active data and instance revisions are switched in the same original transaction.
create table plugin_state_schemas (
    instance_id text not null references plugin_instances(id) on delete cascade,
    namespace text not null,
    declaration jsonb not null check (jsonb_typeof(declaration) = 'object'),
    primary key (instance_id, namespace)
);
insert into plugin_state_schemas(instance_id, namespace, declaration)
select i.id, state->>'namespace', state
from plugin_instances i
join plugin_artifacts a on a.sha256=i.artifact_sha256
cross join lateral jsonb_array_elements(coalesce(a.info->'manifest'->'state', '[]'::jsonb)) state;
