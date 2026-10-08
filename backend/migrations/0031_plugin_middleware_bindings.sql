-- Host-owned middleware placement is published with the instance revision.
alter table plugin_instances add column middleware jsonb
    check (middleware is null or jsonb_typeof(middleware) = 'object');
