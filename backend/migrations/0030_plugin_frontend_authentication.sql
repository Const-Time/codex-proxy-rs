-- Host-owned principal bindings never enter plugin configuration or handshake.
alter table plugin_instances add column frontend_authentication jsonb
    check (frontend_authentication is null or jsonb_typeof(frontend_authentication) = 'object');
create unique index plugin_single_frontend_authentication on plugin_instances ((true))
    where enabled and frontend_authentication is not null;
