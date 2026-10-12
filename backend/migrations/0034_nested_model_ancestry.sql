-- Nested calls share the existing ledger. A request plugin may start its child before
-- the parent's observational row is queued, so historical ancestry has no FK.
alter table model_requests
    add column parent_request_id text,
    add column initiating_plugin_instance_id text,
    add constraint model_requests_nested_ancestry_check check (
        (parent_request_id is null and initiating_plugin_instance_id is null)
        or (
            parent_request_id is not null and initiating_plugin_instance_id is not null
            and length(parent_request_id) between 1 and 128
            and parent_request_id <> id
            and length(initiating_plugin_instance_id) between 1 and 128
            and parent_request_id !~ '[[:cntrl:]]'
            and initiating_plugin_instance_id !~ '[[:cntrl:]]'
        )
    );
create index model_requests_parent_request_id_idx on model_requests (parent_request_id)
    where parent_request_id is not null;
