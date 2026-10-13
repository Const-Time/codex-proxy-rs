-- A trusted plugin is audited as its own generation, never as an admin session.
alter table admin_audit_events drop constraint admin_audit_events_actor_kind_ck;
alter table admin_audit_events add constraint admin_audit_events_actor_kind_ck check (
  actor_kind in ('admin_session', 'admin_api_key', 'plugin', 'system', 'anonymous')
);
alter table admin_audit_events add constraint admin_audit_events_plugin_actor_ck check (
  actor_kind <> 'plugin'
  or (
    actor_admin_user_id is null
    and case
      when actor_ref ~ '^plugin:[A-Za-z0-9_-]+@[1-9][0-9]{0,19}$'
      then octet_length(split_part(actor_ref, '@', 1)) <= 263
        and split_part(actor_ref, '@', 2)::numeric <= 18446744073709551615
      else false
    end
  )
);
