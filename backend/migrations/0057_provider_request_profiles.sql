alter table runtime_settings
  add column provider_request_profiles_json jsonb not null default '{}',
  add constraint runtime_settings_provider_request_profiles_ck
    check (jsonb_typeof(provider_request_profiles_json) = 'object');
