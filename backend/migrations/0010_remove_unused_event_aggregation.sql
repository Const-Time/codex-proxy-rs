-- Each row records one event; aggregation is performed by the read queries.
alter table ops_events drop constraint ops_events_values_ck;
alter table ops_events drop column occurrence_count;
alter table ops_events add constraint ops_events_values_ck check (
  (status_code is null or status_code between 100 and 599)
  and (retry_after_ms is null or retry_after_ms >= 0)
  and (latency_ms is null or latency_ms >= 0)
);
