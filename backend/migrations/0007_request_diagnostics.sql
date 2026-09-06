-- Bounded request evidence is retained with its owning model request.
-- NULL identifies historical requests for which no trace was captured.
alter table model_requests add column diagnostic_trace_json jsonb;
alter table model_requests add constraint model_requests_diagnostic_trace_object
    check (diagnostic_trace_json is null or jsonb_typeof(diagnostic_trace_json) = 'object');
