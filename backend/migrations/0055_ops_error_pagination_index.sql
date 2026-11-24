-- Match the request-error count and page predicates. Errors may occur in any outcome.
CREATE INDEX model_requests_ops_error_completed_idx
    ON model_requests (completed_at DESC, id DESC)
    WHERE error_kind IS NOT NULL AND error_kind <> 'cancelled';
