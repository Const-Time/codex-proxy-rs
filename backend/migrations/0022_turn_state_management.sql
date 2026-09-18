-- Provider-owned encrypted document. Secrets are encrypted before reaching SQL.
CREATE TABLE turn_state_management (
    id integer PRIMARY KEY CHECK (id = 1),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    sealed bytea,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO turn_state_management (id) VALUES (1);
