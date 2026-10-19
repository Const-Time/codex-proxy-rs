ALTER TABLE outbound_proxies ADD COLUMN max_websocket_connections bigint NOT NULL DEFAULT 0 CHECK (max_websocket_connections BETWEEN 0 AND 4294967295);
