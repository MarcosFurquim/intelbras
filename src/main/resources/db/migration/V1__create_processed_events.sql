CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    source_row INTEGER NOT NULL,
    device_id VARCHAR(128) NOT NULL,
    message_type VARCHAR(80) NOT NULL,
    category VARCHAR(40) NOT NULL,
    detail VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX processed_events_type_idx ON processed_events (message_type);
CREATE INDEX processed_events_device_idx ON processed_events (device_id, processed_at);
