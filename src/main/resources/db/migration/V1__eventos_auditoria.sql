CREATE TABLE eventos_auditoria (
    event_id VARCHAR(100) PRIMARY KEY,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    producer VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    correlation_id VARCHAR(100),
    schema_version INTEGER NOT NULL,
    stream VARCHAR(128) NOT NULL,
    shard VARCHAR(128) NOT NULL,
    sequence_number VARCHAR(128) NOT NULL,
    sub_sequence_number BIGINT NOT NULL,
    payload JSONB NOT NULL
);
CREATE INDEX idx_auditoria_fecha ON eventos_auditoria (occurred_at);
CREATE INDEX idx_auditoria_tipo_fecha ON eventos_auditoria (event_type, occurred_at);
CREATE INDEX idx_auditoria_productor_fecha ON eventos_auditoria (producer, occurred_at);
CREATE INDEX idx_auditoria_correlacion ON eventos_auditoria (correlation_id);
