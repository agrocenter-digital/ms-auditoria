CREATE TABLE eventos_cuarentena (
    id VARCHAR(64) PRIMARY KEY,
    stream VARCHAR(128) NOT NULL,
    shard VARCHAR(128) NOT NULL,
    sequence_number VARCHAR(128) NOT NULL,
    sub_sequence_number BIGINT NOT NULL,
    payload_base64 TEXT NOT NULL,
    tipo_error VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX idx_cuarentena_fecha ON eventos_cuarentena (created_at);
