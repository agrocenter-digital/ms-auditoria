package com.agrocenter.auditoria.eventos;

import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AuditoriaRepository {
    private final JdbcTemplate jdbc;
    public AuditoriaRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** El conflicto de ID es idempotente, incluso con dos consumidores concurrentes. */
    public boolean insertar(RegistroAuditoria evento) {
        int inserted = jdbc.update("""
                INSERT INTO eventos_auditoria
                (event_id, occurred_at, received_at, event_type, producer, aggregate_type, aggregate_id,
                 correlation_id, schema_version, stream, shard, sequence_number, sub_sequence_number, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
                ON CONFLICT (event_id) DO NOTHING
                """, evento.eventId(), Timestamp.from(evento.occurredAt()), Timestamp.from(evento.receivedAt()),
                evento.eventType(), evento.producer(), evento.aggregateType(), evento.aggregateId(),
                evento.correlationId(), evento.schemaVersion(), evento.stream(), evento.shard(), evento.sequenceNumber(),
                evento.subSequenceNumber(), evento.payloadJson());
        if (inserted == 1) return true;
        Boolean igual = jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM eventos_auditoria
                    WHERE event_id = ? AND occurred_at = ? AND event_type = ? AND producer = ?
                      AND aggregate_type = ? AND aggregate_id = ? AND correlation_id IS NOT DISTINCT FROM ?
                      AND schema_version = ? AND payload = CAST(? AS jsonb))
                """, Boolean.class, evento.eventId(), Timestamp.from(evento.occurredAt()), evento.eventType(),
                evento.producer(), evento.aggregateType(), evento.aggregateId(), evento.correlationId(),
                evento.schemaVersion(), evento.payloadJson());
        if (!Boolean.TRUE.equals(igual)) throw new ColisionEventoException();
        return false;
    }
}
