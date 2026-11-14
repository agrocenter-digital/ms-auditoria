package com.agrocenter.auditoria.consulta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConsultaAuditoriaRepository {
    private static final String COLUMNAS = "event_id, occurred_at, received_at, event_type, producer, aggregate_type, aggregate_id, correlation_id, schema_version, stream, shard, sequence_number, sub_sequence_number";
    private final JdbcTemplate jdbc;
    public ConsultaAuditoriaRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Optional<EventoAuditado> buscar(String id) {
        return jdbc.query("SELECT " + COLUMNAS + " FROM eventos_auditoria WHERE event_id = ?", this::mapear, id).stream().findFirst();
    }

    public List<EventoAuditado> listar(Instant desde, Instant hasta, String tipo, String productor,
                                      String correlacion, int pagina, int tamano) {
        StringBuilder sql = new StringBuilder("SELECT ").append(COLUMNAS).append(" FROM eventos_auditoria WHERE 1=1");
        List<Object> parametros = new ArrayList<>();
        if (desde != null) { sql.append(" AND occurred_at >= ?"); parametros.add(Timestamp.from(desde)); }
        if (hasta != null) { sql.append(" AND occurred_at < ?"); parametros.add(Timestamp.from(hasta)); }
        if (tipo != null) { sql.append(" AND event_type = ?"); parametros.add(tipo); }
        if (productor != null) { sql.append(" AND producer = ?"); parametros.add(productor); }
        if (correlacion != null) { sql.append(" AND correlation_id = ?"); parametros.add(correlacion); }
        sql.append(" ORDER BY occurred_at DESC, event_id DESC LIMIT ? OFFSET ?");
        parametros.add(tamano + 1);
        parametros.add((long) pagina * tamano);
        return jdbc.query(sql.toString(), this::mapear, parametros.toArray());
    }

    private EventoAuditado mapear(ResultSet rs, int n) throws SQLException {
        return new EventoAuditado(rs.getString("event_id"), rs.getTimestamp("occurred_at").toInstant(),
                rs.getTimestamp("received_at").toInstant(), rs.getString("event_type"), rs.getString("producer"),
                rs.getString("aggregate_type"), rs.getString("aggregate_id"), rs.getString("correlation_id"),
                rs.getInt("schema_version"), rs.getString("stream"), rs.getString("shard"),
                rs.getString("sequence_number"), rs.getLong("sub_sequence_number"));
    }
}
