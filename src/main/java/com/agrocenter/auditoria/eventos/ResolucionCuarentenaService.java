package com.agrocenter.auditoria.eventos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Operación interna para un proceso administrativo aislado; no tiene endpoint HTTP. */
@Service
@ConditionalOnProperty(name = "agrocenter.auditoria.resolucion.enabled", havingValue = "true")
public class ResolucionCuarentenaService {
    private final JdbcTemplate jdbc;
    private final AuditoriaProcessor processor;
    private final ObjectMapper mapper;

    public ResolucionCuarentenaService(JdbcTemplate jdbc, AuditoriaProcessor processor, ObjectMapper mapper) {
        this.jdbc = jdbc; this.processor = processor; this.mapper = mapper;
    }

    @Transactional
    public void resolver(String id, String operador, String motivo, byte[] eventoCorregido) {
        if (id == null || !id.matches("[0-9a-f]{64}") || operador == null || operador.isBlank() || operador.length() > 200
                || motivo == null || motivo.isBlank() || motivo.length() > 500 || eventoCorregido == null
                || eventoCorregido.length == 0 || eventoCorregido.length > 1024 * 1024)
            throw new IllegalArgumentException("Datos de resolucion invalidos");
        List<Registro> registros = jdbc.query("""
                SELECT stream, shard, sequence_number, sub_sequence_number, resuelto_en
                FROM eventos_cuarentena WHERE id = ? FOR UPDATE
                """, (rs, n) -> new Registro(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getTimestamp(5) != null), id);
        if (registros.isEmpty()) throw new IllegalArgumentException("Registro de cuarentena inexistente");
        Registro registro = registros.getFirst();
        if (registro.resuelto()) throw new IllegalStateException("Registro de cuarentena ya resuelto");
        if (!processor.procesar(registro.stream(), registro.shard(), registro.sequence(), registro.subSequence(), eventoCorregido))
            throw new IllegalStateException("El evento corregido ya estaba auditado");
        try {
            JsonNode node = mapper.readTree(eventoCorregido);
            String eventId = node.path("eventId").textValue(); // El procesador ya validó este campo.
            int actualizadas = jdbc.update("""
                    UPDATE eventos_cuarentena
                    SET resuelto_en = ?, resuelto_por = ?, motivo_resolucion = ?, event_id_corregido = ?
                    WHERE id = ? AND resuelto_en IS NULL
                    """, Timestamp.from(Instant.now()), operador, motivo, eventId, id);
            if (actualizadas != 1) throw new IllegalStateException("No se pudo registrar la resolucion");
        } catch (java.io.IOException e) { throw new EventoInvalidoException("Evento corregido invalido", e); }
    }

    private record Registro(String stream, String shard, String sequence, long subSequence, boolean resuelto) { }
}
