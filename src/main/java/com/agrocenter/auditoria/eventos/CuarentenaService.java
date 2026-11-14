package com.agrocenter.auditoria.eventos;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HexFormat;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CuarentenaService {
    private final JdbcTemplate jdbc;
    public CuarentenaService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void guardar(String stream, String shard, String sequence, long subSequence, byte[] payload, String tipo) {
        if (!valido(stream) || !valido(shard) || !valido(sequence) || subSequence < 0 || payload == null
                || !("EVENTO_INVALIDO".equals(tipo) || "COLISION_ID".equals(tipo)))
            throw new IllegalArgumentException("Metadatos de cuarentena invalidos");
        String id = idRegistro(stream, shard, sequence, subSequence);
        jdbc.update("""
                INSERT INTO eventos_cuarentena
                (id, stream, shard, sequence_number, sub_sequence_number, payload_base64, tipo_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING
                """, id, stream, shard, sequence, subSequence, Base64.getEncoder().encodeToString(payload), tipo, Timestamp.from(Instant.now()));
    }

    /** Solo permite omitir exactamente los bytes originales con una corrección ya auditada. */
    @Transactional(readOnly = true)
    public boolean resuelta(String stream, String shard, String sequence, long subSequence, byte[] payload) {
        if (!valido(stream) || !valido(shard) || !valido(sequence) || subSequence < 0 || payload == null)
            throw new IllegalArgumentException("Metadatos de cuarentena invalidos");
        Boolean resultado = jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM eventos_cuarentena c
                    JOIN eventos_auditoria a ON a.event_id = c.event_id_corregido
                    WHERE c.id = ? AND c.payload_base64 = ? AND c.resuelto_en IS NOT NULL)
                """, Boolean.class, idRegistro(stream, shard, sequence, subSequence), Base64.getEncoder().encodeToString(payload));
        return Boolean.TRUE.equals(resultado);
    }

    private static String idRegistro(String stream, String shard, String sequence, long subSequence) {
        String clave = stream + "\n" + shard + "\n" + sequence + "\n" + subSequence;
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(clave.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    private static boolean valido(String value) {
        return value != null && !value.isBlank() && value.length() <= 128 && !value.contains("\n") && !value.contains("\r");
    }
}
