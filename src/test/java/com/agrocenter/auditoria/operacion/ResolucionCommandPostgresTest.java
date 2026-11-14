package com.agrocenter.auditoria.operacion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Se ejecuta solamente contra una BD temporal local preparada para este test. */
@EnabledIfEnvironmentVariable(named = "AUDIT_OPERATOR_PG_TEST", matches = "true")
class ResolucionCommandPostgresTest {
    @TempDir Path temp;

    @Test void comandoAisladoUsaRolOperadorYRegistraSuIdentidadDeBD() throws Exception {
        String url = System.getenv("DB_URL");
        if (url == null || !url.matches("jdbc:postgresql://localhost:[0-9]+/auditoria_test"))
            throw new IllegalStateException("Este test solo permite una BD temporal local auditoria_test");
        String sequence = UUID.randomUUID().toString();
        String id = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(("negocio\nshard\n" + sequence + "\n0").getBytes(StandardCharsets.UTF_8)));
        String eventId = "corregido-" + UUID.randomUUID();
        Path archivo = temp.resolve("corregido.json");
        Files.writeString(archivo, ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,
                 "occurredAt":"2026-11-12T22:00:00Z","producer":"ms-ventas","aggregateType":"Venta",
                 "aggregateId":"1","payload":{"ventaId":1}}
                """).formatted(eventId));
        try (var admin = DriverManager.getConnection(url, "postgres", "localtest")) {
            try (var insert = admin.prepareStatement("""
                    INSERT INTO eventos_cuarentena(id, stream, shard, sequence_number, sub_sequence_number,
                    payload_base64, tipo_error, created_at) VALUES (?, 'negocio', 'shard', ?, 0, 'e21hbA==', 'EVENTO_INVALIDO', now())
                    """)) {
                insert.setString(1, id); insert.setString(2, sequence); insert.executeUpdate();
            }
            try {
                String[] args = {"--confirmo-consumidores-detenidos", id, archivo.toString(), "Correccion revisada"};
                String anterior = System.getProperty("AUDIT_CONSUMER_DB_USERNAME");
                try {
                    System.setProperty("AUDIT_CONSUMER_DB_USERNAME", "auditoria_operator_test");
                    assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalStateException.class);
                } finally {
                    if (anterior == null) System.clearProperty("AUDIT_CONSUMER_DB_USERNAME");
                    else System.setProperty("AUDIT_CONSUMER_DB_USERNAME", anterior);
                }
                ResolucionCommand.ejecutar(args);
                assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalStateException.class);
                try (var operador = DriverManager.getConnection(url, "auditoria_operator_test", "localtest")) {
                    try (var query = operador.createStatement(); var rs = query.executeQuery("""
                            SELECT has_table_privilege(current_user, 'eventos_auditoria', 'DELETE'),
                                   has_any_column_privilege(current_user, 'eventos_auditoria', 'UPDATE'),
                                   has_column_privilege(current_user, 'eventos_cuarentena', 'payload_base64', 'UPDATE'),
                                   has_column_privilege(current_user, 'eventos_cuarentena', 'resuelto_en', 'UPDATE')
                            """)) {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getBoolean(1)).isFalse();
                        assertThat(rs.getBoolean(2)).isFalse();
                        assertThat(rs.getBoolean(3)).isFalse();
                        assertThat(rs.getBoolean(4)).isTrue();
                    }
                }
                try (var select = admin.prepareStatement("SELECT resuelto_por, event_id_corregido, payload_base64 FROM eventos_cuarentena WHERE id=?")) {
                    select.setString(1, id);
                    try (var rs = select.executeQuery()) {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getString(1)).isEqualTo("db:auditoria_operator_test");
                        assertThat(rs.getString(2)).isEqualTo(eventId);
                        assertThat(rs.getString(3)).isEqualTo("e21hbA==");
                    }
                }
            } finally {
                try (var delete = admin.prepareStatement("DELETE FROM eventos_cuarentena WHERE id=?")) {
                    delete.setString(1, id); delete.executeUpdate();
                }
                try (var delete = admin.prepareStatement("DELETE FROM eventos_auditoria WHERE event_id=?")) {
                    delete.setString(1, eventId); delete.executeUpdate();
                }
            }
        }
    }
}
