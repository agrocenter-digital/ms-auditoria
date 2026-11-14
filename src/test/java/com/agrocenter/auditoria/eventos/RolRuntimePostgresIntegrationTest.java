package com.agrocenter.auditoria.eventos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Solo para base temporal dedicada: debe ejecutarse con el rol de runtime restringido. */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "AUDIT_RESTRICTED_PG_TEST", matches = "true")
class RolRuntimePostgresIntegrationTest {
    @Autowired AuditoriaProcessor processor;
    @Autowired CuarentenaService cuarentena;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean JwtDecoder decoder;

    @Test void runtimeInsertaYConsultaPeroNoAlteraRegistros() {
        assertThat(jdbc.queryForObject("SELECT current_user", String.class)).isEqualTo("auditoria_runtime_test");
        String eventId = UUID.randomUUID().toString();
        byte[] payload = ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"%s",
                 "producer":"ms-ventas","aggregateType":"Venta","aggregateId":"1","payload":{"ventaId":1}}
                """).formatted(eventId, Instant.now()).getBytes(StandardCharsets.UTF_8);
        assertThat(processor.procesar("negocio", "shard", "123", 0, payload)).isTrue();
        assertThat(processor.procesar("negocio", "shard", "123", 0, payload)).isFalse();
        cuarentena.guardar("negocio", "shard", "124", 0, "invalido".getBytes(StandardCharsets.UTF_8), "EVENTO_INVALIDO");
        assertThat(cuarentena.resuelta("negocio", "shard", "124", 0, "invalido".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria WHERE event_id = ?", Long.class, eventId)).isEqualTo(1);
        for (String tabla : new String[] {"eventos_auditoria", "eventos_cuarentena"}) {
            assertThat(jdbc.queryForObject("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = to_regclass(?)", String.class, "public." + tabla))
                    .isNotEqualTo("auditoria_runtime_test");
            for (String permiso : new String[] {"UPDATE", "DELETE", "TRUNCATE"}) {
                assertThat(jdbc.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, tabla, permiso))
                        .as("%s en %s", permiso, tabla).isFalse();
            }
        }
        assertThatThrownBy(() -> jdbc.update("DELETE FROM eventos_auditoria WHERE false"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE eventos_cuarentena SET tipo_error = tipo_error WHERE false"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(jdbc.queryForObject("SELECT has_schema_privilege(current_user, 'public', 'CREATE')", Boolean.class)).isFalse();
        assertThat(jdbc.queryForObject("SELECT has_table_privilege(current_user, 'public.flyway_schema_history', 'SELECT')", Boolean.class)).isFalse();
    }
}
