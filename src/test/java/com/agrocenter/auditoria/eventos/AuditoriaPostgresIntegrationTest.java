package com.agrocenter.auditoria.eventos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import com.agrocenter.auditoria.consulta.ConsultaAuditoriaRepository;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@EnabledIfEnvironmentVariable(named = "AUDIT_PG_TEST", matches = "true")
class AuditoriaPostgresIntegrationTest {
    @Autowired AuditoriaProcessor processor;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactions;
    @Autowired ConsultaAuditoriaRepository consultas;
    @MockitoBean JwtDecoder decoder;

    private static final byte[] EVENTO = """
            {"eventId":"evento-1","eventType":"VentaConfirmada","schemaVersion":1,
             "occurredAt":"2026-11-12T22:00:00Z","producer":"ms-ventas","aggregateType":"Venta",
             "aggregateId":"1","correlationId":"trace-1","payload":{"ventaId":1}}
            """.getBytes(StandardCharsets.UTF_8);

    @BeforeEach void limpiar() { jdbc.update("DELETE FROM eventos_auditoria"); }

    @Test void duplicadosConcurrentesProducenUnaSolaTraza() throws Exception {
        var inicio = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var uno = executor.submit(() -> { inicio.await(); return processor.procesar("negocio", "shard", "1", 0, EVENTO); });
            var dos = executor.submit(() -> { inicio.await(); return processor.procesar("negocio", "shard", "1", 0, EVENTO); });
            inicio.countDown();
            assertThat(java.util.List.of(uno.get(20, TimeUnit.SECONDS), dos.get(20, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT payload->>'ventaId' FROM eventos_auditoria WHERE event_id='evento-1'", String.class)).isEqualTo("1");
    }

    @Test void rollbackNoGuardaTraza() {
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            processor.procesar("stock", "shard", "2", 1, EVENTO);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isZero();
    }

    @Test void identidadReutilizadaConOtroContenidoNoSeDescarta() {
        assertThat(processor.procesar("negocio", "shard", "1", 0, EVENTO)).isTrue();
        byte[] distinto = new String(EVENTO, StandardCharsets.UTF_8).replace("\"ventaId\":1", "\"ventaId\":2")
                .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> processor.procesar("negocio", "shard", "2", 0, distinto))
                .isInstanceOf(ColisionEventoException.class);
        assertThat(jdbc.queryForObject("SELECT payload->>'ventaId' FROM eventos_auditoria WHERE event_id='evento-1'", String.class)).isEqualTo("1");
    }

    @Test void consultasFiltranConParametrosYPaginan() {
        processor.procesar("negocio", "shard", "1", 0, EVENTO);
        byte[] otro = new String(EVENTO, StandardCharsets.UTF_8).replace("evento-1", "evento-2")
                .replace("VentaConfirmada", "CompraCreada").replace("trace-1", "trace-2")
                .replace("2026-11-12T22:00:00Z", "2026-11-13T22:00:00Z").getBytes(StandardCharsets.UTF_8);
        processor.procesar("negocio", "shard", "2", 0, otro);
        var desde = Instant.parse("2026-11-12T00:00:00Z");
        var hasta = Instant.parse("2026-11-13T00:00:00Z");
        assertThat(consultas.listar(desde, hasta, "VentaConfirmada", "ms-ventas", "trace-1", 0, 25))
                .extracting(e -> e.eventId()).containsExactly("evento-1");
        assertThat(consultas.listar(null, null, "' OR 1=1 --", null, null, 0, 25)).isEmpty();
        assertThat(consultas.listar(null, null, null, null, null, 0, 1))
                .extracting(e -> e.eventId()).containsExactly("evento-2", "evento-1");
        assertThat(consultas.listar(null, null, null, null, null, 1, 1))
                .extracting(e -> e.eventId()).containsExactly("evento-1");
        assertThat(consultas.buscar("evento-2")).isPresent();
        assertThat(consultas.buscar("ausente")).isEmpty();
    }
}
