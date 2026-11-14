package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import com.agrocenter.auditoria.eventos.EventoInvalidoException;
import com.agrocenter.auditoria.eventos.ResolucionCuarentenaService;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@SpringBootTest(properties = "agrocenter.auditoria.resolucion.enabled=true")
@EnabledIfEnvironmentVariable(named = "AUDIT_PG_TEST", matches = "true")
class ResolucionCuarentenaPostgresTest {
    @Autowired ResolucionCuarentenaService resolver;
    @Autowired CuarentenaService cuarentena;
    @Autowired AuditoriaProcessor auditoria;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactions;
    @MockitoBean JwtDecoder decoder;
    private static final byte[] ORIGINAL = "{mal".getBytes(StandardCharsets.UTF_8);

    @BeforeEach void preparar() {
        limpiar();
        cuarentena.guardar("negocio", "shard", "123", 2, ORIGINAL, "EVENTO_INVALIDO");
    }

    @AfterEach void limpiar() {
        jdbc.update("DELETE FROM eventos_cuarentena");
        jdbc.update("DELETE FROM eventos_auditoria");
    }

    @Test void correccionEsAtomicaYRelecturaSoloOmiteElOriginalExacto() throws Exception {
        String id = id();
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isFalse();
        resolver.resolver(id, "admin-123", "Correccion revisada", corregido("corregido-1"));
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isTrue();
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, "otro".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThat(jdbc.queryForObject("SELECT resuelto_por FROM eventos_cuarentena WHERE id=?", String.class, id)).isEqualTo("admin-123");
        assertThat(jdbc.queryForObject("SELECT event_id_corregido FROM eventos_cuarentena WHERE id=?", String.class, id)).isEqualTo("corregido-1");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
        assertThatThrownBy(() -> resolver.resolver(id, "admin-123", "otra", corregido("corregido-2")))
                .isInstanceOf(IllegalStateException.class);

        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var processor = new AuditoriaShardProcessor("negocio", auditoria, cuarentena,
                new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        processor.initialize(InitializationInput.builder().shardId("shard").build());
        var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(2).data(ByteBuffer.wrap(ORIGINAL)).build();
        processor.processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
        verify(checkpoint).checkpoint();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
    }

    @Test void correccionInvalidaOTransactionRollbackNoResuelven() {
        String id = id();
        assertThatThrownBy(() -> resolver.resolver(id, "admin", "revisado", ORIGINAL))
                .isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            resolver.resolver(id, "admin", "revisado", corregido("rollback-1"));
            throw new IllegalStateException("rollback simulado");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isZero();
    }

    @Test void eventoCorregidoYaExistenteNoPermiteResolver() {
        byte[] evento = corregido("ya-existe");
        auditoria.procesar("negocio", "shard", "otro", 0, evento);
        assertThatThrownBy(() -> resolver.resolver(id(), "admin", "revisado", evento)).isInstanceOf(IllegalStateException.class);
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isFalse();
    }

    @Test void dosCorreccionesSimultaneasSoloAdmitenUna() throws Exception {
        String registro = id();
        var inicio = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> intentar(inicio, registro, "corregido-a"));
            var b = executor.submit(() -> intentar(inicio, registro, "corregido-b"));
            inicio.countDown();
            assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isTrue();
    }

    private boolean intentar(CountDownLatch inicio, String registro, String eventId) throws InterruptedException {
        inicio.await();
        try { resolver.resolver(registro, "admin", "revisado", corregido(eventId)); return true; }
        catch (IllegalStateException e) { return false; }
    }

    private String id() { return jdbc.queryForObject("SELECT id FROM eventos_cuarentena", String.class); }

    private byte[] corregido(String eventId) {
        return ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,
                 "occurredAt":"2026-11-12T22:00:00Z","producer":"ms-ventas","aggregateType":"Venta",
                 "aggregateId":"1","correlationId":"trace-1","payload":{"ventaId":1}}
                """).formatted(eventId).getBytes(StandardCharsets.UTF_8);
    }
}
