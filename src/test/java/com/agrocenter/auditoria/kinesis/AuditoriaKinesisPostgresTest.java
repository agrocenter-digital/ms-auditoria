package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import com.agrocenter.auditoria.eventos.ResolucionCuarentenaService;
import com.agrocenter.auditoria.consulta.ConsultaCuarentenaRepository;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.exceptions.ThrottlingException;

@SpringBootTest
@AutoConfigureMockMvc
@EnabledIfEnvironmentVariable(named = "AUDIT_PG_TEST", matches = "true")
class AuditoriaKinesisPostgresTest {
    @Autowired AuditoriaProcessor auditoria;
    @Autowired CuarentenaService cuarentena;
    @Autowired ConsultaCuarentenaRepository consultaCuarentena;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectProvider<AuditoriaKinesisConsumers> consumers;
    @Autowired ObjectProvider<ResolucionCuarentenaService> resolucion;
    @MockitoBean JwtDecoder decoder;
    @Autowired MockMvc mvc;

    @BeforeEach void limpiar() { jdbc.update("DELETE FROM eventos_auditoria"); jdbc.update("DELETE FROM eventos_cuarentena"); }

    @Test void dosStreamsPersistenYDuplicadosNoAgreganTrazasAntesDelCheckpoint() throws Exception {
        assertThat(consumers.getIfAvailable()).isNull();
        assertThat(resolucion.getIfAvailable()).isNull();
        var checkpointer = mock(RecordProcessorCheckpointer.class);
        doAnswer(call -> {
            assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isPositive();
            return null;
        }).when(checkpointer).checkpoint();
        var negocio = new AuditoriaShardProcessor("negocio", auditoria, cuarentena, new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        var stock = new AuditoriaShardProcessor("stock", auditoria, cuarentena, new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        negocio.initialize(InitializationInput.builder().shardId("shard-negocio").build());
        stock.initialize(InitializationInput.builder().shardId("shard-stock").build());
        var venta = input(checkpointer, "evento-venta", "VentaConfirmada", 0);
        negocio.processRecords(venta);
        negocio.processRecords(venta);
        stock.processRecords(input(checkpointer, "evento-stock", "InventarioCritico", 1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT sub_sequence_number FROM eventos_auditoria WHERE event_id='evento-stock'", Long.class)).isEqualTo(1);
        verify(checkpointer, times(3)).checkpoint();
    }

    @Test void eventoInvalidoSeGuardaUnaVezYNoAvanzaCheckpoint() throws Exception {
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var detenido = mock(Runnable.class);
        for (int i = 0; i < 2; i++) {
            var fatal = new AtomicBoolean();
            var processor = new AuditoriaShardProcessor("stock", auditoria, cuarentena, new EstadoConsumidores.Stream(fatal), () -> false, detenido, 100);
            processor.initialize(InitializationInput.builder().shardId("shard").build());
            var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(2)
                    .data(ByteBuffer.wrap("{".getBytes(StandardCharsets.UTF_8))).build();
            processor.processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
            assertThat(fatal).isTrue();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_cuarentena", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT sub_sequence_number FROM eventos_cuarentena", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT payload_base64 FROM eventos_cuarentena", String.class)).isEqualTo("ew==");
        var lista = consultaCuarentena.listar("EVENTO_INVALIDO", 0, 25);
        assertThat(lista).hasSize(1);
        assertThat(lista.getFirst().subSequenceNumber()).isEqualTo(2);
        assertThat(consultaCuarentena.buscar(lista.getFirst().id())).isPresent();
        assertThat(consultaCuarentena.listar("COLISION_ID", 0, 25)).isEmpty();
        assertThat(consultaCuarentena.listar("' OR 1=1 --", 0, 25)).isEmpty();
        verify(checkpoint, never()).checkpoint();
        verify(detenido, times(2)).run();
    }

    @Test void saludPublicaNoExponeDetallesNiEndpointsInternos() throws Exception {
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/actuator/health"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.status").value("UP"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.components").doesNotExist());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/actuator/health/db"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isUnauthorized());
    }

    @Test void reinicioSimuladoTrasFalloDeCheckpointNoDuplicaAuditoria() throws Exception {
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("indisponible")).when(checkpoint).checkpoint();
        var fatal = new AtomicBoolean();
        var detener = mock(Runnable.class);
        var primero = new AuditoriaShardProcessor("negocio", auditoria, cuarentena,
                new EstadoConsumidores.Stream(fatal), () -> false, detener, 100);
        primero.initialize(InitializationInput.builder().shardId("shard-negocio").build());
        primero.processRecords(input(checkpoint, "evento-reinicio", "VentaConfirmada", 0));
        assertThat(fatal).isTrue();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
        verify(checkpoint, times(3)).checkpoint();
        verify(detener).run();

        var recuperado = mock(RecordProcessorCheckpointer.class);
        var segundo = new AuditoriaShardProcessor("negocio", auditoria, cuarentena,
                new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        segundo.initialize(InitializationInput.builder().shardId("shard-negocio").build());
        segundo.processRecords(input(recuperado, "evento-reinicio", "VentaConfirmada", 0));
        verify(recuperado).checkpoint();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_auditoria", Long.class)).isEqualTo(1);
    }

    private ProcessRecordsInput input(RecordProcessorCheckpointer checkpoint, String id, String tipo, long subsecuencia) {
        byte[] json = ("""
                {"eventId":"%s","eventType":"%s","schemaVersion":1,
                 "occurredAt":"2026-11-12T22:00:00Z","producer":"ms-ventas","aggregateType":"Venta",
                 "aggregateId":"1","correlationId":"trace-1","payload":{"id":1}}
                """).formatted(id, tipo).getBytes(StandardCharsets.UTF_8);
        var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(subsecuencia)
                .data(ByteBuffer.wrap(json)).build();
        return ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build();
    }
}
