package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

class EstadoConsumidoresTest {
    @Test void requiereLecturasRecientesDeAmbosStreams() {
        var estados = new EstadoConsumidores();
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        var negocio = estados.registrar("negocio");
        var stock = estados.registrar("stock");
        negocio.worker = Thread.currentThread();
        stock.worker = Thread.currentThread();
        var p1 = new AuditoriaShardProcessor("negocio", org.mockito.Mockito.mock(AuditoriaProcessor.class),
                org.mockito.Mockito.mock(CuarentenaService.class), negocio, () -> false, () -> {}, 100);
        var p2 = new AuditoriaShardProcessor("stock", org.mockito.Mockito.mock(AuditoriaProcessor.class),
                org.mockito.Mockito.mock(CuarentenaService.class), stock, () -> false, () -> {}, 100);
        p1.initialize(InitializationInput.builder().shardId("n").build());
        p2.initialize(InitializationInput.builder().shardId("s").build());
        p1.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(10L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("UP");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(1001L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(estados.salud(1000, -1).getStatus().getCode()).isEqualTo("DOWN");
        p2.leaseLost(LeaseLostInput.builder().build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        negocio.fatal.set(true);
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
    }

    @Test void reintentoEnUnShardNoSeOcultaPorOtroSano() {
        var estados = new EstadoConsumidores();
        var a = estados.registrar("a");
        var b = estados.registrar("b");
        a.worker = Thread.currentThread(); b.worker = Thread.currentThread();
        var sano = new EstadoConsumidores.Shard();
        sano.lagMs = 0L; sano.observedAt = System.nanoTime();
        var fallido = new EstadoConsumidores.Shard();
        fallido.lagMs = 0L; fallido.observedAt = System.nanoTime(); fallido.retrying = true;
        a.shards.put("sano", sano); b.shards.put("fallido", fallido);
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        fallido.retrying = false;
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("UP");
        a.worker = new Thread();
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
    }
}
