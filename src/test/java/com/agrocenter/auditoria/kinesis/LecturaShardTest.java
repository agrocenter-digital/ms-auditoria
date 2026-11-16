package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LecturaShardTest {
    @Test void muestraAusenciaDeShardSinInventarLag() {
        var estados = new EstadoConsumidores();
        var stream = estados.registrar("app");
        stream.worker = Thread.currentThread();
        var resultado = estados.detalle(Map.of("app", "stream"), 1000, false);
        assertThat(resultado).hasSize(1);
        var lectura = resultado.get(0);
        assertThat(lectura.stream()).isEqualTo("stream");
        assertThat(lectura.shard()).isNull();
        assertThat(lectura.lagMs()).isNull();
        assertThat(lectura.antiguedadMs()).isNull();
        assertThat(lectura.lecturaAntigua()).isTrue();
        assertThat(lectura.consumidorActivo()).isTrue();
    }
    @Test void distingueLecturasRecientesAntiguasYReintentos() {
        var estados = new EstadoConsumidores();
        var stream = estados.registrar("app");
        stream.worker = Thread.currentThread();
        var shard = new EstadoConsumidores.Shard();
        shard.lagMs = 1200L;
        shard.observedAt = System.nanoTime();
        stream.shards.put("shard-1", shard);
        var reciente = estados.detalle(Map.of("app", "stream"), 120000, false).get(0);
        assertThat(reciente.shard()).isEqualTo("shard-1");
        assertThat(reciente.lagMs()).isEqualTo(1200L);
        assertThat(reciente.antiguedadMs()).isNotNull();
        assertThat(reciente.lecturaAntigua()).isFalse();
        shard.observedAt = System.nanoTime() - TimeUnit.MINUTES.toNanos(3);
        shard.retrying = true;
        var antigua = estados.detalle(Map.of("app", "stream"), 120000, false).get(0);
        assertThat(antigua.lecturaAntigua()).isTrue();
        assertThat(antigua.reintentando()).isTrue();
        assertThat(estados.detalle(Map.of("app", "stream"), 120000, true).get(0).consumidorActivo()).isFalse();
    }
}
