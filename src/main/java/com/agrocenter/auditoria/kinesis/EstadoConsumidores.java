package com.agrocenter.auditoria.kinesis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.actuate.health.Health;

/** Señal local de los shards asignados; no prueba la salud global de Kinesis. */
final class EstadoConsumidores {
    private final Map<String, Stream> streams = new ConcurrentHashMap<>();

    Stream registrar(String application) {
        Stream stream = new Stream();
        if (streams.putIfAbsent(application, stream) != null) throw new IllegalArgumentException("Aplicacion KCL duplicada");
        return stream;
    }

    Health salud(long maxLagMs, long maxSilenceMs) {
        if (streams.size() != 2) return Health.down().withDetail("kinesis", "inicializando").build();
        boolean desconocido = false;
        long now = System.nanoTime();
        for (Stream stream : streams.values()) {
            if (stream.fatal.get() || stream.worker == null || !stream.worker.isAlive())
                return Health.down().withDetail("kinesis", "consumidor detenido").build();
            if (stream.shards.isEmpty()) {
                if (elapsedMs(now, stream.startedAt) > maxSilenceMs)
                    return Health.down().withDetail("kinesis", "sin shard asignado").build();
                desconocido = true;
            }
            for (Shard shard : stream.shards.values()) {
                if (shard.retrying) return Health.down().withDetail("kinesis", "reintentando").build();
                if (elapsedMs(now, shard.observedAt == 0 ? shard.assignedAt : shard.observedAt) > maxSilenceMs)
                    return Health.down().withDetail("kinesis", "sin lecturas recientes").build();
                if (shard.lagMs == null) desconocido = true;
                else if (shard.lagMs > maxLagMs)
                    return Health.down().withDetail("kinesis", "retraso elevado").withDetail("lagMs", shard.lagMs).build();
            }
        }
        return desconocido ? Health.down().withDetail("kinesis", "esperando lecturas").build()
                : Health.up().withDetail("kinesis", "lecturas recientes").build();
    }

    private static long elapsedMs(long now, long before) { return TimeUnit.NANOSECONDS.toMillis(now - before); }

    static final class Stream {
        final long startedAt = System.nanoTime();
        final AtomicBoolean fatal;
        final Map<String, Shard> shards = new ConcurrentHashMap<>();
        volatile Thread worker;
        Stream() { this(new AtomicBoolean()); }
        Stream(AtomicBoolean fatal) { this.fatal = fatal; }
    }

    static final class Shard {
        final long assignedAt = System.nanoTime();
        volatile long observedAt;
        volatile Long lagMs;
        volatile boolean retrying;
    }
}
