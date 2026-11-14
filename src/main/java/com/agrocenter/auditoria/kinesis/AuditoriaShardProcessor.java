package com.agrocenter.auditoria.kinesis;

import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.ColisionEventoException;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import com.agrocenter.auditoria.eventos.EventoInvalidoException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException;

/** Nunca confirma un registro hasta que la transacción de auditoría haya terminado. */
final class AuditoriaShardProcessor implements ShardRecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaShardProcessor.class);
    private final String stream;
    private final AuditoriaProcessor auditoria;
    private final CuarentenaService cuarentena;
    private final EstadoConsumidores.Stream estadoStream;
    private final AtomicBoolean fatal;
    private final EstadoConsumidores.Shard estadoShard = new EstadoConsumidores.Shard();
    private final BooleanSupplier stopping;
    private final Runnable detenerScheduler;
    private final long retryDelayMs;
    private String shard;

    AuditoriaShardProcessor(String stream, AuditoriaProcessor auditoria, CuarentenaService cuarentena, EstadoConsumidores.Stream estadoStream,
                           BooleanSupplier stopping, Runnable detenerScheduler, long retryDelayMs) {
        this.stream = stream; this.auditoria = auditoria; this.cuarentena = cuarentena;
        this.estadoStream = estadoStream; this.fatal = estadoStream.fatal;
        this.stopping = stopping; this.detenerScheduler = detenerScheduler; this.retryDelayMs = retryDelayMs;
    }

    @Override public void initialize(InitializationInput input) { shard = input.shardId(); estadoStream.shards.put(shard, estadoShard); }

    @Override public void processRecords(ProcessRecordsInput input) {
        if (fatal.get() || stopping.getAsBoolean()) return;
        try {
            for (var record : input.records()) {
                var buffer = record.data().asReadOnlyBuffer();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                while (!fatal.get() && !stopping.getAsBoolean()) {
                    try {
                        if (cuarentena.resuelta(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes)) {
                            log.info("Registro previamente corregido y auditado stream={} shard={} sequence={} subsequence={}",
                                    stream, shard, record.sequenceNumber(), record.subSequenceNumber());
                            estadoShard.retrying = false;
                            break;
                        }
                        auditoria.procesar(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes);
                        estadoShard.retrying = false;
                        break;
                    } catch (EventoInvalidoException | ColisionEventoException e) {
                        estadoShard.retrying = true;
                        try {
                            cuarentena.guardar(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes,
                                    e instanceof ColisionEventoException ? "COLISION_ID" : "EVENTO_INVALIDO");
                        } catch (RuntimeException cuarentenaFallida) {
                            log.error("No se pudo guardar cuarentena stream={} shard={} tipo={}; se reintentara",
                                    stream, shard, cuarentenaFallida.getClass().getSimpleName());
                            Thread.sleep(Math.max(100, retryDelayMs));
                            continue;
                        }
                        log.error("Registro en cuarentena; se detiene auditoria sin checkpoint stream={} shard={} sequence={}",
                                stream, shard, record.sequenceNumber());
                        fatal.set(true);
                        detenerScheduler.run();
                        return;
                    } catch (RuntimeException e) {
                        estadoShard.retrying = true;
                        log.error("Auditoria detenida en registro stream={} shard={} sequence={} tipo={}; se reintentara",
                                stream, shard, record.sequenceNumber(), e.getClass().getSimpleName());
                        Thread.sleep(Math.max(100, retryDelayMs));
                    }
                }
                if (fatal.get() || stopping.getAsBoolean()) return;
            }
            if (!input.records().isEmpty() && !confirmar(input.checkpointer())) return;
            estadoShard.lagMs = input.millisBehindLatest();
            estadoShard.observedAt = System.nanoTime();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fatal.set(true);
            detenerScheduler.run();
        } catch (Exception e) {
            fatal.set(true);
            log.error("Fallo de checkpoint de auditoria stream={} shard={} tipo={}", stream, shard, e.getClass().getSimpleName());
            detenerScheduler.run();
        }
    }

    @Override public void leaseLost(LeaseLostInput input) {
        if (shard != null) estadoStream.shards.remove(shard, estadoShard);
        log.warn("Lease de auditoria perdido stream={} shard={}", stream, shard);
    }
    private boolean confirmar(RecordProcessorCheckpointer checkpointer) throws Exception {
        for (int intento = 1; intento <= 3 && !fatal.get() && !stopping.getAsBoolean(); intento++) {
            try {
                checkpointer.checkpoint();
                estadoShard.retrying = false;
                return true;
            } catch (KinesisClientLibRetryableException e) {
                estadoShard.retrying = true;
                if (intento == 3) throw e;
                log.warn("Checkpoint temporalmente fallido stream={} shard={} intento={} tipo={}",
                        stream, shard, intento, e.getClass().getSimpleName());
                Thread.sleep(200L * intento);
            }
        }
        return false;
    }

    @Override public void shardEnded(ShardEndedInput input) {
        if (fatal.get() || stopping.getAsBoolean()) return;
        try { if (confirmar(input.checkpointer()) && shard != null) estadoStream.shards.remove(shard, estadoShard); }
        catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            fatal.set(true);
            log.error("Fallo de checkpoint al finalizar shard stream={} shard={} tipo={}", stream, shard, e.getClass().getSimpleName());
            detenerScheduler.run();
        }
    }
    @Override public void shutdownRequested(ShutdownRequestedInput input) { /* No avanzar sin procesar. */ }
}
