package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import com.agrocenter.auditoria.eventos.EventoInvalidoException;
import com.agrocenter.auditoria.eventos.ColisionEventoException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.exceptions.ShutdownException;

class AuditoriaShardProcessorTest {
    @Test void guardaLosDosRegistrosAntesDelCheckpointConSubsecuencia() throws Exception {
        var auditoria = mock(AuditoriaProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var processor = processor(auditoria, new AtomicBoolean(), new AtomicBoolean(), mock(Runnable.class));
        processor.processRecords(input(checkpoint));
        var order = inOrder(auditoria, checkpoint);
        order.verify(auditoria).procesar(eq("stream"), eq("shard"), eq("seq"), eq(0L), any());
        order.verify(auditoria).procesar(eq("stream"), eq("shard"), eq("seq"), eq(1L), any());
        order.verify(checkpoint).checkpoint();
    }

    @Test void siNoSePuedeGuardarYSeDetieneNoConfirma() throws Exception {
        var auditoria = mock(AuditoriaProcessor.class);
        var fatal = new AtomicBoolean();
        var stopping = new AtomicBoolean();
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doAnswer(invocation -> { stopping.set(true); throw new IllegalStateException("sin base"); })
                .when(auditoria).procesar(anyString(), anyString(), anyString(), anyLong(), any());
        processor(auditoria, fatal, stopping, mock(Runnable.class)).processRecords(input(checkpoint));
        verify(checkpoint, never()).checkpoint();
        verify(auditoria, times(1)).procesar(anyString(), anyString(), anyString(), anyLong(), any());
    }

    @Test void falloDeCheckpointDetieneYNoConfirmaElFinalDeShard() throws Exception {
        var fatal = new AtomicBoolean();
        var auditoria = mock(AuditoriaProcessor.class);
        var detener = mock(Runnable.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new IllegalStateException("checkpoint")).when(checkpoint).checkpoint();
        var processor = processor(auditoria, fatal, new AtomicBoolean(), detener);
        processor.processRecords(input(checkpoint));
        processor.shardEnded(ShardEndedInput.builder().checkpointer(checkpoint).build());
        assertThat(fatal).isTrue();
        verify(checkpoint, times(1)).checkpoint();
        verify(detener).run();
    }

    @Test void invalidoYColisionSeGuardanAntesDeDetenerSinCheckpoint() throws Exception {
        for (boolean colision : new boolean[] {false, true}) {
            var auditoria = mock(AuditoriaProcessor.class);
            var cuarentena = mock(CuarentenaService.class);
            var fatal = new AtomicBoolean();
            var detener = mock(Runnable.class);
            var checkpoint = mock(RecordProcessorCheckpointer.class);
            doThrow(colision ? new ColisionEventoException() : new EventoInvalidoException("invalido"))
                    .when(auditoria).procesar(anyString(), anyString(), anyString(), anyLong(), any());
            var processor = new AuditoriaShardProcessor("stream", auditoria, cuarentena, new EstadoConsumidores.Stream(fatal), () -> false, detener, 100);
            processor.initialize(InitializationInput.builder().shardId("shard").build());
            processor.processRecords(input(checkpoint));
            var order = inOrder(cuarentena, detener);
            order.verify(cuarentena).guardar(eq("stream"), eq("shard"), eq("seq"), eq(0L), any(),
                    eq(colision ? "COLISION_ID" : "EVENTO_INVALIDO"));
            order.verify(detener).run();
            verify(checkpoint, never()).checkpoint();
            assertThat(fatal).isTrue();
        }
    }

    @Test void falloAlGuardarCuarentenaNoConfirma() throws Exception {
        var auditoria = mock(AuditoriaProcessor.class);
        var cuarentena = mock(CuarentenaService.class);
        var stopping = new AtomicBoolean();
        var fatal = new AtomicBoolean();
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new EventoInvalidoException("invalido")).when(auditoria).procesar(anyString(), anyString(), anyString(), anyLong(), any());
        doAnswer(call -> { stopping.set(true); throw new IllegalStateException("sin base"); })
                .when(cuarentena).guardar(anyString(), anyString(), anyString(), anyLong(), any(), anyString());
        var processor = new AuditoriaShardProcessor("stream", auditoria, cuarentena, new EstadoConsumidores.Stream(fatal), stopping::get, mock(Runnable.class), 100);
        processor.initialize(InitializationInput.builder().shardId("shard").build());
        processor.processRecords(input(checkpoint));
        verify(checkpoint, never()).checkpoint();
        assertThat(fatal).isFalse();
    }

    @Test void checkpointTemporalSeRecuperaSinRepetirLaPersistencia() throws Exception {
        var auditoria = mock(AuditoriaProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("temporal")).doNothing().when(checkpoint).checkpoint();
        var fatal = new AtomicBoolean();
        var detener = mock(Runnable.class);
        processor(auditoria, fatal, new AtomicBoolean(), detener).processRecords(input(checkpoint));
        verify(auditoria, times(2)).procesar(anyString(), anyString(), anyString(), anyLong(), any());
        verify(checkpoint, times(2)).checkpoint();
        verifyNoInteractions(detener);
        assertThat(fatal).isFalse();
    }

    @Test void agotamientoYPerdidaDeLeaseDetienenSinAvanzarMas() throws Exception {
        for (boolean lease : new boolean[] {false, true}) {
            var auditoria = mock(AuditoriaProcessor.class);
            var checkpoint = mock(RecordProcessorCheckpointer.class);
            if (lease) doThrow(new ShutdownException("lease perdido")).when(checkpoint).checkpoint();
            else doThrow(new ThrottlingException("temporal")).when(checkpoint).checkpoint();
            var fatal = new AtomicBoolean();
            var detener = mock(Runnable.class);
            var processor = processor(auditoria, fatal, new AtomicBoolean(), detener);
            processor.processRecords(input(checkpoint));
            processor.processRecords(input(checkpoint));
            verify(checkpoint, times(lease ? 1 : 3)).checkpoint();
            verify(auditoria, times(2)).procesar(anyString(), anyString(), anyString(), anyLong(), any());
            verify(detener).run();
            assertThat(fatal).isTrue();
        }
    }

    @Test void cierreDuranteReintentoNoVuelveAConfirmar() throws Exception {
        var auditoria = mock(AuditoriaProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var stopping = new AtomicBoolean();
        doAnswer(call -> { stopping.set(true); throw new ThrottlingException("temporal"); }).when(checkpoint).checkpoint();
        processor(auditoria, new AtomicBoolean(), stopping, mock(Runnable.class)).processRecords(input(checkpoint));
        verify(checkpoint, times(1)).checkpoint();
    }

    @Test void reintentaCheckpointAlFinalizarShard() throws Exception {
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("temporal")).doNothing().when(checkpoint).checkpoint();
        var fatal = new AtomicBoolean();
        var detener = mock(Runnable.class);
        var processor = processor(mock(AuditoriaProcessor.class), fatal, new AtomicBoolean(), detener);
        processor.shardEnded(ShardEndedInput.builder().checkpointer(checkpoint).build());
        verify(checkpoint, times(2)).checkpoint();
        verifyNoInteractions(detener);
        assertThat(fatal).isFalse();
    }

    private AuditoriaShardProcessor processor(AuditoriaProcessor auditoria, AtomicBoolean fatal,
                                               AtomicBoolean stopping, Runnable detener) {
        var processor = new AuditoriaShardProcessor("stream", auditoria, mock(CuarentenaService.class), new EstadoConsumidores.Stream(fatal), stopping::get, detener, 100);
        processor.initialize(InitializationInput.builder().shardId("shard").build());
        return processor;
    }

    private ProcessRecordsInput input(RecordProcessorCheckpointer checkpoint) {
        var uno = KinesisClientRecord.builder().sequenceNumber("seq").subSequenceNumber(0).data(ByteBuffer.wrap(new byte[] {1})).build();
        var dos = KinesisClientRecord.builder().sequenceNumber("seq").subSequenceNumber(1).data(ByteBuffer.wrap(new byte[] {2})).build();
        return ProcessRecordsInput.builder().records(List.of(uno, dos)).checkpointer(checkpoint).build();
    }
}
