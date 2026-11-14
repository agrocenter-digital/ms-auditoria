package com.agrocenter.auditoria.eventos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AuditoriaProcessorTest {
    private final AuditoriaRepository repository = mock(AuditoriaRepository.class);
    private final AuditoriaProcessor processor = new AuditoriaProcessor(new ObjectMapper(), repository);
    private static final String EVENTO = """
            {"eventId":"evento-1","eventType":"VentaConfirmada","schemaVersion":1,
             "occurredAt":"2026-11-12T22:00:00Z","producer":"ms-ventas","aggregateType":"Venta",
             "aggregateId":"1","correlationId":"trace-1","payload":{"ventaId":1}}
            """;

    @Test void preservaDatosYMetadatosSinReemitir() {
        when(repository.insertar(any())).thenReturn(true);
        assertThat(processor.procesar("negocio", "shard-1", "123", 2, EVENTO.getBytes(StandardCharsets.UTF_8))).isTrue();
        var captor = ArgumentCaptor.forClass(RegistroAuditoria.class);
        verify(repository).insertar(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo("evento-1");
        assertThat(captor.getValue().correlationId()).isEqualTo("trace-1");
        assertThat(captor.getValue().subSequenceNumber()).isEqualTo(2);
        assertThat(captor.getValue().payloadJson()).contains("\"ventaId\":1");
    }

    @Test void rechazaSobreInvalidoSinGuardar() {
        assertThatThrownBy(() -> processor.procesar("s", "sh", "1", 0, "{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> processor.procesar("s", "sh", "1", 0, "{".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> processor.procesar("s", "sh", "1", 0,
                EVENTO.replace("2026-11-12T22:00:00Z", "invalida").getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(EventoInvalidoException.class);
        verifyNoInteractions(repository);
    }

    @Test void fallaDeBaseSePropaga() {
        when(repository.insertar(any())).thenThrow(new IllegalStateException("base caida"));
        assertThatThrownBy(() -> processor.procesar("s", "sh", "1", 0, EVENTO.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
    }
}
