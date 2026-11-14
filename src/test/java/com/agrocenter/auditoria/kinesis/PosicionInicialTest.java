package com.agrocenter.auditoria.kinesis;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import com.agrocenter.auditoria.eventos.AuditoriaProcessor;
import com.agrocenter.auditoria.eventos.CuarentenaService;
import org.springframework.test.util.ReflectionTestUtils;
import static org.mockito.Mockito.mock;

class PosicionInicialTest {
    @Test void aceptaSoloPosicionesExplicitas() {
        assertThat(AuditoriaKinesisConsumers.validarPosicionInicial("TRIM_HORIZON")).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
        assertThat(AuditoriaKinesisConsumers.validarPosicionInicial("LATEST")).isEqualTo(InitialPositionInStream.LATEST);
        assertThatThrownBy(() -> AuditoriaKinesisConsumers.validarPosicionInicial("AT_TIMESTAMP")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void rechazaValorInvalidoAntesDeIniciarClientesAws() {
        var consumer = new AuditoriaKinesisConsumers(mock(AuditoriaProcessor.class), mock(CuarentenaService.class));
        ReflectionTestUtils.setField(consumer, "businessApplication", "negocio");
        ReflectionTestUtils.setField(consumer, "stockApplication", "stock");
        ReflectionTestUtils.setField(consumer, "initialPosition", "invalido");
        assertThatThrownBy(consumer::start).isInstanceOf(IllegalArgumentException.class);
    }
}
