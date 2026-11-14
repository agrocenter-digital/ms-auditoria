package com.agrocenter.auditoria.consulta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.agrocenter.auditoria.kinesis.AuditoriaKinesisConsumers;
import com.agrocenter.auditoria.security.SeguridadConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(EstadoConsumidorController.class)
@Import(SeguridadConfig.class)
class EstadoConsumidorControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean JwtDecoder decoder;

    @Test void desactivadoSoloDisponibleParaAdmin() throws Exception {
        var path = "/api/auditoria/estado-consumidor";
        mvc.perform(get(path)).andExpect(status().isUnauthorized());
        mvc.perform(get(path).with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE")))).andExpect(status().isForbidden());
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get(path).with(admin)).andExpect(status().isOk()).andExpect(jsonPath("$.estado").value("DESACTIVADO"));
        mvc.perform(post(path).with(admin)).andExpect(status().isForbidden());
    }

    @Test void indicaSoloSaludLocal() {
        @SuppressWarnings("unchecked") ObjectProvider<AuditoriaKinesisConsumers> provider = mock(ObjectProvider.class);
        var controller = new EstadoConsumidorController(true, provider);
        assertThat(controller.estado().estado()).isEqualTo("DESCONOCIDO");
        var consumer = mock(AuditoriaKinesisConsumers.class);
        when(provider.getIfAvailable()).thenReturn(consumer);
        when(consumer.health()).thenReturn(Health.up().build(), Health.down().build());
        assertThat(controller.estado().estado()).isEqualTo("LECTURAS_RECIENTES");
        assertThat(controller.estado().estado()).isEqualTo("NO_SALUDABLE");
        assertThat(new EstadoConsumidorController(false, provider).estado().estado()).isEqualTo("DESACTIVADO");
    }
}
